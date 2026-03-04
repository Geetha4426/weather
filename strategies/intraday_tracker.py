"""
Intraday Tracker Strategy — Resolution Day Real-Time Trading

THE EDGE ON RESOLUTION DAY:
  Polymarket resolves weather markets on the HIGHEST temperature of the day.
  By tracking actual hourly temperatures LIVE, we can predict the final max
  with near-certainty as the day progresses.

Example (London, March 5):
  8 AM: actual temp 9°C, forecast max 14°C → uncertain
  11 AM: temp already hit 13°C and rising → 13°C+ is GUARANTEED
  1 PM: temp peaked at 14°C, now cooling → 14°C is very likely the max
  → Buy "14°C" if market hasn't caught up

HOW:
  1. Fetch CURRENT hourly temperature from Open-Meteo real-time API
  2. Track the running maximum for the day
  3. Compare actual running max vs forecast remaining max
  4. Trade outcomes that are now near-certain but still mispriced

TIMING:
  - Morning (6-10 AM): Use forecast + early actuals
  - Midday (10 AM-2 PM): Running max is strong signal  
  - Afternoon (2-6 PM): Max is likely locked in, trade with high confidence
"""

import time
import math
import requests
from typing import Dict, List, Optional
from datetime import datetime, date, timedelta, timezone

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from weather_prediction.strategies.base_strategy import BaseStrategy, TradeSignal
from weather_prediction.config import Config


class IntradayTrackerStrategy(BaseStrategy):
    """
    On resolution day: tracks actual hourly temps and trades
    when actual data makes certain outcomes near-certain.
    """

    name = "intraday_tracker"
    description = "Resolution-day real-time temperature tracking"

    CITY_COORDS = {
        'nyc': (40.7128, -74.0060, 'America/New_York'),
        'london': (51.5074, -0.1278, 'Europe/London'),
        'chicago': (41.8781, -87.6298, 'America/Chicago'),
        'miami': (25.7617, -80.1918, 'America/New_York'),
        'seattle': (47.6062, -122.3321, 'America/Los_Angeles'),
        'atlanta': (33.7490, -84.3880, 'America/New_York'),
        'dallas': (32.7767, -96.7970, 'America/Chicago'),
        'munich': (48.1351, 11.5820, 'Europe/Berlin'),
        'lucknow': (26.8467, 80.9462, 'Asia/Kolkata'),
    }

    # Track city temperature units
    FAHRENHEIT_CITIES = {'nyc', 'chicago', 'miami', 'seattle', 'atlanta', 'dallas', 'los-angeles'}

    def __init__(self):
        self._hourly_cache: Dict[str, Dict] = {}
        self._cache_ttl = 180  # 3 minutes
        self.session = requests.Session()

    async def analyze(self, weather_market: Dict, context: Dict) -> List[TradeSignal]:
        """
        Only activates on RESOLUTION DAY (today's markets).
        Fetches actual hourly temperatures and trades based on running max.
        """
        city = weather_market.get('city', '')
        target_date_str = weather_market.get('date', '')
        today = date.today().isoformat()

        # ONLY run on resolution day
        if target_date_str != today:
            return []

        forecast = context.get('forecast')
        clob = context.get('clob')
        if not clob:
            return []

        unit = 'fahrenheit' if city in self.FAHRENHEIT_CITIES else 'celsius'

        # Fetch actual hourly temperatures for today
        actuals = self._get_today_actuals(city, unit)
        if not actuals:
            return []

        # Running maximum so far
        running_max = max(actuals)
        hours_recorded = len(actuals)

        # Get forecast for remaining hours
        remaining_max = self._estimate_remaining_max(
            city, forecast, actuals, unit)

        # The final max will be at least the running max
        # AND could be higher if the day's peak hasn't happened yet
        likely_final_max = max(running_max, remaining_max) if remaining_max else running_max

        # Confidence: how certain are we about the final max?
        now_hour = datetime.now().hour

        # Late afternoon → very high confidence (peak already passed for most cities)
        if now_hour >= 16:
            max_confidence = 0.95
        elif now_hour >= 14:
            max_confidence = 0.85
        elif now_hour >= 12:
            max_confidence = 0.70
        elif now_hour >= 10:
            max_confidence = 0.55
        else:
            max_confidence = 0.35

        # Boost if temperature is clearly dropping (peak passed)
        if len(actuals) >= 3 and actuals[-1] < actuals[-2] < actuals[-3]:
            max_confidence = min(0.98, max_confidence + 0.15)

        unit_sym = '°F' if unit == 'fahrenheit' else '°C'

        signals = []
        outcomes = weather_market.get('outcomes', [])

        for outcome in outcomes:
            temp_low = outcome.get('temp_low')
            if temp_low is None:
                continue

            market_price = outcome.get('price_yes', 0.5)
            yes_token = outcome.get('token_id_yes', '')
            no_token = outcome.get('token_id_no', '')

            # ═══ CASE 1: Running max already EXCEEDS this boundary ═══
            # e.g., "14°C or higher" and running max is already 15°C
            if outcome.get('is_upper_bound'):
                if running_max >= temp_low and market_price < 0.90:
                    edge = 1.0 - market_price
                    if edge > 0.05 and yes_token:
                        signals.append(self._make_signal(
                            outcome, city, target_date_str, yes_token,
                            market_price, 0.95, edge,
                            f"🎯 LOCKED: Max already {running_max}{unit_sym} ≥ {temp_low}{unit_sym}. "
                            f"Market at {market_price:.0%}, should be ~100%",
                            'intraday_locked', running_max, unit_sym
                        ))

            # ═══ CASE 2: Running max already BELOW this boundary ═══
            # e.g., "46°F or higher" and running max is 43°F with peak passed
            elif outcome.get('is_upper_bound'):
                if running_max < temp_low - 2 and max_confidence > 0.80 and market_price > 0.15:
                    if no_token:
                        signals.append(self._make_signal(
                            outcome, city, target_date_str, no_token,
                            1.0 - market_price, max_confidence,
                            market_price - 0.05,
                            f"📉 UNLIKELY: Max so far {running_max}{unit_sym}, "
                            f"needs {temp_low}{unit_sym}+. Peak likely passed.",
                            'intraday_unlikely', running_max, unit_sym,
                            direction='NO'
                        ))

            # ═══ CASE 3: Exact/Range outcome matches running max ═══
            elif not outcome.get('is_lower_bound'):
                if outcome.get('is_range'):
                    temp_high = outcome.get('temp_high', temp_low)
                    matches = temp_low <= round(running_max) <= temp_high
                else:
                    matches = round(running_max) == temp_low

                if matches and max_confidence > 0.60:
                    actual_prob = max_confidence * 0.9
                    edge = actual_prob - market_price

                    if edge > 0.10 and yes_token:
                        signals.append(self._make_signal(
                            outcome, city, target_date_str, yes_token,
                            market_price, max_confidence, edge,
                            f"🌡️ INTRADAY: Running max={running_max}{unit_sym} "
                            f"matches {outcome.get('label', '')}. "
                            f"Hour {now_hour}, conf={max_confidence:.0%}",
                            'intraday_match', running_max, unit_sym
                        ))

            # ═══ CASE 4: "12°C or below" and running max confirms ═══
            if outcome.get('is_lower_bound'):
                if running_max <= temp_low and max_confidence > 0.70:
                    actual_prob = max_confidence * 0.85
                    edge = actual_prob - market_price
                    if edge > 0.10 and yes_token:
                        signals.append(self._make_signal(
                            outcome, city, target_date_str, yes_token,
                            market_price, max_confidence, edge,
                            f"🌡️ INTRADAY: Max={running_max}{unit_sym} ≤ {temp_low}{unit_sym}. "
                            f"Peak likely passed at hour {now_hour}.",
                            'intraday_below', running_max, unit_sym
                        ))

        signals.sort(key=lambda s: s.confidence, reverse=True)
        return signals[:3]

    def _make_signal(self, outcome, city, date_str, token_id, price,
                     confidence, edge, rationale, sig_type, running_max,
                     unit_sym, direction='YES'):
        label = outcome.get('label', '')
        return TradeSignal(
            strategy=self.name, city=city, target_date=date_str,
            direction=direction, outcome_label=label,
            temp_c=outcome.get('temp_low', 0),
            token_id=token_id,
            market_id=outcome.get('market_id', ''),
            entry_price=price,
            confidence=confidence,
            rationale=rationale,
            metadata={
                'type': sig_type, 'edge': edge,
                'running_max': running_max, 'unit': unit_sym,
            },
        )

    def _get_today_actuals(self, city: str, unit: str) -> Optional[List[float]]:
        """Fetch actual hourly temperatures for today from Open-Meteo."""
        cache_key = f"actual_{city}"
        if cache_key in self._hourly_cache:
            cached = self._hourly_cache[cache_key]
            if time.time() - cached.get('ts', 0) < self._cache_ttl:
                return cached.get('temps', [])

        coords = self.CITY_COORDS.get(city)
        if not coords:
            return None

        lat, lon, tz = coords
        today_str = date.today().isoformat()
        temp_unit_param = '&temperature_unit=fahrenheit' if unit == 'fahrenheit' else ''

        try:
            url = (
                f"https://api.open-meteo.com/v1/forecast"
                f"?latitude={lat}&longitude={lon}"
                f"&hourly=temperature_2m"
                f"&timezone={tz}"
                f"&start_date={today_str}&end_date={today_str}"
                f"&past_hours=24"
                f"{temp_unit_param}"
            )
            resp = self.session.get(url, timeout=10)
            if resp.status_code != 200:
                return None

            data = resp.json()
            hourly = data.get('hourly', {})
            temps = hourly.get('temperature_2m', [])
            times = hourly.get('time', [])

            # Filter to only include hours that have already passed
            now = datetime.now()
            actual_temps = []
            for t_str, temp in zip(times, temps):
                try:
                    t_dt = datetime.fromisoformat(t_str)
                    if t_dt <= now and temp is not None:
                        actual_temps.append(temp)
                except Exception:
                    continue

            self._hourly_cache[cache_key] = {'temps': actual_temps, 'ts': time.time()}
            return actual_temps if actual_temps else None

        except Exception as e:
            return None

    def _estimate_remaining_max(self, city, forecast, actuals, unit):
        """Estimate the maximum temperature for remaining hours."""
        if not forecast:
            return None
        # Use forecast max as upper bound for remaining hours
        return forecast.get('mean_max', max(actuals) if actuals else None)
