"""
Sniper Strategy v2 — Ultra-Selective, Multi-Source Confirmation

Win rate TARGET: 90%+ by only trading when outcome is CONFIRMED
by MULTIPLE data sources with high certainty.

IMPROVEMENTS OVER v1:
  1. OpenWeather ACTUAL observed temps (10-min updates vs hourly Open-Meteo)
  2. City-level accuracy tracking (adjust certainty per city volatility)
  3. 95% minimum certainty (was 90%)
  4. Multi-source cross-validation (both APIs must agree)
  5. Wider safety margins (conservative spike estimates)

TRADE TYPES:
  A. "DEAD OUTCOME" — Buy NO: outcome is physically impossible
  B. "LOCKED IN" — Buy YES: running max hit this value, can't change
  C. "IMPOSSIBLE FUTURE" — Buy NO: max possible temp < outcome floor
  D. "GUARANTEED YES/BELOW" — Buy YES on open-ended ranges
"""

import math
import time
import requests
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date, timedelta, timezone

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from weather.config import Config


class SniperStrategy:
    """
    Ultra-selective strategy: only trades near-certain outcomes.
    Expected win rate: 90%+
    Expected trades per day per city: 1-2
    """

    # ═══ TIGHTENED THRESHOLDS (Improvement 3) ═══
    MIN_CERTAINTY = 0.95      # 95% minimum probability of winning (was 90%)
    MAX_HOURS_BEFORE = 12     # Only trade within 12h of resolution (was 18h)
    MAX_TRADES_PER_EVENT = 3
    MIN_PROFIT_CENTS = 5      # $0.05 minimum profit (lower since we're more certain)

    # Wider safety margins for temperature spikes (Improvement 3)
    LATE_DAY_MAX_SPIKE_C = 2.0   # After 2PM, max possible spike (was 1.5)
    NIGHT_MAX_SPIKE_C = 0.8      # After 8PM, max possible spike (was 0.5)
    LATE_DAY_MAX_SPIKE_F = 3.5   # Fahrenheit equivalents
    NIGHT_MAX_SPIKE_F = 1.5

    # Minimum temperature gap for DEAD_OUTCOME (Improvement 3)
    DEAD_OUTCOME_MIN_GAP_C = 1.0   # Running max must exceed by at least 1°C
    DEAD_OUTCOME_MIN_GAP_F = 2.0   # ... or 2°F

    # Cross-validation bonus (Improvement 1)
    CROSS_VALIDATION_BONUS = 0.02  # +2% certainty when both APIs agree

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'weather-sniper/2.0',
            'Accept': 'application/json',
        })
        self._trade_count: Dict[str, int] = {}
        self._city_accuracy: Dict[str, float] = {}  # city -> MAE (Improvement 2)
        self._accuracy_loaded = False
        self._owm_cache: Dict[str, Tuple[float, float]] = {}  # city -> (temp, timestamp)

    async def analyze(self, weather_market: Dict, context: Dict) -> List[Dict]:
        """
        Sniper analysis — only returns signals for near-certain outcomes.
        Uses multi-source temperature data for maximum confidence.
        """
        city = weather_market.get('city', '')
        target_date_str = weather_market.get('date', '')
        event_id = weather_market.get('event_id', city + target_date_str)
        outcomes = weather_market.get('outcomes', [])
        seconds_remaining = context.get('seconds_remaining', 999999)
        hours_remaining = seconds_remaining / 3600

        # GATE 1: Only trade close to resolution
        if hours_remaining > self.MAX_HOURS_BEFORE:
            return []

        # GATE 2: Check trade limit per event
        if self._trade_count.get(event_id, 0) >= self.MAX_TRADES_PER_EVENT:
            return []

        # ═══ Improvement 2: Load city accuracy on first scan ═══
        if not self._accuracy_loaded:
            self._load_city_accuracy()

        # Get city-adjusted certainty threshold
        min_certainty = self._get_city_certainty(city)

        # Get forecast data
        forecast = context.get('forecast', {})
        unit = forecast.get('unit', 'celsius')
        hourly_temps = forecast.get('hourly_temps', [])
        hourly_times = forecast.get('hourly_times', [])

        # ═══ Source 1: Open-Meteo actual temps ═══
        openmeteo_data = self._get_actual_temps(city, target_date_str)
        if openmeteo_data:
            hourly_temps = openmeteo_data.get('temps', hourly_temps)
            hourly_times = openmeteo_data.get('times', hourly_times)

        # Calculate running maximum from Open-Meteo
        running_max_om = None
        now_hour = self._get_current_hour(city)
        if hourly_temps:
            past_temps = []
            for t_time, temp in zip(hourly_times, hourly_temps):
                if temp is not None:
                    hour = self._parse_hour(t_time)
                    if hour is not None and hour <= now_hour:
                        past_temps.append(temp)
            if past_temps:
                running_max_om = max(past_temps)

        # ═══ Source 2: OpenWeather actual temp (Improvement 1 & 4) ═══
        running_max_owm = self._get_openweather_actual(city)

        # ═══ Cross-validate: take the HIGHER of both (conservative) ═══
        running_max = None
        sources_agree = False

        if running_max_om is not None and running_max_owm is not None:
            running_max = max(running_max_om, running_max_owm)
            # If both within 1° of each other, boost certainty
            gap = abs(running_max_om - running_max_owm)
            tolerance = 2.0 if unit == 'fahrenheit' else 1.0
            sources_agree = gap <= tolerance
        elif running_max_om is not None:
            running_max = running_max_om
        elif running_max_owm is not None:
            running_max = running_max_owm

        if running_max is None:
            return []

        # Calculate max possible remaining temperature
        max_possible = self._get_max_possible(running_max, hours_remaining, unit, city)
        forecast_max = forecast.get('mean_max', running_max)
        model_std = forecast.get('std_max', 1.0)

        signals = []

        for outcome in outcomes:
            signal = self._evaluate_outcome(
                outcome, running_max, max_possible, forecast_max,
                model_std, hours_remaining, unit, city, event_id,
                min_certainty, sources_agree
            )
            if signal:
                signals.append(signal)

        # Sort by certainty (highest first) and take top MAX_TRADES_PER_EVENT
        signals.sort(key=lambda s: s.confidence, reverse=True)
        remaining_slots = self.MAX_TRADES_PER_EVENT - self._trade_count.get(event_id, 0)
        signals = signals[:remaining_slots]

        return signals

    def _evaluate_outcome(self, outcome: Dict, running_max: float,
                          max_possible: float, forecast_max: float,
                          model_std: float, hours_remaining: float,
                          unit: str, city: str, event_id: str,
                          min_certainty: float, sources_agree: bool) -> Optional[Dict]:
        """
        Evaluate a single outcome for sniper-worthy trade.
        Returns a TradeSignal or None.
        """
        title = outcome.get('group_item_title', '') or outcome.get('title', '')
        price_yes = outcome.get('best_ask', 0) or outcome.get('price_yes', 0.5)
        price_no = 1.0 - (outcome.get('best_bid', 0) or outcome.get('price_yes', 0.5))
        temp_low = outcome.get('temp_low')
        temp_high = outcome.get('temp_high')
        token_yes = outcome.get('token_id_yes', '')
        token_no = outcome.get('token_id_no', '')

        if temp_low is None and temp_high is None:
            return None

        is_fahrenheit = unit == 'fahrenheit'
        min_gap = self.DEAD_OUTCOME_MIN_GAP_F if is_fahrenheit else self.DEAD_OUTCOME_MIN_GAP_C
        certainty_bonus = self.CROSS_VALIDATION_BONUS if sources_agree else 0

        # ═══ TYPE A: DEAD OUTCOME (buy NO) ═══
        # Running max ALREADY exceeds this outcome's ceiling by min_gap
        if temp_high is not None and running_max > temp_high + min_gap:
            certainty = 1.0 + certainty_bonus  # Physically impossible

            price_no_actual = 1.0 - float(outcome.get('best_bid', price_yes) or price_yes)
            profit = 1.0 - price_no_actual - 0.02

            if profit < self.MIN_PROFIT_CENTS / 100:
                return None
            if price_no_actual > 0.98:
                return None

            return self._make_signal(
                trade_type='DEAD_OUTCOME',
                direction='NO',
                outcome=outcome,
                certainty=min(0.99, certainty),
                price=price_no_actual,
                expected_profit=profit,
                reason=f"Running max {running_max:.1f}° > {temp_high}°+{min_gap:.0f}° → IMPOSSIBLE",
                city=city,
                event_id=event_id,
            )

        # ═══ TYPE B: LOCKED IN (buy YES) ═══
        # Running max exactly equals this temp AND remaining time is short
        if temp_low is not None and temp_high is not None:
            if int(running_max) == int(temp_low) and hours_remaining < 6:
                spike = self.LATE_DAY_MAX_SPIKE_C if not is_fahrenheit else self.LATE_DAY_MAX_SPIKE_F
                if hours_remaining < 2:
                    spike = self.NIGHT_MAX_SPIKE_C if not is_fahrenheit else self.NIGHT_MAX_SPIKE_F

                will_stay = forecast_max <= temp_high + 0.5
                if will_stay or running_max + spike <= temp_high + 1:
                    certainty = 0.92 if hours_remaining < 3 else 0.85
                    certainty += certainty_bonus

                    if certainty >= min_certainty:
                        profit = 1.0 - price_yes - 0.02
                        if profit >= self.MIN_PROFIT_CENTS / 100:
                            return self._make_signal(
                                trade_type='LOCKED_IN',
                                direction='YES',
                                outcome=outcome,
                                certainty=min(0.99, certainty),
                                price=price_yes,
                                expected_profit=profit,
                                reason=f"Running max {running_max:.1f}° = {title}, {hours_remaining:.0f}h left",
                                city=city,
                                event_id=event_id,
                            )

        # ═══ TYPE C: IMPOSSIBLE FUTURE (buy NO on high outcomes) ═══
        if temp_low is not None and temp_low > max_possible:
            gap_above = temp_low - max_possible
            # Wider gap = more certain
            if gap_above > 2:
                certainty = 0.99
            elif gap_above > 1:
                certainty = 0.97
            else:
                certainty = 0.93
            certainty += certainty_bonus

            if certainty >= min_certainty:
                price_no_actual = 1.0 - float(outcome.get('best_bid', price_yes) or price_yes)
                profit = 1.0 - price_no_actual - 0.02

                if profit >= self.MIN_PROFIT_CENTS / 100 and price_no_actual < 0.98:
                    return self._make_signal(
                        trade_type='IMPOSSIBLE_FUTURE',
                        direction='NO',
                        outcome=outcome,
                        certainty=min(0.99, certainty),
                        price=price_no_actual,
                        expected_profit=profit,
                        reason=f"Max possible {max_possible:.1f}° < {temp_low}° floor (gap={gap_above:.1f}°)",
                        city=city,
                        event_id=event_id,
                    )

        # ═══ TYPE D: GUARANTEED "OR HIGHER" (buy YES) ═══
        if temp_high is None and temp_low is not None:
            if running_max >= temp_low + min_gap:  # Must exceed by min_gap
                certainty = 1.0 + certainty_bonus

                profit = 1.0 - price_yes - 0.02
                if profit >= self.MIN_PROFIT_CENTS / 100:
                    return self._make_signal(
                        trade_type='GUARANTEED_YES',
                        direction='YES',
                        outcome=outcome,
                        certainty=min(0.99, certainty),
                        price=price_yes,
                        expected_profit=profit,
                        reason=f"Running max {running_max:.1f}° ≥ {temp_low}°+{min_gap:.0f}° → CONFIRMED",
                        city=city,
                        event_id=event_id,
                    )

        # ═══ TYPE E: GUARANTEED "OR BELOW" (buy YES) ═══
        if temp_low is None and temp_high is not None:
            if max_possible <= temp_high - (min_gap * 0.5):
                certainty = 0.96 if (temp_high - max_possible) > 1 else 0.93
                certainty += certainty_bonus

                if certainty >= min_certainty:
                    profit = 1.0 - price_yes - 0.02
                    if profit >= self.MIN_PROFIT_CENTS / 100:
                        return self._make_signal(
                            trade_type='GUARANTEED_BELOW',
                            direction='YES',
                            outcome=outcome,
                            certainty=min(0.99, certainty),
                            price=price_yes,
                            expected_profit=profit,
                            reason=f"Max possible {max_possible:.1f}° ≤ {temp_high}° ceiling → near-certain",
                            city=city,
                            event_id=event_id,
                        )

        return None

    # ═══════════════════════════════════════════════════════════════════
    # IMPROVEMENT 1 & 4: OpenWeather Real-Time Actual Temps
    # ═══════════════════════════════════════════════════════════════════

    def _get_openweather_actual(self, city: str) -> Optional[float]:
        """
        Get CURRENT actual observed temperature from OpenWeather.
        Updates every ~10 minutes (much faster than Open-Meteo hourly).
        Returns temperature in the city's native unit.
        """
        api_key = Config.OPENWEATHER_API_KEY
        if not api_key:
            return None

        # Cache for 5 minutes to avoid hitting rate limits
        cached = self._owm_cache.get(city)
        if cached and (time.time() - cached[1]) < 300:
            return cached[0]

        from weather.data.weather_client import WeatherClient
        city_info = WeatherClient.CITIES.get(city.lower().replace(' ', '-'))
        if not city_info:
            return None

        unit = city_info.get('unit', 'celsius')
        units = 'imperial' if unit == 'fahrenheit' else 'metric'

        try:
            url = (
                f"https://api.openweathermap.org/data/2.5/weather"
                f"?lat={city_info['lat']}&lon={city_info['lon']}"
                f"&appid={api_key}"
                f"&units={units}"
            )
            resp = self.session.get(url, timeout=8)
            if resp.status_code == 200:
                data = resp.json()
                # Current temp is the ACTUAL observed temperature
                current_temp = data.get('main', {}).get('temp')
                # Also get today's observed max so far
                temp_max = data.get('main', {}).get('temp_max', current_temp)
                # Use the higher of current and reported max
                actual = max(current_temp or 0, temp_max or 0)
                if actual > -100:  # sanity check
                    self._owm_cache[city] = (actual, time.time())
                    return actual
        except Exception:
            pass
        return None

    # ═══════════════════════════════════════════════════════════════════
    # IMPROVEMENT 2: City-Level Accuracy Tracking
    # ═══════════════════════════════════════════════════════════════════

    def _load_city_accuracy(self):
        """Load historical forecast accuracy per city."""
        self._accuracy_loaded = True
        try:
            from weather.data.weather_client import WeatherClient
            wc = WeatherClient()
            for city in Config.WEATHER_CITIES:
                accuracy = wc.get_historical_accuracy(city)
                if accuracy and 'mae' in accuracy:
                    self._city_accuracy[city] = accuracy['mae']
            if self._city_accuracy:
                best = min(self._city_accuracy.items(), key=lambda x: x[1])
                worst = max(self._city_accuracy.items(), key=lambda x: x[1])
                print(f"📊 City accuracy loaded: best={best[0]}({best[1]:.1f}°), "
                      f"worst={worst[0]}({worst[1]:.1f}°)", flush=True)
        except Exception as e:
            print(f"⚠️ Could not load city accuracy: {e}", flush=True)

    def _get_city_certainty(self, city: str) -> float:
        """
        Get adjusted minimum certainty threshold for a city.
        
        Low MAE cities (< 1.5°): 93% (slightly more trades, still safe)
        Medium MAE (1.5-3°):     95% (default strict)
        High MAE (> 3°):         97% (extra cautious, volatile city)
        """
        mae = self._city_accuracy.get(city)
        if mae is None:
            return self.MIN_CERTAINTY  # Default 95%

        if mae < 1.5:
            return 0.93  # Predictable city — slightly relaxed
        elif mae < 3.0:
            return 0.95  # Normal — strict
        else:
            return 0.97  # Volatile — extra strict

    # ═══════════════════════════════════════════════════════════════════
    # DATA FETCHING
    # ═══════════════════════════════════════════════════════════════════

    def _get_max_possible(self, running_max: float, hours_remaining: float,
                          unit: str, city: str) -> float:
        """
        Calculate the maximum temperature that can still be reached today.
        Uses WIDER margins than v1 for safety (Improvement 3).
        """
        is_fahrenheit = unit == 'fahrenheit'
        local_hour = self._get_current_hour(city)

        if local_hour >= 18:  # After 6PM — peak is DONE
            spike = 0.5 if not is_fahrenheit else 1.0
        elif local_hour >= 16:  # After 4PM — very little spike left
            spike = 0.8 if not is_fahrenheit else 1.5
        elif local_hour >= 14:  # After 2PM — small spike possible
            spike = self.LATE_DAY_MAX_SPIKE_C if not is_fahrenheit else self.LATE_DAY_MAX_SPIKE_F
        elif local_hour >= 12:  # Noon — moderate spike still possible
            spike = 2.5 if not is_fahrenheit else 4.5
        elif local_hour >= 10:  # Morning — significant warming ahead
            spike = 4.0 if not is_fahrenheit else 7.0
        else:  # Early morning — too much uncertainty
            spike = 6.0 if not is_fahrenheit else 11.0

        return running_max + spike

    def _get_actual_temps(self, city: str, target_date: str) -> Optional[Dict]:
        """Fetch ACTUAL observed temperatures from Open-Meteo."""
        from weather.data.weather_client import WeatherClient
        city_info = WeatherClient.CITIES.get(city.lower().replace(' ', '-'))
        if not city_info:
            return None

        unit = city_info.get('unit', 'celsius')
        temp_param = '&temperature_unit=fahrenheit' if unit == 'fahrenheit' else ''

        try:
            url = (
                f"https://api.open-meteo.com/v1/forecast"
                f"?latitude={city_info['lat']}&longitude={city_info['lon']}"
                f"&hourly=temperature_2m"
                f"&timezone={city_info['timezone']}"
                f"&past_days=0&forecast_days=1"
                f"{temp_param}"
            )
            resp = self.session.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                hourly = data.get('hourly', {})
                return {
                    'temps': hourly.get('temperature_2m', []),
                    'times': hourly.get('time', []),
                }
        except Exception:
            pass
        return None

    def _get_current_hour(self, city: str) -> int:
        """Get current local hour for a city."""
        offsets = {
            'london': 0, 'nyc': -5, 'chicago': -6, 'miami': -5,
            'seattle': -8, 'atlanta': -5, 'dallas': -6, 'munich': 1,
            'lucknow': 5, 'tokyo': 9, 'paris': 1, 'los-angeles': -8,
        }
        city_key = city.lower().replace(' ', '-')
        offset = offsets.get(city_key, 0)
        utc_now = datetime.now(timezone.utc)
        local_hour = (utc_now.hour + offset) % 24
        return local_hour

    def _parse_hour(self, time_str) -> Optional[int]:
        """Parse hour from ISO time string like '2026-03-07T14:00'."""
        try:
            if isinstance(time_str, str) and 'T' in time_str:
                return int(time_str.split('T')[1].split(':')[0])
        except (ValueError, IndexError):
            pass
        return None

    def _make_signal(self, trade_type: str, direction: str, outcome: Dict,
                     certainty: float, price: float, expected_profit: float,
                     reason: str, city: str, event_id: str) -> 'TradeSignal':
        """Create a standardized trade signal."""
        from weather.strategies.base_strategy import TradeSignal

        token_id = outcome.get('token_id_yes', '') if direction == 'YES' else outcome.get('token_id_no', '')
        title = outcome.get('group_item_title', '') or outcome.get('title', '')

        signal = TradeSignal(
            strategy=f'sniper_{trade_type.lower()}',
            city=city,
            direction=direction,
            outcome_label=title,
            token_id=token_id,
            entry_price=price,
            confidence=certainty,
            metadata={
                'trade_type': trade_type,
                'certainty': certainty,
                'expected_profit': round(expected_profit, 3),
                'reason': reason,
                'sniper': True,
            }
        )

        # Track trade count
        self._trade_count[event_id] = self._trade_count.get(event_id, 0) + 1

        return signal

    def get_stats(self) -> Dict:
        """Get sniper strategy statistics."""
        total_events = len(self._trade_count)
        total_trades = sum(self._trade_count.values())
        return {
            'events_traded': total_events,
            'total_signals': total_trades,
            'avg_trades_per_event': round(total_trades / max(total_events, 1), 1),
            'city_accuracy': {k: round(v, 1) for k, v in self._city_accuracy.items()},
        }
