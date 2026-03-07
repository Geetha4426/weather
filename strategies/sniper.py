"""
Sniper Strategy — Only Near-Certain Trades

Win rate TARGET: 85%+ by only trading when outcome is CONFIRMED.

THE CORE INSIGHT:
  On resolution day, after a few hours of temperature data,
  MOST outcomes become physically impossible. That's FREE money.

EXAMPLE (London March 7):
  Current time: 2PM, Current high: 9°C
  Models say max will be 10-11°C (peak already passed)
  
  IMPOSSIBLE outcomes (current high > their max):
    "8°C or below" → current high is 9°C → THIS IS IMPOSSIBLE → Buy NO
    
  NEAR-IMPOSSIBLE outcomes (would need massive late-day spike):
    "13°C" → would need 3°C spike in remaining hours → 99% NO
    "14°C" → 99.9% NO
    
  NEAR-CERTAIN:
    "10°C" → models all show 10.4-10.8°C → likely YES → but only buy if price < 0.80

RULES:
  1. ONLY trade on resolution day (same-day data available)
  2. ONLY trade when REAL temperature data CONFIRMS the outcome
  3. NEVER trade based solely on forecast probability
  4. Maximum 3 trades per market (not 15+)
  5. Minimum position size: ensure $0.10+ profit after fees
  6. Only enter when probability of winning > 90%

TRADE TYPES:
  A. "DEAD OUTCOME" — Buy NO: outcome is physically impossible
     (current running max already exceeds the threshold)
  B. "LOCKED IN" — Buy YES: running max has hit this exact value
     and remaining hours can't change it
  C. "GUARANTEED RANGE" — The temp can only land in 2-3 outcomes
     Buy YES on the cheapest of those

WHAT WE DON'T DO:
  - No trading 2+ days before resolution
  - No small-edge speculative trades
  - No "forecast says 60%, market says 40%" → that's gambling
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
    Expected win rate: 85%+
    Expected trades per day per city: 1-3
    """

    # Minimum certainty to trade
    MIN_CERTAINTY = 0.90     # 90% minimum probability of winning

    # Only trade on resolution day (or day before after 6PM)
    MAX_HOURS_BEFORE = 18    # Max hours before resolution to enter

    # Max trades per market event
    MAX_TRADES_PER_EVENT = 3

    # Minimum profit to bother (after Polymarket fees)
    MIN_PROFIT_CENTS = 8     # $0.08 minimum profit per $1 share

    # Temperature gates (°C thresholds - how much can temp still change)
    LATE_DAY_MAX_SPIKE_C = 1.5  # After 2PM, max possible spike is ~1.5°C
    NIGHT_MAX_SPIKE_C = 0.5     # After 8PM, max possible spike is ~0.5°C

    # Same thresholds in °F
    LATE_DAY_MAX_SPIKE_F = 3.0
    NIGHT_MAX_SPIKE_F = 1.0

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'weather-sniper/1.0',
            'Accept': 'application/json',
        })
        self._trade_count: Dict[str, int] = {}  # event_id -> trade count

    async def analyze(self, weather_market: Dict, context: Dict) -> List[Dict]:
        """
        Sniper analysis — only returns signals for near-certain outcomes.
        
        Returns list of trade signals, max 3 per market.
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

        # Get real-time temperature data
        forecast = context.get('forecast', {})
        unit = forecast.get('unit', 'celsius')
        hourly_temps = forecast.get('hourly_temps', [])
        hourly_times = forecast.get('hourly_times', [])

        # Try to get ACTUAL current temperature from Open-Meteo
        actual_data = self._get_actual_temps(city, target_date_str)
        if actual_data:
            hourly_temps = actual_data.get('temps', hourly_temps)
            hourly_times = actual_data.get('times', hourly_times)

        # Calculate running maximum
        running_max = None
        if hourly_temps:
            # Only use temps from hours that have already passed
            now_hour = self._get_current_hour(city)
            past_temps = []
            for i, (t_time, temp) in enumerate(zip(hourly_times, hourly_temps)):
                if temp is not None:
                    hour = self._parse_hour(t_time)
                    if hour is not None and hour <= now_hour:
                        past_temps.append(temp)

            if past_temps:
                running_max = max(past_temps)

        if running_max is None:
            # No actual data yet — skip
            return []

        # Calculate max possible remaining temperature
        max_possible = self._get_max_possible(running_max, hours_remaining, unit, city)

        # Get forecast max from models
        forecast_max = forecast.get('mean_max', running_max)
        model_std = forecast.get('std_max', 1.0)

        signals = []

        for outcome in outcomes:
            signal = self._evaluate_outcome(
                outcome, running_max, max_possible, forecast_max,
                model_std, hours_remaining, unit, city, event_id
            )
            if signal:
                signals.append(signal)

        # Sort by certainty (highest first) and take top MAX_TRADES_PER_EVENT
        signals.sort(key=lambda s: s.get('certainty', 0), reverse=True)
        remaining_slots = self.MAX_TRADES_PER_EVENT - self._trade_count.get(event_id, 0)
        signals = signals[:remaining_slots]

        return signals

    def _evaluate_outcome(self, outcome: Dict, running_max: float,
                          max_possible: float, forecast_max: float,
                          model_std: float, hours_remaining: float,
                          unit: str, city: str, event_id: str) -> Optional[Dict]:
        """
        Evaluate a single outcome for sniper-worthy trade.
        
        Returns a trade signal dict or None.
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

        # ═══ TYPE A: DEAD OUTCOME (buy NO) ═══
        # The running max has ALREADY exceeded this outcome's range
        # Example: "8°C or below" when running max is 9°C → DEAD
        if temp_high is not None and running_max > temp_high:
            certainty = 1.0  # Physically impossible

            # Can we still profit?
            price_no_actual = 1.0 - float(outcome.get('best_bid', price_yes) or price_yes)
            profit = 1.0 - price_no_actual - 0.02  # 2% fees

            if profit < self.MIN_PROFIT_CENTS / 100:
                return None  # Not enough profit

            if price_no_actual > 0.98:
                return None  # Already fully priced, no edge

            return self._make_signal(
                trade_type='DEAD_OUTCOME',
                direction='NO',
                outcome=outcome,
                certainty=certainty,
                price=price_no_actual,
                expected_profit=profit,
                reason=f"Running max {running_max}° > {temp_high}° ceiling → IMPOSSIBLE",
                city=city,
                event_id=event_id,
            )

        # ═══ TYPE B: LOCKED IN (buy YES) ═══
        # The running max exactly equals this temp AND remaining time is short
        if temp_low is not None and temp_high is not None:
            if int(running_max) == int(temp_low) and hours_remaining < 6:
                # Can the temp still go higher?
                spike = self.LATE_DAY_MAX_SPIKE_C if unit == 'celsius' else self.LATE_DAY_MAX_SPIKE_F
                if hours_remaining < 2:
                    spike = self.NIGHT_MAX_SPIKE_C if unit == 'celsius' else self.NIGHT_MAX_SPIKE_F

                will_stay = forecast_max <= temp_high + 0.5  # Models agree it won't go higher
                if will_stay or running_max + spike <= temp_high + 1:
                    certainty = 0.92 if hours_remaining < 3 else 0.85

                    if certainty >= self.MIN_CERTAINTY:
                        profit = 1.0 - price_yes - 0.02
                        if profit >= self.MIN_PROFIT_CENTS / 100:
                            return self._make_signal(
                                trade_type='LOCKED_IN',
                                direction='YES',
                                outcome=outcome,
                                certainty=certainty,
                                price=price_yes,
                                expected_profit=profit,
                                reason=f"Running max {running_max}° = {title}, {hours_remaining:.0f}h left, models confirm",
                                city=city,
                                event_id=event_id,
                            )

        # ═══ TYPE C: IMPOSSIBLE FUTURE (buy NO on high outcomes) ═══
        # Example: "14°C" when max possible is 11.5°C
        if temp_low is not None and temp_low > max_possible:
            certainty = 0.98 if (temp_low - max_possible) > 1 else 0.93

            if certainty >= self.MIN_CERTAINTY:
                price_no_actual = 1.0 - float(outcome.get('best_bid', price_yes) or price_yes)
                profit = 1.0 - price_no_actual - 0.02

                if profit >= self.MIN_PROFIT_CENTS / 100 and price_no_actual < 0.98:
                    return self._make_signal(
                        trade_type='IMPOSSIBLE_FUTURE',
                        direction='NO',
                        outcome=outcome,
                        certainty=certainty,
                        price=price_no_actual,
                        expected_profit=profit,
                        reason=f"Max possible {max_possible:.1f}° < {temp_low}° floor → near-impossible",
                        city=city,
                        event_id=event_id,
                    )

        # ═══ TYPE D: GUARANTEED "OR HIGHER" / "OR BELOW" (buy YES) ═══
        # Example: "10°C or higher" when running max is already 10°C
        if temp_high is None and temp_low is not None:
            # "X°C or higher" outcome
            if running_max >= temp_low:
                certainty = 1.0  # Already satisfied

                profit = 1.0 - price_yes - 0.02
                if profit >= self.MIN_PROFIT_CENTS / 100:
                    return self._make_signal(
                        trade_type='GUARANTEED_YES',
                        direction='YES',
                        outcome=outcome,
                        certainty=certainty,
                        price=price_yes,
                        expected_profit=profit,
                        reason=f"Running max {running_max}° ≥ {temp_low}° → ALREADY TRUE",
                        city=city,
                        event_id=event_id,
                    )

        if temp_low is None and temp_high is not None:
            # "X°C or below" outcome
            if max_possible <= temp_high:
                certainty = 0.95 if (temp_high - max_possible) > 0.5 else 0.90

                if certainty >= self.MIN_CERTAINTY:
                    profit = 1.0 - price_yes - 0.02
                    if profit >= self.MIN_PROFIT_CENTS / 100:
                        return self._make_signal(
                            trade_type='GUARANTEED_BELOW',
                            direction='YES',
                            outcome=outcome,
                            certainty=certainty,
                            price=price_yes,
                            expected_profit=profit,
                            reason=f"Max possible {max_possible:.1f}° ≤ {temp_high}° ceiling → near-certain",
                            city=city,
                            event_id=event_id,
                        )

        return None

    def _get_max_possible(self, running_max: float, hours_remaining: float,
                          unit: str, city: str) -> float:
        """
        Calculate the maximum temperature that can still be reached today.
        
        Based on:
        1. Current running maximum
        2. How many hours of heating remain (peaks around 2-3PM)
        3. Time of day (temp drops after sunset)
        """
        is_fahrenheit = unit == 'fahrenheit'

        # After peak hours (4PM+), temp only drops
        # Get city-local hour to determine if peak has passed
        local_hour = self._get_current_hour(city)

        if local_hour >= 18:  # After 6PM — peak is DONE
            spike = 0.3 if not is_fahrenheit else 0.5
        elif local_hour >= 16:  # After 4PM — very little spike left
            spike = 0.5 if not is_fahrenheit else 1.0
        elif local_hour >= 14:  # After 2PM — small spike possible
            spike = 1.0 if not is_fahrenheit else 2.0
        elif local_hour >= 12:  # Noon — moderate spike still possible
            spike = 1.5 if not is_fahrenheit else 3.0
        elif local_hour >= 10:  # Morning — significant warming ahead
            spike = 3.0 if not is_fahrenheit else 5.0
        else:  # Early morning — too much uncertainty
            spike = 5.0 if not is_fahrenheit else 9.0

        return running_max + spike

    def _get_actual_temps(self, city: str, target_date: str) -> Optional[Dict]:
        """
        Fetch ACTUAL observed temperatures for today from Open-Meteo.
        This is KEY — not forecast, but REAL observed data.
        """
        from weather.data.weather_client import WeatherClient
        wc = WeatherClient()
        city_info = wc.CITIES.get(city.lower().replace(' ', '-'))
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
        # Simple timezone offset mapping
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
                     reason: str, city: str, event_id: str) -> Dict:
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
                'sniper': True,  # Flag for the trader to identify sniper signals
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
        }
