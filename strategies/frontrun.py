"""
Frontrun Strategy — Forecast Shift Detection

THE ALPHA: Weather forecasts update every 1-6 hours. When the forecast
shifts, the market takes TIME to reprice. We detect the shift FIRST.

Example:
  6 hours ago: forecast = 15°C for London tomorrow
  NOW:         forecast shifts to 13°C
  Market still prices 15°C at $0.25 and 13°C at $0.08
  → BUY 13°C immediately, SELL 15°C if holding

HOW IT WORKS:
  1. Store the previous forecast snapshot (mean, std, prob distribution)
  2. On each scan, compare current forecast to previous
  3. If the mean shifted by more than 0.5°C (or 1°F):
     → The NEW likely outcome is underpriced
     → The OLD likely outcome is overpriced
     → Trade the shift before the market catches up

This is weather's equivalent of "frontrunning the oracle update"
in crypto markets — but LEGAL and purely information-based.
"""

import time
import json
from typing import Dict, List, Optional

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
from weather.strategies.base_strategy import BaseStrategy, TradeSignal
from weather.config import Config


class FrontrunStrategy(BaseStrategy):
    """
    Detects forecast shifts and trades before the market reprices.
    Stores previous forecast snapshots and compares on each scan.
    """

    name = "frontrun"
    description = "Frontrun forecast shifts — trade before market reprices"

    def __init__(self):
        # Store previous forecasts: {city_date: {mean, std, prob_dist, timestamp}}
        self._previous_forecasts: Dict[str, Dict] = {}
        # Minimum shift to trigger (°C for celsius, °F for fahrenheit)
        self.min_shift_c = 0.5
        self.min_shift_f = 1.0
        # Track how many times we've seen a shift (avoid noise)
        self._shift_confirmations: Dict[str, int] = {}

    async def analyze(self, weather_market: Dict, context: Dict) -> List[TradeSignal]:
        """
        Compare current forecast to previous snapshot.
        If shift detected → trade the new likely outcomes.
        """
        forecast = context.get('forecast')
        clob = context.get('clob')

        if not forecast or not clob:
            return []

        city = weather_market.get('city', '')
        target_date = weather_market.get('date', '')
        key = f"{city}_{target_date}"
        unit = forecast.get('unit', 'celsius')

        current_mean = forecast.get('mean_max', 0)
        current_std = forecast.get('std_max', 0)
        current_prob = forecast.get('probability_distribution', {})

        # First time seeing this market — just store and skip
        if key not in self._previous_forecasts:
            self._previous_forecasts[key] = {
                'mean': current_mean,
                'std': current_std,
                'prob': current_prob.copy(),
                'timestamp': time.time(),
            }
            return []

        prev = self._previous_forecasts[key]
        prev_mean = prev['mean']
        prev_prob = prev.get('prob', {})
        time_since = time.time() - prev['timestamp']

        # Don't compare if less than 5 minutes old (same data)
        if time_since < 300:
            return []

        # Calculate shift magnitude
        shift = current_mean - prev_mean
        min_shift = self.min_shift_f if unit == 'fahrenheit' else self.min_shift_c

        # No significant shift → update snapshot and skip
        if abs(shift) < min_shift:
            self._previous_forecasts[key] = {
                'mean': current_mean, 'std': current_std,
                'prob': current_prob.copy(), 'timestamp': time.time(),
            }
            self._shift_confirmations.pop(key, None)
            return []

        # Shift detected! Confirm it (avoid single-scan noise)
        self._shift_confirmations[key] = self._shift_confirmations.get(key, 0) + 1

        if self._shift_confirmations[key] < 2:
            # First confirmation — wait for second scan to confirm
            return []

        # CONFIRMED SHIFT — generate signals
        print(
            f"🔄 FORECAST SHIFT: {city.upper()} {target_date} "
            f"shifted {shift:+.1f}{forecast.get('unit_symbol', '°C')} "
            f"({prev_mean:.1f} → {current_mean:.1f})",
            flush=True
        )

        signals = []
        outcomes = weather_market.get('outcomes', [])

        for outcome in outcomes:
            temp_low = outcome.get('temp_low')
            if temp_low is None:
                continue

            # Calculate how much this outcome's probability CHANGED
            current_p = self._get_outcome_prob(outcome, current_prob)
            prev_p = self._get_outcome_prob(outcome, prev_prob)
            prob_change = current_p - prev_p

            market_price = outcome.get('price_yes', 0.5)

            # BUY outcomes whose probability INCREASED significantly
            # (market hasn't caught up yet)
            if prob_change > 0.08 and current_p > market_price:
                edge = current_p - market_price
                if edge < 0.10:
                    continue

                yes_token = outcome.get('token_id_yes', '')
                if not yes_token:
                    continue

                book = clob.get_orderbook(yes_token)
                entry = book['best_ask'] if book and not book.get('_synthetic') else market_price

                # Higher confidence for larger shifts
                shift_magnitude = abs(shift)
                confidence = min(0.90, 0.60 + shift_magnitude * 0.10 + prob_change * 0.5)

                unit_sym = outcome.get('temp_unit', 'c').upper()
                label = outcome.get('label', f'{temp_low}°{unit_sym}')

                rationale = (
                    f"🔄 FRONTRUN: {city.upper()} forecast shifted {shift:+.1f}"
                    f"{forecast.get('unit_symbol', '°C')}\n"
                    f"  {label}: prob {prev_p:.0%} → {current_p:.0%} (+{prob_change:.0%})\n"
                    f"  Market still at {market_price:.0%} → edge: {edge:+.1%}\n"
                    f"  Buy BEFORE market reprices!"
                )

                signals.append(TradeSignal(
                    strategy=self.name, city=city, target_date=target_date,
                    direction='YES',
                    outcome_label=label,
                    temp_c=temp_low,
                    token_id=yes_token,
                    market_id=outcome.get('market_id', ''),
                    entry_price=entry,
                    confidence=confidence,
                    rationale=rationale,
                    metadata={
                        'type': 'frontrun_shift',
                        'shift': shift,
                        'prob_change': prob_change,
                        'prev_mean': prev_mean,
                        'current_mean': current_mean,
                        'prev_prob': prev_p,
                        'current_prob': current_p,
                        'market_price': market_price,
                        'edge': edge,
                        'time_since_shift': time_since,
                    },
                ))

            # SELL / BUY NO on outcomes whose probability DECREASED
            elif prob_change < -0.08 and market_price > current_p + 0.15:
                no_token = outcome.get('token_id_no', '')
                if not no_token:
                    continue

                no_price = outcome.get('price_no', 1.0 - market_price)
                book = clob.get_orderbook(no_token)
                entry = book['best_ask'] if book and not book.get('_synthetic') else no_price

                if entry >= 0.95:
                    continue

                overprice = market_price - current_p
                confidence = min(0.85, 0.55 + abs(prob_change) * 0.5)

                label = outcome.get('label', f'{temp_low}°')

                signals.append(TradeSignal(
                    strategy=self.name, city=city, target_date=target_date,
                    direction='NO',
                    outcome_label=label,
                    temp_c=temp_low,
                    token_id=no_token,
                    market_id=outcome.get('market_id', ''),
                    entry_price=entry,
                    confidence=confidence,
                    rationale=(
                        f"🔄 FRONTRUN SHORT: {label} dropping "
                        f"({prev_p:.0%} → {current_p:.0%}), market={market_price:.0%}"
                    ),
                    metadata={
                        'type': 'frontrun_short',
                        'prob_change': prob_change,
                        'overprice': overprice,
                    },
                ))

        # Update snapshot
        self._previous_forecasts[key] = {
            'mean': current_mean, 'std': current_std,
            'prob': current_prob.copy(), 'timestamp': time.time(),
        }
        self._shift_confirmations.pop(key, None)

        signals.sort(key=lambda s: abs(s.metadata.get('prob_change', 0)), reverse=True)
        return signals[:3]

    def _get_outcome_prob(self, outcome: Dict, prob_dist: Dict) -> float:
        """Get forecast probability for an outcome."""
        temp_low = outcome.get('temp_low', 0)
        if outcome.get('is_upper_bound'):
            return sum(p for t, p in prob_dist.items() if t >= temp_low)
        elif outcome.get('is_lower_bound'):
            return sum(p for t, p in prob_dist.items() if t <= temp_low)
        elif outcome.get('is_range'):
            temp_high = outcome.get('temp_high', temp_low)
            return sum(p for t, p in prob_dist.items() if temp_low <= t <= temp_high)
        return prob_dist.get(temp_low, 0.0)
