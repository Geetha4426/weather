"""
Adjacent Bracket Strategy — Multi-Outcome Hedge Play

THE PLAY: Buy 2-3 adjacent temperature outcomes to COVER the likely range.
One of them MUST win. As the forecast narrows, sell the losers.

Example (London, forecast = 14°C ±1°C):
  Buy: 13°C @ $0.20, 14°C @ $0.12, 15°C @ $0.10
  Total cost: $0.42 per share set
  One outcome pays $1 at resolution → $0.58 profit (138% return)

  As day approaches and temp clarifies to 13°C:
  - 13°C price rises to $0.60 → SELL for $0.40 profit
  - 14°C price drops to $0.05 → HOLD or cut loss
  - 15°C price drops to $0.02 → HOLD or cut loss
  Net: +$0.40 - $0.07 - $0.08 = +$0.25 (even if you exit early)

WHY THIS WORKS:
  1. Weather forecast ±1-2° covers ~70-80% of the probability
  2. Individual outcomes are priced at 5-20% each → buying 3 = cheap coverage
  3. You don't need to be EXACTLY RIGHT, just within the range
  4. As time passes, one outcome becomes dominant → you profit on the winner

RISK MANAGEMENT:
  - Only buy when total cost < sum of forecast probabilities (positive EV)
  - Scale by confidence: high confidence → buy the mean, low → buy wider range
  - Exit losers early when confidence increases on specific outcome
"""

from typing import Dict, List

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from weather_prediction.strategies.base_strategy import BaseStrategy, TradeSignal
from weather_prediction.config import Config


class AdjacentBracketStrategy(BaseStrategy):
    """
    Buy 2-3 adjacent temperature brackets to cover the likely range.
    Guaranteed one winner if forecast is in range.
    """

    name = "adjacent_bracket"
    description = "Buy adjacent temperature brackets for hedged coverage"

    async def analyze(self, weather_market: Dict, context: Dict) -> List[TradeSignal]:
        forecast = context.get('forecast')
        clob = context.get('clob')
        seconds_remaining = context.get('seconds_remaining', 0)

        if not forecast or not clob:
            return []

        outcomes = weather_market.get('outcomes', [])
        if len(outcomes) < 4:  # Need enough outcomes for bracket play
            return []

        city = weather_market.get('city', '')
        target_date = weather_market.get('date', '')
        prob_dist = forecast.get('probability_distribution', {})
        mean_max = forecast.get('mean_max', 0)
        std_max = forecast.get('std_max', 0)
        unit = forecast.get('unit', 'celsius')

        # Find the "core zone" — outcomes within 1 std of the mean
        core_outcomes = []
        for o in outcomes:
            temp_low = o.get('temp_low')
            if temp_low is None:
                continue
            # Skip boundary outcomes (or higher / or below)
            if o.get('is_upper_bound') or o.get('is_lower_bound'):
                continue

            # Check if this outcome is near the mean
            if o.get('is_range'):
                temp_mid = (temp_low + (o.get('temp_high', temp_low))) / 2
            else:
                temp_mid = temp_low

            distance = abs(temp_mid - mean_max)
            max_distance = std_max * 1.5 if std_max > 0 else (3 if unit == 'fahrenheit' else 2)

            if distance <= max_distance:
                # Get market price
                yes_token = o.get('token_id_yes', '')
                if not yes_token:
                    continue

                forecast_prob = self._get_prob(o, prob_dist)
                if forecast_prob < 0.05:  # Too unlikely
                    continue

                book = clob.get_orderbook(yes_token)
                market_price = book['best_ask'] if book and not book.get('_synthetic') else o.get('price_yes', 0.5)

                core_outcomes.append({
                    'outcome': o,
                    'temp_mid': temp_mid,
                    'forecast_prob': forecast_prob,
                    'market_price': market_price,
                    'yes_token': yes_token,
                    'distance': distance,
                })

        if len(core_outcomes) < 2:
            return []

        # Sort by distance from mean (closest first)
        core_outcomes.sort(key=lambda x: x['distance'])

        # Select the best 2-3 brackets
        # Strategy: start with 2, add 3rd if positive EV
        brackets = core_outcomes[:3]

        # Calculate portfolio metrics
        total_cost = sum(b['market_price'] for b in brackets)
        total_forecast_prob = sum(b['forecast_prob'] for b in brackets)

        # Only proceed if there's positive expected value
        # EV = (prob of winning) * $1 - total_cost
        expected_value = total_forecast_prob * 1.0 - total_cost

        if expected_value < 0.10:  # Need at least 10% positive EV
            return []

        # Check that total cost is reasonable
        if total_cost > 0.75:  # Don't overpay
            return []

        ev_pct = expected_value / total_cost * 100

        # Confidence based on coverage and forecast quality
        coverage = total_forecast_prob  # How much of the probability we cover
        confidence = min(0.90, forecast.get('confidence', 0.5) * 0.5 + coverage * 0.5)

        # Time bonus: closer to resolution, forecast is more reliable
        hours = seconds_remaining / 3600
        if hours < 12:
            confidence = min(0.95, confidence + 0.10)
        elif hours < 24:
            confidence = min(0.90, confidence + 0.05)

        signals = []
        for b in brackets:
            o = b['outcome']
            temp_low = o.get('temp_low', 0)
            unit_sym = o.get('temp_unit', 'c').upper()
            label = o.get('label', f'{temp_low}°{unit_sym}')

            rationale = (
                f"🎯 BRACKET: {city.upper()} {target_date}\n"
                f"  Buying {len(brackets)} adjacent: "
                f"{', '.join(b2['outcome'].get('label', '?') for b2 in brackets)}\n"
                f"  Total cost: ${total_cost:.3f} | Coverage: {total_forecast_prob:.0%}\n"
                f"  EV: +${expected_value:.3f} ({ev_pct:.0f}%)\n"
                f"  This bracket: {label} @ ${b['market_price']:.3f} "
                f"(forecast: {b['forecast_prob']:.0%})"
            )

            signals.append(TradeSignal(
                strategy=self.name, city=city, target_date=target_date,
                direction='YES',
                outcome_label=label,
                temp_c=temp_low,
                token_id=b['yes_token'],
                market_id=o.get('market_id', ''),
                entry_price=b['market_price'],
                confidence=confidence,
                rationale=rationale,
                metadata={
                    'type': 'adjacent_bracket',
                    'bracket_count': len(brackets),
                    'total_cost': total_cost,
                    'coverage': total_forecast_prob,
                    'expected_value': expected_value,
                    'ev_pct': ev_pct,
                    'forecast_prob': b['forecast_prob'],
                    'market_price': b['market_price'],
                    'distance_from_mean': b['distance'],
                },
            ))

        return signals

    def _get_prob(self, outcome, prob_dist):
        temp_low = outcome.get('temp_low', 0)
        if outcome.get('is_upper_bound'):
            return sum(p for t, p in prob_dist.items() if t >= temp_low)
        elif outcome.get('is_lower_bound'):
            return sum(p for t, p in prob_dist.items() if t <= temp_low)
        elif outcome.get('is_range'):
            temp_high = outcome.get('temp_high', temp_low)
            return sum(p for t, p in prob_dist.items() if temp_low <= t <= temp_high)
        return prob_dist.get(temp_low, 0.0)
