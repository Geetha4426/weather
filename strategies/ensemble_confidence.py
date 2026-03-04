"""
Ensemble Confidence Strategy — Multi-Model Agreement Trading

Trades ONLY when weather models strongly agree.
When all 6+ models predict same temp range → very high confidence → trade.

Handles Fahrenheit (US) and Celsius (non-US) ranges/boundaries.
"""

from typing import Dict, List

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from weather_prediction.strategies.base_strategy import BaseStrategy, TradeSignal
from weather_prediction.config import Config


class EnsembleConfidenceStrategy(BaseStrategy):
    """Trades when weather models strongly agree on temperature."""

    name = "ensemble_confidence"
    description = "Trades high-confidence outcomes when models strongly agree"

    async def analyze(self, weather_market: Dict, context: Dict) -> List[TradeSignal]:
        forecast = context.get('forecast')
        clob = context.get('clob')

        if not forecast or not clob:
            return []

        std_max = forecast.get('std_max', 99)
        model_confidence = forecast.get('confidence', 0)

        if model_confidence < 0.70:
            return []

        # For Fahrenheit: allow wider std (1°F ≈ 0.56°C)
        unit = forecast.get('unit', 'celsius')
        max_std = 2.5 if unit == 'fahrenheit' else 1.5
        if std_max > max_std:
            return []

        city = weather_market.get('city', '')
        target_date = weather_market.get('date', '')
        outcomes = weather_market.get('outcomes', [])
        prob_dist = forecast.get('probability_distribution', {})
        mean_max = forecast.get('mean_max', 0)

        signals = []

        for outcome in outcomes:
            temp_low = outcome.get('temp_low')
            if temp_low is None:
                continue

            forecast_prob = self._get_prob(outcome, prob_dist)

            # Only consider outcomes near the mean
            if not outcome.get('is_upper_bound') and not outcome.get('is_lower_bound'):
                if not outcome.get('is_range'):
                    if abs(temp_low - mean_max) > std_max * 2:
                        continue

            if forecast_prob < 0.10:
                continue

            market_price = outcome.get('price_yes', 0.5)
            edge = forecast_prob - market_price

            if edge < 0.10:
                continue

            yt = outcome.get('token_id_yes', '')
            if not yt:
                continue

            book = clob.get_orderbook(yt)
            entry = book['best_ask'] if book and not book.get('_synthetic') else market_price
            if entry >= 0.90:
                continue

            confidence = min(0.95, model_confidence * 0.6 + edge * 2.0)
            unit_sym = forecast.get('unit_symbol', '°C')

            signals.append(TradeSignal(
                strategy=self.name, city=city, target_date=target_date,
                direction='YES',
                outcome_label=outcome.get('label', ''),
                temp_c=temp_low,
                token_id=yt,
                market_id=outcome.get('market_id', ''),
                entry_price=entry,
                confidence=confidence,
                rationale=(
                    f"🎯 ENSEMBLE: {city.upper()} {forecast.get('num_models', '?')} models "
                    f"agree: {mean_max}{unit_sym} ±{std_max}{unit_sym}\n"
                    f"  {outcome.get('label', '')} "
                    f"forecast={forecast_prob:.0%} market={market_price:.0%} edge={edge:+.1%}"
                ),
                metadata={
                    'type': 'ensemble_confidence',
                    'edge': edge,
                    'forecast_prob': forecast_prob,
                    'model_confidence': model_confidence,
                    'std_max': std_max,
                },
            ))

        signals.sort(key=lambda s: s.metadata.get('edge', 0), reverse=True)
        return signals[:2]

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
