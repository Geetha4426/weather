"""
Forecast Edge Strategy — Core Oracle Strategy for Weather Markets

THE KEY EDGE: Multi-API ensemble forecast provides probability estimates.
When forecast probability diverges from Polymarket price → tradeable edge.

Real data examples:
  - Forecast says NYC high will be 42-43°F with 25% probability
  - Polymarket prices "42-43°F" YES at $0.06 (6% implied)
  - Edge = 25% - 6% = +19% → BUY YES (exceeds 15% threshold)

Threshold settings (from real profitable bots):
  - Entry: buy when edge > 15%
  - Exit: sell when profit > 45%
  - Max position: $2.00 per trade

Handles:
  - Exact temps: "14°C" 
  - Ranges: "42-43°F"
  - Boundaries: "46°F or higher", "12°C or below"
  - Fahrenheit (US) and Celsius (non-US)
"""

from typing import Dict, List
from datetime import date

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
from weather.strategies.base_strategy import BaseStrategy, TradeSignal
from weather.config import Config


class ForecastEdgeStrategy(BaseStrategy):
    """
    Compares forecast probabilities vs Polymarket prices.
    Buys YES when forecast probability > market price + min_edge.
    """

    name = "forecast_edge"
    description = "Buys underpriced outcomes where forecast probability > market price"

    def __init__(self):
        self.min_edge = Config.WEATHER_MIN_EDGE          # 15%
        self.min_confidence = Config.WEATHER_MIN_CONFIDENCE  # 40%
        self.max_signals = Config.WEATHER_MAX_POSITIONS_PER_EVENT

    async def analyze(self, weather_market: Dict, context: Dict) -> List[TradeSignal]:
        """Analyze each outcome against forecast probabilities."""
        forecast = context.get('forecast')
        clob = context.get('clob')
        seconds_remaining = context.get('seconds_remaining', 0)

        if not forecast or not clob:
            return []

        prob_dist = forecast.get('probability_distribution', {})
        model_confidence = forecast.get('confidence', 0.5)

        if not prob_dist:
            return []

        city = weather_market.get('city', '')
        target_date = weather_market.get('date', '')
        outcomes = weather_market.get('outcomes', [])

        signals = []

        for outcome in outcomes:
            temp_low = outcome.get('temp_low')
            if temp_low is None:
                continue

            # Calculate forecast probability for this outcome
            forecast_prob = self._get_outcome_probability(outcome, prob_dist)

            # Get market price
            yes_token = outcome.get('token_id_yes', '')
            if not yes_token:
                continue

            book = clob.get_orderbook(yes_token) if yes_token else None
            if book:
                market_price = book['mid_price']
            else:
                market_price = outcome.get('price_yes', 0.5)

            if market_price < 0.04 or market_price >= 0.99:
                continue

            # Calculate edge
            edge = forecast_prob - market_price

            if edge < self.min_edge:
                continue

            # Confidence scoring
            time_factor = self._time_confidence(seconds_remaining)
            edge_factor = min(1.0, edge / 0.30)
            confidence = (
                model_confidence * 0.40 +
                edge_factor * 0.35 +
                time_factor * 0.25
            )

            if confidence < self.min_confidence:
                continue

            entry_price = book['best_ask'] if book and not book.get('_synthetic') else market_price
            if entry_price >= 0.90:
                continue

            unit_sym = outcome.get('temp_unit', 'c').upper()
            label = outcome.get('label', f'{temp_low}°{unit_sym}')

            rationale = (
                f"🌡️ FORECAST EDGE: {city.upper()} {target_date}\n"
                f"  {label}\n"
                f"  Forecast: {forecast_prob:.1%} vs Market: {market_price:.1%}\n"
                f"  Edge: {edge:+.1%} | Conf: {confidence:.0%}\n"
                f"  Models: {forecast.get('num_models', '?')} | "
                f"Mean: {forecast.get('mean_max', '?')}{forecast.get('unit_symbol', '°C')} "
                f"±{forecast.get('std_max', '?')}"
            )

            signals.append(TradeSignal(
                strategy=self.name,
                city=city,
                target_date=target_date,
                direction='YES',
                outcome_label=label,
                temp_c=temp_low,
                token_id=yes_token,
                market_id=outcome.get('market_id', ''),
                entry_price=entry_price,
                confidence=confidence,
                rationale=rationale,
                metadata={
                    'forecast_prob': forecast_prob,
                    'market_price': market_price,
                    'edge': edge,
                    'model_confidence': model_confidence,
                    'time_factor': time_factor,
                    'temp_low': temp_low,
                    'temp_high': outcome.get('temp_high'),
                    'temp_unit': outcome.get('temp_unit', 'c'),
                    'is_range': outcome.get('is_range', False),
                    'type': 'forecast_edge',
                },
            ))

        signals.sort(key=lambda s: s.metadata.get('edge', 0), reverse=True)
        return signals[:self.max_signals]

    def _get_outcome_probability(self, outcome: Dict, prob_dist: Dict[int, float]) -> float:
        """
        Calculate forecast probability for this specific outcome.

        Handles:
          - Exact: "14°C" → P(max_temp = 14)
          - Range: "42-43°F" → P(42) + P(43)
          - Lower bound: "12°C or below" → Σ P(t) for t ≤ 12
          - Upper bound: "46°F or higher" → Σ P(t) for t ≥ 46
        """
        temp_low = outcome.get('temp_low', 0)

        if outcome.get('is_upper_bound'):
            return sum(p for t, p in prob_dist.items() if t >= temp_low)
        elif outcome.get('is_lower_bound'):
            return sum(p for t, p in prob_dist.items() if t <= temp_low)
        elif outcome.get('is_range'):
            temp_high = outcome.get('temp_high', temp_low)
            return sum(p for t, p in prob_dist.items() if temp_low <= t <= temp_high)
        else:
            return prob_dist.get(temp_low, 0.0)

    def _time_confidence(self, seconds_remaining: int) -> float:
        """Time-based confidence: closer to resolution → more accurate."""
        hours = seconds_remaining / 3600
        if hours < 6:
            return 0.95
        elif hours < 12:
            return 0.85
        elif hours < 24:
            return 0.70
        elif hours < 48:
            return 0.55
        elif hours < 72:
            return 0.40
        else:
            return 0.25
