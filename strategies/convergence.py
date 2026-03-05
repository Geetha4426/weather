"""
Convergence Trader — Trades as Model Uncertainty Decreases

THE INSIGHT: As resolution approaches, weather model predictions CONVERGE.
3 days out: std = 3°C → models disagree → risky
1 day out: std = 1°C → models agree → high confidence  
6 hours out: std = 0.3°C → models nearly unanimous → near-certainty

This strategy SCALES POSITION SIZE as confidence increases:
  - 3 days out: small discovery positions (0.5x base)
  - 1 day out: standard positions (1x base)
  - 12 hours out: aggressive positions (1.5x base)
  - 6 hours out: maximum positions (2x base)

Also implements EARLY ENTRY + SCALE-IN:
  1. Take a small position early when edge exists
  2. Add to position as forecast converges and confirms
  3. This averages into a better entry price

Works with Kelly Criterion for optimal sizing.
"""

import math
from typing import Dict, List

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
from weather.strategies.base_strategy import BaseStrategy, TradeSignal
from weather.config import Config


class ConvergenceStrategy(BaseStrategy):
    """
    Trades as forecast uncertainty decreases.
    Scales position size proportional to confidence.
    """

    name = "convergence"
    description = "Scale-in as forecast models converge toward resolution"

    def __init__(self):
        # Track positions to implement scale-in
        self._existing_positions: Dict[str, float] = {}  # key -> total invested

    async def analyze(self, weather_market: Dict, context: Dict) -> List[TradeSignal]:
        forecast = context.get('forecast')
        clob = context.get('clob')
        seconds_remaining = context.get('seconds_remaining', 0)

        if not forecast or not clob:
            return []

        std_max = forecast.get('std_max', 99)
        model_confidence = forecast.get('confidence', 0)
        unit = forecast.get('unit', 'celsius')

        city = weather_market.get('city', '')
        target_date = weather_market.get('date', '')
        outcomes = weather_market.get('outcomes', [])
        prob_dist = forecast.get('probability_distribution', {})
        mean_max = forecast.get('mean_max', 0)

        # Time-based scaling
        hours = seconds_remaining / 3600
        time_multiplier = self._get_time_multiplier(hours)

        # Convergence-based minimum confidence
        convergence_threshold = self._get_convergence_threshold(std_max, unit)
        if model_confidence < convergence_threshold:
            return []

        signals = []

        # Find the most likely outcome
        best_outcome = None
        best_prob = 0

        for o in outcomes:
            temp_low = o.get('temp_low')
            if temp_low is None:
                continue
            fp = self._get_prob(o, prob_dist)
            if fp > best_prob:
                best_prob = fp
                best_outcome = o

        if not best_outcome or best_prob < 0.15:
            return []

        market_price = best_outcome.get('price_yes', 0.5)
        edge = best_prob - market_price

        if edge < 0.08:  # Lower threshold for convergence (since we scale in)
            return []

        yes_token = best_outcome.get('token_id_yes', '')
        if not yes_token:
            return []

        book = clob.get_orderbook(yes_token)
        entry = book['best_ask'] if book and not book.get('_synthetic') else market_price

        # Kelly Criterion sizing
        kelly_fraction = self._kelly_criterion(best_prob, entry)

        # Scale confidence by time multiplier
        confidence = min(0.95, model_confidence * 0.5 + edge * 2.0)
        confidence = min(0.98, confidence * time_multiplier)

        unit_sym = forecast.get('unit_symbol', '°C')
        label = best_outcome.get('label', '')

        key = f"{city}_{target_date}_{label}"
        existing = self._existing_positions.get(key, 0)

        rationale = (
            f"📈 CONVERGENCE: {city.upper()} {target_date}\n"
            f"  Models agree: {mean_max}{unit_sym} ±{std_max}{unit_sym} "
            f"({forecast.get('num_models', '?')} models)\n"
            f"  {label}: forecast={best_prob:.0%} vs market={market_price:.0%}\n"
            f"  Edge: {edge:+.1%} | Kelly: {kelly_fraction:.0%}\n"
            f"  Time: {hours:.0f}h remaining → {time_multiplier:.1f}x multiplier\n"
            f"  Convergence level: std={std_max:.1f}{unit_sym}"
        )

        if existing > 0:
            rationale += f"\n  Scale-in: already ${existing:.2f} invested"

        sig = TradeSignal(
            strategy=self.name, city=city, target_date=target_date,
            direction='YES',
            outcome_label=label,
            temp_c=best_outcome.get('temp_low', 0),
            token_id=yes_token,
            market_id=best_outcome.get('market_id', ''),
            entry_price=entry,
            confidence=confidence,
            rationale=rationale,
            metadata={
                'type': 'convergence',
                'edge': edge,
                'kelly_fraction': kelly_fraction,
                'time_multiplier': time_multiplier,
                'forecast_prob': best_prob,
                'std_max': std_max,
                'hours_remaining': hours,
                'scale_in_existing': existing,
            },
        )
        signals.append(sig)

        # Also consider 2nd most likely (if close)
        second_best = None
        second_prob = 0
        for o in outcomes:
            if o == best_outcome:
                continue
            temp_low = o.get('temp_low')
            if temp_low is None:
                continue
            fp = self._get_prob(o, prob_dist)
            if fp > second_prob:
                second_prob = fp
                second_best = o

        if second_best and second_prob > 0.15:
            second_price = second_best.get('price_yes', 0.5)
            second_edge = second_prob - second_price
            second_token = second_best.get('token_id_yes', '')

            if second_edge > 0.10 and second_token:
                second_confidence = min(0.85, confidence * 0.8)
                signals.append(TradeSignal(
                    strategy=self.name, city=city, target_date=target_date,
                    direction='YES',
                    outcome_label=second_best.get('label', ''),
                    temp_c=second_best.get('temp_low', 0),
                    token_id=second_token,
                    market_id=second_best.get('market_id', ''),
                    entry_price=second_price,
                    confidence=second_confidence,
                    rationale=(
                        f"📈 CONVERGENCE (2nd): {second_best.get('label', '')} "
                        f"forecast={second_prob:.0%} market={second_price:.0%} "
                        f"edge={second_edge:+.1%}"
                    ),
                    metadata={
                        'type': 'convergence_secondary',
                        'edge': second_edge,
                        'forecast_prob': second_prob,
                    },
                ))

        return signals

    def _get_time_multiplier(self, hours: float) -> float:
        """Scale position size based on time to resolution."""
        if hours < 6:
            return 2.0      # Near resolution → aggressive
        elif hours < 12:
            return 1.5
        elif hours < 24:
            return 1.2
        elif hours < 48:
            return 1.0
        elif hours < 72:
            return 0.7
        else:
            return 0.5       # Far out → conservative

    def _get_convergence_threshold(self, std, unit):
        """Minimum confidence based on model agreement."""
        if unit == 'fahrenheit':
            std = std * 5 / 9  # Normalize to Celsius equivalent

        if std < 0.5:
            return 0.30   # Very tight → lower threshold
        elif std < 1.0:
            return 0.45
        elif std < 2.0:
            return 0.55
        elif std < 3.0:
            return 0.65
        else:
            return 0.80   # High uncertainty → need high confidence

    def _kelly_criterion(self, win_prob: float, odds: float) -> float:
        """
        Kelly Criterion for optimal bet sizing.
        
        f* = (bp - q) / b
        where:
          b = payout odds (1/price - 1)
          p = probability of winning
          q = probability of losing (1 - p)
        
        Returns fraction of bankroll to bet (0 to 1).
        We use half-Kelly for safety.
        """
        if odds <= 0 or odds >= 1:
            return 0

        b = (1.0 / odds) - 1  # Payout ratio
        p = win_prob
        q = 1 - p

        kelly = (b * p - q) / b if b > 0 else 0
        kelly = max(0, min(1, kelly))

        # Half-Kelly for safety
        return kelly * 0.5

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

    def record_position(self, city: str, target_date: str, label: str, amount: float):
        """Record a new position for scale-in tracking."""
        key = f"{city}_{target_date}_{label}"
        self._existing_positions[key] = self._existing_positions.get(key, 0) + amount
