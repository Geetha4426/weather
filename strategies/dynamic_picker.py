"""
Dynamic Strategy Picker — Master Strategy for Weather Prediction

Runs ALL 7 weather strategies in PRIORITY ORDER:
  1. Intraday Tracker (resolution day ONLY — near-certainty trades)
  2. Frontrun (forecast shift detected — time-critical)
  3. Sum-check arb (guaranteed profit from mispricing)
  4. Adjacent bracket (hedged multi-outcome coverage)
  5. Convergence (scale-in as models converge)
  6. Forecast edge (core oracle — forecast vs market)
  7. Ensemble confidence (high model agreement)

This is the MASTER BRAIN — evaluates everything, deduplicates,
and returns the best signals sorted by priority + confidence.
"""

from typing import Dict, List

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
from weather.strategies.base_strategy import BaseStrategy, TradeSignal
from weather.strategies.intraday_tracker import IntradayTrackerStrategy
from weather.strategies.frontrun import FrontrunStrategy
from weather.strategies.adjacent_bracket import AdjacentBracketStrategy
from weather.strategies.convergence import ConvergenceStrategy
from weather.strategies.forecast_edge import ForecastEdgeStrategy
from weather.strategies.value_hunter import ValueHunterStrategy
from weather.strategies.ensemble_confidence import EnsembleConfidenceStrategy
from weather.config import Config


# Priority weights — higher = more priority when ranking signals
STRATEGY_PRIORITY = {
    'intraday_locked':        1.00,  # Actual data confirms → near-certainty
    'intraday_match':         0.95,  # Running max matches outcome
    'intraday_below':         0.90,  # Max below threshold
    'intraday_unlikely':      0.85,  # Temp won't reach threshold
    'frontrun_shift':         0.85,  # Forecast shifted, market lagging
    'frontrun_short':         0.80,  # Sell overpriced post-shift
    'sum_check_arb':          0.90,  # Guaranteed profit (arb)
    'adjacent_bracket':       0.75,  # Hedged multi-bracket
    'convergence':            0.70,  # Scale-in play
    'convergence_secondary':  0.55,  # 2nd best convergence
    'forecast_edge':          0.65,  # Core oracle strategy
    'ensemble_confidence':    0.60,  # Model agreement
    'concentration':          0.55,  # Market leader confirmed
    'overpriced_tail':        0.50,  # Tail NO play
}


class WeatherDynamicPicker(BaseStrategy):
    """
    Master strategy — evaluates ALL 7 weather strategies,
    deduplicates, and returns the best signals by priority.
    """

    name = "weather_dynamic"
    description = "7 strategies — frontrun, brackets, intraday, convergence, edge, value, ensemble"

    def __init__(self):
        self.strategies: List[BaseStrategy] = [
            IntradayTrackerStrategy(),   # Resolution day (highest priority)
            FrontrunStrategy(),          # Forecast shift detection
            ValueHunterStrategy(),       # Sum-check arb + overpriced tails
            AdjacentBracketStrategy(),   # Multi-bracket hedging
            ConvergenceStrategy(),       # Scale-in as models converge
            ForecastEdgeStrategy(),      # Core oracle strategy
            EnsembleConfidenceStrategy(),# Model agreement
        ]

    async def analyze(self, weather_market: Dict, context: Dict) -> List[TradeSignal]:
        """
        Run ALL strategies on a weather market.
        Returns the best signals sorted by priority + confidence.
        """
        all_signals: List[TradeSignal] = []

        for strategy in self.strategies:
            try:
                signals = await strategy.analyze(weather_market, context)
                if signals:
                    all_signals.extend(signals)
            except Exception as e:
                print(f"⚠️ Strategy {strategy.name} error: {e}", flush=True)
                continue

        if not all_signals:
            return []

        # Apply priority-based scoring
        for s in all_signals:
            sig_type = s.metadata.get('type', '')
            priority = STRATEGY_PRIORITY.get(sig_type, 0.50)

            # Combined score: 60% confidence + 40% priority
            s.metadata['priority'] = priority
            s.metadata['raw_confidence'] = s.confidence
            s.confidence = s.confidence * 0.60 + priority * 0.40

        # Deduplicate: if multiple strategies suggest same token, keep highest score
        seen_tokens = {}
        for s in all_signals:
            key = s.token_id
            if key not in seen_tokens or s.confidence > seen_tokens[key].confidence:
                seen_tokens[key] = s

        unique_signals = list(seen_tokens.values())

        # Sort by final score
        unique_signals.sort(key=lambda s: s.confidence, reverse=True)

        # Limit to max trades per run
        max_signals = Config.WEATHER_MAX_TRADES_PER_RUN

        # Annotate
        for s in unique_signals[:max_signals]:
            s.metadata['alternatives'] = len(unique_signals) - 1
            s.metadata['strategies_checked'] = len(self.strategies)
            s.metadata['total_signals_found'] = len(all_signals)

        return unique_signals[:max_signals]
