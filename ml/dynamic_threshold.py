"""
Dynamic Threshold Engine — Adaptive Entry/Exit Logic

PROBLEM: Fixed thresholds (15% entry, 45% exit) are DUMB.
  - 6 hours before resolution with 10% edge → SHOULD ENTER (high confidence)
  - 72 hours before resolution with 20% edge → SHOULD NOT (too uncertain)
  - High liquidity market with 12% edge → safer than low liquidity with 20% edge

THIS IS THE BRAIN that decides WHEN and HOW MUCH to trade.

Adapts based on:
  1. Time to resolution (exponential confidence curve)
  2. Edge size (larger edge → lower threshold needed)
  3. Model confidence (ensemble agreement)
  4. Market liquidity (deeper = safer to trade)
  5. Historical win rate (learning from past trades)
  6. Volatility (recent price changes)
  7. Position count (risk management)
"""

import math
import time
from typing import Dict, List, Optional


class DynamicThresholdEngine:
    """
    Adaptive entry/exit logic that replaces fixed thresholds.
    Learns from trade history and adjusts in real-time.
    """

    def __init__(self):
        # Base thresholds (will be dynamically adjusted)
        self._base_entry_edge = 0.15       # 15%
        self._base_exit_profit = 0.45      # 45%
        self._base_stop_loss = -0.25       # -25%

        # Learning from trades
        self._trade_history: List[Dict] = []
        self._win_rate: float = 0.0
        self._avg_win: float = 0.0
        self._avg_loss: float = 0.0
        self._recent_pnl: List[float] = []

        # Price tracking for volatility
        self._price_history: Dict[str, List[float]] = {}  # token -> [prices]

    def should_enter(self, edge: float, model_confidence: float,
                     hours_remaining: float, market_liquidity: float = 1000,
                     open_positions: int = 0, max_positions: int = 15,
                     edge_uncertainty: float = 0.0) -> Dict:
        """
        Dynamic entry decision. Returns whether to enter and why.

        Args:
            edge: forecast_prob - market_price
            model_confidence: 0-1 ensemble confidence
            hours_remaining: hours until resolution
            market_liquidity: USD liquidity in the market
            open_positions: current number of open positions
            max_positions: maximum allowed positions
            edge_uncertainty: from Bayesian updater

        Returns:
            {
                'should_enter': bool,
                'adjusted_threshold': float,
                'position_scale': float (0.5-2.0 multiplier),
                'reason': str,
                'urgency': float (0-1, higher = act faster),
            }
        """
        # Risk check first
        if open_positions >= max_positions:
            return self._no_entry("Max positions reached")

        # Dynamic entry threshold
        threshold = self._calculate_entry_threshold(
            model_confidence, hours_remaining, market_liquidity)

        # Position scaling
        scale = self._calculate_position_scale(
            edge, model_confidence, hours_remaining, open_positions, max_positions)

        # Urgency (how quickly to act)
        urgency = self._calculate_urgency(edge, hours_remaining, model_confidence)

        # Main entry decision
        if edge < threshold:
            return {
                'should_enter': False,
                'adjusted_threshold': round(threshold, 4),
                'position_scale': 0,
                'reason': f"Edge {edge:.1%} < threshold {threshold:.1%}",
                'urgency': 0,
            }

        # Additional safety: if uncertainty is too high relative to edge
        if edge_uncertainty > 0 and edge_uncertainty > edge * 0.8:
            return {
                'should_enter': False,
                'adjusted_threshold': round(threshold, 4),
                'position_scale': 0,
                'reason': f"Uncertainty {edge_uncertainty:.1%} too high vs edge {edge:.1%}",
                'urgency': 0,
            }

        # Drawdown protection: if recent P&L is bad, tighten
        if self._is_on_losing_streak():
            threshold *= 1.3  # Raise threshold during losing streaks
            if edge < threshold:
                return self._no_entry(f"Losing streak protection (threshold raised to {threshold:.1%})")

        return {
            'should_enter': True,
            'adjusted_threshold': round(threshold, 4),
            'position_scale': round(scale, 2),
            'reason': f"Edge {edge:.1%} > threshold {threshold:.1%} | scale={scale:.1f}x",
            'urgency': round(urgency, 2),
        }

    def should_exit(self, pnl_pct: float, current_price: float,
                    entry_price: float, hours_remaining: float,
                    model_confidence: float = 0.7,
                    forecast_prob: float = 0.5) -> Dict:
        """
        Dynamic exit decision.

        Returns:
            {
                'should_exit': bool,
                'reason': str,
                'take_profit_level': float,
                'stop_loss_level': float,
                'trailing_stop': float,
            }
        """
        # Dynamic take profit
        tp = self._calculate_take_profit(hours_remaining, model_confidence)

        # Dynamic stop loss
        sl = self._calculate_stop_loss(hours_remaining, model_confidence)

        # Trailing stop (tightens as profit grows)
        trailing = self._calculate_trailing_stop(pnl_pct)

        result = {
            'should_exit': False,
            'reason': '',
            'take_profit_level': round(tp * 100, 1),
            'stop_loss_level': round(sl * 100, 1),
            'trailing_stop': round(trailing * 100, 1),
        }

        # Check exit conditions in priority order

        # 1. Near-certainty exit (price > 0.95)
        if current_price >= 0.95:
            result['should_exit'] = True
            result['reason'] = 'near_certainty'
            return result

        # 2. Near-zero cut (price < 0.02)
        if current_price <= 0.02:
            result['should_exit'] = True
            result['reason'] = 'near_zero'
            return result

        # 3. Take profit
        if pnl_pct >= tp * 100:
            result['should_exit'] = True
            result['reason'] = 'take_profit'
            return result

        # 4. Stop loss
        if pnl_pct <= sl * 100:
            result['should_exit'] = True
            result['reason'] = 'stop_loss'
            return result

        # 5. Trailing stop (only activates after 20% profit)
        if pnl_pct >= 20 and pnl_pct <= trailing * 100:
            result['should_exit'] = True
            result['reason'] = 'trailing_stop'
            return result

        # 6. Edge reversal: if forecast now says our position is wrong
        if forecast_prob < entry_price * 0.6:
            result['should_exit'] = True
            result['reason'] = 'edge_reversal'
            return result

        # 7. Time decay exit: very close to resolution, take small profits
        if hours_remaining < 2 and pnl_pct > 10:
            result['should_exit'] = True
            result['reason'] = 'time_decay_profit'
            return result

        return result

    def record_trade_result(self, pnl: float, pnl_pct: float):
        """Record a completed trade for learning."""
        self._trade_history.append({
            'pnl': pnl, 'pnl_pct': pnl_pct, 'time': time.time(),
        })
        self._recent_pnl.append(pnl)
        if len(self._recent_pnl) > 50:
            self._recent_pnl.pop(0)
        self._update_stats()

    def record_price(self, token_id: str, price: float):
        """Record a price observation for volatility tracking."""
        if token_id not in self._price_history:
            self._price_history[token_id] = []
        self._price_history[token_id].append(price)
        # Keep last 100 observations
        if len(self._price_history[token_id]) > 100:
            self._price_history[token_id].pop(0)

    def get_volatility(self, token_id: str) -> float:
        """Get price volatility for a token."""
        prices = self._price_history.get(token_id, [])
        if len(prices) < 3:
            return 0.0
        returns = [(prices[i] - prices[i-1]) / max(prices[i-1], 0.001)
                   for i in range(1, len(prices))]
        if not returns:
            return 0.0
        mean_ret = sum(returns) / len(returns)
        variance = sum((r - mean_ret) ** 2 for r in returns) / len(returns)
        return math.sqrt(variance)

    # ═══════════════════════════════════════════════════════════════════
    # INTERNAL: Dynamic Threshold Calculations
    # ═══════════════════════════════════════════════════════════════════

    def _calculate_entry_threshold(self, model_confidence, hours_remaining,
                                    market_liquidity):
        """
        Dynamic entry threshold based on conditions.
        
        Key insight: threshold should be LOWER when:
          - Models strongly agree (high confidence)
          - Close to resolution (forecast is more accurate)
          - High liquidity (safer to trade)
        """
        threshold = self._base_entry_edge

        # Time adjustment: closer → lower threshold (more confident)
        if hours_remaining < 6:
            threshold *= 0.50     # Only need 7.5% edge near resolution
        elif hours_remaining < 12:
            threshold *= 0.65     # 10% edge
        elif hours_remaining < 24:
            threshold *= 0.80     # 12% edge
        elif hours_remaining > 72:
            threshold *= 1.40     # 21% edge needed far out

        # Confidence adjustment
        if model_confidence > 0.85:
            threshold *= 0.75
        elif model_confidence > 0.70:
            threshold *= 0.90
        elif model_confidence < 0.50:
            threshold *= 1.30

        # Liquidity adjustment
        if market_liquidity > 3000:
            threshold *= 0.90     # Safer in liquid markets
        elif market_liquidity < 500:
            threshold *= 1.20     # Riskier in thin markets

        # Win rate adjustment (learning from history)
        if self._win_rate > 0.65:
            threshold *= 0.90
        elif self._win_rate < 0.40 and len(self._trade_history) > 10:
            threshold *= 1.20

        return max(0.05, min(0.30, threshold))

    def _calculate_position_scale(self, edge, model_confidence,
                                   hours_remaining, open_positions,
                                   max_positions):
        """
        Dynamic position scaling (0.5x to 2.0x base size).
        
        Higher scale for:
          - Large edge (confident opportunity)
          - High model agreement
          - Close to resolution
          - Few open positions (room to trade)
        """
        scale = 1.0

        # Edge scaling (quadratic — big edge = much bigger position)
        if edge > 0.30:
            scale *= 1.8
        elif edge > 0.20:
            scale *= 1.4
        elif edge > 0.15:
            scale *= 1.2
        elif edge < 0.10:
            scale *= 0.7

        # Confidence scaling
        scale *= (0.5 + model_confidence * 0.7)

        # Time scaling
        if hours_remaining < 6:
            scale *= 1.5
        elif hours_remaining < 12:
            scale *= 1.3
        elif hours_remaining > 48:
            scale *= 0.6

        # Position count scaling (reduce as we fill up)
        utilization = open_positions / max_positions if max_positions > 0 else 1
        if utilization > 0.7:
            scale *= 0.5
        elif utilization > 0.5:
            scale *= 0.7

        return max(0.5, min(2.0, scale))

    def _calculate_take_profit(self, hours_remaining, model_confidence):
        """Dynamic take profit level."""
        tp = self._base_exit_profit

        # Near resolution: take smaller profits (less time for bigger moves)
        if hours_remaining < 6:
            tp *= 0.50   # 22.5% TP near resolution
        elif hours_remaining < 12:
            tp *= 0.65   # 29% TP
        elif hours_remaining < 24:
            tp *= 0.80

        # High confidence: let winners run longer
        if model_confidence > 0.85:
            tp *= 1.20

        return max(0.15, min(0.80, tp))

    def _calculate_stop_loss(self, hours_remaining, model_confidence):
        """Dynamic stop loss level."""
        sl = self._base_stop_loss

        # Near resolution: tighter stop (less time to recover)
        if hours_remaining < 6:
            sl *= 0.60   # -15% stop
        elif hours_remaining < 12:
            sl *= 0.80   # -20% stop

        # Low confidence: tighter stop
        if model_confidence < 0.50:
            sl *= 0.70

        return max(-0.50, min(-0.10, sl))

    def _calculate_trailing_stop(self, pnl_pct):
        """Trailing stop that tightens as profit grows."""
        if pnl_pct < 20:
            return -0.25  # No trailing until 20% profit
        elif pnl_pct < 40:
            return (pnl_pct - 15) / 100  # Lock in 5-25% of profit
        else:
            return (pnl_pct - 10) / 100  # Lock in more as profit grows

    def _calculate_urgency(self, edge, hours_remaining, model_confidence):
        """How urgently to execute (for FOK vs GTC decision)."""
        urgency = 0.5

        if edge > 0.25:
            urgency += 0.20
        if hours_remaining < 12:
            urgency += 0.15
        if model_confidence > 0.80:
            urgency += 0.10

        return max(0, min(1, urgency))

    def _is_on_losing_streak(self) -> bool:
        """Check if we're on a losing streak (3+ consecutive losses)."""
        if len(self._recent_pnl) < 3:
            return False
        return all(p < 0 for p in self._recent_pnl[-3:])

    def _update_stats(self):
        """Update win rate and average P&L from history."""
        if not self._trade_history:
            return
        recent = self._trade_history[-50:]
        wins = [t for t in recent if t['pnl'] > 0]
        losses = [t for t in recent if t['pnl'] <= 0]

        self._win_rate = len(wins) / len(recent) if recent else 0
        self._avg_win = sum(t['pnl'] for t in wins) / len(wins) if wins else 0
        self._avg_loss = sum(t['pnl'] for t in losses) / len(losses) if losses else 0

    def _no_entry(self, reason):
        return {
            'should_enter': False, 'adjusted_threshold': 0,
            'position_scale': 0, 'reason': reason, 'urgency': 0,
        }
