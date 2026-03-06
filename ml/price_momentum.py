"""
Price Momentum Detector — EMA-Based Market Trend Analysis

Detects price momentum in Polymarket weather markets to time entries.

WHY: If "17°C" just went from $0.25 → $0.35 in 1 hour, OTHER traders
are buying in based on updated forecasts. We should either:
  - JOIN the momentum (buy before it goes higher)
  - AVOID buying into a spike (wait for pullback)

SIGNALS:
  - BULLISH: Price rising, EMA crossover → trend building
  - BEARISH: Price falling, EMA crossover → trend weakening
  - BREAKOUT: Sudden price jump > 2x normal volatility
  - MEAN_REVERT: Price spiked and starting to revert → good entry

Uses dual EMA (fast=5, slow=15 observations) for crossover detection.
Calculates RSI-like momentum for overbought/oversold detection.
"""

import math
import time
from typing import Dict, List, Optional, Tuple
from collections import defaultdict


class PriceMomentumDetector:
    """
    EMA-based momentum detector for weather market prices.
    Tracks price history per token and generates momentum signals.
    """

    def __init__(self, fast_period: int = 5, slow_period: int = 15):
        self.fast_period = fast_period
        self.slow_period = slow_period

        # Price history per token: [(timestamp, price), ...]
        self._prices: Dict[str, List[Tuple[float, float]]] = defaultdict(list)

        # EMA values per token
        self._fast_ema: Dict[str, float] = {}
        self._slow_ema: Dict[str, float] = {}

        # Momentum state
        self._prev_signals: Dict[str, str] = {}
        self._max_history = 200

    def record_price(self, token_id: str, price: float, timestamp: float = None):
        """Record a price observation for a token."""
        ts = timestamp or time.time()
        self._prices[token_id].append((ts, price))

        # Trim history
        if len(self._prices[token_id]) > self._max_history:
            self._prices[token_id] = self._prices[token_id][-self._max_history:]

        # Update EMAs
        self._update_ema(token_id, price)

    def get_momentum(self, token_id: str) -> Dict:
        """
        Get current momentum analysis for a token.

        Returns:
            {
                'signal': str ('bullish', 'bearish', 'breakout', 'mean_revert', 'neutral'),
                'strength': float (0-1, higher = stronger signal),
                'fast_ema': float,
                'slow_ema': float,
                'trend': str ('up', 'down', 'flat'),
                'rsi': float (0-100, >70 = overbought, <30 = oversold),
                'volatility': float,
                'price_change_pct': float (recent price change %),
                'recommendation': str ('buy', 'sell', 'wait', 'neutral'),
            }
        """
        prices = self._prices.get(token_id, [])

        if len(prices) < 3:
            return self._neutral_momentum()

        current = prices[-1][1]
        fast = self._fast_ema.get(token_id, current)
        slow = self._slow_ema.get(token_id, current)

        # Price changes
        recent_prices = [p for _, p in prices[-10:]]
        price_change = (current - recent_prices[0]) / max(recent_prices[0], 0.001)

        # Volatility (standard deviation of returns)
        volatility = self._calculate_volatility(token_id)

        # RSI-like momentum
        rsi = self._calculate_rsi(token_id)

        # EMA crossover detection
        trend = 'flat'
        if fast > slow * 1.01:
            trend = 'up'
        elif fast < slow * 0.99:
            trend = 'down'

        # Signal generation
        signal, strength = self._generate_signal(
            current, fast, slow, rsi, volatility, price_change, token_id)

        # Trading recommendation
        recommendation = self._get_recommendation(signal, rsi, trend)

        return {
            'signal': signal,
            'strength': round(strength, 3),
            'fast_ema': round(fast, 4),
            'slow_ema': round(slow, 4),
            'trend': trend,
            'rsi': round(rsi, 1),
            'volatility': round(volatility, 4),
            'price_change_pct': round(price_change * 100, 2),
            'recommendation': recommendation,
        }

    def get_entry_timing(self, token_id: str, edge: float) -> Dict:
        """
        Should we enter NOW or WAIT for better timing?

        Combines edge with momentum to decide:
          - Edge + bullish momentum → ENTER NOW (ride the wave)
          - Edge + overbought → WAIT (might pull back)
          - Edge + bearish momentum → ENTER (contrarian, mean revert expected)
        """
        momentum = self.get_momentum(token_id)
        signal = momentum['signal']
        rsi = momentum['rsi']
        strength = momentum['strength']

        # Default: enter based on edge alone
        timing = {
            'action': 'enter_now',
            'delay_seconds': 0,
            'price_target': None,
            'reason': '',
        }

        # Overbought: wait for pullback
        if rsi > 75 and edge < 0.20:
            timing['action'] = 'wait'
            timing['delay_seconds'] = 300  # 5 minutes
            timing['reason'] = f"Overbought (RSI={rsi:.0f}), wait for pullback"
            return timing

        # Oversold with edge: great entry
        if rsi < 30 and edge > 0.10:
            timing['action'] = 'enter_now'
            timing['reason'] = f"Oversold (RSI={rsi:.0f}) + edge {edge:.1%} = great entry"
            return timing

        # Breakout with edge: ride it
        if signal == 'breakout' and edge > 0.10:
            timing['action'] = 'enter_now'
            timing['reason'] = f"Breakout detected + edge → ride the wave"
            return timing

        # Strong bearish without edge: avoid
        if signal == 'bearish' and strength > 0.7 and edge < 0.15:
            timing['action'] = 'wait'
            timing['delay_seconds'] = 600
            timing['reason'] = f"Strong bearish momentum, edge may disappear"
            return timing

        # Mean revert with edge: enter
        if signal == 'mean_revert' and edge > 0.10:
            timing['action'] = 'enter_now'
            timing['reason'] = f"Mean reversion + edge {edge:.1%}"
            return timing

        timing['reason'] = f"Normal conditions, edge {edge:.1%}"
        return timing

    def _update_ema(self, token_id: str, price: float):
        """Update fast and slow EMAs."""
        # Fast EMA
        if token_id not in self._fast_ema:
            self._fast_ema[token_id] = price
        else:
            alpha = 2.0 / (self.fast_period + 1)
            self._fast_ema[token_id] = alpha * price + (1 - alpha) * self._fast_ema[token_id]

        # Slow EMA
        if token_id not in self._slow_ema:
            self._slow_ema[token_id] = price
        else:
            alpha = 2.0 / (self.slow_period + 1)
            self._slow_ema[token_id] = alpha * price + (1 - alpha) * self._slow_ema[token_id]

    def _calculate_volatility(self, token_id: str) -> float:
        """Price volatility as standard deviation of returns."""
        prices = [p for _, p in self._prices.get(token_id, [])]
        if len(prices) < 3:
            return 0.0

        returns = []
        for i in range(1, len(prices)):
            if prices[i-1] > 0:
                returns.append((prices[i] - prices[i-1]) / prices[i-1])

        if not returns:
            return 0.0

        mean = sum(returns) / len(returns)
        variance = sum((r - mean) ** 2 for r in returns) / len(returns)
        return math.sqrt(variance)

    def _calculate_rsi(self, token_id: str, period: int = 14) -> float:
        """RSI-like momentum indicator (0-100)."""
        prices = [p for _, p in self._prices.get(token_id, [])]
        if len(prices) < period + 1:
            return 50.0  # Neutral default

        changes = [prices[i] - prices[i-1] for i in range(-period, 0)]

        gains = [c for c in changes if c > 0]
        losses = [-c for c in changes if c < 0]

        avg_gain = sum(gains) / period if gains else 0
        avg_loss = sum(losses) / period if losses else 0

        if avg_loss == 0:
            return 100.0 if avg_gain > 0 else 50.0

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        return rsi

    def _generate_signal(self, current, fast, slow, rsi, volatility,
                          price_change, token_id):
        """Generate momentum signal."""
        # Breakout: sudden move > 2x volatility
        if abs(price_change) > max(volatility * 2, 0.10):
            if price_change > 0:
                return 'breakout', min(1.0, abs(price_change) / 0.20)
            else:
                return 'breakdown', min(1.0, abs(price_change) / 0.20)

        # Mean reversion: after spike, price moving back
        prev = self._prev_signals.get(token_id, '')
        if prev in ('breakout', 'breakdown') and abs(price_change) < volatility * 0.5:
            self._prev_signals[token_id] = 'mean_revert'
            return 'mean_revert', 0.6

        # EMA crossover
        ema_diff = (fast - slow) / max(slow, 0.001)

        if ema_diff > 0.02:
            signal = 'bullish'
            strength = min(1.0, ema_diff / 0.10)
        elif ema_diff < -0.02:
            signal = 'bearish'
            strength = min(1.0, abs(ema_diff) / 0.10)
        else:
            signal = 'neutral'
            strength = 0.0

        # RSI override
        if rsi > 80:
            signal = 'overbought'
            strength = (rsi - 70) / 30
        elif rsi < 20:
            signal = 'oversold'
            strength = (30 - rsi) / 30

        self._prev_signals[token_id] = signal
        return signal, strength

    def _get_recommendation(self, signal, rsi, trend):
        """Get trading recommendation from momentum."""
        if signal in ('bullish', 'breakout', 'oversold'):
            return 'buy'
        elif signal in ('bearish', 'breakdown', 'overbought'):
            return 'sell' if rsi > 75 else 'wait'
        elif signal == 'mean_revert':
            return 'buy' if trend == 'up' else 'wait'
        return 'neutral'

    def _neutral_momentum(self):
        return {
            'signal': 'neutral', 'strength': 0, 'fast_ema': 0, 'slow_ema': 0,
            'trend': 'flat', 'rsi': 50, 'volatility': 0,
            'price_change_pct': 0, 'recommendation': 'neutral',
        }
