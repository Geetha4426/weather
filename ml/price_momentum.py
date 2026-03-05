"""
Price Momentum Tracker — Market Microstructure Signal

Tracks Polymarket price changes over time to detect:
1. Momentum: Price steadily rising → likely continues (smart money moving)
2. Mean-reversion: Price spiked too fast → likely pulls back
3. Volume-weighted signals: Large trades moving price → stronger signal
4. Stale prices: No movement = no information = opportunistic entry

Example:
  NYC "42-43°F" price: 6¢ → 8¢ → 12¢ → 15¢ over 4 scans
  → Momentum signal: +2.25¢/scan → smart money buying → ADD to position
  
  London "14°C" price: 20¢ → 35¢ in one scan  
  → Spike signal: +15¢ jump → likely overreaction → WAIT for pullback

This adds a market-side signal to complement weather forecast signals.
"""

import time
import math
from typing import Dict, List, Optional, Tuple


class PriceMomentumTracker:
    """
    Track price movements on Polymarket weather markets.
    Provides momentum, mean-reversion, and volatility signals.
    """

    def __init__(self):
        # Price history: {token_id: [(price, timestamp), ...]}
        self._price_history: Dict[str, List[Tuple[float, float]]] = {}
        # Computed signals: {token_id: signal_dict}
        self._signals: Dict[str, Dict] = {}
        # Max history length per token
        self._max_history = 50
        # Minimum observations for signal generation
        self._min_observations = 3

    def record_price(self, token_id: str, price: float,
                     volume_24h: float = 0):
        """Record a price observation for a token."""
        now = time.time()
        if token_id not in self._price_history:
            self._price_history[token_id] = []

        history = self._price_history[token_id]
        # Avoid duplicate records within 30 seconds
        if history and now - history[-1][0] < 30:
            return

        history.append((now, price, volume_24h))
        # Trim old entries
        if len(history) > self._max_history:
            self._price_history[token_id] = history[-self._max_history:]

        # Recompute signal
        self._compute_signal(token_id)

    def _compute_signal(self, token_id: str):
        """Compute momentum/mean-reversion signals for a token."""
        history = self._price_history.get(token_id, [])
        if len(history) < self._min_observations:
            self._signals[token_id] = {'momentum': 0, 'strength': 'none'}
            return

        prices = [h[1] for h in history]
        times = [h[0] for h in history]
        volumes = [h[2] for h in history if len(h) > 2]

        # Short-term momentum (last 3-5 observations)
        recent = prices[-min(5, len(prices)):]
        if len(recent) >= 2:
            short_momentum = (recent[-1] - recent[0]) / len(recent)
        else:
            short_momentum = 0

        # Long-term momentum (all observations)
        time_span = times[-1] - times[0]
        if time_span > 0:
            long_momentum = (prices[-1] - prices[0]) / (time_span / 60)  # per minute
        else:
            long_momentum = 0

        # Volatility (standard deviation of price changes)
        if len(prices) >= 3:
            changes = [prices[i] - prices[i-1] for i in range(1, len(prices))]
            avg_change = sum(changes) / len(changes)
            variance = sum((c - avg_change)**2 for c in changes) / len(changes)
            volatility = math.sqrt(variance) if variance > 0 else 0
        else:
            volatility = 0

        # Spike detection (large single-scan move)
        last_change = prices[-1] - prices[-2] if len(prices) >= 2 else 0
        is_spike = abs(last_change) > 0.10  # >10¢ move in one scan

        # RSI-like signal (relative strength)
        if len(prices) >= 5:
            gains = [max(0, prices[i] - prices[i-1]) for i in range(1, len(prices))]
            losses = [max(0, prices[i-1] - prices[i]) for i in range(1, len(prices))]
            avg_gain = sum(gains[-5:]) / 5
            avg_loss = sum(losses[-5:]) / 5
            if avg_loss > 0:
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
            else:
                rsi = 100 if avg_gain > 0 else 50
        else:
            rsi = 50

        # Trend direction
        if short_momentum > 0.005:
            trend = 'up'
        elif short_momentum < -0.005:
            trend = 'down'
        else:
            trend = 'flat'

        # Signal strength
        abs_momentum = abs(short_momentum)
        if abs_momentum > 0.02:
            strength = 'strong'
        elif abs_momentum > 0.005:
            strength = 'moderate'
        else:
            strength = 'weak'

        # Staleness: time since last change
        last_change_time = 0
        for i in range(len(prices) - 1, 0, -1):
            if abs(prices[i] - prices[i-1]) > 0.001:
                last_change_time = times[i]
                break
        stale_seconds = time.time() - last_change_time if last_change_time else float('inf')

        self._signals[token_id] = {
            'momentum': round(short_momentum, 4),
            'long_momentum': round(long_momentum, 6),
            'trend': trend,
            'strength': strength,
            'volatility': round(volatility, 4),
            'rsi': round(rsi, 1),
            'is_spike': is_spike,
            'last_change': round(last_change, 4),
            'stale_seconds': round(stale_seconds, 0),
            'observations': len(prices),
            'current_price': prices[-1],
        }

    def get_signal(self, token_id: str) -> Dict:
        """Get the current momentum signal for a token."""
        return self._signals.get(token_id, {
            'momentum': 0, 'trend': 'flat', 'strength': 'none',
            'observations': 0
        })

    def should_delay_entry(self, token_id: str) -> Tuple[bool, str]:
        """
        Should we delay entering a position?
        Returns (should_delay, reason).
        """
        signal = self.get_signal(token_id)

        # Spike detected — wait for pullback
        if signal.get('is_spike') and signal.get('trend') == 'up':
            return True, "Price spike detected — wait for pullback"

        # RSI overbought
        if signal.get('rsi', 50) > 80:
            return True, f"RSI {signal['rsi']:.0f} — overbought, wait"

        return False, ""

    def get_momentum_adjustment(self, token_id: str) -> float:
        """
        Get a confidence adjustment factor based on price momentum.
        > 1.0 = momentum confirms our direction → boost confidence
        < 1.0 = momentum against us → reduce confidence
        """
        signal = self.get_signal(token_id)
        momentum = signal.get('momentum', 0)
        trend = signal.get('trend', 'flat')

        if trend == 'up' and momentum > 0.005:
            # Price rising → smart money buying → boost
            return min(1.15, 1.0 + momentum * 5)
        elif trend == 'down' and momentum < -0.005:
            # Price falling → reduce confidence
            return max(0.85, 1.0 + momentum * 5)

        # Stale price → slight opportunity (market hasn't reacted yet)
        stale = signal.get('stale_seconds', 0)
        if stale > 600:  # No price change in 10+ minutes
            return 1.05  # Small boost — market might be slow to react

        return 1.0  # Neutral
