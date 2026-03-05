"""
Dynamic Threshold Engine — Adaptive Entry/Exit Thresholds

Instead of fixed 15% edge → buy, this module adapts thresholds based on:

1. TIME TO RESOLUTION:
   - 3 days out: need 20%+ edge (forecast is uncertain)
   - 1 day out: 12% edge is fine (forecast more reliable)
   - 6 hours out: 8% edge is enough (near-certainty from forecast)
   - Resolution day + actual data: 5% edge (data confirms outcome)

2. MARKET LIQUIDITY:
   - Thick orderbook → lower threshold (can fill without slippage)
   - Thin/spread >5% → higher threshold (need more edge for cost)

3. FORECAST CONFIDENCE:
   - All 6+ models agree (std < 0.5°C) → lower threshold
   - Models diverge (std > 2°C) → much higher threshold

4. HISTORICAL ACCURACY:
   - City with 0.5°C MAE → lower threshold (trust the forecast)
   - City with 2°C MAE → higher threshold (forecast is noisy)

5. CURRENT DRAWDOWN:
   - In drawdown → tighter thresholds (protect capital)
   - In profit → can afford to be slightly more aggressive

This replaces the static Config.WEATHER_MIN_EDGE with intelligent thresholds.
"""

from typing import Dict, Optional, Tuple


class DynamicThresholdEngine:
    """Compute adaptive entry and exit thresholds for weather trades."""

    def __init__(self):
        # Baseline thresholds (from Config defaults)
        self._base_entry_edge = 0.15
        self._base_exit_pct = 0.45
        # Per-city MAE (loaded from bias_corrector)
        self._city_mae: Dict[str, float] = {}
        # Current session P&L for drawdown adjustment
        self._session_pnl = 0.0

    def set_city_accuracy(self, city: str, mae: float):
        """Set the mean absolute error for a city (from bias_corrector)."""
        self._city_mae[city] = mae

    def set_session_pnl(self, pnl: float):
        """Update session P&L for drawdown-based adjustments."""
        self._session_pnl = pnl

    def get_entry_threshold(self, city: str,
                            seconds_remaining: int,
                            forecast_std: float,
                            liquidity: float = 0,
                            spread: float = 0,
                            forecast_unit: str = 'celsius') -> float:
        """
        Compute the minimum edge required to enter a trade.
        
        Returns:
            Minimum edge threshold (0.0-1.0). E.g., 0.12 = need 12% edge.
        """
        base = self._base_entry_edge

        # 1. Time adjustment
        hours = seconds_remaining / 3600
        if hours < 3:
            time_factor = 0.55   # 6 hours out → 55% of base
        elif hours < 6:
            time_factor = 0.65
        elif hours < 12:
            time_factor = 0.75
        elif hours < 24:
            time_factor = 0.85
        elif hours < 48:
            time_factor = 1.0
        elif hours < 72:
            time_factor = 1.15
        else:
            time_factor = 1.30   # 3+ days out → 130% of base

        # 2. Forecast confidence adjustment (based on model std)
        # Normalize std to celsius equivalent
        std_c = forecast_std
        if forecast_unit == 'fahrenheit':
            std_c = forecast_std * 5 / 9

        if std_c < 0.5:
            confidence_factor = 0.70   # Very tight → low threshold
        elif std_c < 1.0:
            confidence_factor = 0.85
        elif std_c < 2.0:
            confidence_factor = 1.0
        elif std_c < 3.0:
            confidence_factor = 1.20
        else:
            confidence_factor = 1.50   # Very uncertain → high threshold

        # 3. Liquidity/spread adjustment
        if spread > 0.10:
            liquidity_factor = 1.30  # Wide spread → need more edge
        elif spread > 0.05:
            liquidity_factor = 1.15
        elif spread > 0.02:
            liquidity_factor = 1.0
        elif liquidity > 100:
            liquidity_factor = 0.90  # Deep liquidity → slight edge reduction
        else:
            liquidity_factor = 1.0

        # 4. Historical accuracy adjustment
        mae = self._city_mae.get(city)
        if mae is not None:
            if forecast_unit == 'fahrenheit':
                mae_c = mae * 5 / 9
            else:
                mae_c = mae

            if mae_c < 0.5:
                accuracy_factor = 0.80
            elif mae_c < 1.0:
                accuracy_factor = 0.90
            elif mae_c < 2.0:
                accuracy_factor = 1.0
            else:
                accuracy_factor = 1.20
        else:
            accuracy_factor = 1.0

        # 5. Drawdown adjustment
        if self._session_pnl < -5.0:
            drawdown_factor = 1.30   # In significant drawdown → tighten
        elif self._session_pnl < -2.0:
            drawdown_factor = 1.15
        elif self._session_pnl > 5.0:
            drawdown_factor = 0.90   # Profitable → can be slightly aggressive
        else:
            drawdown_factor = 1.0

        # Combine all factors
        threshold = base * time_factor * confidence_factor * liquidity_factor \
                    * accuracy_factor * drawdown_factor

        # Clamp to reasonable range
        return max(0.04, min(0.35, threshold))

    def get_exit_thresholds(self, city: str,
                            seconds_remaining: int,
                            entry_price: float,
                            confidence: float) -> Dict[str, float]:
        """
        Compute dynamic exit thresholds.
        
        Returns:
            Dict with 'take_profit_pct', 'stop_loss_pct', 'trailing_pct'
        """
        hours = seconds_remaining / 3600

        # Take profit: tighter as resolution approaches
        if hours < 6:
            tp_pct = 25   # Close to resolution → take smaller profits
        elif hours < 12:
            tp_pct = 35
        elif hours < 24:
            tp_pct = 45   # Default
        else:
            tp_pct = 55   # Far out → let it run

        # High confidence → wider take-profit (let winners run)
        if confidence > 0.85:
            tp_pct *= 1.2

        # Stop loss: tighter for low confidence, wider for high
        if confidence > 0.80:
            sl_pct = -30
        elif confidence > 0.60:
            sl_pct = -25
        else:
            sl_pct = -20

        # Near-resolution: widen stop (market will resolve soon)
        if hours < 3:
            sl_pct = -35

        # Trailing stop (activated when in profit)
        trailing_pct = max(10, tp_pct * 0.4)

        return {
            'take_profit_pct': round(tp_pct, 1),
            'stop_loss_pct': round(sl_pct, 1),
            'trailing_pct': round(trailing_pct, 1),
        }

    def should_enter(self, edge: float, city: str,
                     seconds_remaining: int,
                     forecast_std: float,
                     liquidity: float = 0,
                     spread: float = 0,
                     forecast_unit: str = 'celsius') -> Tuple[bool, float, str]:
        """
        Convenience method: should we enter this trade?
        
        Returns:
            (should_enter, threshold_used, reason)
        """
        threshold = self.get_entry_threshold(
            city, seconds_remaining, forecast_std,
            liquidity, spread, forecast_unit
        )

        if edge >= threshold:
            return True, threshold, f"Edge {edge:.1%} ≥ threshold {threshold:.1%}"
        else:
            return False, threshold, f"Edge {edge:.1%} < threshold {threshold:.1%}"
