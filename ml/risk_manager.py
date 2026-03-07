"""
Risk Manager — Portfolio-Level Risk Controls

Implements the safety net that prevents catastrophic losses:

1. DAILY LOSS CIRCUIT BREAKER:
   - If daily P&L drops below -MAX_DAILY_LOSS_PCT → pause ALL trading
   - Resumes next day or after manual override

2. PORTFOLIO HEAT LIMIT:
   - Total exposure (sum of open position sizes) capped at % of balance
   - Prevents over-leveraging into correlated weather outcomes

3. PER-CITY LIMITS:
   - Max exposure per city prevents concentration risk
   - E.g., don't put 80% of capital into NYC weather

4. POSITION SIZE GOVERNOR:
   - Uses Kelly criterion with calibrated probabilities
   - Caps at max_position_usd regardless of Kelly output
   - Reduces sizing during drawdowns

5. CORRELATION GUARD:
   - Nearby cities often have correlated weather
   - Don't bet the same direction on NYC + Chicago simultaneously
"""

import time
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date


class RiskManager:
    """Portfolio-level risk management for weather trading."""

    # Cities that tend to be weather-correlated
    CORRELATED_GROUPS = [
        {'nyc', 'chicago', 'atlanta', 'dallas', 'miami', 'seattle'},  # US cities
        {'london', 'munich', 'paris'},  # European cities
    ]

    def __init__(self, config=None):
        # Import config here to avoid circular imports
        if config is None:
            import sys, os
            sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
            from weather.config import Config
            config = Config

        self.max_daily_loss_pct = config.MAX_DAILY_LOSS_PCT / 100
        self.max_position_usd = config.WEATHER_MAX_POSITION_USD
        self.max_total_positions = config.WEATHER_MAX_TOTAL_POSITIONS
        self.max_per_event = config.WEATHER_MAX_POSITIONS_PER_EVENT
        self.risk_mode = config.RISK_MODE

        # Daily tracking
        self._daily_pnl = 0.0
        self._daily_trades = 0
        self._daily_wins = 0
        self._day_start = date.today().isoformat()
        self._starting_balance = 0.0

        # Exposure tracking: {city: total_usd}
        self._city_exposure: Dict[str, float] = {}
        self._total_exposure = 0.0

        # Circuit breaker state
        self._is_paused = False
        self._pause_reason = ''
        self._pause_time = 0.0

        # Trade frequency limiter
        self._recent_trades: List[float] = []  # timestamps
        self._max_trades_per_hour = 20

        # Drawdown tracking
        self._peak_balance = 0.0
        self._current_drawdown_pct = 0.0

    def set_starting_balance(self, balance: float):
        """Set the starting balance for daily loss calculations."""
        self._starting_balance = balance
        self._peak_balance = max(self._peak_balance, balance)

    def reset_daily(self):
        """Reset daily counters (call at start of new trading day)."""
        today = date.today().isoformat()
        if self._day_start != today:
            self._daily_pnl = 0.0
            self._daily_trades = 0
            self._daily_wins = 0
            self._day_start = today
            # Auto-unpause on new day
            if self._is_paused and self._pause_reason == 'daily_loss':
                self._is_paused = False
                self._pause_reason = ''

    def can_trade(self, city: str, size_usd: float,
                  current_balance: float) -> Tuple[bool, str]:
        """
        Check if a trade is allowed by risk controls.
        
        Returns:
            (allowed, reason)
        """
        self.reset_daily()

        # Circuit breaker
        if self._is_paused:
            return False, f"Trading paused: {self._pause_reason}"

        # Daily loss limit
        if self._starting_balance > 0:
            loss_limit = self._starting_balance * self.max_daily_loss_pct
            if self._daily_pnl < -loss_limit:
                self._is_paused = True
                self._pause_reason = 'daily_loss'
                self._pause_time = time.time()
                return False, (
                    f"Daily loss limit hit: ${self._daily_pnl:.2f} "
                    f"(max: -${loss_limit:.2f})"
                )

        # Balance check
        if size_usd > current_balance * 0.5:
            return False, f"Size ${size_usd:.2f} > 50% of balance ${current_balance:.2f}"

        # Total exposure limit (80% of balance in conservative, 120% in aggressive)
        portfolio_limit = self._get_portfolio_limit(current_balance)
        if self._total_exposure + size_usd > portfolio_limit:
            return False, (
                f"Portfolio heat: ${self._total_exposure:.2f} + ${size_usd:.2f} "
                f"> limit ${portfolio_limit:.2f}"
            )

        # Per-city exposure limit
        city_limit = self._get_city_limit(current_balance)
        city_exp = self._city_exposure.get(city, 0)
        if city_exp + size_usd > city_limit:
            return False, (
                f"{city} exposure: ${city_exp:.2f} + ${size_usd:.2f} "
                f"> limit ${city_limit:.2f}"
            )

        # Trade frequency
        now = time.time()
        self._recent_trades = [t for t in self._recent_trades if now - t < 3600]
        if len(self._recent_trades) >= self._max_trades_per_hour:
            return False, f"Rate limit: {len(self._recent_trades)} trades/hour"

        # Drawdown-based reduction
        if self._current_drawdown_pct > 15:
            return False, f"In {self._current_drawdown_pct:.0f}% drawdown — pausing"

        return True, "OK"

    def check_correlation(self, city: str, direction: str,
                          open_positions: List[Dict]) -> Tuple[bool, str]:
        """
        Check if this trade has excessive correlation with existing positions.
        """
        # Find which correlation group this city belongs to
        city_group = None
        for group in self.CORRELATED_GROUPS:
            if city in group:
                city_group = group
                break

        if city_group is None:
            return True, "OK"

        # Count same-direction positions in the same correlation group
        same_direction_count = 0
        for pos in open_positions:
            pos_city = pos.get('city', '')
            pos_direction = pos.get('direction', '')
            if pos_city in city_group and pos_direction == direction:
                same_direction_count += 1

        # Allow max 3 same-direction positions in correlated group
        max_correlated = 3 if self.risk_mode == 'aggressive' else 2
        if same_direction_count >= max_correlated:
            return False, (
                f"Correlated: {same_direction_count} {direction} positions "
                f"in {city_group}"
            )

        return True, "OK"

    def record_trade(self, pnl: float = None, size_usd: float = 0,
                     city: str = '', is_open: bool = True):
        """Record a trade for risk tracking."""
        if is_open:
            # Opening position
            self._total_exposure += size_usd
            self._city_exposure[city] = self._city_exposure.get(city, 0) + size_usd
            self._recent_trades.append(time.time())
            self._daily_trades += 1
        else:
            # Closing position
            self._total_exposure = max(0, self._total_exposure - size_usd)
            if city:
                self._city_exposure[city] = max(
                    0, self._city_exposure.get(city, 0) - size_usd
                )
            if pnl is not None:
                self._daily_pnl += pnl
                if pnl > 0:
                    self._daily_wins += 1

    def update_balance(self, current_balance: float):
        """Update drawdown tracking with current balance."""
        self._peak_balance = max(self._peak_balance, current_balance)
        if self._peak_balance > 0:
            self._current_drawdown_pct = (
                (self._peak_balance - current_balance) / self._peak_balance * 100
            )
        else:
            self._current_drawdown_pct = 0

    def get_kelly_size(self, win_prob: float, win_return: float,
                       loss_fraction: float, balance: float) -> float:
        """
        Kelly criterion position sizing.
        
        Args:
            win_prob: Calibrated probability of winning (0-1)
            win_return: Expected return if win (e.g., 0.5 for 50%)
            loss_fraction: Fraction lost if lose (e.g., 0.25 for -25%)
            balance: Current balance
            
        Returns:
            Recommended position size in USD
        """
        if win_prob <= 0 or loss_fraction <= 0 or win_return <= 0:
            return 0

        # Kelly fraction: f* = (p * b - q) / b
        # where p = win_prob, q = 1-p, b = odds ratio
        b = win_return / loss_fraction
        q = 1 - win_prob
        kelly_f = (win_prob * b - q) / b

        if kelly_f <= 0:
            return 0  # Negative EV — don't trade

        # Use fractional Kelly (25-50% of full Kelly) for safety
        if self.risk_mode == 'conservative':
            kelly_fraction = 0.25
        elif self.risk_mode == 'moderate':
            kelly_fraction = 0.35
        else:
            kelly_fraction = 0.50

        size = balance * kelly_f * kelly_fraction

        # Apply drawdown reduction
        if self._current_drawdown_pct > 5:
            dd_factor = max(0.3, 1 - self._current_drawdown_pct / 30)
            size *= dd_factor

        # Cap at max position
        size = min(size, self.max_position_usd)
        return round(max(0, size), 2)

    def _get_portfolio_limit(self, balance: float) -> float:
        """Max total exposure as % of balance."""
        if self.risk_mode == 'conservative':
            return balance * 0.60
        elif self.risk_mode == 'moderate':
            return balance * 0.80
        return balance * 1.0

    def _get_city_limit(self, balance: float) -> float:
        """Max exposure per city."""
        if self.risk_mode == 'conservative':
            return balance * 0.20
        elif self.risk_mode == 'moderate':
            return balance * 0.30
        return balance * 0.40

    def force_unpause(self) -> str:
        """Manual override to unpause trading."""
        if self._is_paused:
            reason = self._pause_reason
            self._is_paused = False
            self._pause_reason = ''
            return f"Unpaused (was: {reason})"
        return "Not paused"

    def get_status(self) -> Dict:
        """Get current risk status."""
        return {
            'is_paused': self._is_paused,
            'pause_reason': self._pause_reason,
            'daily_pnl': round(self._daily_pnl, 4),
            'daily_trades': self._daily_trades,
            'daily_wins': self._daily_wins,
            'total_exposure': round(self._total_exposure, 2),
            'city_exposure': dict(self._city_exposure),
            'drawdown_pct': round(self._current_drawdown_pct, 1),
            'peak_balance': round(self._peak_balance, 2),
            'risk_mode': self.risk_mode,
        }
