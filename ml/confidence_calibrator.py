"""
Confidence Calibrator — Calibrate Predicted Confidence vs Actual Hit Rate

Tracks "when we say X% confidence, how often do we actually win?"
to correct systematic overconfidence or underconfidence.

Example:
  Our bot says 80% confidence → but we only win 55% of those trades
  → We're overconfident! Calibrator maps 80% → ~58%
  
  Our bot says 60% confidence → we actually win 72% of those trades  
  → We're underconfident! Calibrator maps 60% → ~70%

This is CRITICAL for proper position sizing (Kelly criterion needs
calibrated probabilities, not raw model outputs).

Uses isotonic regression-style calibration binning.
"""

from typing import Dict, List, Optional, Tuple


class ConfidenceCalibrator:
    """
    Calibrates model confidence using historical trade outcomes.
    Maps raw confidence → calibrated probability based on actual hit rates.
    """

    # Confidence bins for calibration curve
    BINS = [
        (0.0, 0.30),
        (0.30, 0.40),
        (0.40, 0.50),
        (0.50, 0.60),
        (0.60, 0.70),
        (0.70, 0.80),
        (0.80, 0.90),
        (0.90, 1.01),
    ]

    def __init__(self, db=None):
        self.db = db
        # {bin_idx: (total_trades, wins)}
        self._bin_counts: Dict[int, Tuple[int, int]] = {}
        # Calibration curve: {bin_idx: calibrated_prob}
        self._calibration: Dict[int, float] = {}
        # Per-strategy calibration
        self._strategy_counts: Dict[str, Dict[int, Tuple[int, int]]] = {}
        self._min_samples = 5  # Minimum samples per bin before applying

    async def init(self, db):
        """Load historical trades and build calibration curve."""
        self.db = db
        if not db or not db.db:
            return
        # Load closed trades with confidence and P&L
        cursor = await db.db.execute(
            "SELECT confidence, pnl, strategy FROM trades "
            "WHERE status = 'closed' AND confidence IS NOT NULL "
            "ORDER BY exit_time DESC LIMIT 500"
        )
        rows = await cursor.fetchall()
        for confidence, pnl, strategy in rows:
            if confidence is None or pnl is None:
                continue
            won = pnl > 0
            self._record_outcome(confidence, won, strategy)
        self._rebuild_calibration()

    def _record_outcome(self, confidence: float, won: bool,
                        strategy: str = ''):
        """Record a trade outcome for calibration."""
        bin_idx = self._get_bin(confidence)
        if bin_idx is None:
            return

        total, wins = self._bin_counts.get(bin_idx, (0, 0))
        self._bin_counts[bin_idx] = (total + 1, wins + (1 if won else 0))

        if strategy:
            if strategy not in self._strategy_counts:
                self._strategy_counts[strategy] = {}
            st, sw = self._strategy_counts[strategy].get(bin_idx, (0, 0))
            self._strategy_counts[strategy][bin_idx] = (
                st + 1, sw + (1 if won else 0)
            )

    def _rebuild_calibration(self):
        """Rebuild calibration curve from bin counts."""
        self._calibration.clear()
        for bin_idx, (total, wins) in self._bin_counts.items():
            if total >= self._min_samples:
                self._calibration[bin_idx] = wins / total

    def record_trade(self, confidence: float, pnl: float,
                     strategy: str = ''):
        """Record a completed trade for future calibration."""
        won = pnl > 0
        self._record_outcome(confidence, won, strategy)
        self._rebuild_calibration()

    def calibrate(self, raw_confidence: float,
                  strategy: str = '') -> float:
        """
        Map raw confidence to calibrated probability.
        
        Returns:
            Calibrated confidence (0.0-1.0). Returns raw if insufficient data.
        """
        bin_idx = self._get_bin(raw_confidence)
        if bin_idx is None:
            return raw_confidence

        # Try strategy-specific calibration first
        if strategy and strategy in self._strategy_counts:
            st_data = self._strategy_counts[strategy].get(bin_idx)
            if st_data and st_data[0] >= self._min_samples:
                return st_data[1] / st_data[0]

        # Fall back to global calibration
        if bin_idx in self._calibration:
            cal = self._calibration[bin_idx]
            # Blend: 70% calibrated + 30% raw (avoid over-correction with limited data)
            total = self._bin_counts.get(bin_idx, (0, 0))[0]
            blend = min(0.9, total / 50)  # More data → trust calibration more
            return cal * blend + raw_confidence * (1 - blend)

        return raw_confidence

    def _get_bin(self, confidence: float) -> Optional[int]:
        """Find which calibration bin a confidence value falls into."""
        for i, (lo, hi) in enumerate(self.BINS):
            if lo <= confidence < hi:
                return i
        return None

    def get_calibration_curve(self) -> Dict[str, Dict]:
        """Get the full calibration curve for display."""
        curve = {}
        for i, (lo, hi) in enumerate(self.BINS):
            total, wins = self._bin_counts.get(i, (0, 0))
            bin_label = f"{lo:.0%}-{hi:.0%}"
            curve[bin_label] = {
                'predicted': (lo + hi) / 2,
                'actual': wins / total if total > 0 else None,
                'trades': total,
                'wins': wins,
            }
        return curve

    def get_overconfidence_score(self) -> float:
        """
        How overconfident are we? 
        Positive = overconfident, Negative = underconfident.
        """
        weighted_diff = 0.0
        total_weight = 0.0
        for i, (lo, hi) in enumerate(self.BINS):
            total, wins = self._bin_counts.get(i, (0, 0))
            if total < self._min_samples:
                continue
            predicted = (lo + hi) / 2
            actual = wins / total
            weighted_diff += total * (predicted - actual)
            total_weight += total
        if total_weight == 0:
            return 0.0
        return round(weighted_diff / total_weight, 3)
