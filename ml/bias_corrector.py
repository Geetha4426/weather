"""
Bias Corrector — Learn Systematic Forecast Errors per City

Tracks forecast vs actual temperatures over time and learns per-city,
per-season bias patterns. Uses exponential weighted moving average
for fast adaptation to changing forecast quality.

Example:
  - Open-Meteo consistently over-predicts NYC temps by 1.2°F in March
  - bias_corrector.correct(city='nyc', raw_forecast=52.0) → 50.8°F
  - This small correction can flip a marginal trade from loss to profit

Storage: SQLite table `bias_history` for persistence across restarts.
"""

import time
import math
from typing import Dict, Optional, Tuple
from datetime import date, timedelta


class BiasCorrector:
    """Persistent per-city forecast bias correction using EWMA."""

    def __init__(self, db=None):
        self.db = db
        # In-memory bias data: {city: [(forecast, actual, timestamp), ...]}
        self._history: Dict[str, list] = {}
        # Computed biases: {city: bias_value}
        self._biases: Dict[str, float] = {}
        # EWMA decay factor (0.9 = recent data weighs more)
        self._alpha = 0.15
        # Minimum observations before applying correction
        self._min_observations = 3
        # Per-city, per-month bias for seasonal patterns
        self._seasonal_bias: Dict[str, float] = {}

    async def init(self, db):
        """Initialize with database and create table if needed."""
        self.db = db
        if not db or not db.db:
            return
        await db.db.execute('''
            CREATE TABLE IF NOT EXISTS bias_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city TEXT,
                target_date TEXT,
                forecast_temp REAL,
                actual_temp REAL,
                model TEXT DEFAULT 'ensemble',
                month INTEGER,
                error REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await db.db.commit()
        await self._load_from_db()

    async def _load_from_db(self):
        """Load historical bias data from database."""
        if not self.db or not self.db.db:
            return
        cursor = await self.db.db.execute(
            "SELECT city, forecast_temp, actual_temp, month "
            "FROM bias_history ORDER BY created_at DESC LIMIT 500"
        )
        rows = await cursor.fetchall()
        for city, forecast, actual, month in rows:
            if city not in self._history:
                self._history[city] = []
            self._history[city].append((forecast, actual, month))
        self._recompute_all()

    def _recompute_all(self):
        """Recompute all biases from loaded history."""
        for city, history in self._history.items():
            if len(history) < self._min_observations:
                continue
            # EWMA bias: more recent observations weighted higher
            ewma_bias = 0.0
            weight_sum = 0.0
            for i, (fc, actual, _month) in enumerate(history):
                weight = (1 - self._alpha) ** i  # newest first
                error = fc - actual
                ewma_bias += weight * error
                weight_sum += weight
            if weight_sum > 0:
                self._biases[city] = ewma_bias / weight_sum

        # Seasonal bias: group by city+month
        seasonal_data: Dict[str, list] = {}
        for city, history in self._history.items():
            for fc, actual, month in history:
                key = f"{city}_{month}"
                if key not in seasonal_data:
                    seasonal_data[key] = []
                seasonal_data[key].append(fc - actual)
        for key, errors in seasonal_data.items():
            if len(errors) >= 2:
                self._seasonal_bias[key] = sum(errors) / len(errors)

    async def record(self, city: str, target_date: str,
                     forecast_temp: float, actual_temp: float,
                     model: str = 'ensemble'):
        """Record a forecast vs actual observation."""
        error = forecast_temp - actual_temp
        try:
            month = int(target_date.split('-')[1])
        except (IndexError, ValueError):
            month = date.today().month

        if city not in self._history:
            self._history[city] = []
        # Prepend (newest first for EWMA)
        self._history[city].insert(0, (forecast_temp, actual_temp, month))
        # Keep last 100 per city
        self._history[city] = self._history[city][:100]

        # Update EWMA bias incrementally
        old_bias = self._biases.get(city, 0.0)
        self._biases[city] = self._alpha * error + (1 - self._alpha) * old_bias

        # Update seasonal bias
        key = f"{city}_{month}"
        old_seasonal = self._seasonal_bias.get(key, 0.0)
        self._seasonal_bias[key] = self._alpha * error + (1 - self._alpha) * old_seasonal

        # Persist to DB
        if self.db and self.db.db:
            await self.db.db.execute(
                "INSERT INTO bias_history "
                "(city, target_date, forecast_temp, actual_temp, model, month, error) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (city, target_date, forecast_temp, actual_temp, model, month, error)
            )
            await self.db.db.commit()

    def correct(self, city: str, raw_forecast: float,
                month: int = None) -> float:
        """Apply bias correction to a raw forecast temperature."""
        # Try seasonal bias first (more specific)
        if month is None:
            month = date.today().month
        seasonal_key = f"{city}_{month}"
        if seasonal_key in self._seasonal_bias:
            bias = self._seasonal_bias[seasonal_key]
            n = len([h for h in self._history.get(city, []) if h[2] == month])
            if n >= self._min_observations:
                return round(raw_forecast - bias, 1)

        # Fall back to overall city bias
        bias = self._biases.get(city, 0.0)
        if abs(bias) < 0.1:
            return raw_forecast
        return round(raw_forecast - bias, 1)

    def get_bias(self, city: str, month: int = None) -> float:
        """Get current bias estimate for a city."""
        if month is not None:
            key = f"{city}_{month}"
            if key in self._seasonal_bias:
                return round(self._seasonal_bias[key], 2)
        return round(self._biases.get(city, 0.0), 2)

    def get_mae(self, city: str) -> float:
        """Get Mean Absolute Error for a city."""
        history = self._history.get(city, [])
        if not history:
            return 0.0
        errors = [abs(fc - actual) for fc, actual, _ in history]
        return round(sum(errors) / len(errors), 2)

    def get_stats(self, city: str) -> Dict:
        """Get comprehensive bias statistics for a city."""
        history = self._history.get(city, [])
        if not history:
            return {'observations': 0}
        errors = [fc - actual for fc, actual, _ in history]
        abs_errors = [abs(e) for e in errors]
        return {
            'city': city,
            'observations': len(history),
            'bias': self.get_bias(city),
            'mae': round(sum(abs_errors) / len(abs_errors), 2),
            'max_error': round(max(abs_errors), 1),
            'rmse': round(math.sqrt(sum(e**2 for e in errors) / len(errors)), 2),
        }
