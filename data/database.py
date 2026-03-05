"""
Database — SQLite Trade Storage for Weather Prediction Bot

Stores trade history, position tracking, and performance metrics.
"""

import aiosqlite
import json
from typing import Dict, List, Optional
from datetime import datetime

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
from weather.config import Config


class Database:
    """SQLite database for weather trade tracking."""

    def __init__(self):
        self.path = Config.DATABASE_PATH
        self.db = None

    async def init(self):
        """Initialize database and create tables."""
        # Ensure directory exists
        db_dir = os.path.dirname(self.path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        self.db = await aiosqlite.connect(self.path)
        await self.db.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id TEXT PRIMARY KEY,
                order_id TEXT,
                market_id TEXT,
                city TEXT,
                target_date TEXT,
                strategy TEXT,
                direction TEXT,
                outcome_label TEXT,
                temp_c INTEGER,
                token_id TEXT,
                entry_price REAL,
                exit_price REAL,
                size_usd REAL,
                shares REAL,
                pnl REAL,
                pnl_pct REAL,
                confidence REAL,
                entry_time TEXT,
                exit_time TEXT,
                exit_reason TEXT,
                status TEXT DEFAULT 'open',
                rationale TEXT,
                metadata TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        await self.db.execute('''
            CREATE TABLE IF NOT EXISTS forecasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city TEXT,
                target_date TEXT,
                forecast_temp REAL,
                actual_temp REAL,
                model TEXT,
                confidence REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        await self.db.commit()
        print(f"📊 Database initialized: {self.path}", flush=True)

    async def save_trade(self, trade: Dict):
        """Save a trade to the database."""
        if not self.db:
            return

        metadata_str = json.dumps(trade.get('metadata', {})) if trade.get('metadata') else '{}'

        await self.db.execute('''
            INSERT OR REPLACE INTO trades
            (id, order_id, market_id, city, target_date, strategy, direction,
             outcome_label, temp_c, token_id, entry_price, exit_price,
             size_usd, shares, pnl, pnl_pct, confidence, entry_time,
             exit_time, exit_reason, status, rationale, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            trade.get('id', ''),
            trade.get('order_id', ''),
            trade.get('market_id', ''),
            trade.get('city', ''),
            trade.get('target_date', ''),
            trade.get('strategy', ''),
            trade.get('direction', ''),
            trade.get('outcome_label', ''),
            trade.get('temp_c'),
            trade.get('token_id', ''),
            trade.get('entry_price'),
            trade.get('exit_price'),
            trade.get('size_usd'),
            trade.get('shares'),
            trade.get('pnl'),
            trade.get('pnl_pct'),
            trade.get('confidence'),
            trade.get('entry_time', ''),
            trade.get('exit_time', ''),
            trade.get('exit_reason', ''),
            trade.get('status', 'open'),
            trade.get('rationale', ''),
            metadata_str,
        ))
        await self.db.commit()

    async def save_forecast(self, city: str, target_date: str, forecast_temp: float,
                             actual_temp: float = None, model: str = '', confidence: float = 0):
        """Save a forecast record for accuracy tracking."""
        if not self.db:
            return
        await self.db.execute('''
            INSERT INTO forecasts (city, target_date, forecast_temp, actual_temp, model, confidence)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (city, target_date, forecast_temp, actual_temp, model, confidence))
        await self.db.commit()

    async def get_open_trades(self) -> List[Dict]:
        """Get all open trades."""
        if not self.db:
            return []
        cursor = await self.db.execute(
            "SELECT * FROM trades WHERE status IN ('open', 'pending') ORDER BY entry_time DESC"
        )
        rows = await cursor.fetchall()
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row)) for row in rows]

    async def get_trade_history(self, limit: int = 20) -> List[Dict]:
        """Get recent trade history."""
        if not self.db:
            return []
        cursor = await self.db.execute(
            "SELECT * FROM trades ORDER BY entry_time DESC LIMIT ?", (limit,)
        )
        rows = await cursor.fetchall()
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row)) for row in rows]

    async def get_performance_summary(self) -> Dict:
        """Get overall performance summary."""
        if not self.db:
            return {}

        cursor = await self.db.execute(
            "SELECT COUNT(*) as total, "
            "SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins, "
            "SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) as losses, "
            "SUM(pnl) as total_pnl, "
            "AVG(pnl) as avg_pnl "
            "FROM trades WHERE status = 'closed'"
        )
        row = await cursor.fetchone()
        if not row:
            return {}

        total, wins, losses, total_pnl, avg_pnl = row
        return {
            'total_trades': total or 0,
            'wins': wins or 0,
            'losses': losses or 0,
            'win_rate': (wins / total * 100) if total and total > 0 else 0,
            'total_pnl': total_pnl or 0,
            'avg_pnl': avg_pnl or 0,
        }

    async def close(self):
        """Close the database connection."""
        if self.db:
            await self.db.close()
