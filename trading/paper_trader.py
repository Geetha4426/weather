"""
Paper Trader — Simulated Trading for Weather Prediction Bot

Executes trades in paper mode with virtual balance.
No real orders placed on Polymarket.
"""

import uuid
import time
from typing import Dict, List, Optional
from datetime import datetime

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
from weather.strategies.base_strategy import TradeSignal
from weather.config import Config


class PaperTrader:
    """Simulated paper trading for weather markets."""

    def __init__(self, db):
        self.db = db
        self.balance = Config.STARTING_BALANCE
        self.positions: Dict[str, Dict] = {}  # trade_id -> trade
        self.trade_history: List[Dict] = []
        self.total_trades = 0
        self.wins = 0
        self.losses = 0
        self.total_pnl = 0.0

    @property
    def is_ready(self):
        return True

    def can_trade(self) -> tuple:
        """Check if trading is allowed."""
        if self.balance < Config.POLYMARKET_MIN_ORDER_SIZE:
            return False, "Insufficient balance"
        if len(self.positions) >= Config.WEATHER_MAX_TOTAL_POSITIONS:
            return False, "Max positions reached"
        return True, "OK"

    def get_position_size(self, confidence: float) -> float:
        """Calculate position size. Capped at WEATHER_MAX_POSITION_USD ($2 default)."""
        base_pct = Config.WEATHER_POSITION_SIZE_PCT / 100
        scale = 0.6 + (confidence - 0.4) * (0.4 / 0.5)
        scale = max(0.5, min(1.0, scale))
        size = self.balance * base_pct * scale
        # Cap at max position size ($2 default — conservative mode)
        size = min(size, Config.WEATHER_MAX_POSITION_USD)
        return max(Config.POLYMARKET_MIN_ORDER_SIZE, round(size, 2))

    async def execute_signal(self, signal: TradeSignal) -> Optional[Dict]:
        """Execute a paper trade."""
        can, reason = self.can_trade()
        if not can:
            return None

        # Reject junk outcomes
        if signal.entry_price < Config.WEATHER_MIN_MARKET_PRICE:
            return None

        size = self.get_position_size(signal.confidence)
        if size > self.balance:
            size = max(Config.POLYMARKET_MIN_ORDER_SIZE, self.balance * 0.5)

        if size < Config.POLYMARKET_MIN_ORDER_SIZE:
            return None

        price = max(0.01, min(0.99, round(signal.entry_price * 100) / 100))
        shares = round(size / price, 2)

        trade_id = str(uuid.uuid4())[:8]
        now = datetime.now().isoformat()

        trade = {
            'id': trade_id,
            'order_id': f'paper_{trade_id}',
            'market_id': signal.market_id,
            'city': signal.city,
            'target_date': signal.target_date,
            'strategy': signal.strategy,
            'direction': signal.direction,
            'outcome_label': signal.outcome_label,
            'temp_c': signal.temp_c,
            'token_id': signal.token_id,
            'entry_price': price,
            'exit_price': None,
            'size_usd': round(price * shares, 2),
            'shares': shares,
            'pnl': None,
            'pnl_pct': None,
            'confidence': signal.confidence,
            'entry_time': now,
            'exit_time': None,
            'exit_reason': None,
            'status': 'open',
            'rationale': signal.rationale,
            'metadata': signal.metadata,
        }

        self.positions[trade_id] = trade
        self.balance -= trade['size_usd']

        await self.db.save_trade(trade)

        print(f"📋 [PAPER] BUY {signal.direction} | {signal.city} "
              f"{signal.outcome_label} @ ${price:.3f} "
              f"(${trade['size_usd']:.2f}, {shares:.1f} shares)", flush=True)

        return trade

    async def check_positions(self, current_prices: Dict[str, float]) -> List[Dict]:
        """Check open positions for exit signals."""
        closed = []

        for trade_id, pos in list(self.positions.items()):
            token_id = pos['token_id']
            current_price = current_prices.get(token_id)
            if current_price is None:
                continue

            entry_price = pos['entry_price']
            shares = pos.get('shares', 0)

            # Calculate P&L
            pnl = (current_price - entry_price) * shares
            pnl_pct = (current_price / entry_price - 1) * 100 if entry_price > 0 else 0

            # Exit conditions
            should_exit = False
            reason = ''

            exit_pct = Config.WEATHER_EXIT_EDGE * 100  # 45% default
            if pnl_pct >= exit_pct:  # Take profit at 45%
                should_exit = True
                reason = 'take_profit'
            elif entry_price < 0.10 and pnl_pct <= -50:  # Cheap position: -50% stop
                should_exit = True
                reason = 'stop_loss'
            elif entry_price >= 0.10 and pnl_pct <= -30:  # Standard position: -30% stop
                should_exit = True
                reason = 'stop_loss'
            elif current_price >= 0.95:  # Near-certainty → take profit
                should_exit = True
                reason = 'near_certainty'
            elif current_price <= 0.01 and entry_price > 0.03:  # Collapsed to nothing
                should_exit = True
                reason = 'near_zero'

            if should_exit:
                pos['exit_price'] = current_price
                pos['pnl'] = round(pnl, 4)
                pos['pnl_pct'] = round(pnl_pct, 2)
                pos['exit_time'] = datetime.now().isoformat()
                pos['exit_reason'] = reason
                pos['status'] = 'closed'

                self.balance += current_price * shares
                self.total_pnl += pnl
                self.total_trades += 1
                if pnl > 0:
                    self.wins += 1
                else:
                    self.losses += 1

                del self.positions[trade_id]
                self.trade_history.append(pos)
                closed.append(pos)

                await self.db.save_trade(pos)

                emoji = '✅' if pnl >= 0 else '❌'
                print(f"📋 [PAPER] {emoji} CLOSE {pos['city']} "
                      f"{pos['outcome_label']} | P&L: ${pnl:+.4f} "
                      f"({pnl_pct:+.1f}%) [{reason}]", flush=True)

        return closed

    def get_summary(self) -> Dict:
        """Get trading summary."""
        win_rate = (self.wins / self.total_trades * 100) if self.total_trades > 0 else 0
        return {
            'balance': self.balance,
            'total_trades': self.total_trades,
            'wins': self.wins,
            'losses': self.losses,
            'win_rate': win_rate,
            'total_pnl': self.total_pnl,
            'open_positions': len(self.positions),
        }

    def get_open_positions(self) -> List[Dict]:
        return list(self.positions.values())
