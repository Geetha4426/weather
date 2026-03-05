"""
Live Trader — Real CLOB Order Execution for Weather Markets

Uses py-clob-client to place real orders on Polymarket.
FOK (Fill-or-Kill) for instant fills at $1 minimum.
GTC for positioned entries at specific prices.

Adapted from the crypto 5min_trade live_trader for weather multi-outcome markets.
"""

import math
import time
import uuid
import requests
from typing import Dict, List, Optional
from datetime import datetime

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
from weather.strategies.base_strategy import TradeSignal
from weather.config import Config


class LiveTrader:
    """Real order execution on Polymarket CLOB for weather markets."""

    BASE_TAKER_FEE_RATE = 0.02  # ~2% for weather markets

    def __init__(self, db):
        self.db = db
        self.clob_client = None
        self.is_ready = False
        self.balance = 0.0
        self.positions: Dict[str, Dict] = {}
        self.pending_orders: Dict[str, Dict] = {}
        self.total_trades = 0
        self.wins = 0
        self.losses = 0
        self.total_pnl = 0.0
        self._sig_type = 0
        self._cached_real_balance = None
        self._last_balance_check = 0
        self._consecutive_failures = 0
        self._trading_paused = False
        self.ORDER_TIMEOUT = 120  # seconds

    async def init(self) -> bool:
        """Initialize CLOB client with credentials."""
        pk = Config.POLY_PRIVATE_KEY.strip() if Config.POLY_PRIVATE_KEY else ''
        if not pk:
            print("⚠️ No POLY_PRIVATE_KEY set — live trading disabled", flush=True)
            return False

        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds

            if not pk.startswith('0x'):
                pk = '0x' + pk

            self._sig_type = Config.POLY_SIGNATURE_TYPE
            chain_id = Config.POLY_CHAIN_ID

            # Initialize client
            host = Config.CLOB_API_URL
            self.clob_client = ClobClient(
                host,
                key=pk,
                chain_id=chain_id,
                signature_type=self._sig_type,
                funder=Config.get_funder_address() or None,
            )

            # Derive or use API credentials
            api_key = Config.POLY_API_KEY.strip() if Config.POLY_API_KEY else ''
            if api_key:
                self.clob_client.set_api_creds(ApiCreds(
                    api_key=api_key,
                    api_secret=Config.POLY_API_SECRET,
                    api_passphrase=Config.POLY_PASSPHRASE,
                ))
                print("🔑 Using manually set API credentials", flush=True)
            else:
                try:
                    creds = self.clob_client.create_or_derive_api_creds()
                    self.clob_client.set_api_creds(creds)
                    print("🔑 API credentials auto-derived from private key", flush=True)
                except Exception as e:
                    try:
                        creds = self.clob_client.derive_api_key()
                        self.clob_client.set_api_creds(creds)
                        print("🔑 API key derived (fallback)", flush=True)
                    except Exception as e2:
                        print(f"❌ Could not derive API credentials: {e2}", flush=True)
                        return False

            self.is_ready = True
            print("🟢 Live trader initialized for weather markets", flush=True)
            return True

        except ImportError:
            print("❌ py-clob-client not installed", flush=True)
            return False
        except Exception as e:
            print(f"❌ Live trader init failed: {e}", flush=True)
            return False

    async def fetch_balance(self) -> Optional[float]:
        """Fetch real USDC balance from Polymarket."""
        if not self.is_ready:
            return None

        # Try on-chain balance
        try:
            from eth_account import Account
            pk = Config.POLY_PRIVATE_KEY.strip()
            if not pk.startswith('0x'):
                pk = '0x' + pk
            wallet = Account.from_key(pk)
            eoa_addr = wallet.address

            proxy_wallet = Config.POLY_PROXY_WALLET.strip() if Config.POLY_PROXY_WALLET else ''

            usdc_contracts = [
                ('0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174', 'USDC'),
                ('0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359', 'USDC.e'),
            ]

            rpc_endpoints = [
                'https://polygon-rpc.com',
                'https://rpc.ankr.com/polygon',
                'https://polygon.llamarpc.com',
            ]

            addresses_to_check = [(eoa_addr, 'EOA')]
            if proxy_wallet:
                addresses_to_check.append((proxy_wallet, 'Proxy'))

            no_proxy = {"http": "", "https": ""}
            total_balance = 0.0

            for addr, addr_label in addresses_to_check:
                padded_addr = addr[2:].lower().zfill(64)
                for contract, token_label in usdc_contracts:
                    for rpc_url in rpc_endpoints:
                        try:
                            call_data = f"0x70a08231{padded_addr}"
                            resp = requests.post(
                                rpc_url,
                                headers={"Content-Type": "application/json"},
                                json={
                                    "jsonrpc": "2.0",
                                    "method": "eth_call",
                                    "params": [{"to": contract, "data": call_data}, "latest"],
                                    "id": 1,
                                },
                                timeout=10,
                                proxies=no_proxy,
                            )
                            if resp.status_code == 200:
                                rpc_data = resp.json()
                                if "error" in rpc_data:
                                    continue
                                result = rpc_data.get("result", "0x0")
                                balance_wei = int(result, 16)
                                balance = balance_wei / 1e6
                                if balance > 0:
                                    total_balance += balance
                                break
                        except Exception:
                            continue

            if total_balance > 0:
                self.balance = round(total_balance, 2)
                print(f"💰 Balance: ${self.balance:.2f}", flush=True)
                return self.balance

        except Exception as e:
            print(f"⚠️ Balance fetch error: {e}", flush=True)

        # Fallback
        if Config.STARTING_BALANCE > 0:
            self.balance = Config.STARTING_BALANCE
            return self.balance

        return None

    async def execute_signal(self, signal: TradeSignal) -> Optional[Dict]:
        """Execute a real trade on Polymarket."""
        if not self.is_ready:
            return None

        if self._trading_paused:
            return None

        if self.balance < Config.POLYMARKET_MIN_ORDER_SIZE:
            return None

        if len(self.positions) >= Config.WEATHER_MAX_TOTAL_POSITIONS:
            return None

        size = self._get_position_size(signal.confidence)
        if size < Config.POLYMARKET_MIN_ORDER_SIZE:
            return None

        use_fok = Config.USE_FOK_ORDERS and signal.confidence >= 0.75

        try:
            from py_clob_client.clob_types import OrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY

            price = max(0.01, min(0.99, round(signal.entry_price * 100) / 100))

            if use_fok:
                shares = round(size / price, 2)
                if shares < 1:
                    shares = 1
                order_type = OrderType.FOK
                tag = 'FOK'
            else:
                shares = max(5, round(size / price, 2))
                order_type = OrderType.GTC
                tag = 'GTC'

            actual_cost = round(price * shares, 6)

            if actual_cost > self.balance:
                return None

            trade_id = str(uuid.uuid4())[:8]
            now = datetime.now().isoformat()

            print(f">> [{tag}] {signal.city} {signal.direction} "
                  f"{signal.outcome_label} | ${actual_cost:.2f} @ ${price:.3f} "
                  f"({shares:.1f} shares)", flush=True)

            # Place order
            order_args = OrderArgs(
                price=price,
                size=shares,
                side=BUY,
                token_id=signal.token_id,
            )
            signed_order = self.clob_client.create_order(order_args)
            resp = self.clob_client.post_order(signed_order, order_type)

            if not resp or resp.get('status') == 'error':
                error_msg = resp.get('errorMsg', 'Unknown') if resp else 'No response'
                print(f"❌ Order rejected: {error_msg}", flush=True)

                # FOK → GTC fallback
                if use_fok and error_msg and 'not fill' in str(error_msg).lower():
                    print(f"🔄 FOK didn't fill — trying GTC", flush=True)
                    return await self._place_gtc(signal, size)
                return None

            order_id = resp.get('orderID', resp.get('id', trade_id))
            print(f"✅ [{tag}] ORDER {'FILLED' if use_fok else 'PLACED'}: {order_id}", flush=True)

            trade = {
                'id': trade_id,
                'order_id': order_id,
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
                'size_usd': actual_cost,
                'shares': shares,
                'pnl': None,
                'pnl_pct': None,
                'confidence': signal.confidence,
                'entry_time': now,
                'exit_time': None,
                'exit_reason': None,
                'status': 'open' if use_fok else 'pending',
                'rationale': signal.rationale,
                'metadata': signal.metadata,
                'placed_at': time.time(),
                '_live': True,
            }

            if use_fok:
                self.positions[trade_id] = trade
            else:
                self.pending_orders[trade_id] = trade

            self.balance -= actual_cost
            await self.db.save_trade(trade)
            self._consecutive_failures = 0
            return trade

        except Exception as e:
            error_str = str(e).lower()
            print(f"❌ Order error: {e}", flush=True)

            if 'balance' in error_str or 'allowance' in error_str:
                self._consecutive_failures += 1
                if self._consecutive_failures >= 5:
                    self._trading_paused = True
                    print(f"🛑 TRADING PAUSED: {self._consecutive_failures} consecutive failures", flush=True)

            return None

    async def _place_gtc(self, signal: TradeSignal, size: float) -> Optional[Dict]:
        """Fallback GTC order placement."""
        try:
            from py_clob_client.clob_types import OrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY

            price = max(0.01, min(0.99, round(signal.entry_price * 100) / 100))
            shares = max(5, round(size / price, 2))
            actual_cost = round(price * shares, 6)

            if actual_cost > self.balance:
                return None

            order_args = OrderArgs(price=price, size=shares, side=BUY, token_id=signal.token_id)
            signed_order = self.clob_client.create_order(order_args)
            resp = self.clob_client.post_order(signed_order, OrderType.GTC)

            if resp and resp.get('status') != 'error':
                order_id = resp.get('orderID', resp.get('id', ''))
                trade_id = str(uuid.uuid4())[:8]
                trade = {
                    'id': trade_id, 'order_id': order_id,
                    'market_id': signal.market_id, 'city': signal.city,
                    'target_date': signal.target_date, 'strategy': signal.strategy,
                    'direction': signal.direction, 'outcome_label': signal.outcome_label,
                    'temp_c': signal.temp_c, 'token_id': signal.token_id,
                    'entry_price': price, 'exit_price': None,
                    'size_usd': actual_cost, 'shares': shares,
                    'pnl': None, 'pnl_pct': None,
                    'confidence': signal.confidence,
                    'entry_time': datetime.now().isoformat(),
                    'exit_time': None, 'exit_reason': None,
                    'status': 'pending', 'rationale': signal.rationale,
                    'metadata': signal.metadata, 'placed_at': time.time(),
                    '_live': True,
                }
                self.pending_orders[trade_id] = trade
                self.balance -= actual_cost
                await self.db.save_trade(trade)
                return trade
        except Exception as e:
            print(f"❌ GTC fallback failed: {e}", flush=True)
        return None

    async def check_positions(self, current_prices: Dict[str, float]) -> List[Dict]:
        """Check open positions for exit signals."""
        closed = []

        # Check pending orders first
        await self._check_pending()

        for trade_id, pos in list(self.positions.items()):
            token_id = pos['token_id']
            current_price = current_prices.get(token_id)
            if current_price is None:
                continue

            entry = pos['entry_price']
            shares = pos.get('shares', 0)
            pnl = (current_price - entry) * shares
            pnl_pct = (current_price / entry - 1) * 100 if entry > 0 else 0

            should_exit = False
            reason = ''

            exit_pct = Config.WEATHER_EXIT_EDGE * 100  # Use config threshold
            if pnl_pct >= exit_pct:
                should_exit, reason = True, 'take_profit'
            elif pnl_pct <= -25:
                should_exit, reason = True, 'stop_loss'
            elif current_price >= 0.95:
                should_exit, reason = True, 'near_certainty'
            elif current_price <= 0.02:
                should_exit, reason = True, 'near_zero'

            if should_exit:
                result = await self._close_position(trade_id, current_price, pnl, reason)
                if result:
                    closed.append(pos)

        return closed

    async def _check_pending(self):
        """Check pending GTC orders for fills."""
        if not self.is_ready:
            return

        now = time.time()
        to_remove = []

        for trade_id, order in list(self.pending_orders.items()):
            order_id = order.get('order_id', '')
            placed_at = order.get('placed_at', now)

            try:
                clob_order = self.clob_client.get_order(order_id)
                if clob_order:
                    status = clob_order.get('status', '').lower()
                    if status in ('matched', 'filled'):
                        order['status'] = 'open'
                        self.positions[trade_id] = order
                        to_remove.append(trade_id)
                    elif status == 'cancelled':
                        to_remove.append(trade_id)
                        self.balance += order['size_usd']
            except Exception:
                pass

            if now - placed_at > self.ORDER_TIMEOUT:
                try:
                    self.clob_client.cancel(order_id)
                except Exception:
                    pass
                to_remove.append(trade_id)
                self.balance += order['size_usd']

        for tid in to_remove:
            self.pending_orders.pop(tid, None)

    async def _close_position(self, trade_id: str, exit_price: float,
                                pnl: float, reason: str) -> bool:
        """Close a position by selling."""
        pos = self.positions.get(trade_id)
        if not pos:
            return False

        shares = pos.get('shares', 0)
        if shares <= 0:
            return False

        try:
            from py_clob_client.clob_types import OrderArgs, OrderType
            from py_clob_client.order_builder.constants import SELL

            sell_price = max(0.01, min(0.99, round(exit_price * 100) / 100))

            # Set conditional allowance for proxy wallets
            if self._sig_type == 2:
                try:
                    from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
                    params = BalanceAllowanceParams(
                        asset_type=AssetType.CONDITIONAL,
                        token_id=pos['token_id'],
                        signature_type=self._sig_type,
                    )
                    self.clob_client.update_balance_allowance(params)
                except Exception:
                    pass

            sell_shares = math.ceil(shares)
            order_args = OrderArgs(
                price=sell_price, size=sell_shares,
                side=SELL, token_id=pos['token_id'],
            )
            signed_order = self.clob_client.create_order(order_args)
            resp = self.clob_client.post_order(signed_order, OrderType.FOK)

            sell_ok = resp and resp.get('status') != 'error'
            if not sell_ok:
                # GTC fallback
                try:
                    resp = self.clob_client.post_order(signed_order, OrderType.GTC)
                    sell_ok = resp and resp.get('status') != 'error'
                except Exception:
                    pass

        except Exception as e:
            print(f"\u26a0\ufe0f Sell error: {e}", flush=True)
            sell_ok = False

        if not sell_ok:
            print(f"\u26a0\ufe0f Could not sell {pos.get('outcome_label','')} — keeping position", flush=True)
            return False

        # Finalize only on successful sell
        pnl_pct = (exit_price / pos['entry_price'] - 1) * 100 if pos['entry_price'] > 0 else 0
        pos['exit_price'] = exit_price
        pos['pnl'] = round(pnl, 4)
        pos['pnl_pct'] = round(pnl_pct, 2)
        pos['exit_time'] = datetime.now().isoformat()
        pos['exit_reason'] = reason
        pos['status'] = 'closed'

        self.balance += exit_price * shares
        self.total_pnl += pnl
        self.total_trades += 1
        if pnl > 0:
            self.wins += 1
        else:
            self.losses += 1

        del self.positions[trade_id]
        await self.db.save_trade(pos)

        emoji = '✅' if pnl >= 0 else '❌'
        print(f"🔴 [LIVE] {emoji} CLOSE {pos['city']} {pos['outcome_label']} | "
              f"P&L: ${pnl:+.4f} ({pnl_pct:+.1f}%) [{reason}]", flush=True)
        return True

    def _get_position_size(self, confidence: float) -> float:
        """Calculate position size."""
        base_pct = Config.WEATHER_POSITION_SIZE_PCT / 100
        scale = 0.6 + (confidence - 0.4) * (0.4 / 0.5)
        scale = max(0.5, min(1.0, scale))
        size = self.balance * base_pct * scale
        size = min(size, Config.WEATHER_MAX_POSITION_USD)
        return max(Config.POLYMARKET_MIN_ORDER_SIZE, round(size, 2))

    def get_summary(self) -> Dict:
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
