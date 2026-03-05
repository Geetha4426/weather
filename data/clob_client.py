"""
CLOB API Client — Orderbook & Order Execution for Weather Markets

Fetches orderbooks, prices, and supports order placement on Polymarket's CLOB.
Adapted for weather multi-outcome markets (YES/NO per temperature bracket).
"""

import requests
from typing import Dict, List, Optional

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
from weather.config import Config


class ClobClient:
    """Client for Polymarket's Central Limit Order Book API."""

    def __init__(self):
        self.base_url = Config.CLOB_API_URL
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'weather-trade-bot/1.0',
            'Accept': 'application/json',
        })
        self.fallback_prices: Dict[str, float] = {}

    def set_fallback_price(self, token_id: str, price: float):
        """Set a fallback price from market data."""
        self.fallback_prices[token_id] = price

    def get_price(self, token_id: str) -> Optional[float]:
        """Get current mid-price for a token."""
        try:
            url = f"{self.base_url}/price?token_id={token_id}"
            resp = self.session.get(url, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                return float(data.get('price', 0))
        except Exception:
            pass
        return self.fallback_prices.get(token_id)

    def get_prices(self, token_ids: List[str]) -> Dict[str, float]:
        """Get prices for multiple tokens."""
        prices = {}
        for tid in token_ids:
            price = self.get_price(tid)
            if price is not None:
                prices[tid] = price
        return prices

    def get_orderbook(self, token_id: str) -> Optional[Dict]:
        """
        Fetch full orderbook for a token.
        Falls back to synthetic orderbook from stored prices.
        """
        try:
            url = f"{self.base_url}/book?token_id={token_id}"
            resp = self.session.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()

                bids = sorted(
                    [(float(b['price']), float(b['size'])) for b in data.get('bids', [])],
                    key=lambda x: x[0], reverse=True
                )
                asks = sorted(
                    [(float(a['price']), float(a['size'])) for a in data.get('asks', [])],
                    key=lambda x: x[0]
                )

                if bids or asks:
                    best_bid = bids[0][0] if bids else 0.0
                    best_ask = asks[0][0] if asks else 1.0
                    spread = best_ask - best_bid
                    mid = (best_bid + best_ask) / 2 if (best_bid + best_ask) > 0 else 0.5

                    bid_depth = sum(p * s for p, s in bids[:10])
                    ask_depth = sum(p * s for p, s in asks[:10])
                    total_depth = bid_depth + ask_depth
                    imbalance = (bid_depth - ask_depth) / total_depth if total_depth > 0 else 0

                    return {
                        'token_id': token_id,
                        'bids': bids,
                        'asks': asks,
                        'best_bid': best_bid,
                        'best_ask': best_ask,
                        'spread': spread,
                        'spread_pct': (spread / best_ask * 100) if best_ask > 0 else 0,
                        'mid_price': mid,
                        'bid_depth': bid_depth,
                        'ask_depth': ask_depth,
                        'imbalance': imbalance,
                    }

        except Exception:
            pass

        # Fallback: synthetic orderbook
        price = self.fallback_prices.get(token_id)
        if price and price > 0:
            spread = 0.02
            best_bid = max(0.01, price - spread / 2)
            best_ask = min(0.99, price + spread / 2)

            return {
                'token_id': token_id,
                'bids': [(best_bid, 100.0)],
                'asks': [(best_ask, 100.0)],
                'best_bid': best_bid,
                'best_ask': best_ask,
                'spread': spread,
                'spread_pct': (spread / best_ask * 100) if best_ask > 0 else 0,
                'mid_price': price,
                'bid_depth': best_bid * 100,
                'ask_depth': best_ask * 100,
                'imbalance': 0.0,
                '_synthetic': True,
            }

        return None

    def calculate_slippage(self, orderbook: Dict, amount_usd: float, side: str) -> float:
        """Calculate expected slippage for a given order size."""
        levels = orderbook['asks'] if side == 'buy' else orderbook['bids']
        if not levels:
            return float('inf')

        remaining = amount_usd
        weighted_price = 0.0
        total_filled = 0.0

        for price, size in levels:
            if remaining <= 0:
                break
            level_value = price * size
            fill = min(remaining, level_value)
            weighted_price += price * fill
            total_filled += fill
            remaining -= fill

        if total_filled == 0:
            return float('inf')

        avg_price = weighted_price / total_filled
        ref_price = levels[0][0]
        return abs(avg_price - ref_price) / ref_price * 100
