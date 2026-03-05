"""
Value Hunter Strategy — Multi-Outcome Inefficiency Exploiter

1. SUM-CHECK: If all YES prices sum to < $0.93 → guaranteed profit
2. OVERPRICED TAIL: Buy NO on outcomes market overvalues vs forecast
3. CONCENTRATION: When leader is confirmed by forecast, back it

Handles Fahrenheit ranges (42-43°F) and Celsius (14°C).
"""

from typing import Dict, List

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))
from weather.strategies.base_strategy import BaseStrategy, TradeSignal
from weather.config import Config


class ValueHunterStrategy(BaseStrategy):
    """Exploits multi-outcome pricing inefficiencies."""

    name = "value_hunter"
    description = "Sum-check arb + overpriced tail detection"

    async def analyze(self, weather_market: Dict, context: Dict) -> List[TradeSignal]:
        forecast = context.get('forecast')
        clob = context.get('clob')

        if not forecast or not clob:
            return []

        outcomes = weather_market.get('outcomes', [])
        if len(outcomes) < 3:
            return []

        city = weather_market.get('city', '')
        target_date = weather_market.get('date', '')
        prob_dist = forecast.get('probability_distribution', {})

        signals = []

        # ═══ Strategy 1: Sum-Check Arb ═══
        signals.extend(self._check_sum_arb(weather_market, city, target_date, clob))

        # ═══ Strategy 2: Overpriced Tails (buy NO) ═══
        signals.extend(self._check_overpriced_tails(
            weather_market, city, target_date, prob_dist, clob))

        # ═══ Strategy 3: Concentration Play ═══
        signals.extend(self._check_concentration(
            weather_market, city, target_date, prob_dist, forecast, clob))

        return signals

    def _check_sum_arb(self, market, city, target_date, clob):
        """If all YES prices sum < $0.93, buy all for guaranteed profit."""
        outcomes = market.get('outcomes', [])
        total_cost = 0.0
        valid = []

        for o in outcomes:
            yt = o.get('token_id_yes', '')
            if not yt:
                continue
            book = clob.get_orderbook(yt)
            price = book['best_ask'] if book and not book.get('_synthetic') else o.get('price_yes', 0.5)
            if 0 < price < 1:
                total_cost += price
                valid.append((o, price, yt))

        if not valid or total_cost >= 0.93:
            return []

        profit = 1.0 - total_cost
        profit_pct = profit / total_cost * 100

        signals = []
        for o, price, yt in valid:
            signals.append(TradeSignal(
                strategy=self.name, city=city, target_date=target_date,
                direction='YES',
                outcome_label=o.get('label', ''),
                temp_c=o.get('temp_low', 0),
                token_id=yt,
                market_id=o.get('market_id', ''),
                entry_price=price,
                confidence=0.95,
                rationale=(
                    f"🎯 SUM-CHECK: All outcomes=${total_cost:.3f} < $1\n"
                    f"  Guaranteed profit: ${profit:.3f} ({profit_pct:.1f}%)"
                ),
                metadata={'type': 'sum_check_arb', 'total_cost': total_cost,
                          'guaranteed_profit': profit},
            ))
        return signals

    def _check_overpriced_tails(self, market, city, target_date, prob_dist, clob):
        """Buy NO on outcomes overpriced vs forecast."""
        signals = []
        for o in market.get('outcomes', []):
            temp_low = o.get('temp_low')
            if temp_low is None:
                continue

            forecast_prob = self._get_prob(o, prob_dist)
            market_price = o.get('price_yes', 0.5)
            overprice = market_price - forecast_prob

            if overprice < 0.15:  # 15% overprice threshold
                continue

            no_token = o.get('token_id_no', '')
            if not no_token:
                continue

            no_price = o.get('price_no', 1.0 - market_price)
            book = clob.get_orderbook(no_token)
            entry = book['best_ask'] if book and not book.get('_synthetic') else no_price

            if entry >= 0.95:
                continue

            signals.append(TradeSignal(
                strategy=self.name, city=city, target_date=target_date,
                direction='NO',
                outcome_label=o.get('label', ''),
                temp_c=temp_low,
                token_id=no_token,
                market_id=o.get('market_id', ''),
                entry_price=entry,
                confidence=min(0.85, 0.50 + overprice),
                rationale=(
                    f"📉 OVERPRICED: {o.get('label', '')} "
                    f"market={market_price:.0%} vs forecast={forecast_prob:.0%}"
                ),
                metadata={'type': 'overpriced_tail', 'overprice': overprice,
                          'forecast_prob': forecast_prob},
            ))
        return signals

    def _check_concentration(self, market, city, target_date, prob_dist, forecast, clob):
        """When leader confirmed by forecast, back it."""
        outcomes = market.get('outcomes', [])
        best = max(outcomes, key=lambda x: x.get('price_yes', 0), default=None)
        if not best or best.get('price_yes', 0) < 0.30:
            return []

        temp_low = best.get('temp_low')
        if temp_low is None:
            return []

        forecast_prob = self._get_prob(best, prob_dist)
        market_price = best.get('price_yes', 0)
        edge = forecast_prob - market_price

        if edge < 0.05 or forecast_prob < 0.35:
            return []

        yt = best.get('token_id_yes', '')
        if not yt:
            return []

        book = clob.get_orderbook(yt)
        entry = book['best_ask'] if book and not book.get('_synthetic') else market_price

        return [TradeSignal(
            strategy=self.name, city=city, target_date=target_date,
            direction='YES',
            outcome_label=best.get('label', ''),
            temp_c=temp_low,
            token_id=yt,
            market_id=best.get('market_id', ''),
            entry_price=entry,
            confidence=min(0.90, forecast.get('confidence', 0.5) * 1.2) if edge > 0.10 else 0.60,
            rationale=(
                f"🎯 CONCENTRATION: {best.get('label', '')} "
                f"forecast={forecast_prob:.0%} market={market_price:.0%} edge={edge:+.1%}"
            ),
            metadata={'type': 'concentration', 'edge': edge,
                      'forecast_prob': forecast_prob},
        )]

    def _get_prob(self, outcome, prob_dist):
        """Get forecast probability for an outcome (handles ranges/bounds)."""
        temp_low = outcome.get('temp_low', 0)
        if outcome.get('is_upper_bound'):
            return sum(p for t, p in prob_dist.items() if t >= temp_low)
        elif outcome.get('is_lower_bound'):
            return sum(p for t, p in prob_dist.items() if t <= temp_low)
        elif outcome.get('is_range'):
            temp_high = outcome.get('temp_high', temp_low)
            return sum(p for t, p in prob_dist.items() if temp_low <= t <= temp_high)
        return prob_dist.get(temp_low, 0.0)
