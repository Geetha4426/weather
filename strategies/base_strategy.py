"""
Base Strategy — Abstract interface for weather trading strategies.

Trade signals for weather markets use 'YES' or 'NO' direction
(betting on or against a specific temperature outcome).
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional


class TradeSignal:
    """Represents a trading signal for a weather outcome."""

    def __init__(
        self,
        strategy: str,
        city: str,
        direction: str,        # 'YES' or 'NO' (betting for/against a temp outcome)
        outcome_label: str,    # e.g., "14°C"
        token_id: str,         # YES or NO token
        entry_price: float,
        confidence: float,
        target_date: str = '',
        temp_c: int = 0,
        market_id: str = '',
        rationale: str = '',
        metadata: Dict = None,
    ):
        self.strategy = strategy
        self.city = city
        self.target_date = target_date
        self.direction = direction
        self.outcome_label = outcome_label
        self.temp_c = temp_c
        self.token_id = token_id
        self.market_id = market_id
        self.entry_price = entry_price
        self.confidence = confidence
        self.rationale = rationale
        self.metadata = metadata or {}

    def to_dict(self) -> Dict:
        return {
            'strategy': self.strategy,
            'city': self.city,
            'target_date': self.target_date,
            'direction': self.direction,
            'outcome_label': self.outcome_label,
            'temp_c': self.temp_c,
            'token_id': self.token_id,
            'market_id': self.market_id,
            'entry_price': self.entry_price,
            'confidence': self.confidence,
            'rationale': self.rationale,
            'metadata': self.metadata,
        }

    def __repr__(self):
        return (f"Signal({self.strategy} {self.city} {self.direction} "
                f"{self.outcome_label} @{self.entry_price:.4f} "
                f"conf={self.confidence:.0%})")


class BaseStrategy(ABC):
    """Abstract base class for weather trading strategies."""

    name: str = "base"
    description: str = ""

    @abstractmethod
    async def analyze(self, weather_market: Dict, context: Dict) -> List[TradeSignal]:
        """
        Analyze a weather market and return trade signals.

        Args:
            weather_market: Parsed weather market from WeatherMarketClient
            context: {
                'clob': ClobClient,
                'weather_client': WeatherClient,
                'forecast': dict (ensemble forecast data),
                'seconds_remaining': int,
            }

        Returns:
            List of TradeSignals (can be multiple for multi-outcome markets)
        """
        pass
