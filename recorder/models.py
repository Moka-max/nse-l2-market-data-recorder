"""Core Market Data Schemas & Pluggable Adapter Interface."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, List
import time


def now_ns() -> int:
    return time.time_ns()


def mono_ns() -> int:
    return time.monotonic_ns()


@dataclass(frozen=True)
class OrderBookSnapshot:
    symbol: str
    exchange_time_ns: int
    receive_time_ns: int
    receive_monotonic_ns: int
    bid_prices: List[float]
    bid_quantities: List[int]
    ask_prices: List[float]
    ask_quantities: List[int]

    @property
    def mid_price(self) -> float:
        if self.bid_prices and self.ask_prices and self.bid_prices[0] > 0 and self.ask_prices[0] > 0:
            return (self.bid_prices[0] + self.ask_prices[0]) / 2.0
        return 0.0

    @property
    def spread(self) -> float:
        if self.bid_prices and self.ask_prices and self.bid_prices[0] > 0 and self.ask_prices[0] > 0:
            return self.ask_prices[0] - self.bid_prices[0]
        return 0.0


class BaseMarketDataAdapter(ABC):
    """Abstract interface decoupling recording logic from broker protocols."""

    def __init__(self, adapter_name: str) -> None:
        self.adapter_name = adapter_name
        self.handlers: List[Callable[[OrderBookSnapshot], None]] = []

    def add_handler(self, handler: Callable[[OrderBookSnapshot], None]) -> None:
        self.handlers.append(handler)

    def _dispatch(self, snap: OrderBookSnapshot) -> None:
        for h in self.handlers:
            h(snap)

    @abstractmethod
    def start(self) -> None:
        pass

    @abstractmethod
    def stop(self) -> None:
        pass


class MockMarketDataAdapter(BaseMarketDataAdapter):
    """Deterministic simulated L2 feed for testing and demonstrations."""

    def __init__(self) -> None:
        super().__init__(adapter_name="MOCK_NSE_L2")
        self.running = False

    def start(self) -> None:
        self.running = True

    def stop(self) -> None:
        self.running = False

    def emit_snapshot(self, symbol: str, mid_price: float, spread: float = 2.0) -> OrderBookSnapshot:
        t_wall = now_ns()
        t_mono = mono_ns()
        half = spread / 2.0
        snap = OrderBookSnapshot(
            symbol=symbol,
            exchange_time_ns=t_wall,
            receive_time_ns=t_wall,
            receive_monotonic_ns=t_mono,
            bid_prices=[round(mid_price - half - i * 0.5, 2) for i in range(5)],
            bid_quantities=[100 * (i + 1) for i in range(5)],
            ask_prices=[round(mid_price + half + i * 0.5, 2) for i in range(5)],
            ask_quantities=[100 * (i + 1) for i in range(5)],
        )
        self._dispatch(snap)
        return snap
