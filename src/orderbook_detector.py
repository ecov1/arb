from collections import deque
from dataclasses import dataclass


@dataclass
class OrderBookSignal:
    market_ticker: str
    ts_ms: int
    contracts_consumed: float
    window_ms: int


class OrderBookDetector:
    """
    Detects a flood of buy orders hitting the book by watching for rapid
    negative YES-side deltas — sell orders being consumed or cancelled.
    When an event happens, market makers pull their asks and buyers market-in;
    both produce large negative YES-side delta_fp values in a short window.
    """

    def __init__(
        self,
        window_ms: int = 500,
        min_contracts: float = 5.0,
        min_deltas: int = 2,
        max_single_delta: float = 2000.0,  # cap to filter fake demo money noise
        cooldown_ms: int = 30_000,
    ):
        self.window_ms = window_ms
        self.min_contracts = min_contracts
        self.min_deltas = min_deltas
        self.max_single_delta = max_single_delta
        self.cooldown_ms = cooldown_ms
        self._consumed: dict[str, deque] = {}
        self._last_signal: dict[str, int] = {}

    def update(self, msg: dict) -> OrderBookSignal | None:
        ticker = msg.get("market_ticker")
        ts_ms = msg.get("ts_ms")
        delta_raw = msg.get("delta_fp")
        side = msg.get("side")

        if not (ticker and ts_ms and delta_raw is not None and side == "yes"):
            return None

        delta = float(delta_raw)

        # Only track negative deltas — ask volume disappearing
        if delta >= 0:
            return None

        # Filter out unrealistically large single orders (demo fake-money noise)
        if abs(delta) > self.max_single_delta:
            return None

        buf = self._consumed.setdefault(ticker, deque())
        buf.append((ts_ms, abs(delta)))

        cutoff = ts_ms - self.window_ms
        while buf and buf[0][0] < cutoff:
            buf.popleft()

        if ts_ms - self._last_signal.get(ticker, 0) < self.cooldown_ms:
            return None

        if len(buf) < self.min_deltas:
            return None

        total = sum(c for _, c in buf)
        if total < self.min_contracts:
            return None

        self._last_signal[ticker] = ts_ms
        elapsed = (ts_ms - buf[0][0]) if len(buf) > 1 else 0
        return OrderBookSignal(
            market_ticker=ticker,
            ts_ms=ts_ms,
            contracts_consumed=total,
            window_ms=elapsed,
        )
