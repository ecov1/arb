from collections import deque
from dataclasses import dataclass


@dataclass
class SpikeSignal:
    market_ticker: str
    ts_ms: int
    baseline_ask: float
    current_ask: float
    pct_change: float
    elapsed_ms: int


class SpikeDetector:
    def __init__(
        self,
        window_ms: int = 3000,
        threshold_pct: float = 0.20,
        min_ticks: int = 2,
        cooldown_ms: int = 30_000,
        min_baseline_ask: float = 0.02,  # filter near-zero noise but allow cheap props
        min_abs_move: float = 0.08,      # minimum absolute price move in dollars
        min_margin: float = 0.05,        # minimum dollars remaining to resolution
    ):
        self.window_ms = window_ms
        self.threshold_pct = threshold_pct
        self.min_ticks = min_ticks
        self.cooldown_ms = cooldown_ms
        self.min_baseline_ask = min_baseline_ask
        self.min_abs_move = min_abs_move
        self.min_margin = min_margin
        self._history: dict[str, deque] = {}
        self._last_signal: dict[str, int] = {}

    def update(self, tick: dict) -> SpikeSignal | None:
        ticker = tick.get("market_ticker")
        ts_ms = tick.get("ts_ms")
        ask_raw = tick.get("yes_ask_dollars")

        if not (ticker and ts_ms and ask_raw):
            return None

        ask = float(ask_raw)
        buf = self._history.setdefault(ticker, deque())
        buf.append((ts_ms, ask))

        # Prune entries older than the window
        cutoff = ts_ms - self.window_ms
        while buf and buf[0][0] < cutoff:
            buf.popleft()

        if len(buf) < self.min_ticks:
            return None

        # Suppress repeated signals during cooldown
        if ts_ms - self._last_signal.get(ticker, 0) < self.cooldown_ms:
            return None

        baseline_ts, baseline_ask = buf[0]
        if baseline_ask == 0:
            return None

        if baseline_ask < self.min_baseline_ask:
            return None

        if ask >= 1.0:
            return None

        if (1.0 - ask) < self.min_margin:
            return None

        if (ask - baseline_ask) < self.min_abs_move:
            return None

        pct_change = (ask - baseline_ask) / baseline_ask
        if pct_change < self.threshold_pct:
            return None

        self._last_signal[ticker] = ts_ms
        return SpikeSignal(
            market_ticker=ticker,
            ts_ms=ts_ms,
            baseline_ask=baseline_ask,
            current_ask=ask,
            pct_change=pct_change,
            elapsed_ms=ts_ms - baseline_ts,
        )
