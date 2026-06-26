import asyncio
from dataclasses import dataclass, field


@dataclass
class Track:
    market_ticker: str
    signal_ts_ms: int
    ask_at_signal: float
    peak_ask: float = 0.0
    resolved_ms: int | None = None  # ms from signal to hitting 1.00


class SignalTracker:
    """
    After an orderbook signal fires, watches the ticker price for that market
    and reports on where it went — tells us if we had a real profit window.
    """

    TIMEOUT_MS = 60_000

    def __init__(self, on_result):
        self._tracks: dict[str, Track] = {}
        self._on_result = on_result  # async callback(track)

    def start(self, market_ticker: str, ts_ms: int, ask_at_signal: float):
        self._tracks[market_ticker] = Track(
            market_ticker=market_ticker,
            signal_ts_ms=ts_ms,
            ask_at_signal=ask_at_signal,
            peak_ask=ask_at_signal,
        )

    async def on_tick(self, tick: dict):
        ticker = tick.get("market_ticker")
        if ticker not in self._tracks:
            return

        track = self._tracks[ticker]
        ts_ms = tick.get("ts_ms", 0)
        ask_raw = tick.get("yes_ask_dollars")
        if ask_raw is None:
            return

        ask = float(ask_raw)
        track.peak_ask = max(track.peak_ask, ask)

        elapsed = ts_ms - track.signal_ts_ms
        resolved = ask >= 1.0

        if resolved or elapsed >= self.TIMEOUT_MS:
            if resolved:
                track.resolved_ms = elapsed
            del self._tracks[ticker]
            await self._on_result(track)
