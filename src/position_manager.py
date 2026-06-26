import asyncio
import time
from dataclasses import dataclass, field
from executor import buy, sell

TRADE_DOLLARS = 10.0    # max spend per trade
TARGET_ASK = 0.98       # sell when bid hits this
STOP_LOSS_PCT = 0.15    # sell if price drops 15% below entry
TIMEOUT_S = 60          # sell after 60 seconds regardless


@dataclass
class Position:
    ticker: str
    count: int
    entry_ask: float
    current_bid: float = 0.0
    opened_at: float = field(default_factory=time.monotonic)


class PositionManager:
    def __init__(self, api_key_id: str, private_key, dry_run: bool = False):
        self._api_key_id = api_key_id
        self._private_key = private_key
        self._dry_run = dry_run
        self._positions: dict[str, Position] = {}

    async def open(self, ticker: str, entry_ask: float):
        if ticker in self._positions:
            return  # already in this market

        count = max(1, int(TRADE_DOLLARS / entry_ask))

        if self._dry_run:
            print(f"\n[DRY RUN buy]  {ticker}  {count} contracts @ ~{entry_ask:.4f}  (not sent)")
            self._positions[ticker] = Position(ticker=ticker, count=count, entry_ask=entry_ask)
            return

        print(f"\n[buy]  {ticker}  {count} contracts @ ~{entry_ask:.4f}")
        try:
            order = await buy(self._api_key_id, self._private_key, ticker, count, ask=entry_ask)
        except Exception as e:
            print(f"[buy]  FAILED: {e}")
            return

        order_id = order.get("order_id", "?")
        print(f"[buy]  order {order_id} placed")
        self._positions[ticker] = Position(ticker=ticker, count=count, entry_ask=entry_ask)

    async def on_tick(self, tick: dict):
        ticker = tick.get("market_ticker")
        if ticker not in self._positions:
            return

        pos = self._positions[ticker]
        bid_raw = tick.get("yes_bid_dollars")
        ask_raw = tick.get("yes_ask_dollars")
        if not bid_raw or not ask_raw:
            return

        bid = float(bid_raw)
        ask = float(ask_raw)
        pos.current_bid = bid
        elapsed = time.monotonic() - pos.opened_at
        stop_price = pos.entry_ask * (1 - STOP_LOSS_PCT)

        if bid >= TARGET_ASK:
            await self._exit(pos, f"target hit  bid={bid:.4f}")
        elif ask <= stop_price:
            await self._exit(pos, f"stop loss   ask={ask:.4f} < {stop_price:.4f}")
        elif elapsed >= TIMEOUT_S:
            await self._exit(pos, f"timeout     {elapsed:.0f}s elapsed")

    async def _exit(self, pos: Position, reason: str):
        del self._positions[pos.ticker]
        profit_est = (pos.current_bid - pos.entry_ask) * pos.count
        print(f"\n[{'DRY RUN ' if self._dry_run else ''}sell] {pos.ticker}  {pos.count} contracts — {reason}  est. P&L: ${profit_est:+.2f}")
        if self._dry_run:
            return
        try:
            order = await sell(self._api_key_id, self._private_key, pos.ticker, pos.count, bid=pos.current_bid)
            print(f"[sell] order {order.get('order_id', '?')} placed")
        except Exception as e:
            print(f"[sell] FAILED: {e}")
