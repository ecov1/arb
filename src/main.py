import asyncio
import json
import os
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from dotenv import load_dotenv
from client import load_private_key, fetch_markets, stream, MarketCache
from orderbook_detector import OrderBookDetector
from tracker import SignalTracker
from position_manager import PositionManager

load_dotenv()

API_KEY_ID = os.environ["KALSHI_API_KEY_ID"]
PRIVATE_KEY = (
    os.environ.get("KALSHI_KEY")
    or os.environ.get("KALSHI_PRIVATE_KEY")
    or os.environ["KALSHI_PRIVATE_KEY_PATH"]
)

SPORTS_PREFIXES = ("KXWC", "KXMLB", "KXNBA", "KXNFL", "KXNHL", "KXSOC")

# Exact-score markets are automated market-maker noise, not tradeable events
EXCLUDED_PREFIXES = ("KXWCSCORE",)

# Today's date as it appears in Kalshi tickers e.g. "26JUN26"
_today = date.today()
TODAY_STR = _today.strftime("%d%b%y").upper()  # e.g. "26JUN26"

detector = OrderBookDetector(
    window_ms=500,
    min_contracts=5.0,
    min_deltas=2,
    max_single_delta=2000.0,
    cooldown_ms=30_000,
)

latest_ask: dict[str, float] = {}
_ob_msg_count = 0
_top_deltas: list[tuple[float, str]] = []


async def check_exchange_status(api_key_id: str, private_key) -> bool:
    """Returns True if the exchange is active and accepting orders."""
    import httpx
    from client import _sign, DEMO_REST_URL
    path = "/trade-api/v2/exchange/status"
    headers = _sign(api_key_id, private_key, "GET", path)
    try:
        async with httpx.AsyncClient(base_url=DEMO_REST_URL, timeout=5) as c:
            r = await c.get("/exchange/status", headers=headers)
            data = r.json()
            active = data.get("exchange_active", False)
            trading = data.get("trading_active", False)
            print(f"[arb] exchange status: active={active}  trading={trading}")
            if not active:
                print(f"[arb] WARNING: exchange is paused — orders will be rejected (409)")
            return active
    except Exception as e:
        print(f"[arb] could not fetch exchange status: {e}")
        return True  # assume open if we can't check


async def main():
    private_key = load_private_key(PRIVATE_KEY)
    cache = MarketCache(API_KEY_ID, private_key)
    await check_exchange_status(API_KEY_ID, private_key)

    async def on_result(track):
        title = await cache.title(track.market_ticker)
        profit = track.peak_ask - track.ask_at_signal
        outcome = (
            f"resolved in {track.resolved_ms}ms"
            if track.resolved_ms is not None
            else "timed out (60s)"
        )
        print(
            f"\n--- SIGNAL RESULT ---"
            f"\n  market:     {title}"
            f"\n  ask@signal: {track.ask_at_signal:.4f}"
            f"\n  peak ask:   {track.peak_ask:.4f}  ({'+' if profit >= 0 else ''}{profit:.4f})"
            f"\n  outcome:    {outcome}"
            f"\n"
        )

    tracker = SignalTracker(on_result=on_result)
    positions = PositionManager(API_KEY_ID, private_key)

    cache_file = Path(".markets_cache.json")
    cache_max_age = timedelta(hours=1)

    if cache_file.exists():
        age = datetime.now(timezone.utc) - datetime.fromtimestamp(cache_file.stat().st_mtime, tz=timezone.utc)
        if age < cache_max_age:
            print(f"[arb] loading markets from cache (age: {int(age.total_seconds())}s) ...")
            all_markets = json.loads(cache_file.read_text())
        else:
            print(f"[arb] cache expired, re-fetching ...")
            all_markets = await fetch_markets(API_KEY_ID, private_key)
            cache_file.write_text(json.dumps(all_markets))
    else:
        print(f"[arb] fetching open markets ...")
        all_markets = await fetch_markets(API_KEY_ID, private_key)
        cache_file.write_text(json.dumps(all_markets))

    sports_markets = [
        m for m in all_markets
        if m["ticker"].startswith(SPORTS_PREFIXES)
        and not m["ticker"].startswith(EXCLUDED_PREFIXES)
        and TODAY_STR in m["ticker"]
    ]

    print(f"[arb] {len(sports_markets)} sports markets for {TODAY_STR} (from {len(all_markets)} total)")

    # Pre-populate ask prices from REST response
    for m in sports_markets:
        ask = m.get("yes_ask_dollars")
        if ask is not None:
            latest_ask[m["ticker"]] = float(ask)
    print(f"[arb] pre-populated prices for {len(latest_ask)} markets")

    tickers = [m["ticker"] for m in sports_markets]

    async def heartbeat():
        while True:
            await asyncio.sleep(30)
            top = sorted(_top_deltas, reverse=True)[:5]
            top_str = "  ".join(f"{s:.1f}({t})" for s, t in top)
            print(f"[arb] heartbeat — {_ob_msg_count} msgs  top neg deltas: {top_str or 'none'}")

    asyncio.ensure_future(heartbeat())

    async def on_orderbook(msg: dict):
        global _ob_msg_count
        _ob_msg_count += 1
        delta_raw = msg.get("delta_fp")
        if delta_raw is not None and msg.get("side") == "yes":
            delta = float(delta_raw)
            if delta < 0 and abs(delta) <= detector.max_single_delta:
                _top_deltas.append((abs(delta), msg.get("market_ticker", "?")))
                if len(_top_deltas) > 500:
                    _top_deltas.sort(reverse=True)
                    del _top_deltas[100:]

        signal = detector.update(msg)
        if not signal:
            return

        ask = latest_ask.get(signal.market_ticker)
        if ask is None:
            # Fallback: fetch current price from REST
            ask = await cache.ask(signal.market_ticker)
            if ask is not None:
                latest_ask[signal.market_ticker] = ask

        title = await cache.title(signal.market_ticker)
        print(
            f"\n*** ORDER BOOK SIGNAL ***"
            f"\n  market:    {title}"
            f"\n  ticker:    {signal.market_ticker}"
            f"\n  consumed:  {signal.contracts_consumed:.1f} contracts in {signal.window_ms}ms"
            f"\n  ask now:   {ask:.4f}"
            f"\n"
        )
        tracker.start(signal.market_ticker, signal.ts_ms, ask)
        await positions.open(signal.market_ticker, ask)

    async def on_ticker(tick: dict):
        ticker = tick.get("market_ticker")
        ask_raw = tick.get("yes_ask_dollars")
        if ticker and ask_raw:
            latest_ask[ticker] = float(ask_raw)
        await tracker.on_tick(tick)
        await positions.on_tick(tick)

    while True:
        try:
            await stream(
                API_KEY_ID, private_key,
                on_orderbook,
                orderbook_tickers=tickers,
                on_ticker=on_ticker,
            )
        except Exception as e:
            print(f"[arb] connection lost: {e} — reconnecting in 5s ...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
