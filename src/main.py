import asyncio
import json
import os
import re
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from dotenv import load_dotenv
from client import (
    load_private_key, fetch_markets, stream, MarketCache,
    DEMO_WS_URL, DEMO_REST_URL, PROD_WS_URL, PROD_REST_URL,
)
from orderbook_detector import OrderBookDetector
from tracker import SignalTracker
from position_manager import PositionManager

load_dotenv()

# Set KALSHI_ENV=prod to use production data feed (dry-run by default on prod)
ENV = os.environ.get("KALSHI_ENV", "demo").lower()

if ENV == "prod":
    API_KEY_ID = os.environ["KALSHI_PROD_API_KEY_ID"]
    PRIVATE_KEY = (
        os.environ.get("KALSHI_PROD_KEY")
        or os.environ["KALSHI_PROD_KEY_PATH"]
    )
    WS_URL = PROD_WS_URL
    REST_URL = PROD_REST_URL
    # Never place real orders unless explicitly opted in
    DRY_RUN = os.environ.get("KALSHI_LIVE_ORDERS", "").lower() != "true"
else:
    API_KEY_ID = os.environ["KALSHI_API_KEY_ID"]
    PRIVATE_KEY = (
        os.environ.get("KALSHI_KEY")
        or os.environ.get("KALSHI_PRIVATE_KEY")
        or os.environ["KALSHI_PRIVATE_KEY_PATH"]
    )
    WS_URL = DEMO_WS_URL
    REST_URL = DEMO_REST_URL
    DRY_RUN = False

SPORTS_PREFIXES = ("KXWC", "KXMLB", "KXNBA", "KXNFL", "KXNHL", "KXSOC")

# Exact-score markets are automated market-maker noise, not tradeable events
EXCLUDED_PREFIXES = ("KXWCSCORE",)

# Today's date as it appears in Kalshi tickers e.g. "26JUN26"
_today = date.today()
TODAY_STR = _today.strftime("%d%b%y").upper()  # e.g. "26JUN26"

detector = OrderBookDetector(
    window_ms=500,
    min_contracts=200.0,
    min_deltas=3,
    max_single_delta=2000.0,
    cooldown_ms=30_000,
)

latest_ask: dict[str, float] = {}
_ob_msg_count = 0
_top_deltas: list[tuple[float, str]] = []


async def check_exchange_status(api_key_id: str, private_key, rest_url: str) -> bool:
    """Returns True if the exchange is active and accepting orders."""
    import httpx
    from client import _sign
    path = "/trade-api/v2/exchange/status"
    headers = _sign(api_key_id, private_key, "GET", path)
    try:
        async with httpx.AsyncClient(base_url=rest_url, timeout=5) as c:
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
    env_label = "PROD (dry-run)" if ENV == "prod" and DRY_RUN else ENV.upper()
    print(f"[arb] starting  env={env_label}")

    private_key = load_private_key(PRIVATE_KEY)
    cache = MarketCache(API_KEY_ID, private_key, rest_url=REST_URL)
    await check_exchange_status(API_KEY_ID, private_key, REST_URL)

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
    positions = PositionManager(API_KEY_ID, private_key, dry_run=DRY_RUN)

    cache_file = Path(".markets_cache.json")
    cache_max_age = timedelta(hours=1)

    if cache_file.exists():
        age = datetime.now(timezone.utc) - datetime.fromtimestamp(cache_file.stat().st_mtime, tz=timezone.utc)
        if age < cache_max_age:
            print(f"[arb] loading markets from cache (age: {int(age.total_seconds())}s) ...")
            all_markets = json.loads(cache_file.read_text())
        else:
            print(f"[arb] cache expired, re-fetching ...")
            all_markets = await fetch_markets(API_KEY_ID, private_key, rest_url=REST_URL)
            cache_file.write_text(json.dumps(all_markets))
    else:
        print(f"[arb] fetching open markets ...")
        all_markets = await fetch_markets(API_KEY_ID, private_key, rest_url=REST_URL)
        cache_file.write_text(json.dumps(all_markets))

    now = datetime.now(timezone.utc)

    def game_start_time(ticker: str) -> datetime | None:
        """Parse UTC start time from ticker if present (e.g. KXMLB...-26JUN261840...)."""
        m = re.search(r"\d{2}[A-Z]{3}\d{2}(\d{4})", ticker)
        if not m:
            return None
        t = m.group(1)
        return now.replace(hour=int(t[:2]), minute=int(t[2:]), second=0, microsecond=0)

    def is_live(m: dict) -> bool:
        ticker = m["ticker"]
        start = game_start_time(ticker)
        if start:
            # Ticker encodes start time (MLB-style) — game must have begun
            return start <= now
        else:
            # No start time in ticker (WC/soccer) — use cached ask as proxy:
            # $0.00 = pre-game/not yet trading, $0.99+ = already resolved
            ask = float(m.get("yes_ask_dollars") or 0)
            return 0.02 < ask < 0.98

    sports_markets = [
        m for m in all_markets
        if m["ticker"].startswith(SPORTS_PREFIXES)
        and not m["ticker"].startswith(EXCLUDED_PREFIXES)
        and TODAY_STR in m["ticker"]
        and is_live(m)
    ]

    print(f"[arb] {len(sports_markets)} live sports markets for {TODAY_STR} (from {len(all_markets)} total)")

    # Pre-populate ask prices from REST response
    for m in sports_markets:
        ask = m.get("yes_ask_dollars")
        if ask is not None:
            latest_ask[m["ticker"]] = float(ask)
    print(f"[arb] pre-populated prices for {len(latest_ask)} markets")

    tickers = [m["ticker"] for m in sports_markets]

    _exchange_was_active = False

    async def heartbeat():
        nonlocal _exchange_was_active
        while True:
            await asyncio.sleep(30)
            top = sorted(_top_deltas, reverse=True)[:5]
            top_str = "  ".join(f"{s:.1f}({t})" for s, t in top)
            exchange_active = await check_exchange_status(API_KEY_ID, private_key, REST_URL)
            if exchange_active and not _exchange_was_active:
                print(f"\n[arb] *** EXCHANGE IS NOW OPEN — orders will go through ***\n")
            _exchange_was_active = exchange_active
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
            ask = await cache.ask(signal.market_ticker)
            if ask is not None:
                latest_ask[signal.market_ticker] = ask

        # Ignore signal if market has no real price — game probably not live yet
        if not ask or not (0.02 < ask < 0.97):
            return

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
                ws_url=WS_URL,
            )
        except Exception as e:
            print(f"[arb] connection lost: {e} — reconnecting in 5s ...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
