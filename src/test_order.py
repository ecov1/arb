"""
Finds the first open sports market accepting orders and places a 1-contract
buy then immediate sell to verify end-to-end order execution.

Usage:
    .venv/bin/python src/test_order.py
"""
import asyncio
import os
import httpx
from dotenv import load_dotenv
from client import load_private_key, _sign, DEMO_REST_URL
from executor import buy, sell

load_dotenv()

API_KEY_ID = os.environ["KALSHI_API_KEY_ID"]
PRIVATE_KEY = load_private_key(
    os.environ.get("KALSHI_KEY")
    or os.environ.get("KALSHI_PRIVATE_KEY")
    or os.environ["KALSHI_PRIVATE_KEY_PATH"]
)

SPORTS_PREFIXES = ("KXWC", "KXMLB", "KXNBA", "KXNFL", "KXNHL", "KXSOC")
# Known-good sports tickers to try directly (faster than full pagination)
_SEED_TICKERS = [
    "KXMLBGAME-26JUN271910SEACLE-CLE",
    "KXMLBGAME-26JUN271910SEACLE-SEA",
    "KXWCGAME-26JUN14GERCUW-CUW",
]


async def find_tradeable_market() -> dict | None:
    """Find a sports market with an active ask price we can buy."""
    path_base = "/trade-api/v2/markets"

    # Try seed tickers first — no pagination needed
    async with httpx.AsyncClient(base_url=DEMO_REST_URL, timeout=10) as client:
        for ticker in _SEED_TICKERS:
            path = f"{path_base}/{ticker}"
            headers = _sign(API_KEY_ID, PRIVATE_KEY, "GET", path)
            r = await client.get(f"/markets/{ticker}", headers=headers)
            if r.status_code != 200:
                continue
            m = r.json().get("market", {})
            ask = m.get("yes_ask_dollars")
            if ask and 0.02 < float(ask) < 0.98 and m.get("status") == "active":
                return m

    # Fall back: paginate until we find a sports market with a real ask
    cursor = None
    async with httpx.AsyncClient(base_url=DEMO_REST_URL, timeout=15) as client:
        while True:
            params = {"status": "open", "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            headers = _sign(API_KEY_ID, PRIVATE_KEY, "GET", path_base)
            r = await client.get("/markets", headers=headers, params=params)
            r.raise_for_status()
            data = r.json()
            for m in data.get("markets", []):
                ask = m.get("yes_ask_dollars")
                if ask and 0.02 < float(ask) < 0.98 and m["ticker"].startswith(SPORTS_PREFIXES):
                    return m
            cursor = data.get("cursor")
            if not cursor:
                break
    return None


async def main():
    print("[test] searching for a tradeable sports market ...")
    market = await find_tradeable_market()
    if not market:
        print("[test] no tradeable market found")
        return

    ticker = market["ticker"]
    ask = float(market["yes_ask_dollars"])
    bid = float(market.get("yes_bid_dollars") or max(0.01, ask - 0.05))
    print(f"[test] found: {market.get('title')}")
    print(f"[test] ticker: {ticker}  bid: {bid:.4f}  ask: {ask:.4f}")

    print(f"\n[test] placing 1-contract BUY ...")
    try:
        order = await buy(API_KEY_ID, PRIVATE_KEY, ticker, count=1, ask=ask)
        print(f"[test] BUY success:")
        print(f"       order_id:    {order.get('order_id')}")
        print(f"       status:      {order.get('status')}")
        print(f"       filled:      {order.get('count_filled', '?')}")
        print(f"       fill_price:  {order.get('yes_price', '?')}")
    except Exception as e:
        print(f"[test] BUY failed: {e}")
        return

    await asyncio.sleep(1)

    print(f"\n[test] placing 1-contract SELL ...")
    try:
        order = await sell(API_KEY_ID, PRIVATE_KEY, ticker, count=1, bid=bid)
        print(f"[test] SELL success:")
        print(f"       order_id:    {order.get('order_id')}")
        print(f"       status:      {order.get('status')}")
        print(f"       fill_price:  {order.get('yes_price', '?')}")
    except Exception as e:
        print(f"[test] SELL failed: {e}")


if __name__ == "__main__":
    asyncio.run(main())
