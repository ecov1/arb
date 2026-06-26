import httpx
from client import _sign, DEMO_REST_URL

_ORDER_PATH = "/trade-api/v2/portfolio/events/orders"


async def place_order(
    api_key_id: str,
    private_key,
    ticker: str,
    side: str,        # "bid" = buy YES, "ask" = sell YES
    count: int,
    price: float,     # dollars (0.01–0.99); set aggressively to ensure IOC fill
    time_in_force: str = "immediate_or_cancel",
) -> dict:
    headers = _sign(api_key_id, private_key, "POST", _ORDER_PATH)
    body = {
        "ticker": ticker,
        "side": side,
        "count": str(count),
        "price": f"{price:.2f}",
        "time_in_force": time_in_force,
        "self_trade_prevention_type": "taker_at_cross",
    }
    async with httpx.AsyncClient(base_url=DEMO_REST_URL, timeout=5) as client:
        r = await client.post("/portfolio/events/orders", headers=headers, json=body)
        if not r.is_success:
            raise Exception(f"HTTP {r.status_code}: {r.text}")
        return r.json().get("order", {})


async def buy(api_key_id: str, private_key, ticker: str, count: int, ask: float = 0.99) -> dict:
    # Bid slightly above current ask to guarantee IOC fill
    price = min(0.99, round(ask + 0.02, 2))
    return await place_order(api_key_id, private_key, ticker, "bid", count, price)


async def sell(api_key_id: str, private_key, ticker: str, count: int, bid: float = 0.01) -> dict:
    # Ask slightly below current bid to guarantee IOC fill
    price = max(0.01, round(bid - 0.02, 2))
    return await place_order(api_key_id, private_key, ticker, "ask", count, price)
