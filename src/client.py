import asyncio
import base64
import json
import time

import httpx
import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

DEMO_WS_URL = "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2"
DEMO_REST_URL = "https://demo-api.kalshi.co/trade-api/v2"
_WS_PATH = "/trade-api/ws/v2"

_ORDERBOOK_BATCH = 200  # max tickers per subscribe message


def load_private_key(pem: str):
    if pem.strip().startswith("-----"):
        pem_bytes = pem.encode()
    else:
        with open(pem, "rb") as f:
            pem_bytes = f.read()
    pem_bytes = pem_bytes.replace(b"\\n", b"\n")
    return serialization.load_pem_private_key(pem_bytes, password=None)


def _sign(api_key_id: str, private_key, method: str, path: str) -> dict:
    ts = int(time.time() * 1000)
    message = f"{ts}{method}{path}".encode()
    signature = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=hashes.SHA256.digest_size,
        ),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": api_key_id,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode(),
        "KALSHI-ACCESS-TIMESTAMP": str(ts),
    }


def _auth_headers(api_key_id: str, private_key) -> dict:
    return _sign(api_key_id, private_key, "GET", _WS_PATH)


async def fetch_markets(api_key_id: str, private_key, on_first_page=None) -> list[dict]:
    """Fetch all open markets from the REST API, returning full market objects."""
    markets_out = []
    cursor = None
    path = "/trade-api/v2/markets"
    page = 0

    async with httpx.AsyncClient(base_url=DEMO_REST_URL, timeout=15) as client:
        while True:
            params = {"status": "open", "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            headers = _sign(api_key_id, private_key, "GET", path)
            r = await client.get("/markets", headers=headers, params=params)
            r.raise_for_status()
            data = r.json()
            markets = data.get("markets", [])
            markets_out.extend(markets)
            page += 1
            if page == 1 and on_first_page and markets:
                on_first_page(markets[0])
            if page % 10 == 0:
                print(f"[arb] fetched {len(markets_out)} markets so far ...")
            cursor = data.get("cursor")
            if not cursor or not markets:
                break

    return markets_out


class MarketCache:
    def __init__(self, api_key_id: str, private_key):
        self._api_key_id = api_key_id
        self._private_key = private_key
        self._cache: dict[str, str] = {}

    async def ask(self, market_ticker: str) -> float | None:
        path = f"/trade-api/v2/markets/{market_ticker}"
        headers = _sign(self._api_key_id, self._private_key, "GET", path)
        try:
            async with httpx.AsyncClient(base_url=DEMO_REST_URL) as client:
                r = await client.get(f"/markets/{market_ticker}", headers=headers)
                r.raise_for_status()
                m = r.json().get("market", {})
                # Store title while we're here
                if "title" in m:
                    self._cache[market_ticker] = m["title"]
                # yes_ask is in cents (0-100), convert to dollars
                yes_ask = m.get("yes_ask")
                return float(yes_ask) / 100 if yes_ask is not None else None
        except Exception:
            return None

    async def title(self, market_ticker: str) -> str:
        if market_ticker in self._cache:
            return self._cache[market_ticker]
        path = f"/trade-api/v2/markets/{market_ticker}"
        headers = _sign(self._api_key_id, self._private_key, "GET", path)
        try:
            async with httpx.AsyncClient(base_url=DEMO_REST_URL) as client:
                r = await client.get(f"/markets/{market_ticker}", headers=headers)
                r.raise_for_status()
                title = r.json().get("market", {}).get("title", market_ticker)
        except Exception:
            title = market_ticker
        self._cache[market_ticker] = title
        return title


async def stream(
    api_key_id: str,
    private_key,
    on_orderbook,
    orderbook_tickers: list[str],
    on_ticker=None,
):
    """
    Connect to Kalshi demo WebSocket and stream:
      - orderbook_delta for specified markets (primary signal)
      - ticker for all markets (optional, secondary)
    """
    headers = _auth_headers(api_key_id, private_key)

    async with websockets.connect(
        DEMO_WS_URL,
        additional_headers=headers,
        max_size=2**23,  # 8MB — large snapshots can exceed default
    ) as ws:
        print(f"[kalshi] connected")

        # Subscribe ticker (all markets) if a handler is provided
        if on_ticker:
            await ws.send(json.dumps({
                "id": 1,
                "cmd": "subscribe",
                "params": {"channels": ["ticker"]},
            }))

        # Subscribe orderbook_delta in batches
        for i, start in enumerate(range(0, len(orderbook_tickers), _ORDERBOOK_BATCH)):
            batch = orderbook_tickers[start:start + _ORDERBOOK_BATCH]
            await ws.send(json.dumps({
                "id": 100 + i,
                "cmd": "subscribe",
                "params": {
                    "channels": ["orderbook_delta"],
                    "market_tickers": batch,
                },
            }))

        print(f"[kalshi] subscribed to orderbook_delta for {len(orderbook_tickers)} markets")

        async for raw in ws:
            msg = json.loads(raw)
            msg_type = msg.get("type")

            if msg_type == "orderbook_delta":
                await on_orderbook(msg.get("msg", {}))
            elif msg_type == "ticker" and on_ticker:
                await on_ticker(msg.get("msg", {}))
