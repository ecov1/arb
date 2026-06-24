import asyncio
import base64
import json
import time

import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

DEMO_WS_URL = "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2"
_WS_PATH = "/trade-api/ws/v2"


def load_private_key(pem: str):
    # Accept either a file path or the PEM content itself
    if pem.strip().startswith("-----"):
        pem_bytes = pem.encode()
    else:
        with open(pem, "rb") as f:
            pem_bytes = f.read()
    # Handle escaped newlines that env vars sometimes contain
    pem_bytes = pem_bytes.replace(b"\\n", b"\n")
    return serialization.load_pem_private_key(pem_bytes, password=None)


def _auth_headers(api_key_id: str, private_key) -> dict:
    ts = int(time.time() * 1000)
    message = f"{ts}GET{_WS_PATH}".encode()
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


async def stream_ticker(api_key_id: str, private_key, on_tick, market_ticker: str = None):
    """
    Connect to Kalshi demo WebSocket and stream ticker updates.
    Calls on_tick(msg) for each ticker message received.
    """
    headers = _auth_headers(api_key_id, private_key)

    async with websockets.connect(DEMO_WS_URL, additional_headers=headers) as ws:
        print(f"[kalshi] connected")

        await ws.send(json.dumps({
            "id": 1,
            "cmd": "subscribe",
            "params": {"channels": ["ticker"]},
        }))

        async for raw in ws:
            msg = json.loads(raw)

            # Skip non-ticker messages (ack, heartbeat, etc.)
            if msg.get("type") != "ticker":
                continue

            tick = msg.get("msg", {})

            # Filter to specific market if requested
            if market_ticker and tick.get("market_ticker") != market_ticker:
                continue

            on_tick(tick)
