import asyncio
import os
import sys
from dotenv import load_dotenv
from client import load_private_key, stream_ticker

load_dotenv()

API_KEY_ID = os.environ["KALSHI_API_KEY_ID"]
# Accepts either PEM content directly or a file path
PRIVATE_KEY = (
    os.environ.get("KALSHI_KEY")
    or os.environ.get("KALSHI_PRIVATE_KEY")
    or os.environ["KALSHI_PRIVATE_KEY_PATH"]
)

# Pass a market ticker as a CLI arg to filter, e.g.:
#   python main.py SOCCER-UEFA-EURO-2024-0.5GOALS
MARKET_TICKER = sys.argv[1] if len(sys.argv) > 1 else None


def on_tick(tick: dict):
    ticker = tick.get("market_ticker", "?")
    yes_bid = tick.get("yes_bid")
    yes_ask = tick.get("yes_ask")
    # Kalshi prices are in cents (0-100), convert to dollars
    bid = yes_bid / 100 if yes_bid is not None else None
    ask = yes_ask / 100 if yes_ask is not None else None
    print(f"{ticker}  bid={bid:.2f}  ask={ask:.2f}" if bid and ask else tick)


async def main():
    private_key = load_private_key(PRIVATE_KEY)
    label = MARKET_TICKER or "all markets"
    print(f"[arb] streaming ticker for {label} ...")
    await stream_ticker(API_KEY_ID, private_key, on_tick, market_ticker=MARKET_TICKER)


if __name__ == "__main__":
    asyncio.run(main())
