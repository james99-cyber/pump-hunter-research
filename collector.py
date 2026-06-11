import asyncio
import json
import time
from pathlib import Path

import websockets

PUMP_WS = "wss://pumpportal.fun/api/data"
DATA_FILE = Path("research_data.json")


def load_data():
    if DATA_FILE.exists():
        try:
            return json.loads(DATA_FILE.read_text())
        except Exception:
            pass

    return {"tokens": []}


def save_data(data):
    DATA_FILE.write_text(json.dumps(data, indent=2))


def token_exists(data, mint):
    return any(t.get("mint") == mint for t in data["tokens"])


def parse_token(message):
    try:
        data = json.loads(message)
    except Exception:
        return None

    mint = data.get("mint")

    if not mint:
        return None

    return {
        "mint": mint,
        "name": data.get("name") or data.get("tokenName") or "Unknown",
        "symbol": data.get("symbol") or data.get("ticker") or "UNKNOWN",
        "launch_time": int(time.time()),
        "launch_market_cap_sol": data.get("marketCapSol"),
        "raw": data,
        "snapshots": []
    }


async def listen_for_tokens():
    print("🚀 Pump Hunter Research Started")
    print("Connecting to PumpPortal...")

    research_data = load_data()

    while True:
        try:
            async with websockets.connect(PUMP_WS, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({"method": "subscribeNewToken"}))

                print("✅ Connected. Listening for new Pump.fun tokens...")

                async for message in ws:
                    token = parse_token(message)

                    if not token:
                        continue

                    if token_exists(research_data, token["mint"]):
                        continue

                    research_data["tokens"].append(token)
                    save_data(research_data)

                    print(
                        f"🆕 {token['symbol']} | "
                        f"{token['name']} | "
                        f"{token['mint']}"
                    )

        except Exception as e:
            print(f"Connection error: {e}")
            print("Reconnecting in 10 seconds...")
            await asyncio.sleep(10)


def collect_tokens():
    asyncio.run(listen_for_tokens())
