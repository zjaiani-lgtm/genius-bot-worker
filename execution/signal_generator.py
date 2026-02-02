import ccxt
import os
from datetime import datetime
from execution.signal_client import append_signal

EXCHANGE = ccxt.binance({"enableRateLimit": True})

SYMBOL = os.getenv("BOT_SYMBOL", "BTC/USDT")
TF = os.getenv("BOT_TIMEFRAME", "1m")

def generate_signal():
    ohlcv = EXCHANGE.fetch_ohlcv(SYMBOL, timeframe=TF, limit=50)
    closes = [c[4] for c in ohlcv]  # close prices

    ma20 = sum(closes[-20:]) / 20
    last = closes[-1]
    prev = closes[-2]

    verdict = "NO_TRADE"
    direction = None

    if last > ma20 and last > prev:
        verdict = "TRADE"
        direction = "LONG"

    if verdict != "TRADE":
        return None

    return {
        "signal_id": f"GBM-AUTO-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        "timestamp_utc": datetime.utcnow().isoformat() + "Z",
        "final_verdict": "TRADE",
        "certified_signal": True,
        "confidence": 0.55,
        "mode_allowed": {"demo": True, "live": False},
        "execution": {
            "symbol": SYMBOL,
            "direction": direction,
            "entry": {"type": "MARKET", "price": None},
            "position_size": 0.0001,
            "risk": {"stop_loss": None, "take_profit": None},
        },
    }

def run_once(outbox_path: str):
    sig = generate_signal()
    if sig:
        append_signal(sig, outbox_path)
        return True
    return False
