# execution/signal_generator.py
import os
import time
import uuid
from datetime import datetime
from typing import Optional, Dict, Any, Tuple

import ccxt

from execution.signal_client import append_signal

# -----------------------
# CONFIG
# -----------------------
SYMBOL = os.getenv("BOT_SYMBOL", "BTC/USDT")
TIMEFRAME = os.getenv("BOT_TIMEFRAME", "1m")
CANDLE_LIMIT = int(os.getenv("BOT_CANDLE_LIMIT", "50"))

COOLDOWN_SECONDS = int(os.getenv("BOT_SIGNAL_COOLDOWN_SECONDS", "60"))

# IMPORTANT:
# Default: allow DEMO only. (Safe-by-default)
ALLOW_LIVE_SIGNALS = os.getenv("ALLOW_LIVE_SIGNALS", "false").lower() == "true"

POSITION_SIZE = float(os.getenv("BOT_POSITION_SIZE", "0.0001"))  # base amount for demo
CONFIDENCE = float(os.getenv("BOT_SIGNAL_CONFIDENCE", "0.55"))

# -----------------------
# INTERNAL STATE (memory)
# -----------------------
_last_emit_ts: float = 0.0
_last_signature: Optional[Tuple[str, str]] = None  # (symbol, direction)

# -----------------------
# EXCHANGE (read-only)
# -----------------------
EXCHANGE = ccxt.binance({"enableRateLimit": True})


def _now_utc_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def generate_signal() -> Optional[Dict[str, Any]]:
    """
    Minimal example strategy:
      - Pull candles
      - Compute MA20
      - If last close > MA20 AND last close > prev close => TRADE LONG
      - Else: no signal
    """
    ohlcv = EXCHANGE.fetch_ohlcv(SYMBOL, timeframe=TIMEFRAME, limit=CANDLE_LIMIT)
    if not ohlcv or len(ohlcv) < 25:
        return None

    closes = [c[4] for c in ohlcv]
    last = closes[-1]
    prev = closes[-2]
    ma20 = sum(closes[-20:]) / 20.0

    verdict = "NO_TRADE"
    direction = None

    if last > ma20 and last > prev:
        verdict = "TRADE"
        direction = "LONG"

    if verdict != "TRADE":
        return None

    # Safe mode_allowed
    mode_allowed = {"demo": True, "live": bool(ALLOW_LIVE_SIGNALS)}

    return {
        "signal_id": f"GBM-AUTO-{uuid.uuid4().hex}",
        "timestamp_utc": _now_utc_iso(),
        "final_verdict": "TRADE",
        "certified_signal": True,
        "confidence": CONFIDENCE,
        "mode_allowed": mode_allowed,
        "execution": {
            "symbol": SYMBOL,
            "direction": direction,
            "entry": {"type": "MARKET", "price": None},
            "position_size": POSITION_SIZE,
            "risk": {"stop_loss": None, "take_profit": None},
        },
    }


def run_once(outbox_path: str) -> bool:
    """
    Called from main loop.
    Adds:
      - cooldown
      - dedupe on (symbol, direction)
    """
    global _last_emit_ts, _last_signature

    now = time.time()

    # cooldown
    if now - _last_emit_ts < COOLDOWN_SECONDS:
        return False

    sig = generate_signal()
    if not sig:
        return False

    symbol = (sig.get("execution") or {}).get("symbol")
    direction = (sig.get("execution") or {}).get("direction")
    signature = (str(symbol), str(direction))

    # dedupe: if last signal same signature, skip (still respects cooldown)
    if _last_signature == signature:
        _last_emit_ts = now  # still move the timer to avoid spamming checks
        return False

    append_signal(sig, outbox_path)
    _last_emit_ts = now
    _last_signature = signature
    return True
