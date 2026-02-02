# execution/signal_generator.py
import os
import time
import uuid
import logging
from datetime import datetime
from typing import Optional, Dict, Any, Tuple

import ccxt

from execution.signal_client import append_signal

logger = logging.getLogger("gbm")

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

# Extra debug flags
GEN_DEBUG = os.getenv("GEN_DEBUG", "true").lower() == "true"     # verbose generator logs
GEN_LOG_EVERY_TICK = os.getenv("GEN_LOG_EVERY_TICK", "true").lower() == "true"  # log NO_TRADE details too

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


def _log_cfg_once() -> None:
    """
    Called implicitly on first tick via module import, but we keep it safe to call multiple times.
    """
    # Not strictly once, but harmless; we keep it simple.
    if GEN_DEBUG:
        logger.info(
            f"[GEN] CONFIG | SYMBOL={SYMBOL} TF={TIMEFRAME} LIMIT={CANDLE_LIMIT} "
            f"COOLDOWN={COOLDOWN_SECONDS}s POS_SIZE={POSITION_SIZE} "
            f"ALLOW_LIVE_SIGNALS={ALLOW_LIVE_SIGNALS} CONF={CONFIDENCE}"
        )


def generate_signal() -> Optional[Dict[str, Any]]:
    """
    Minimal example strategy:
      - Pull candles
      - Compute MA20
      - If last close > MA20 AND last close > prev close => TRADE LONG
      - Else: no signal
    """
    _log_cfg_once()

    # 1) fetch candles
    try:
        t0 = time.time()
        ohlcv = EXCHANGE.fetch_ohlcv(SYMBOL, timeframe=TIMEFRAME, limit=CANDLE_LIMIT)
        dt_ms = int((time.time() - t0) * 1000)
        if GEN_DEBUG:
            logger.info(f"[GEN] FETCH_OK | symbol={SYMBOL} tf={TIMEFRAME} candles={len(ohlcv) if ohlcv else 0} dt={dt_ms}ms")
    except Exception as e:
        logger.exception(f"[GEN] FETCH_FAIL | symbol={SYMBOL} tf={TIMEFRAME} err={e}")
        return None

    if not ohlcv or len(ohlcv) < 25:
        if GEN_LOG_EVERY_TICK:
            logger.info(f"[GEN] NO_SIGNAL | reason=not_enough_candles got={len(ohlcv) if ohlcv else 0} need>=25")
        return None

    # 2) compute indicators
    closes = [c[4] for c in ohlcv]
    last = float(closes[-1])
    prev = float(closes[-2])
    ma20 = float(sum(closes[-20:]) / 20.0)

    cond_ma = last > ma20
    cond_mom = last > prev

    if GEN_LOG_EVERY_TICK:
        logger.info(
            f"[GEN] SNAPSHOT | last={last:.2f} prev={prev:.2f} ma20={ma20:.2f} "
            f"cond(last>ma20)={cond_ma} cond(last>prev)={cond_mom}"
        )

    verdict = "NO_TRADE"
    direction = None

    if cond_ma and cond_mom:
        verdict = "TRADE"
        direction = "LONG"

    if verdict != "TRADE":
        if GEN_LOG_EVERY_TICK:
            reason = []
            if not cond_ma:
                reason.append("last<=ma20")
            if not cond_mom:
                reason.append("last<=prev")
            reason_txt = ",".join(reason) if reason else "unknown"
            logger.info(f"[GEN] NO_SIGNAL | reason={reason_txt}")
        return None

    # 3) build signal
    mode_allowed = {"demo": True, "live": bool(ALLOW_LIVE_SIGNALS)}
    signal_id = f"GBM-AUTO-{uuid.uuid4().hex}"

    sig = {
        "signal_id": signal_id,
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

    if GEN_DEBUG:
        logger.info(
            f"[GEN] SIGNAL_READY | id={signal_id} verdict=TRADE symbol={SYMBOL} dir={direction} "
            f"mode_allowed={mode_allowed} pos_size={POSITION_SIZE}"
        )

    return sig


def run_once(outbox_path: str) -> bool:
    """
    Called from main loop.
    Adds:
      - cooldown
      - dedupe on (symbol, direction)
      - logs each gate decision
    """
    global _last_emit_ts, _last_signature

    now = time.time()

    # cooldown gate
    elapsed = now - _last_emit_ts
    if elapsed < COOLDOWN_SECONDS:
        if GEN_DEBUG:
            left = int(COOLDOWN_SECONDS - elapsed)
            logger.info(f"[GEN] SKIP | cooldown_active left~{left}s")
        return False

    sig = generate_signal()
    if not sig:
        # no signal this tick
        return False

    symbol = (sig.get("execution") or {}).get("symbol")
    direction = (sig.get("execution") or {}).get("direction")
    signature = (str(symbol), str(direction))

    # dedupe gate
    if _last_signature == signature:
        _last_emit_ts = now  # still advance timer to prevent rapid rechecks
        if GEN_DEBUG:
            logger.info(f"[GEN] SKIP | dedupe_hit signature={signature}")
        return False

    # append to outbox
    try:
        append_signal(sig, outbox_path)
        _last_emit_ts = now
        _last_signature = signature

        if GEN_DEBUG:
            logger.info(f"[GEN] OUTBOX_APPEND_OK | path={outbox_path} id={sig.get('signal_id')} signature={signature}")

        return True

    except Exception as e:
        logger.exception(f"[GEN] OUTBOX_APPEND_FAIL | path={outbox_path} err={e}")
        return False
