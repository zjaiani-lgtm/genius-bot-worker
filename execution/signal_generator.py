import os
import time
import uuid
import logging
from datetime import datetime
from typing import Optional, Dict, Any, Tuple

import ccxt

from execution.signal_client import append_signal
from execution.db.repository import list_active_oco_links

logger = logging.getLogger("gbm")

SYMBOL = os.getenv("BOT_SYMBOL", "BTC/USDT")
TIMEFRAME = os.getenv("BOT_TIMEFRAME", "1m")
CANDLE_LIMIT = int(os.getenv("BOT_CANDLE_LIMIT", "50"))
COOLDOWN_SECONDS = int(os.getenv("BOT_SIGNAL_COOLDOWN_SECONDS", "60"))

ALLOW_LIVE_SIGNALS = os.getenv("ALLOW_LIVE_SIGNALS", "false").lower() == "true"
POSITION_SIZE = float(os.getenv("BOT_POSITION_SIZE", "0.0001"))
CONFIDENCE = float(os.getenv("BOT_SIGNAL_CONFIDENCE", "0.55"))

GEN_DEBUG = os.getenv("GEN_DEBUG", "true").lower() == "true"
GEN_LOG_EVERY_TICK = os.getenv("GEN_LOG_EVERY_TICK", "true").lower() == "true"

# If there is an ACTIVE OCO -> do not create any new signals (avoid double-buy)
BLOCK_SIGNALS_WHEN_ACTIVE_OCO = os.getenv("BLOCK_SIGNALS_WHEN_ACTIVE_OCO", "true").lower() == "true"

_last_emit_ts: float = 0.0
_last_signature: Optional[Tuple[str, str]] = None

EXCHANGE = ccxt.binance({"enableRateLimit": True})


def _now_utc_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _has_active_oco() -> bool:
    try:
        rows = list_active_oco_links(limit=1)
        return len(rows) > 0
    except Exception as e:
        # be conservative: if DB read fails, assume we have an active position
        logger.warning(f"[GEN] ACTIVE_OCO_CHECK_FAIL | err={e} -> assume active_oco=True")
        return True


def generate_signal() -> Optional[Dict[str, Any]]:
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

    if not (cond_ma and cond_mom):
        if GEN_LOG_EVERY_TICK:
            reason = []
            if not cond_ma:
                reason.append("last<=ma20")
            if not cond_mom:
                reason.append("last<=prev")
            logger.info(f"[GEN] NO_SIGNAL | reason={','.join(reason) if reason else 'unknown'}")
        return None

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
            "direction": "LONG",
            "entry": {"type": "MARKET", "price": None},
            "position_size": POSITION_SIZE,
            "risk": {"stop_loss": None, "take_profit": None},
        },
    }

    if GEN_DEBUG:
        logger.info(
            f"[GEN] SIGNAL_READY | id={signal_id} verdict=TRADE symbol={SYMBOL} dir=LONG "
            f"mode_allowed={mode_allowed} pos_size={POSITION_SIZE}"
        )

    return sig


def run_once(outbox_path: str) -> bool:
    global _last_emit_ts, _last_signature

    now = time.time()
    active_oco = _has_active_oco()

    # ✅ hard block while position/OCO exists
    if BLOCK_SIGNALS_WHEN_ACTIVE_OCO and active_oco:
        if GEN_DEBUG:
            logger.info("[GEN] SKIP | active_oco=True -> block new signals")
        return False

    elapsed = now - _last_emit_ts
    if elapsed < COOLDOWN_SECONDS:
        if GEN_DEBUG:
            logger.info(f"[GEN] SKIP | cooldown_active left~{int(COOLDOWN_SECONDS - elapsed)}s")
        return False

    sig = generate_signal()
    if not sig:
        return False

    symbol = (sig.get("execution") or {}).get("symbol")
    direction = (sig.get("execution") or {}).get("direction")
    signature = (str(symbol), str(direction))

    # basic dedupe (now safe because we already blocked active_oco)
    if _last_signature == signature:
        _last_emit_ts = now
        if GEN_DEBUG:
            logger.info(f"[GEN] SKIP | dedupe_hit signature={signature}")
        return False

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
