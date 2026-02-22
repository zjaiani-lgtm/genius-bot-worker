# execution/signal_generator.py
import os
import time
import uuid
import logging
from datetime import datetime
from typing import Optional, Dict, Any, Tuple, List

import ccxt

from execution.signal_client import append_signal
from execution.db.repository import has_active_oco_for_symbol
from execution.excel_live_core import ExcelLiveCore, CoreInputs

logger = logging.getLogger("gbm")

TIMEFRAME = os.getenv("BOT_TIMEFRAME", "15m")
CANDLE_LIMIT = int(os.getenv("BOT_CANDLE_LIMIT", "80"))
COOLDOWN_SECONDS = int(os.getenv("BOT_SIGNAL_COOLDOWN_SECONDS", "180"))

ALLOW_LIVE_SIGNALS = os.getenv("ALLOW_LIVE_SIGNALS", "false").strip().lower() == "true"

# USDT per trade (prevents NOTIONAL issues when you size in quote)
BOT_QUOTE_PER_TRADE = float(os.getenv("BOT_QUOTE_PER_TRADE", "15"))

# ---- Risk/Edge gates ----
MIN_MOVE_PCT = float(os.getenv("MIN_MOVE_PCT", "0.60"))

# NOTE: MA was removed from this generator. MA_GAP_PCT is kept for backward compatibility,
# but it is NOT used anymore.
MA_GAP_PCT = float(os.getenv("MA_GAP_PCT", "0.15"))

BUY_CONFIDENCE_MIN = float(os.getenv("BUY_CONFIDENCE_MIN", "0.70"))

ESTIMATED_ROUNDTRIP_FEE_PCT = float(os.getenv("ESTIMATED_ROUNDTRIP_FEE_PCT", "0.20"))
ESTIMATED_SLIPPAGE_PCT = float(os.getenv("ESTIMATED_SLIPPAGE_PCT", "0.15"))
TP_PCT = float(os.getenv("TP_PCT", "1.3"))
MIN_NET_PROFIT_PCT = float(os.getenv("MIN_NET_PROFIT_PCT", "0.60"))

BLOCK_SIGNALS_WHEN_ACTIVE_OCO = os.getenv("BLOCK_SIGNALS_WHEN_ACTIVE_OCO", "true").strip().lower() == "true"

GEN_DEBUG = os.getenv("GEN_DEBUG", "true").strip().lower() == "true"
GEN_LOG_EVERY_TICK = os.getenv("GEN_LOG_EVERY_TICK", "true").strip().lower() == "true"

# ---- Excel model path (sanitized) ----
EXCEL_MODEL_PATH = os.getenv("EXCEL_MODEL_PATH", "/var/data/DYZEN_CAPITAL_OS_AI_LIVE_CORE_READY.xlsx").strip()
if EXCEL_MODEL_PATH.lower().startswith("excel_model_path="):
    EXCEL_MODEL_PATH = EXCEL_MODEL_PATH.split("=", 1)[1].strip()

_last_emit_ts: float = 0.0
_last_signature: Optional[Tuple[str, str]] = None  # reserved (if you later want de-dup)

# ---- Exchange (Binance) ----
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "").strip()

EXCHANGE = ccxt.binance({
    "enableRateLimit": True,
    "apiKey": BINANCE_API_KEY,
    "secret": BINANCE_API_SECRET,
})

_CORE: Optional[ExcelLiveCore] = None


def _now_utc_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _parse_symbols() -> List[str]:
    raw = os.getenv("BOT_SYMBOLS", "").strip()
    if not raw:
        raw = os.getenv("SYMBOL_WHITELIST", "").strip()
    if not raw:
        raw = os.getenv("BOT_SYMBOL", "BTC/USDT").strip()

    syms: List[str] = []
    for s in raw.split(","):
        s = s.strip()
        if not s:
            continue
        syms.append(s.upper())
    return syms


SYMBOLS = _parse_symbols()


def _has_active_oco(symbol: str) -> bool:
    try:
        return has_active_oco_for_symbol(symbol)
    except Exception as e:
        # safe default: assume active_oco to avoid uncontrolled trades
        logger.warning(f"[GEN] ACTIVE_OCO_CHECK_FAIL | symbol={symbol} err={e} -> assume active_oco=True")
        return True


def _resolve_excel_path(env_path: str) -> str:
    candidates = [
        env_path,
        "/var/data/DYZEN_CAPITAL_OS_AI_LIVE_CORE_READY.xlsx",
        "/opt/render/project/src/assets/DYZEN_CAPITAL_OS_AI_LIVE_CORE_READY.xlsx",
    ]

    for p in candidates:
        if p and os.path.exists(p):
            return p

    try:
        assets_list = os.listdir("/opt/render/project/src/assets")
    except Exception:
        assets_list = []

    try:
        var_data_list = os.listdir("/var/data")
    except Exception:
        var_data_list = []

    raise FileNotFoundError(
        f"EXCEL_MODEL_NOT_FOUND | env={env_path} | "
        f"assets={assets_list} | var_data={var_data_list}"
    )


def _core() -> ExcelLiveCore:
    global _CORE
    if _CORE is None:
        resolved = _resolve_excel_path(EXCEL_MODEL_PATH)
        logger.info(
            f"[GEN] EXCEL_PATH | env={EXCEL_MODEL_PATH} resolved={resolved} "
            f"exists_env={os.path.exists(EXCEL_MODEL_PATH)}"
        )
        _CORE = ExcelLiveCore(resolved)
        logger.info(f"[GEN] EXCEL_CORE_LOADED | path={resolved}")
    return _CORE


def _pct(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a - b) / b * 100.0


def _sma(vals: List[float], n: int) -> float:
    if len(vals) < n:
        return sum(vals) / max(1, len(vals))
    w = vals[-n:]
    return sum(w) / n


def _atr_pct(ohlcv: List[List[float]], n: int = 14) -> float:
    if len(ohlcv) < n + 1:
        return 0.0
    trs: List[float] = []
    for i in range(-n, 0):
        high = float(ohlcv[i][2])
        low = float(ohlcv[i][3])
        prev_close = float(ohlcv[i - 1][4])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    atr = sum(trs) / n
    last_close = float(ohlcv[-1][4])
    return (atr / last_close) * 100.0 if last_close else 0.0


def _vol_regime(atr_pct: float) -> str:
    if atr_pct >= 2.0:
        return "EXTREME"
    if atr_pct <= 0.30:
        return "LOW"
    return "NORMAL"


def _edge_ok(atr_pct: float) -> Tuple[bool, str]:
    if atr_pct < MIN_MOVE_PCT:
        return False, f"ATR_TOO_LOW atr%={atr_pct:.2f} < MIN_MOVE_PCT={MIN_MOVE_PCT:.2f}"

    assumed_gross_edge = TP_PCT
    assumed_cost = ESTIMATED_ROUNDTRIP_FEE_PCT + ESTIMATED_SLIPPAGE_PCT
    assumed_net = assumed_gross_edge - assumed_cost

    if assumed_net < MIN_NET_PROFIT_PCT:
        return False, (
            "EDGE_TOO_SMALL "
            f"TP_PCT={assumed_gross_edge:.2f} cost={assumed_cost:.2f} net={assumed_net:.2f} "
            f"< MIN_NET_PROFIT_PCT={MIN_NET_PROFIT_PCT:.2f}"
        )

    if atr_pct < (assumed_gross_edge * 0.75):
        return False, f"ATR_BELOW_TP atr%={atr_pct:.2f} < 0.75*TP_PCT={assumed_gross_edge*0.75:.2f}"

    return True, "OK"


# -----------------------------
# MA REMOVED: NEW TREND / STRUCT / CONF
# -----------------------------

def _trend_strength(closes: List[float]) -> float:
    """
    MA-free trend strength using:
    - momentum over last N bars
    - slope proxy: short SMA vs longer SMA
    Output normalized to 0..1.
    """
    if len(closes) < 21:
        return 0.0

    last = closes[-1]
    prev = closes[-2]
    if prev <= 0 or last <= 0:
        return 0.0

    # momentum: last vs prev (very short)
    mom1 = (last - prev) / prev  # ~0.001 = 0.1%

    # swing momentum: last vs 10 bars ago
    base = closes[-11]
    mom10 = (last - base) / base if base else 0.0

    # slope proxy: SMA(5) vs SMA(20)
    sma5 = _sma(closes, 5)
    sma20 = _sma(closes, 20)
    slope = (sma5 - sma20) / sma20 if sma20 else 0.0

    # Normalize each component to 0..1 with conservative caps
    # 0% => 0.5, +0.5% => ~0.8, -0.5% => ~0.2
    def _norm(x: float, scale: float) -> float:
        return max(0.0, min(1.0, 0.5 + (x / scale)))

    n_mom1 = _norm(mom1, 0.005)     # 0.5% scale
    n_mom10 = _norm(mom10, 0.010)   # 1% scale
    n_slope = _norm(slope, 0.005)   # 0.5% scale

    # weighted blend
    trend = (0.45 * n_slope) + (0.35 * n_mom10) + (0.20 * n_mom1)
    return max(0.0, min(1.0, trend))


def _structure_ok(closes: List[float]) -> bool:
    """
    MA-free structure:
    - last > prev
    - SMA(5) > SMA(10) (local up-structure)
    - last 3 bars show at least 2 green closes (anti-chop)
    """
    if len(closes) < 12:
        return False

    last = closes[-1]
    prev = closes[-2]
    if not (last > prev):
        return False

    sma5 = _sma(closes, 5)
    sma10 = _sma(closes, 10)
    if not (sma5 > sma10):
        return False

    # last 3 closes: count "up" bars
    ups = 0
    for i in range(-3, 0):
        if closes[i] > closes[i - 1]:
            ups += 1
    return ups >= 2


def _volume_score(vols: List[float]) -> float:
    if len(vols) < 20:
        return 0.0
    v_last = vols[-1]
    v_avg = sum(vols[-20:]) / 20.0
    if v_avg <= 0:
        return 0.0
    ratio = v_last / v_avg
    return max(0.0, min(1.0, ratio / 2.0))


def _confidence_score(closes: List[float], ohlcv: List[List[float]]) -> float:
    """
    MA-free confidence:
    - cond_mom: last > prev
    - cond_struct: SMA(5) > SMA(10)
    - cond_atr: atr not extreme
    """
    if len(closes) < 12:
        return 0.0

    last = closes[-1]
    prev = closes[-2]
    atrp = _atr_pct(ohlcv, 14)

    sma5 = _sma(closes, 5)
    sma10 = _sma(closes, 10)

    cond_mom = 1.0 if last > prev else 0.0
    cond_struct = 1.0 if sma5 > sma10 else 0.0
    cond_atr = 1.0 if atrp < 2.0 else 0.0

    return (0.45 * cond_struct) + (0.35 * cond_mom) + (0.20 * cond_atr)


def _risk_state(vol_regime: str, ai_score: float) -> str:
    if vol_regime == "EXTREME":
        return "KILL"
    if ai_score < 0.45:
        return "REDUCE"
    return "OK"


def _cooldown_ok() -> bool:
    global _last_emit_ts
    return (time.time() - _last_emit_ts) >= COOLDOWN_SECONDS


def _emit(signal: Dict[str, Any], outbox_path: str) -> None:
    global _last_emit_ts
    append_signal(signal, outbox_path)
    _last_emit_ts = time.time()


def _get_outbox_path() -> str:
    return (
        os.getenv("OUTBOX_PATH")
        or os.getenv("SIGNAL_OUTBOX_PATH")
        or "/var/data/signal_outbox.json"
    )


def generate_signal() -> Optional[Dict[str, Any]]:
    """
    Excel Live Core based generator (MA REMOVED):
    - If no active OCO: emits TRADE only when final_trade_decision == EXECUTE.
    - If active OCO: can emit SELL if risk_state == KILL (protective override).
    """
    outbox_path = _get_outbox_path()

    if not _cooldown_ok():
        return None

    core = _core()

    for symbol in SYMBOLS:
        active_oco = _has_active_oco(symbol)

        try:
            t0 = time.time()
            ohlcv = EXCHANGE.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=CANDLE_LIMIT)
            dt_ms = int((time.time() - t0) * 1000)
            if GEN_DEBUG:
                logger.info(f"[GEN] FETCH_OK | symbol={symbol} tf={TIMEFRAME} candles={len(ohlcv) if ohlcv else 0} dt={dt_ms}ms")
        except Exception as e:
            logger.exception(f"[GEN] FETCH_FAIL | symbol={symbol} tf={TIMEFRAME} err={e}")
            continue

        if not ohlcv or len(ohlcv) < 30:
            if GEN_LOG_EVERY_TICK:
                logger.info(f"[GEN] NO_SIGNAL | symbol={symbol} reason=not_enough_candles got={len(ohlcv) if ohlcv else 0} need>=30")
            continue

        closes = [float(c[4]) for c in ohlcv]
        vols = [float(c[5]) for c in ohlcv]
        last = closes[-1]
        prev = closes[-2]

        atrp = _atr_pct(ohlcv, 14)
        vol_reg = _vol_regime(atrp)

        trend = _trend_strength(closes)
        struct_ok = _structure_ok(closes)
        vol_score = _volume_score(vols)
        conf = _confidence_score(closes, ohlcv)

        # First pass ai_score without risk (risk uses ai_score)
        tmp_inp = CoreInputs(
            trend_strength=trend,
            structure_ok=struct_ok,
            volume_score=vol_score,
            risk_state="OK",
            confidence_score=conf,
            volatility_regime=vol_reg,
        )
        tmp_dec = core.decide(tmp_inp)
        ai_score = float(tmp_dec["ai_score"])

        risk = _risk_state(vol_reg, ai_score)

        inp = CoreInputs(
            trend_strength=trend,
            structure_ok=struct_ok,
            volume_score=vol_score,
            risk_state=risk,
            confidence_score=conf,
            volatility_regime=vol_reg,
        )
        decision = core.decide(inp)

        if GEN_DEBUG:
            logger.info(
                f"[GEN] CORE_DECISION | symbol={symbol} "
                f"ai={decision['ai_score']:.3f} macro={decision['macro_gate']} strat={decision['active_strategy']} "
                f"final={decision['final_trade_decision']} risk={risk} volReg={vol_reg} atr%={atrp:.2f} "
                f"last={last:.6f} prev={prev:.6f} outbox={outbox_path}"
            )
            logger.info(
                f"[GEN] DIAG | symbol={symbol} "
                f"last={last:.6f} prev={prev:.6f} atr%={atrp:.2f} volReg={vol_reg} "
                f"trend={trend:.3f} conf={conf:.3f} struct={struct_ok} vol_score={vol_score:.3f} "
                f"macro={decision['macro_gate']} active={decision['active_strategy']} final={decision['final_trade_decision']} "
                f"ai={decision['ai_score']:.3f}"
            )

        # Protective SELL if active OCO and risk is KILL
        if active_oco and risk == "KILL":
            signal_id = str(uuid.uuid4())
            sig = {
                "signal_id": signal_id,
                "ts_utc": _now_utc_iso(),
                "certified_signal": True,
                "final_verdict": "SELL",
                "meta": {
                    "source": "DYZEN_EXCEL_LIVE_CORE",
                    "symbol": symbol,
                    "reason": "RISK_KILL_OVERRIDE",
                    "decision": decision,
                },
                "execution": {
                    "symbol": symbol,
                    "direction": "LONG",
                    "entry": {"type": "MARKET"},
                }
            }
            _emit(sig, outbox_path)
            return sig

        # If active OCO → we do not open new TRADE
        if active_oco and BLOCK_SIGNALS_WHEN_ACTIVE_OCO:
            continue

        # TRADE only if final decision says EXECUTE
        if decision["final_trade_decision"] != "EXECUTE":
            continue

        # -----------------------------
        # EXTRA LIVE GUARDS (fee-aware)
        # -----------------------------

        # 1) Confidence floor (extra check)
        if conf < BUY_CONFIDENCE_MIN:
            if GEN_DEBUG:
                logger.info(
                    f"[GEN] BLOCKED_BY_CONF | symbol={symbol} conf={conf:.3f} < BUY_CONFIDENCE_MIN={BUY_CONFIDENCE_MIN:.3f}"
                )
            continue

        # 2) Fee-aware edge gate
        ok_edge, edge_reason = _edge_ok(atrp)
        if not ok_edge:
            if GEN_DEBUG:
                logger.info(f"[GEN] BLOCKED_BY_EDGE | symbol={symbol} reason={edge_reason}")
            continue

        # Safety: don't emit live trades if not allowed
        if not ALLOW_LIVE_SIGNALS:
            if GEN_DEBUG:
                logger.info(f"[GEN] BLOCKED_BY_ENV | symbol={symbol} reason=ALLOW_LIVE_SIGNALS=false")
            continue

        signal_id = str(uuid.uuid4())
        sig = {
            "signal_id": signal_id,
            "ts_utc": _now_utc_iso(),
            "certified_signal": True,
            "final_verdict": "TRADE",
            "meta": {
                "source": "DYZEN_EXCEL_LIVE_CORE",
                "symbol": symbol,
                "decision": decision,
            },
            "execution": {
                "symbol": symbol,
                "direction": "LONG",
                "entry": {"type": "MARKET"},
                "quote_amount": BOT_QUOTE_PER_TRADE,
            }
        }

        _emit(sig, outbox_path)
        return sig

    return None


# -----------------------------
# COMPATIBILITY ENTRYPOINTS
# -----------------------------

def run_once(*args, **kwargs) -> Optional[Dict[str, Any]]:
    """
    Backwards-compatible entrypoint expected by bootstrap:
    some versions do: `from execution.signal_generator import run_once`.
    We ignore args/kwargs intentionally.
    """
    return generate_signal()
