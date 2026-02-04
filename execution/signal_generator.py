# execution/signal_client.py
import json
import os
import hashlib
import logging
from typing import Any, Dict, List, Optional
from tempfile import NamedTemporaryFile

logger = logging.getLogger("gbm")

ALLOWED_ACTIONS = {"SELL", "BUY", "HOLD", "TRADE"}
ALLOWED_SELL_MODES = {"EMERGENCY", "PARTIAL", "NORMAL"}


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _fingerprint(signal: Dict[str, Any]) -> str:
    """
    Stable fingerprint across restarts.
    Prefer Excel decision keys if present; otherwise use execution payload.
    """
    action = str(signal.get("action") or "").upper().strip()
    mode = str(signal.get("mode") or "").upper().strip()
    pct = signal.get("pct")

    # Excel-style fingerprint
    if action in ("SELL", "BUY", "HOLD"):
        symbol = str(signal.get("symbol") or "").upper().strip()
        pct_f = _safe_float(pct)
        base = f"v2:{action}:{mode}:{pct_f}:{symbol}"
        return hashlib.sha256(base.encode("utf-8")).hexdigest()

    # Generator-style / trade payload
    execution = signal.get("execution") or {}
    symbol = str(execution.get("symbol") or "").upper().strip()
    direction = str(execution.get("direction") or "").upper().strip()
    entry = (execution.get("entry") or {}).get("type")
    entry_type = str(entry or "").upper().strip()
    pos_size = _safe_float(execution.get("position_size"))
    # Use "TRADE" as action
    base = f"v1:TRADE:{symbol}:{direction}:{entry_type}:{pos_size}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def validate_signal(signal: Dict[str, Any]) -> None:
    """
    Strict contract validation:
    Supports:
    - Excel decision schema: {action, mode, pct, symbol}
    - Generator schema: {final_verdict, certified_signal, execution{...}}
    """
    if not isinstance(signal, dict):
        raise ValueError("SIGNAL_NOT_DICT")

    # Case 1: Excel decision
    if "action" in signal:
        action = str(signal.get("action") or "").upper().strip()
        if action not in ALLOWED_ACTIONS:
            raise ValueError("INVALID_ACTION")

        if action == "SELL":
            mode = str(signal.get("mode") or "").upper().strip()
            if mode not in ALLOWED_SELL_MODES:
                raise ValueError("INVALID_SELL_MODE")

            pct = _safe_float(signal.get("pct"))
            if pct is None or not (0.0 < pct <= 1.0):
                raise ValueError("INVALID_SELL_PCT")

            symbol = str(signal.get("symbol") or "").strip()
            if not symbol:
                raise ValueError("MISSING_SYMBOL")

        return

    # Case 2: Generator / trade payload
    verdict = str(signal.get("final_verdict") or "").upper().strip()
    if verdict not in ("TRADE", "HOLD"):
        raise ValueError("INVALID_VERDICT")

    if signal.get("certified_signal") is not True:
        raise ValueError("NOT_CERTIFIED")

    execution = signal.get("execution") or {}
    symbol = execution.get("symbol")
    direction = str(execution.get("direction") or "").upper().strip()
    entry = execution.get("entry") or {}
    entry_type = str(entry.get("type") or "").upper().strip()

    if not symbol:
        raise ValueError("MISSING_EXEC_SYMBOL")
    if direction not in ("LONG", "SHORT"):
        raise ValueError("INVALID_DIRECTION")
    if entry_type not in ("MARKET", "LIMIT"):
        raise ValueError("INVALID_ENTRY_TYPE")

    # Optional numeric sanity
    ps = _safe_float(execution.get("position_size"))
    if ps is not None and ps <= 0:
        raise ValueError("INVALID_POSITION_SIZE")

    # mode_allowed sanity (if present)
    ma = signal.get("mode_allowed")
    if ma is not None and not isinstance(ma, dict):
        raise ValueError("INVALID_MODE_ALLOWED")


def _read_outbox(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {"signals": []}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        return {"signals": []}
    if "signals" not in data or not isinstance(data["signals"], list):
        data["signals"] = []
    return data


def _atomic_write_json(path: str, data: Dict[str, Any]) -> None:
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)

    with NamedTemporaryFile("w", delete=False, dir=d, encoding="utf-8") as tf:
        json.dump(data, tf, ensure_ascii=False, indent=2)
        tf.flush()
        os.fsync(tf.fileno())
        tmp = tf.name

    os.replace(tmp, path)


def append_signal(signal: Dict[str, Any], outbox_path: str) -> None:
    validate_signal(signal)

    # attach stable fingerprint for downstream dedupe
    fp = _fingerprint(signal)
    signal["_fingerprint"] = fp

    data = _read_outbox(outbox_path)
    signals: List[Dict[str, Any]] = data.get("signals", [])

    # Soft outbox-level dedupe (still DB dedupe is the real protection)
    if any((s.get("_fingerprint") == fp) for s in signals[-50:]):
        logger.info(f"OUTBOX_DEDUPED | fingerprint={fp}")
        return

    signals.append(signal)
    data["signals"] = signals
    _atomic_write_json(outbox_path, data)
