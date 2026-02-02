# execution/signal_client.py

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional


# Default path on Render persistent disk
DEFAULT_OUTBOX_PATH = os.getenv("SIGNAL_OUTBOX_PATH", "/var/data/signal_outbox.json")


class SignalClientError(Exception):
    pass


# ---------------- core helpers ----------------

def ensure_signal_outbox_exists(path: str = DEFAULT_OUTBOX_PATH) -> Path:
    """
    Ensures that SIGNAL_OUTBOX exists and contains valid JSON:
      {"signals": []}

    If file is missing -> creates it.
    If file exists but is corrupt/invalid -> heals it.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    if not p.exists():
        _atomic_write_json(p, {"signals": []})
        return p

    # heal if invalid
    try:
        raw = p.read_text(encoding="utf-8")
        data = json.loads(raw) if raw.strip() else {}
        if not isinstance(data, dict) or "signals" not in data or not isinstance(data["signals"], list):
            raise ValueError("invalid schema")
    except Exception:
        _atomic_write_json(p, {"signals": []})

    return p


def _read_outbox(path: str = DEFAULT_OUTBOX_PATH) -> Dict[str, Any]:
    p = ensure_signal_outbox_exists(path)
    try:
        raw = p.read_text(encoding="utf-8")
        return json.loads(raw) if raw.strip() else {"signals": []}
    except Exception as e:
        raise SignalClientError(f"Failed to read outbox JSON | path={p} | err={e}") from e


def _write_outbox(payload: Dict[str, Any], path: str = DEFAULT_OUTBOX_PATH) -> None:
    p = ensure_signal_outbox_exists(path)
    if not isinstance(payload, dict):
        raise SignalClientError("Outbox payload must be a dict")
    if "signals" not in payload or not isinstance(payload["signals"], list):
        raise SignalClientError("Outbox payload must contain list field: signals")
    _atomic_write_json(p, payload)


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    """
    Atomic write: write temp -> fsync -> replace.
    Prevents half-written JSON on crashes/redeploys.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_fd, tmp_name = tempfile.mkstemp(prefix="outbox_", suffix=".json", dir=str(path.parent))
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, path)
    finally:
        try:
            if os.path.exists(tmp_name):
                os.remove(tmp_name)
        except Exception:
            pass


# ---------------- NEW API (optional) ----------------

def pop_next_signal(path: str = DEFAULT_OUTBOX_PATH) -> Optional[Dict[str, Any]]:
    """
    Pops first signal FIFO and persists.
    """
    data = _read_outbox(path)
    signals: List[Dict[str, Any]] = data.get("signals", [])
    if not signals:
        return None
    sig = signals.pop(0)
    _write_outbox({"signals": signals}, path)
    return sig


def append_signal(signal: Dict[str, Any], path: str = DEFAULT_OUTBOX_PATH) -> None:
    """
    Appends a signal to outbox queue.
    """
    if not isinstance(signal, dict):
        raise SignalClientError("signal must be a dict")
    data = _read_outbox(path)
    signals: List[Dict[str, Any]] = data.get("signals", [])
    signals.append(signal)
    _write_outbox({"signals": signals}, path)


# ---------------- BACKWARD-COMPAT API (for your current main.py) ----------------
# main.py imports these names:
#   from execution.signal_client import get_latest_signal, acknowledge_processed

def get_latest_signal(path: str = DEFAULT_OUTBOX_PATH) -> Optional[Dict[str, Any]]:
    """
    Backward-compatible:
    Returns the *first* signal in queue (FIFO) WITHOUT removing it.
    Returns None if no signals.
    """
    data = _read_outbox(path)
    signals: List[Dict[str, Any]] = data.get("signals", [])
    if not signals:
        return None
    sig = signals[0]
    if not isinstance(sig, dict):
        return None
    return sig


def acknowledge_processed(signal_id: str, path: str = DEFAULT_OUTBOX_PATH) -> bool:
    """
    Backward-compatible:
    Removes a signal from outbox after it is processed.

    Strategy:
      - Remove the first signal whose signal_id matches provided signal_id.
      - If not found, do nothing (return False).
    """
    if not signal_id:
        return False

    data = _read_outbox(path)
    signals: List[Dict[str, Any]] = data.get("signals", [])

    new_signals: List[Dict[str, Any]] = []
    removed = False

    for s in signals:
        if not removed and isinstance(s, dict) and s.get("signal_id") == signal_id:
            removed = True
            continue
        new_signals.append(s)

    if removed:
        _write_outbox({"signals": new_signals}, path)

    return removed
