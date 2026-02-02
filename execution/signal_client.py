# execution/signal_client.py

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional


DEFAULT_OUTBOX_PATH = os.getenv("SIGNAL_OUTBOX_PATH", "/var/data/signal_outbox.json")


class SignalClientError(Exception):
    pass


def ensure_signal_outbox_exists(path: str = DEFAULT_OUTBOX_PATH) -> Path:
    p = Path(path)

    # ensure parent dir exists (Render persistent disk mounted at /var/data)
    p.parent.mkdir(parents=True, exist_ok=True)

    if not p.exists():
        _atomic_write_json(p, {"signals": []})

    # if exists but empty/corrupt, heal it (optional but very helpful)
    try:
        data = json.loads(p.read_text(encoding="utf-8") or "")
        if not isinstance(data, dict) or "signals" not in data or not isinstance(data["signals"], list):
            raise ValueError("invalid schema")
    except Exception:
        _atomic_write_json(p, {"signals": []})

    return p


def read_outbox(path: str = DEFAULT_OUTBOX_PATH) -> Dict[str, Any]:
    p = ensure_signal_outbox_exists(path)
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        raise SignalClientError(f"Failed to read outbox JSON | path={p} | err={e}") from e


def pop_next_signal(path: str = DEFAULT_OUTBOX_PATH) -> Optional[Dict[str, Any]]:
    """
    Pop first signal from the queue (FIFO) and persist updated file atomically.
    """
    p = ensure_signal_outbox_exists(path)
    data = read_outbox(str(p))
    signals: List[Dict[str, Any]] = data.get("signals", [])

    if not signals:
        return None

    sig = signals.pop(0)
    _atomic_write_json(p, {"signals": signals})
    return sig


def append_signal(signal: Dict[str, Any], path: str = DEFAULT_OUTBOX_PATH) -> None:
    p = ensure_signal_outbox_exists(path)
    data = read_outbox(str(p))
    signals: List[Dict[str, Any]] = data.get("signals", [])
    signals.append(signal)
    _atomic_write_json(p, {"signals": signals})


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    """
    Atomic write: write to temp file then replace -> no half-written JSON.
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
