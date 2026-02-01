# execution/signal_client.py

import json
from pathlib import Path
from execution.logger import log_info, log_warning
from execution.db.repository import log_event, has_executed_signal

SIGNAL_FILE = Path("signal_outbox.json")


def _validate(signal: dict) -> bool:
    required = ["signal_id", "timestamp_utc", "final_verdict", "certified_signal", "confidence", "mode_allowed"]
    for k in required:
        if k not in signal:
            log_warning(f"Signal missing field: {k}")
            return False
    return True


def _load_signal() -> dict | None:
    if not SIGNAL_FILE.exists():
        return None
    try:
        return json.loads(SIGNAL_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        log_warning(f"Signal JSON read error: {e}")
        return None


def _save_signal(signal: dict) -> None:
    # Pretty format for readability
    SIGNAL_FILE.write_text(json.dumps(signal, indent=2, ensure_ascii=False), encoding="utf-8")


def acknowledge_processed(signal: dict, reason: str) -> None:
    """
    Marks current signal file as processed so worker stops re-reading it forever.
    Safe even if called multiple times.
    """
    try:
        signal["processed"] = True
        signal["processed_reason"] = reason
        _save_signal(signal)
        log_info(f"SIGNAL_ACK: processed=true | reason={reason}")
        log_event("SIGNAL_ACK", f"id={signal.get('signal_id')} reason={reason}")
    except Exception as e:
        log_warning(f"Signal ACK write failed: {e}")


def get_latest_signal():
    """
    Returns:
      - dict signal if should be handled now
      - None if no signal / invalid / already processed / already executed
    """

    signal = _load_signal()
    if signal is None:
        return None

    if not isinstance(signal, dict) or not _validate(signal):
        return None

    # If already marked processed in file -> ignore quietly
    if signal.get("processed") is True:
        return None

    signal_id = signal.get("signal_id")
    verdict = signal.get("final_verdict")

    # If already executed -> ACK and stop spam
    if has_executed_signal(signal_id):
        acknowledge_processed(signal, "already_executed")
        return None

    # Log that we saw it (once, before handling)
    log_event("SIGNAL_SEEN", f"signal_id={signal_id} verdict={verdict}")
    log_info(f"SIGNAL_SEEN: {signal_id} | verdict={verdict}")

    return signal
