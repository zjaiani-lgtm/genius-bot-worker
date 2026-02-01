# execution/signal_client.py

import json
from pathlib import Path
from execution.logger import log_info, log_warning
from execution.db.repository import log_event

SIGNAL_FILE = Path("signal_outbox.json")


def _validate(signal: dict) -> bool:
    required = ["signal_id", "timestamp_utc", "final_verdict", "certified_signal", "confidence", "mode_allowed"]
    for k in required:
        if k not in signal:
            log_warning(f"Signal missing field: {k}")
            return False
    return True


def get_latest_signal():
    if not SIGNAL_FILE.exists():
        return None

    try:
        signal = json.loads(SIGNAL_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        log_warning(f"Signal JSON read error: {e}")
        return None

    if not isinstance(signal, dict) or not _validate(signal):
        return None

    # Always log that we saw a signal (audit trail)
    log_event("SIGNAL_SEEN", f"signal_id={signal.get('signal_id')} verdict={signal.get('final_verdict')}")
    log_info(f"SIGNAL_SEEN: {signal.get('signal_id')} | verdict={signal.get('final_verdict')}")

    return signal

