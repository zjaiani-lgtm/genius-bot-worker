# execution/kill_switch.py
import os
import logging
from typing import Any

from execution.db.repository import get_system_state

logger = logging.getLogger("gbm")


def _to_bool01(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return int(v) != 0
    if isinstance(v, str):
        s = v.strip().lower()
        return s in ("1", "true", "yes", "y", "on")
    return False


def is_kill_switch_active() -> bool:
    """
    Absolute authority kill switch.
    - ENV KILL_SWITCH=true -> block
    - DB system_state.kill_switch=1 -> block
    If DB read fails -> conservative True (block).
    """
    # 1) ENV
    env_kill = os.getenv("KILL_SWITCH", "false").lower() == "true"
    if env_kill:
        return True

    # 2) DB
    try:
        raw = get_system_state()
        # expected tuple schema: (id, status, startup_sync_ok, kill_switch, updated_at)
        if isinstance(raw, (list, tuple)):
            kill = raw[3] if len(raw) > 3 else 0
            return _to_bool01(kill)
        if isinstance(raw, dict):
            return _to_bool01(raw.get("kill_switch"))
        return False
    except Exception as e:
        logger.error(f"KILL_SWITCH_READ_FAIL | err={e} -> assume ACTIVE")
        return True


# Optional backward compatibility if somewhere you used old name:
def is_kill_active() -> bool:
    return is_kill_switch_active()
