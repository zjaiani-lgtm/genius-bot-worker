import os
from execution.db.repository import get_system_state


def _to_bool01(v):
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return int(v) != 0
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes", "y", "on")
    return False


def is_kill_switch_active() -> bool:
    # Env kill switch always wins
    env_kill = os.getenv("KILL_SWITCH", "false").lower() == "true"
    if env_kill:
        return True

    # DB kill switch
    try:
        raw = get_system_state()
        # tuple: (id, status, startup_sync_ok, kill_switch, updated_at)
        if isinstance(raw, (list, tuple)) and len(raw) >= 4:
            return _to_bool01(raw[3])
        if isinstance(raw, dict):
            return _to_bool01(raw.get("kill_switch"))
    except Exception:
        # conservative: if DB can't be read, do NOT block by default
        # (you can flip this to True if you want "fail closed")
        return False

    return False
