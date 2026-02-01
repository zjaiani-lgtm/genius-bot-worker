# execution/startup_sync.py

from execution.logger import log_info, log_warning
from execution.db.repository import get_open_positions, update_system_state, log_event
from execution.config import MODE


def run_startup_sync() -> bool:
    """
    Returns True if sync ok and trading can proceed.
    In DEMO: always ok (but logs what it checked).
    In LIVE: will be real exchange checks later.
    """

    open_positions = get_open_positions()

    if not open_positions:
        log_info("STARTUP_SYNC: no open positions in DB -> OK")
        update_system_state(startup_sync_ok=True, status="RUNNING")
        log_event("STARTUP_SYNC_OK", "no_open_positions")
        return True

    # DEMO behavior: assume OK (for now)
    if MODE == "DEMO":
        log_info(f"STARTUP_SYNC: DEMO mode -> assume OK | db_open_positions={len(open_positions)}")
        update_system_state(startup_sync_ok=True, status="RUNNING")
        log_event("STARTUP_SYNC_OK", f"demo_assumed_ok count={len(open_positions)}")
        return True

    # LIVE behavior placeholder (we'll implement with Binance adapter)
    log_warning("STARTUP_SYNC: LIVE mode not implemented -> PAUSE")
    update_system_state(startup_sync_ok=False, status="PAUSED")
    log_event("STARTUP_SYNC_FAIL", "live_sync_not_implemented")
    return False
