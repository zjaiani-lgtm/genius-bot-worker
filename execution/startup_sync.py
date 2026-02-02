# execution/startup_sync.py

from __future__ import annotations

from execution.logger import log_info, log_warning
from execution.db.repository import (
    get_open_positions,
    update_system_state,
    log_event,
)
from execution.config import MODE

# ✅ IMPORTANT: import path should be absolute from execution package
from execution.exchange_client import BinanceSpotClient


def run_startup_sync() -> bool:
    """
    Startup Sync responsibilities:

    - DEMO:
        - If no open positions -> OK
        - If open positions -> still OK (demo assumed)
    - TESTNET / LIVE:
        - Always do connectivity check (fetch_balance)
        - If DB has open positions -> PAUSE (until reconciliation implemented)
        - If DB has no open positions -> OK

    Returns True if sync ok and trading can proceed, else False.
    """

    mode = (MODE or "DEMO").upper()

    # --- 1) Read DB state ---
    open_positions = get_open_positions()
    open_count = len(open_positions) if open_positions else 0

    # --- 2) DEMO: keep it simple and safe ---
    if mode == "DEMO":
        if open_count == 0:
            log_info("STARTUP_SYNC: DEMO -> no open positions in DB -> OK")
            update_system_state(startup_sync_ok=True, status="RUNNING")
            log_event("STARTUP_SYNC_OK", "demo_no_open_positions")
            return True

        # In DEMO, we still proceed (as you had)
        log_info(f"STARTUP_SYNC: DEMO -> assume OK | db_open_positions={open_count}")
        update_system_state(startup_sync_ok=True, status="RUNNING")
        log_event("STARTUP_SYNC_OK", f"demo_assumed_ok count={open_count}")
        return True

    # --- 3) TESTNET / LIVE: do exchange connectivity check ---
    # We don't place any orders here. Only validate that API/endpoint works.
    try:
        client = BinanceSpotClient()
        bal = client.fetch_balance()

        free = bal.get("free") or {}
        usdt_free = free.get("USDT")

        log_info(f"STARTUP_SYNC: {mode} -> EXCHANGE_OK | USDT_free={usdt_free}")
        log_event("EXCHANGE_CONNECT_OK", f"mode={mode} usdt_free={usdt_free}")

    except Exception as e:
        # If we can't even connect/fetch balance -> PAUSE
        log_warning(f"STARTUP_SYNC: {mode} -> EXCHANGE_CONNECT_FAILED -> PAUSE | err={e}")
        update_system_state(startup_sync_ok=False, status="PAUSED")
        log_event("EXCHANGE_CONNECT_FAIL", f"mode={mode} err={e}")
        return False

    # --- 4) Safety rule: if DB says OPEN positions exist, we must reconcile with exchange ---
    # (Not implemented yet, so PAUSE is correct behavior)
    if open_count > 0:
        # Show a short preview for easier debugging
        preview = []
        try:
            for p in open_positions[:3]:
                # Works whether p is dict or object (best effort)
                sym = p.get("symbol") if isinstance(p, dict) else getattr(p, "symbol", None)
                side = p.get("side") if isinstance(p, dict) else getattr(p, "side", None)
                size = p.get("size") if isinstance(p, dict) else getattr(p, "size", None)
                preview.append(f"{sym} {side} size={size}")
        except Exception:
            pass

        preview_txt = " | ".join(preview) if preview else "n/a"

        log_warning(
            f"STARTUP_SYNC: {mode} -> DB has OPEN positions ({open_count}) "
            f"but reconcile not implemented -> PAUSE | preview={preview_txt}"
        )
        update_system_state(startup_sync_ok=False, status="PAUSED")
        log_event("STARTUP_SYNC_FAIL", f"open_positions_require_reconcile count={open_count}")
        return False

    # --- 5) No open positions in DB -> OK to proceed ---
    log_info(f"STARTUP_SYNC: {mode} -> no open positions in DB -> OK")
    update_system_state(startup_sync_ok=True, status="RUNNING")
    log_event("STARTUP_SYNC_OK", "no_open_positions")
    return True
