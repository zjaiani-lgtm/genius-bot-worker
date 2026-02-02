# execution/startup_sync.py
from __future__ import annotations

import os

from execution.logger import log_info, log_warning
from execution.db.repository import (
    get_open_positions,
    update_system_state,
    log_event,
    get_system_state,
)
from execution.config import MODE
from execution.exchange_client import BinanceSpotClient


def run_startup_sync() -> bool:
    """
    Startup Sync responsibilities:

    - Always:
        - Respect kill-switch gate (ENV/DB)
    - DEMO:
        - OK (positions handled virtually)
        - Marks system ACTIVE + startup_sync_ok=True
    - TESTNET / LIVE:
        - 1) Public connectivity check
        - 2) Private connectivity check (keys)
        - If DB has OPEN positions -> PAUSE (until full reconcile is implemented)
        - If DB has no open positions -> ACTIVE
    """

    mode = (MODE or "DEMO").upper()

    # --- 0) Kill switch gate (ENV OR DB) ---
    env_kill = os.getenv("KILL_SWITCH", "false").lower() == "true"

    try:
        st = get_system_state()
        # supports dict or tuple
        db_kill = (st.get("kill_switch") if isinstance(st, dict) else st[3]) if st else 0
        db_kill = bool(int(db_kill))
    except Exception:
        db_kill = False

    if env_kill or db_kill:
        log_warning(f"STARTUP_SYNC: {mode} -> KILL_SWITCH=ON -> KILLED")
        update_system_state(startup_sync_ok=False, status="KILLED", kill_switch=True)
        log_event("KILL_SWITCH_ACTIVATED", f"mode={mode} env={env_kill} db={db_kill}")
        return False

    # --- 1) Read DB open positions ---
    open_positions = get_open_positions()
    open_count = len(open_positions) if open_positions else 0

    # --- 2) DEMO: always OK ---
    if mode == "DEMO":
        if open_count == 0:
            log_info("STARTUP_SYNC: DEMO -> no open positions in DB -> OK")
            update_system_state(startup_sync_ok=True, status="ACTIVE")
            log_event("STARTUP_SYNC_OK", "demo_no_open_positions")
            return True

        log_info(f"STARTUP_SYNC: DEMO -> assume OK | db_open_positions={open_count}")
        update_system_state(startup_sync_ok=True, status="ACTIVE")
        log_event("STARTUP_SYNC_OK", f"demo_assumed_ok count={open_count}")
        return True

    # --- 3) TESTNET / LIVE: connectivity checks ---
    try:
        client = BinanceSpotClient()

        # Safe diagnostics (no secrets)
        diag = client.diagnostics()
        log_info(
            f"STARTUP_SYNC: {mode} -> DIAG base={diag.get('base_public')} "
            f"key_len={diag.get('key_len')} secret_len={diag.get('secret_len')} key_prefix={diag.get('key_prefix')}"
        )

        pub = client.public_health_check()
        log_info(f"STARTUP_SYNC: {mode} -> PUBLIC_OK | symbols={pub.get('symbols')}")
        log_event("EXCHANGE_PUBLIC_OK", f"mode={mode} symbols={pub.get('symbols')}")

        priv = client.private_health_check()
        log_info(f"STARTUP_SYNC: {mode} -> PRIVATE_OK | free_keys_sample={priv.get('free_keys')}")
        log_event("EXCHANGE_PRIVATE_OK", f"mode={mode} free_keys_sample={priv.get('free_keys')}")

    except Exception as e:
        log_warning(f"STARTUP_SYNC: {mode} -> EXCHANGE_CONNECT_FAILED -> PAUSE | err={e}")
        update_system_state(startup_sync_ok=False, status="PAUSED")
        log_event("EXCHANGE_CONNECT_FAIL", f"mode={mode} err={e}")
        return False

    # --- 4) Safety rule: if DB says OPEN positions exist, reconcile required ---
    if open_count > 0:
        preview = []
        try:
            for p in open_positions[:3]:
                if isinstance(p, dict):
                    preview.append(f"{p.get('symbol')} {p.get('side')} size={p.get('size')}")
                else:
                    # fallback if row/tuple-like
                    preview.append(str(p))
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

    # --- 5) No open positions in DB -> OK ---
    log_info(f"STARTUP_SYNC: {mode} -> no open positions in DB -> OK")
    update_system_state(startup_sync_ok=True, status="ACTIVE")
    log_event("STARTUP_SYNC_OK", "no_open_positions")
    return True
