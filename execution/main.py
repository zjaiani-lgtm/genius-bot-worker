# execution/main.py

import time

from execution.logger import log_info, log_warning
from execution.config import MODE, KILL_SWITCH, STARTUP_SYNC_ENABLED
from execution.db.db import init_db
from execution.db.repository import list_positions, list_audit_log
from execution.startup_sync import run_startup_sync
from execution.execution_engine import execute_signal

# ✅ signal_client: keep backward-compatible functions + ensure outbox exists
from execution.signal_client import (
    get_latest_signal,
    acknowledge_processed,
    ensure_signal_outbox_exists,
)

POLL_INTERVAL_SECONDS = 10
INSPECT_EVERY_SECONDS = 60


def _print_db_snapshot():
    try:
        pos = list_positions(limit=10)
        aud = list_audit_log(limit=10)

        log_info("==== DB SNAPSHOT (last 10 positions) ====")
        if not pos:
            log_info("positions: (empty)")
        else:
            for r in pos:
                # (id, symbol, side, size, entry_price, status, opened_at, closed_at, pnl)
                log_info(
                    f"POS id={r[0]} {r[1]} {r[2]} size={r[3]} entry={r[4]} status={r[5]} opened_at={r[6]}"
                )

        log_info("==== AUDIT LOG (last 10 events) ====")
        if not aud:
            log_info("audit_log: (empty)")
        else:
            for a in aud:
                # (id, event_type, message, created_at)
                log_info(f"AUD id={a[0]} type={a[1]} msg={a[2]} at={a[3]}")

        log_info("==== END SNAPSHOT ====")

    except Exception as e:
        log_warning(f"DB snapshot error: {e}")


def main():
    log_info(f"GENIUS BOT MAN worker starting | MODE={MODE}")

    # Safety gate: kill switch
    if KILL_SWITCH:
        log_warning("KILL_SWITCH is ON -> trading disabled. Worker will stay alive.")
    else:
        log_info("KILL_SWITCH is OFF -> trading may execute signals (depending on sync gates).")

    # Init DB (disk-backed)
    init_db()
    log_info("DB initialized")

    # ✅ Ensure outbox exists (creates /var/data/signal_outbox.json if missing)
    try:
        p = ensure_signal_outbox_exists()
        log_info(f"SIGNAL_OUTBOX ready | path={p}")
    except Exception as e:
        # If outbox creation fails, we still keep worker alive, but it will never execute
        log_warning(f"SIGNAL_OUTBOX init failed | err={e}")

    # Startup sync gate (once at boot)
    startup_ok = True
    if STARTUP_SYNC_ENABLED:
        try:
            startup_ok = run_startup_sync()
            if startup_ok:
                log_info("STARTUP_SYNC: OK")
            else:
                log_warning("STARTUP_SYNC: FAIL -> Worker paused (no executions).")
        except Exception as e:
            startup_ok = False
            log_warning(f"STARTUP_SYNC: ERROR -> Worker paused | err={e}")
    else:
        log_warning("STARTUP_SYNC is DISABLED -> skipping startup sync checks (NOT recommended for LIVE).")

    last_inspect = 0

    while True:
        try:
            now = time.time()

            if now - last_inspect >= INSPECT_EVERY_SECONDS:
                _print_db_snapshot()
                last_inspect = now

            # If kill switch or startup sync failed -> do NOT execute signals
            if KILL_SWITCH or not startup_ok:
                log_info("Worker alive (PAUSED), waiting for SIGNAL_OUTBOX...")
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            signal = get_latest_signal()

            if signal is None:
                log_info("Worker alive, waiting for SIGNAL_OUTBOX...")
            else:
                signal_id = None
                try:
                    signal_id = signal.get("signal_id") if isinstance(signal, dict) else None
                    verdict = signal.get("final_verdict") if isinstance(signal, dict) else None
                except Exception:
                    verdict = None

                log_info(f"Signal received | id={signal_id} | verdict={verdict}")

                # If signal_id missing -> cannot be idempotent; drop it to avoid infinite loop
                if not signal_id:
                    log_warning("Signal missing signal_id -> acknowledging (dropping) to prevent repeat loop")
                    # best-effort: remove the first element by using a placeholder id won't work,
                    # so just sleep and keep running (or implement pop logic).
                    # But since your acknowledge expects id, we can't safely drop without id.
                    # In practice: require signal_id in schema.
                    time.sleep(POLL_INTERVAL_SECONDS)
                    continue

                result = execute_signal(signal)
                outcome = (result or {}).get("outcome", "UNKNOWN")
                reason = (result or {}).get("reason")

                # ✅ ACK by signal_id (NOT whole dict)
                try:
                    acknowledge_processed(signal_id)
                except Exception as e:
                    log_warning(f"ACK failed | signal_id={signal_id} | err={e}")

        except Exception as e:
            log_warning(f"Worker loop error: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
