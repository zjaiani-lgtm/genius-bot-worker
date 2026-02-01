# execution/main.py

import time

from execution.logger import log_info, log_warning
from execution.config import MODE, KILL_SWITCH, STARTUP_SYNC_ENABLED
from execution.db.db import init_db
from execution.signal_client import get_latest_signal, acknowledge_processed
from execution.execution_engine import execute_signal
from execution.db.repository import list_positions, list_audit_log
from execution.startup_sync import run_startup_sync

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
                signal_id = signal.get("signal_id")
                verdict = signal.get("final_verdict")
                log_info(f"Signal received | id={signal_id} | verdict={verdict}")

                result = execute_signal(signal)
                outcome = result.get("outcome")
                reason = result.get("reason")

                # ACK immediately to prevent repeats/spam (success or fail)
                acknowledge_processed(signal, f"{outcome}:{reason}" if reason else outcome)

        except Exception as e:
            log_warning(f"Worker loop error: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
