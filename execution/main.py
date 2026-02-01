# execution/main.py

import time

from execution.logger import log_info, log_warning
from execution.config import MODE
from execution.db.db import init_db
from execution.signal_client import get_latest_signal
from execution.execution_engine import execute_signal
from execution.db.repository import list_positions, list_audit_log

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
                log_info(f"POS id={r[0]} {r[1]} {r[2]} size={r[3]} entry={r[4]} status={r[5]} opened_at={r[6]}")

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

    init_db()
    log_info("DB initialized")

    last_inspect = 0

    while True:
        try:
            now = time.time()

            # periodic DB snapshot
            if now - last_inspect >= INSPECT_EVERY_SECONDS:
                _print_db_snapshot()
                last_inspect = now

            signal = get_latest_signal()

            if signal is None:
                log_info("Worker alive, waiting for SIGNAL_OUTBOX...")
            else:
                signal_id = signal.get("signal_id")
                verdict = signal.get("final_verdict")
                log_info(f"Signal received | id={signal_id} | verdict={verdict}")

                execute_signal(signal)

        except Exception as e:
            log_warning(f"Worker loop error: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
