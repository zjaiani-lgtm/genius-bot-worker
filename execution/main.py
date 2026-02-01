# execution/main.py

import time

from execution.logger import log_info, log_warning
from execution.config import MODE
from execution.db.db import init_db
from execution.signal_client import get_latest_signal, acknowledge_processed
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
                log_info(f"POS id={r[0]} {r[1]} {r[2]} size={r[3]} entry={r[4]} status={r[5]} opened_at={r[6]}")

        log_info("==== AUDIT LOG (last 10 events) ====")
        if not aud:
            log_info("audit_log: (empty)")
        else:
            for a in aud:
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

                result = execute_signal(signal)
                outcome = result.get("outcome")
                reason = result.get("reason")

                # IMPORTANT: ACK immediately to prevent repeats/spam
                # We ACK on: success, blocked, invalid, already executed
                if outcome in (
                    "OPENED",
                    "CLOSED",
                    "IGNORE_ALREADY_EXECUTED",
                    "TRADE_BLOCKED_OPEN_EXISTS",
                    "CLOSE_BLOCKED_NO_OPEN",
                    "INVALID_FIELDS",
                    "ERROR",
                    "UNSUPPORTED_VERDICT",
                ):
                    acknowledge_processed(signal, f"{outcome}:{reason}" if reason else outcome)

        except Exception as e:
            log_warning(f"Worker loop error: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
