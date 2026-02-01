# execution/main.py

import time

from execution.logger import log_info, log_warning
from execution.config import MODE
from execution.db.db import init_db
from execution.signal_client import get_latest_signal
from execution.execution_engine import execute_signal

POLL_INTERVAL_SECONDS = 10


def main():
    # --- BOOT ---
    log_info(f"GENIUS BOT MAN worker starting | MODE={MODE}")

    # --- DB INIT ---
    init_db()
    log_info("DB initialized")

    # --- MAIN LOOP ---
    while True:
        try:
            signal = get_latest_signal()

            if signal is None:
                log_info("Worker alive, waiting for SIGNAL_OUTBOX...")
            else:
                signal_id = signal.get("signal_id")
                verdict = signal.get("final_verdict")

                log_info(f"Signal received | id={signal_id} | verdict={verdict}")

                # Phase 3: DEMO execution engine (safe)
                execute_signal(signal)

        except Exception as e:
            # Worker must never crash; any error -> log and continue
            log_warning(f"Worker loop error: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
