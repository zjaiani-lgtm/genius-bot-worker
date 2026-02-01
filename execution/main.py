# execution/main.py

import time

from execution.logger import log_info, log_warning
from execution.config import MODE
from execution.db.db import init_db
from execution.signal_client import get_latest_signal


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
                # no signal found or invalid
                log_info("Worker alive, waiting for SIGNAL_OUTBOX...")
            else:
                signal_id = signal.get("signal_id")
                verdict = signal.get("final_verdict")

                log_info(f"Signal received | id={signal_id} | verdict={verdict}")

                # IMPORTANT:
                # Execution logic is NOT wired yet
                # We only observe & log signals at this phase
                if verdict != "TRADE":
                    log_info(f"NO_TRADE respected | id={signal_id}")
                else:
                    log_warning(
                        f"TRADE signal detected but execution is DISABLED (SAFE MODE) | id={signal_id}"
                    )

        except Exception as e:
            # Any unexpected error must NEVER crash the worker
            log_warning(f"Worker loop error: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
