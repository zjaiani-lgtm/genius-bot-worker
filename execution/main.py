# execution/main.py
import os
import time
import logging

from execution.db.db import init_db
from execution.execution_engine import ExecutionEngine
from execution.signal_client import pop_next_signal
from execution.signal_generator import run_once as generate_once

logger = logging.getLogger("gbm")


def main():
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s - %(message)s')

    mode = os.getenv("MODE", "DEMO").upper()
    outbox_path = os.getenv("SIGNAL_OUTBOX_PATH", "/var/data/signal_outbox.json")

    init_db()
    engine = ExecutionEngine()

    # Optional: startup sync already handled by your startup_sync.py in previous steps
    try:
        engine.reconcile_oco()  # quick start
    except Exception:
        pass

    logger.info(f"GENIUS BOT MAN worker starting | MODE={mode}")
    logger.info(f"OUTBOX_PATH={outbox_path}")

    while True:
        try:
            # ✅ reconcile synthetic OCO (cancel opposite if one filled)
            try:
                engine.reconcile_oco()
            except Exception as e:
                logger.warning(f"OCO_RECONCILE_LOOP_WARN | err={e}")

            # 1) generate signal
            created = generate_once(outbox_path)
            if created:
                logger.info("SIGNAL_GENERATOR | signal created")

            # 2) pop signal
            sig = pop_next_signal(outbox_path)
            if sig:
                logger.info(f"Signal received | id={sig.get('signal_id')} | verdict={sig.get('final_verdict')}")
                engine.execute_signal(sig)
            else:
                logger.info("Worker alive, waiting for SIGNAL_OUTBOX...")

        except Exception as e:
            logger.exception(f"WORKER_LOOP_ERROR | err={e}")

        time.sleep(10)


if __name__ == "__main__":
    main()
