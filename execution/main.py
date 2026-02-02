# execution/main.py
import os
import time
import logging

from execution.execution_engine import ExecutionEngine
from execution.signal_client import ensure_signal_outbox_exists, pop_next_signal

logger = logging.getLogger("gbm")
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s - %(message)s")

SIGNAL_OUTBOX_PATH = os.getenv("SIGNAL_OUTBOX_PATH", "/var/data/signal_outbox.json")
POLL_SECONDS = int(os.getenv("SIGNAL_POLL_SECONDS", "10"))

def run_worker() -> None:
    mode = os.getenv("MODE", "DEMO").upper()
    kill_switch = os.getenv("KILL_SWITCH", "false").lower() == "true"
    live_confirmation = os.getenv("LIVE_CONFIRMATION", "false").lower() == "true"

    logger.info(f"GENIUS BOT MAN worker starting | MODE={mode}")
    logger.info(f"KILL_SWITCH={'ON' if kill_switch else 'OFF'} | LIVE_CONFIRMATION={'ON' if live_confirmation else 'OFF'}")

    ensure_signal_outbox_exists(SIGNAL_OUTBOX_PATH)

    engine = ExecutionEngine()
    engine.startup_sync()  # ამას ქვემოთ გავასწორებთ რომ რეალური sync იყოს

    while True:
        sig = pop_next_signal(SIGNAL_OUTBOX_PATH)  # ✅ FIFO + ამოღება

        if not sig:
            logger.info("Worker alive, waiting for SIGNAL_OUTBOX...")
            time.sleep(POLL_SECONDS)
            continue

        signal_id = str(sig.get("signal_id", "UNKNOWN"))
        verdict = str(sig.get("final_verdict", "UNKNOWN"))
        logger.info(f"Signal received | id={signal_id} | verdict={verdict}")

        try:
            engine.execute_signal(sig)
        except Exception as e:
            logger.exception(f"EXECUTION ERROR | id={signal_id} err={e}")

if __name__ == "__main__":
    run_worker()
