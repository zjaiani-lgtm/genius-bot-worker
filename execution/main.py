# execution/main.py
import json
import os
import time
import logging
from typing import Any, Dict, List

from execution.execution_engine import ExecutionEngine

logger = logging.getLogger("gbm")
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s - %(message)s")


SIGNAL_OUTBOX_PATH = os.getenv("SIGNAL_OUTBOX_PATH", "/var/data/signal_outbox.json")
POLL_SECONDS = int(os.getenv("SIGNAL_POLL_SECONDS", "10"))


def ensure_outbox_exists(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"signals": []}, f, indent=2)
        logger.info(f"SIGNAL_OUTBOX created | path={path}")
    else:
        logger.info(f"SIGNAL_OUTBOX ready | path={path}")


def read_outbox(path: str) -> List[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        signals = data.get("signals", [])
        if not isinstance(signals, list):
            logger.warning("SIGNAL_OUTBOX invalid format: 'signals' is not a list")
            return []
        return signals
    except json.JSONDecodeError as e:
        logger.error(f"SIGNAL_OUTBOX JSON decode error: {e}")
        return []
    except FileNotFoundError:
        logger.error("SIGNAL_OUTBOX missing (FileNotFoundError)")
        return []
    except Exception as e:
        logger.exception(f"SIGNAL_OUTBOX read failed: {e}")
        return []


def acknowledge_processed(path: str) -> None:
    # clear outbox so it won't repeat
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"signals": []}, f, indent=2)
    except Exception as e:
        logger.exception(f"ACK failed (could not clear outbox): {e}")


def run_worker() -> None:
    mode = os.getenv("MODE", "DEMO").upper()
    kill_switch = os.getenv("KILL_SWITCH", "false").lower() == "true"
    live_confirmation = os.getenv("LIVE_CONFIRMATION", "false").lower() == "true"

    logger.info(f"GENIUS BOT MAN worker starting | MODE={mode}")
    logger.info(f"KILL_SWITCH={'ON' if kill_switch else 'OFF'} | LIVE_CONFIRMATION={'ON' if live_confirmation else 'OFF'}")

    ensure_outbox_exists(SIGNAL_OUTBOX_PATH)

    engine = ExecutionEngine()

    # optional: startup sync placeholder (you already have this in your project)
    engine.startup_sync()

    while True:
        signals = read_outbox(SIGNAL_OUTBOX_PATH)

        if not signals:
            logger.info("Worker alive, waiting for SIGNAL_OUTBOX...")
            time.sleep(POLL_SECONDS)
            continue

        # process all signals found
        for signal in signals:
            signal_id = str(signal.get("signal_id", "UNKNOWN"))
            verdict = str(signal.get("final_verdict", "UNKNOWN"))

            logger.info(f"Signal received | id={signal_id} | verdict={verdict}")

            # ✅ LOG #1 — confirms we call execute_signal
            logger.info(f"AFTER_RECEIVE | calling execute_signal | id={signal_id}")

            try:
                engine.execute_signal(signal)
            except Exception as e:
                logger.exception(f"EXECUTION ERROR | id={signal_id} err={e}")

        # ACK/clear after processing batch
        acknowledge_processed(SIGNAL_OUTBOX_PATH)


if __name__ == "__main__":
    run_worker()
