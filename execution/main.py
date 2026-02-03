# execution/main.py
import os
import time
import logging

from execution.db.db import init_db
from execution.db.repository import get_system_state, update_system_state
from execution.execution_engine import ExecutionEngine
from execution.signal_client import pop_next_signal
from execution.signal_generator import run_once as generate_once

logger = logging.getLogger("gbm")


def _bootstrap_state_if_needed():
    """
    Prevent getting stuck after redeploys when DB is persisted on Render Disk.
    If user intentionally paused via DB, keep KILL_SWITCH=true to enforce stop.
    """
    raw = get_system_state()
    # tuple: (id, status, startup_sync_ok, kill_switch, updated_at)
    if not isinstance(raw, (list, tuple)) or len(raw) < 5:
        return

    status = str(raw[1] or "").upper()
    startup_sync_ok = int(raw[2] or 0)
    kill_switch_db = int(raw[3] or 0)

    env_kill = os.getenv("KILL_SWITCH", "false").lower() == "true"

    logger.info(f"BOOTSTRAP_STATE | status={status} startup_sync_ok={startup_sync_ok} kill_db={kill_switch_db} env_kill={env_kill}")

    # If kill switch is on (env or db), do not override anything
    if env_kill or kill_switch_db == 1:
        logger.warning("BOOTSTRAP_STATE | kill switch ON -> skip overrides")
        return

    # If stuck, self-heal to RUNNING + sync_ok=1
    if status == "PAUSED" or startup_sync_ok == 0:
        logger.warning("BOOTSTRAP_STATE | applying self-heal -> status=RUNNING startup_sync_ok=1")
        update_system_state(status="RUNNING", startup_sync_ok=1, kill_switch=0)


def main():
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s - %(message)s')

    mode = os.getenv("MODE", "DEMO").upper()
    outbox_path = os.getenv("SIGNAL_OUTBOX_PATH", "/var/data/signal_outbox.json")

    init_db()
    _bootstrap_state_if_needed()

    engine = ExecutionEngine()

    # Optional: quick reconcile at start
    try:
        engine.reconcile_oco()
    except Exception:
        pass

    logger.info(f"GENIUS BOT MAN worker starting | MODE={mode}")
    logger.info(f"OUTBOX_PATH={outbox_path}")

    while True:
        try:
            # reconcile synthetic OCO
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
