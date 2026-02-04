# execution/main.py
import os
import time
import logging
from typing import Optional, Dict, Any

from execution.db.db import init_db
from execution.db.repository import get_system_state, update_system_state, log_event
from execution.execution_engine import ExecutionEngine
from execution.signal_client import pop_next_signal
from execution.kill_switch import is_kill_switch_active

logger = logging.getLogger("gbm")


def _bootstrap_state_if_needed() -> None:
    """
    Prevent getting stuck after redeploys when DB is persisted on Render Disk.
    If user intentionally paused via DB, keep KILL_SWITCH=true to enforce stop.
    """
    raw = get_system_state()
    # tuple: (id, status, startup_sync_ok, kill_switch, updated_at)
    if not isinstance(raw, (list, tuple)) or len(raw) < 5:
        logger.warning("BOOTSTRAP_STATE | system_state row missing or invalid -> skip")
        return

    status = str(raw[1] or "").upper()
    startup_sync_ok = int(raw[2] or 0)
    kill_switch_db = int(raw[3] or 0)

    env_kill = os.getenv("KILL_SWITCH", "false").lower() == "true"

    logger.info(
        f"BOOTSTRAP_STATE | status={status} startup_sync_ok={startup_sync_ok} "
        f"kill_db={kill_switch_db} env_kill={env_kill}"
    )

    # If kill switch is on (env or db), do not override anything
    if env_kill or kill_switch_db == 1:
        logger.warning("BOOTSTRAP_STATE | kill switch ON -> skip overrides")
        return

    # If stuck, self-heal to RUNNING + sync_ok=1
    if status == "PAUSED" or startup_sync_ok == 0:
        logger.warning("BOOTSTRAP_STATE | applying self-heal -> status=RUNNING startup_sync_ok=1 kill_switch=0")
        update_system_state(status="RUNNING", startup_sync_ok=1, kill_switch=0)


def _try_import_generator():
    """
    Make generator import robust: worker should still run even if generator code is missing/broken.
    Returns a callable generate_once(outbox_path) or None.
    """
    try:
        from execution.signal_generator import run_once as generate_once
        return generate_once
    except Exception as e:
        logger.error(f"GENERATOR_IMPORT_FAIL | err={e} -> generator disabled (consumer will still run)")
        try:
            log_event("GENERATOR_IMPORT_FAIL", f"err={e}")
        except Exception:
            pass
        return None


def _safe_pop_next_signal(outbox_path: str) -> Optional[Dict[str, Any]]:
    """
    Wrapper to safely pop signals; if pop fails we log and keep loop alive.
    """
    try:
        return pop_next_signal(outbox_path)
    except Exception as e:
        logger.exception(f"OUTBOX_POP_FAIL | path={outbox_path} err={e}")
        try:
            log_event("OUTBOX_POP_FAIL", f"path={outbox_path} err={e}")
        except Exception:
            pass
        return None


def main():
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s - %(message)s')

    mode = os.getenv("MODE", "DEMO").upper()

    # IMPORTANT: keep your env name consistent
    outbox_path = os.getenv("SIGNAL_OUTBOX_PATH", "/var/data/signal_outbox.json")
    sleep_s = float(os.getenv("LOOP_SLEEP_SECONDS", "10"))

    init_db()
    _bootstrap_state_if_needed()

    engine = ExecutionEngine()

    # Optional: quick reconcile at start
    try:
        engine.reconcile_oco()
    except Exception as e:
        logger.warning(f"OCO_RECONCILE_START_WARN | err={e}")

    generate_once = _try_import_generator()

    logger.info(f"GENIUS BOT MAN worker starting | MODE={mode}")
    logger.info(f"OUTBOX_PATH={outbox_path}")
    logger.info(f"LOOP_SLEEP_SECONDS={sleep_s}")

    while True:
        try:
            # reconcile native/synthetic OCO
            try:
                engine.reconcile_oco()
            except Exception as e:
                logger.warning(f"OCO_RECONCILE_LOOP_WARN | err={e}")

            # 1) generate signal (optional)
            if generate_once is not None:
                try:
                    created = generate_once(outbox_path)
                    if created:
                        logger.info("SIGNAL_GENERATOR | signal created")
                except Exception as e:
                    logger.exception(f"SIGNAL_GENERATOR_FAIL | err={e}")
                    try:
                        log_event("SIGNAL_GENERATOR_FAIL", f"err={e}")
                    except Exception:
                        pass

            # 2) kill switch absolute gate (before consume)
            # If kill switch active, do NOT execute anything.
            if is_kill_switch_active():
                logger.warning("KILL_SWITCH_ACTIVE | worker will not execute signals")
                try:
                    log_event("WORKER_KILL_SWITCH_ACTIVE", "execution blocked in main loop")
                except Exception:
                    pass

                # Option A (default safety): still pop one signal per loop to prevent outbox growth,
                # but DO NOT execute it. This avoids a backlog bomb after re-enable.
                sig = _safe_pop_next_signal(outbox_path)
                if sig:
                    sid = sig.get("signal_id")
                    logger.warning(f"KILL_SWITCH_DROP_SIGNAL | id={sid}")
                    try:
                        log_event("KILL_SWITCH_DROP_SIGNAL", f"id={sid}")
                    except Exception:
                        pass

                time.sleep(sleep_s)
                continue

            # 3) pop + execute
            sig = _safe_pop_next_signal(outbox_path)
            if sig:
                logger.info(f"Signal received | id={sig.get('signal_id')} | verdict={sig.get('final_verdict')}")
                engine.execute_signal(sig)
            else:
                logger.info("Worker alive, waiting for SIGNAL_OUTBOX...")

        except Exception as e:
            logger.exception(f"WORKER_LOOP_ERROR | err={e}")
            try:
                log_event("WORKER_LOOP_ERROR", f"err={e}")
            except Exception:
                pass

        time.sleep(sleep_s)


if __name__ == "__main__":
    main()
