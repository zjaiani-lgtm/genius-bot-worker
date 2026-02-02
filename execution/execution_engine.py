# execution/execution_engine.py
import os
import logging
from typing import Any, Dict, Optional

import ccxt

from execution.db.repository import (
    get_system_state,
    update_system_state,
    log_event,
)
from execution.db.db import get_connection

from execution.virtual_wallet import (
    get_balance,
    simulate_market_entry,
    simulate_market_close,
)

logger = logging.getLogger("gbm")


def _to_bool01(v: Any) -> bool:
    """
    Fail-safe bool parser:
    - Accepts: 0/1, "0"/"1", True/False, "true"/"false", etc.
    - Unknown strings (e.g. timestamps) => False
    """
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return int(v) != 0
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("1", "true", "yes", "y", "on"):
            return True
        if s in ("0", "false", "no", "n", "off", ""):
            return False
        return False
    return False


class ExecutionEngine:
    def __init__(self):
        self.mode = os.getenv("MODE", "DEMO").upper()  # DEMO | TESTNET | LIVE
        self.env_kill_switch = os.getenv("KILL_SWITCH", "false").lower() == "true"
        self.live_confirmation = os.getenv("LIVE_CONFIRMATION", "false").lower() == "true"

        # Read-only exchange for price discovery (safe for DEMO)
        self.price_feed = ccxt.binance({"enableRateLimit": True})

        # Optional: wire exchange client for LIVE later
        self.exchange = None

        # Optional debug for state raw dumps
        self.state_debug = os.getenv("STATE_DEBUG", "false").lower() == "true"

    # -----------------------------
    # Helpers: system_state parsing
    # -----------------------------
    def _load_system_state(self) -> Dict[str, Any]:
        raw = get_system_state()

        if self.state_debug:
            logger.info(f"SYSTEM_STATE_RAW | type={type(raw)} value={raw}")

        # After DB fixes, repository will return dict (recommended)
        if isinstance(raw, dict):
            status = raw.get("status")
            kill = raw.get("kill_switch")
            sync = raw.get("startup_sync_ok")
            return {
                "status": str(status).upper() if status is not None else "",
                "kill_switch": _to_bool01(kill),
                "startup_sync_ok": _to_bool01(sync),
            }

        # Backward compatibility: tuple/list from sqlite without row_factory
        # DB schema order: (id, status, startup_sync_ok, kill_switch, updated_at)
        if isinstance(raw, (list, tuple)):
            status = raw[1] if len(raw) > 1 else ""
            sync = raw[2] if len(raw) > 2 else 0
            kill = raw[3] if len(raw) > 3 else 0
            return {
                "status": str(status).upper() if status is not None else "",
                "kill_switch": _to_bool01(kill),
                "startup_sync_ok": _to_bool01(sync),
            }

        return {"status": "", "kill_switch": False, "startup_sync_ok": False}

    # -----------------------------
    # Idempotency (DB audit_log)
    # -----------------------------
    def _audit_has_signal(self, signal_id: str) -> bool:
        """
        True if this signal_id already processed (SEEN/EXECUTED).
        Fail-safe: if check fails -> block.
        """
        if not signal_id:
            return False
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT 1
                FROM audit_log
                WHERE event_type IN ('SIGNAL_SEEN', 'TRADE_EXECUTED', 'SIGNAL_PROCESSED')
                  AND message LIKE ?
                LIMIT 1
                """,
                (f"{signal_id}%",),
            )
            row = cur.fetchone()
            conn.close()
            return row is not None
        except Exception as e:
            logger.warning(f"IDEMPOTENCY_CHECK_FAILED -> BLOCK | id={signal_id} err={e}")
            return True

    # -----------------------------
    # Startup Sync hook
    # -----------------------------
    def startup_sync(self) -> None:
        from execution.startup_sync import run_startup_sync
        ok = run_startup_sync()
        if ok:
            logger.info("STARTUP_SYNC: OK")
        else:
            logger.warning("STARTUP_SYNC: FAILED -> system PAUSED (NO TRADE)")

    # -----------------------------
    # Price helper (DEMO)
    # -----------------------------
    def _get_last_price(self, symbol: str) -> float:
        t = self.price_feed.fetch_ticker(symbol)
        return float(t["last"])

    # -----------------------------
    # Main execution
    # -----------------------------
    def execute_signal(self, signal: Dict[str, Any]) -> None:
        signal_id = str(signal.get("signal_id", "UNKNOWN"))
        verdict = str(signal.get("final_verdict", "")).upper()

        logger.info(
            f"EXEC_ENTER | id={signal_id} verdict={verdict} "
            f"ENV_KILL_SWITCH={self.env_kill_switch} MODE={self.mode}"
        )

        # 0) supported verdicts
        if verdict not in ("TRADE", "CLOSE", "NO_TRADE"):
            logger.warning(f"EXEC_REJECT | unknown verdict={verdict} | id={signal_id}")
            return

        if verdict == "NO_TRADE":
            logger.info(f"EXEC_NO_TRADE | id={signal_id}")
            log_event("NO_TRADE", f"{signal_id} verdict=NO_TRADE")
            return

        # 1) contract gates
        if signal.get("certified_signal") is not True:
            logger.info(f"EXEC_REJECT | not certified | id={signal_id}")
            log_event("REJECT_NOT_CERTIFIED", f"{signal_id} certified_signal=false")
            return

        mode_allowed = signal.get("mode_allowed") or {}
        if self.mode == "DEMO" and not bool(mode_allowed.get("demo", False)):
            logger.warning(f"EXEC_REJECT | not allowed in DEMO | id={signal_id}")
            log_event("REJECT_MODE_NOT_ALLOWED", f"{signal_id} mode=DEMO")
            return

        if self.mode in ("LIVE", "TESTNET") and not bool(mode_allowed.get("live", False)):
            logger.warning(f"EXEC_REJECT | not allowed in LIVE/TESTNET | id={signal_id}")
            log_event("REJECT_MODE_NOT_ALLOWED", f"{signal_id} mode={self.mode}")
            return

        # 2) system gates (DB + ENV)
        state = self._load_system_state()
        db_status = str(state.get("status") or "").upper()
        db_kill_switch = bool(state.get("kill_switch"))
        startup_sync_ok = bool(state.get("startup_sync_ok"))

        if self.env_kill_switch or db_kill_switch:
            logger.warning(f"EXEC_BLOCKED | KILL_SWITCH=ON | id={signal_id}")
            log_event("EXEC_BLOCKED_KILL_SWITCH", f"{signal_id} env={self.env_kill_switch} db={db_kill_switch}")
            return

        # Accept ACTIVE or RUNNING (because DB default is RUNNING in your init_db)
        if not startup_sync_ok or db_status not in ("ACTIVE", "RUNNING"):
            logger.warning(
                f"EXEC_BLOCKED | system not ACTIVE/synced | id={signal_id} status={db_status} startup_sync_ok={startup_sync_ok}"
            )
            log_event("EXEC_BLOCKED_SYSTEM_STATE", f"{signal_id} status={db_status} startup_sync_ok={startup_sync_ok}")
            return

        if self.mode == "LIVE" and not self.live_confirmation:
            logger.warning(f"EXEC_BLOCKED | LIVE_CONFIRMATION=OFF | id={signal_id}")
            log_event("EXEC_BLOCKED_LIVE_CONFIRMATION", f"{signal_id} live_confirmation=false")
            return

        # 3) idempotency
        if self._audit_has_signal(signal_id):
            logger.warning(f"DUPLICATE_SIGNAL_BLOCKED | id={signal_id}")
            log_event("DUPLICATE_SIGNAL_BLOCKED", f"{signal_id} duplicate")
            return

        # Mark seen first
        log_event("SIGNAL_SEEN", f"{signal_id} verdict={verdict} mode={self.mode}")

        # 4) parse payload
        execution = signal.get("execution") or {}
        symbol = execution.get("symbol")
        direction = str(execution.get("direction", "")).upper()
        entry = execution.get("entry") or {}
        entry_type = str(entry.get("type", "")).upper()

        position_size = execution.get("position_size")
        quote_amount = execution.get("quote_amount")

        if not symbol or direction not in ("LONG", "SHORT"):
            logger.warning(f"EXEC_REJECT | invalid symbol/direction | id={signal_id} symbol={symbol} dir={direction}")
            log_event("REJECT_BAD_PAYLOAD", f"{signal_id} missing symbol/direction")
            return

        if entry_type != "MARKET":
            logger.warning(f"EXEC_REJECT | unsupported entry.type={entry_type} | id={signal_id}")
            log_event("REJECT_BAD_PAYLOAD", f"{signal_id} unsupported entry={entry_type}")
            return

        if quote_amount is None and position_size is None:
            logger.warning(f"EXEC_REJECT | missing quote_amount/position_size | id={signal_id}")
            log_event("REJECT_BAD_PAYLOAD", f"{signal_id} missing quote_amount/position_size")
            return

        side_txt = "buy" if direction == "LONG" else "sell"
        logger.info(
            f"EXEC_PARSED | id={signal_id} symbol={symbol} dir={direction} side={side_txt} "
            f"quote_amount={quote_amount} position_size={position_size}"
        )

        # 5) execute
        if self.mode == "DEMO":
            try:
                last_price = self._get_last_price(symbol)

                if position_size is None:
                    base_size = float(quote_amount) / float(last_price)
                else:
                    base_size = float(position_size)

                resp = simulate_market_entry(
                    symbol=symbol,
                    side=direction,
                    size=base_size,
                    price=last_price,
                )

                bal = get_balance()
                logger.info(f"EXEC_DEMO_OK | id={signal_id} resp={resp} wallet_balance={bal}")
                log_event("TRADE_EXECUTED", f"{signal_id} DEMO {symbol} {direction} size={base_size} price={last_price}")
                return

            except Exception as e:
                logger.exception(f"EXEC_DEMO_ERROR | id={signal_id} err={e}")
                log_event("EXEC_DEMO_ERROR", f"{signal_id} err={e}")
                return

        # LIVE/TESTNET: still blocked unless exchange wired
        if self.exchange is None:
            logger.warning(f"EXEC_BLOCKED | exchange client not wired | id={signal_id}")
            log_event("EXEC_BLOCKED_NO_EXCHANGE", f"{signal_id} exchange=None")
            return

        try:
            amount = float(position_size) if position_size is not None else None
            resp = self.exchange.place_market_order(symbol=symbol, side=side_txt, size=amount)
            logger.info(f"EXEC_LIVE_OK | id={signal_id} resp_status={resp.get('status')}")
            log_event("TRADE_EXECUTED", f"{signal_id} LIVE {symbol} {side_txt} size={amount}")
        except Exception as e:
            logger.exception(f"EXEC_LIVE_ERROR | id={signal_id} err={e}")
            update_system_state(status="PAUSED", startup_sync_ok=False)
            log_event("EXEC_LIVE_ERROR", f"{signal_id} err={e}")
            return
