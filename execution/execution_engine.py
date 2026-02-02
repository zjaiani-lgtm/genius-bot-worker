# execution/execution_engine.py
import os
import logging
from typing import Any, Dict, Optional

from execution.db.repository import (
    get_system_state,
    update_system_state,
    log_event,
)

# DB-level idempotency check (audit_log scan)
from execution.db.db import get_connection

logger = logging.getLogger("gbm")


class ExecutionEngine:
    def __init__(self):
        self.mode = os.getenv("MODE", "DEMO").upper()  # DEMO | TESTNET | LIVE
        self.env_kill_switch = os.getenv("KILL_SWITCH", "false").lower() == "true"
        self.live_confirmation = os.getenv("LIVE_CONFIRMATION", "false").lower() == "true"

        # Optional: wire your exchange client here if you already have it.
        # from execution.exchange_client import BinanceSpotClient
        # self.exchange = BinanceSpotClient()
        self.exchange = None

    # -----------------------------
    # Helpers: system_state parsing
    # -----------------------------
    def _state_get(self, state: Any, key: str, idx: int) -> Any:
        """
        Supports both dict-like and tuple-like returns from get_system_state().
        Expected tuple order: (id, mode, status, kill_switch, startup_sync_ok, updated_at)
        """
        if state is None:
            return None
        if isinstance(state, dict):
            return state.get(key)
        if isinstance(state, (list, tuple)) and len(state) > idx:
            return state[idx]
        return None

    def _load_system_state(self) -> Dict[str, Any]:
        """
        Normalized state dict:
          mode, status, kill_switch, startup_sync_ok
        """
        state = get_system_state()
        return {
            "mode": self._state_get(state, "mode", 1),
            "status": self._state_get(state, "status", 2),
            "kill_switch": self._state_get(state, "kill_switch", 3),
            "startup_sync_ok": self._state_get(state, "startup_sync_ok", 4),
        }

    # -----------------------------
    # Idempotency (DB audit_log)
    # -----------------------------
    def _audit_has_signal(self, signal_id: str) -> bool:
        """
        Returns True if this signal_id was already seen/executed.
        Uses audit_log where we store message beginning with signal_id.
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
            # Fail-safe: if we can't verify idempotency, do NOT trade.
            logger.warning(f"IDEMPOTENCY_CHECK_FAILED -> BLOCK | id={signal_id} err={e}")
            return True

    # -----------------------------
    # Startup Sync
    # -----------------------------
    def startup_sync(self) -> None:
        # IMPORTANT: call real startup sync flow
        from execution.startup_sync import run_startup_sync  # local import to avoid circular deps

        ok = run_startup_sync()
        if ok:
            logger.info("STARTUP_SYNC: OK")
        else:
            logger.warning("STARTUP_SYNC: FAILED -> system PAUSED (NO TRADE)")

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

        # -----------------------------
        # 0) Supported verdicts
        # -----------------------------
        if verdict not in ("TRADE", "CLOSE", "NO_TRADE"):
            logger.warning(f"EXEC_REJECT | unknown verdict={verdict} | id={signal_id}")
            return

        if verdict == "NO_TRADE":
            logger.info(f"EXEC_NO_TRADE | id={signal_id}")
            log_event("NO_TRADE", f"{signal_id} verdict=NO_TRADE")
            return

        # -----------------------------
        # 1) Contract gates (Decision -> Execution)
        # -----------------------------
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

        # -----------------------------
        # 2) System gates (DB + ENV)
        # -----------------------------
        state = self._load_system_state()
        db_status = str(state.get("status") or "").upper()
        db_kill_switch = bool(int(state.get("kill_switch") or 0))
        startup_sync_ok = bool(int(state.get("startup_sync_ok") or 0))

        # Kill switch from ENV OR DB blocks everything
        if self.env_kill_switch or db_kill_switch:
            logger.warning(f"EXEC_BLOCKED | KILL_SWITCH=ON | id={signal_id}")
            log_event("EXEC_BLOCKED_KILL_SWITCH", f"{signal_id} env={self.env_kill_switch} db={db_kill_switch}")
            return

        # Startup sync must be OK and status must be ACTIVE
        if not startup_sync_ok or db_status != "ACTIVE":
            logger.warning(
                f"EXEC_BLOCKED | system not ACTIVE/synced | id={signal_id} "
                f"status={db_status} startup_sync_ok={startup_sync_ok}"
            )
            log_event("EXEC_BLOCKED_SYSTEM_STATE", f"{signal_id} status={db_status} startup_sync_ok={startup_sync_ok}")
            return

        # LIVE confirmation gate
        if self.mode == "LIVE" and not self.live_confirmation:
            logger.warning(f"EXEC_BLOCKED | LIVE_CONFIRMATION=OFF | id={signal_id}")
            log_event("EXEC_BLOCKED_LIVE_CONFIRMATION", f"{signal_id} live_confirmation=false")
            return

        # -----------------------------
        # 3) Idempotency gate (must be BEFORE any execution)
        # -----------------------------
        if self._audit_has_signal(signal_id):
            logger.warning(f"DUPLICATE_SIGNAL_BLOCKED | id={signal_id}")
            log_event("DUPLICATE_SIGNAL_BLOCKED", f"{signal_id} duplicate")
            return

        # Mark seen immediately (prevents retry-doubles)
        log_event("SIGNAL_SEEN", f"{signal_id} verdict={verdict} mode={self.mode}")

        # -----------------------------
        # 4) Parse execution payload
        # -----------------------------
        execution = signal.get("execution") or {}
        symbol = execution.get("symbol")
        direction = str(execution.get("direction", "")).upper()
        entry = execution.get("entry") or {}
        entry_type = str(entry.get("type", "")).upper()

        quote_amount = execution.get("quote_amount")      # e.g. 5 USDT
        position_size = execution.get("position_size")    # e.g. 0.0001 BTC

        risk = execution.get("risk") or {}
        stop_loss = risk.get("stop_loss")
        take_profit = risk.get("take_profit")

        # Validate
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

        side = "buy" if direction == "LONG" else "sell"
        logger.info(
            f"EXEC_PARSED | id={signal_id} symbol={symbol} side={side} "
            f"quote_amount={quote_amount} position_size={position_size} entry={entry_type} "
            f"sl={stop_loss} tp={take_profit}"
        )

        # -----------------------------
        # 5) Execute (DEMO vs LIVE/TESTNET)
        # -----------------------------
        if self.mode == "DEMO":
            # If you have a real virtual wallet implementation, call it here.
            # Keeping safe: we only log + audit.
            logger.info(f"EXEC_DEMO | would execute MARKET {side} on {symbol} | id={signal_id}")
            log_event("TRADE_EXECUTED", f"{signal_id} DEMO {symbol} {side} quote={quote_amount} size={position_size}")
            return

        # LIVE/TESTNET: integrate with exchange client (ccxt adapter)
        if self.exchange is None:
            logger.warning(f"EXEC_BLOCKED | exchange client not wired | id={signal_id}")
            log_event("EXEC_BLOCKED_NO_EXCHANGE", f"{signal_id} exchange=None")
            return

        try:
            # Example: place market order by base amount
            # (If you support quoteOrderQty on Binance, implement that in exchange_client)
            amount = position_size
            resp = self.exchange.place_market_order(symbol=symbol, side=side, size=amount)

            logger.info(f"EXEC_LIVE_OK | id={signal_id} resp_status={resp.get('status')}")
            log_event("TRADE_EXECUTED", f"{signal_id} LIVE {symbol} {side} size={amount}")

        except Exception as e:
            logger.exception(f"EXEC_LIVE_ERROR | id={signal_id} err={e}")
            # Fail-safe: PAUSE system on unexpected execution errors
            update_system_state(status="PAUSED", startup_sync_ok=False)
            log_event("EXEC_LIVE_ERROR", f"{signal_id} err={e}")
            return
