# execution/execution_engine.py
import os
import logging
from typing import Any, Dict

import ccxt

from execution.db.repository import (
    get_system_state,
    update_system_state,
    log_event,
)
from execution.db.db import get_connection

# ✅ YOUR virtual_wallet (functions, no class)
from execution.virtual_wallet import (
    get_balance,
    simulate_market_entry,
    simulate_market_close,
)

logger = logging.getLogger("gbm")


class ExecutionEngine:
    def __init__(self):
        self.mode = os.getenv("MODE", "DEMO").upper()  # DEMO | TESTNET | LIVE
        self.env_kill_switch = os.getenv("KILL_SWITCH", "false").lower() == "true"
        self.live_confirmation = os.getenv("LIVE_CONFIRMATION", "false").lower() == "true"

        # Read-only exchange for price discovery (safe for DEMO)
        self.price_feed = ccxt.binance({"enableRateLimit": True})

        # Optional: wire exchange client for LIVE later
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
        True if this signal_id already processed (SEEN or EXECUTED).
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
        db_kill_switch = bool(int(state.get("kill_switch") or 0))
        startup_sync_ok = bool(int(state.get("startup_sync_ok") or 0))

        if self.env_kill_switch or db_kill_switch:
            logger.warning(f"EXEC_BLOCKED | KILL_SWITCH=ON | id={signal_id}")
            log_event("EXEC_BLOCKED_KILL_SWITCH", f"{signal_id} env={self.env_kill_switch} db={db_kill_switch}")
            return

        if not startup_sync_ok or db_status != "ACTIVE":
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
                # If only quote_amount provided, you can convert using last price
                last_price = self._get_last_price(symbol)

                if position_size is None:
                    # base_size = quote_amount / price
                    base_size = float(quote_amount) / float(last_price)
                else:
                    base_size = float(position_size)

                # Use your wallet simulation: requires explicit price
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
