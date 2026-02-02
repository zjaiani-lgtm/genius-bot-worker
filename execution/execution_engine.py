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

from execution.virtual_wallet import (
    get_balance,
    simulate_market_entry,
    simulate_market_close,
)

logger = logging.getLogger("gbm")


def _to_bool01(v: Any) -> bool:
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

        # Read-only feed (safe)
        self.price_feed = ccxt.binance({"enableRateLimit": True})

        # LIVE/TESTNET execution client
        self.exchange = None
        if self.mode in ("LIVE", "TESTNET"):
            from execution.exchange_client import BinanceSpotClient
            self.exchange = BinanceSpotClient()

        # Optional debug
        self.state_debug = os.getenv("STATE_DEBUG", "false").lower() == "true"

        # For LIVE: after BUY, place a LIMIT SELL to show as OPEN ORDER
        self.tp_pct = float(os.getenv("TP_PCT", "0.30"))  # 0.30% default

    def _load_system_state(self) -> Dict[str, Any]:
        raw = get_system_state()
        if self.state_debug:
            logger.info(f"SYSTEM_STATE_RAW | type={type(raw)} value={raw}")

        if isinstance(raw, dict):
            return {
                "status": str(raw.get("status") or "").upper(),
                "kill_switch": _to_bool01(raw.get("kill_switch")),
                "startup_sync_ok": _to_bool01(raw.get("startup_sync_ok")),
            }

        # fallback tuple order: (id, status, startup_sync_ok, kill_switch, updated_at)
        if isinstance(raw, (list, tuple)):
            status = raw[1] if len(raw) > 1 else ""
            sync = raw[2] if len(raw) > 2 else 0
            kill = raw[3] if len(raw) > 3 else 0
            return {
                "status": str(status or "").upper(),
                "kill_switch": _to_bool01(kill),
                "startup_sync_ok": _to_bool01(sync),
            }

        return {"status": "", "kill_switch": False, "startup_sync_ok": False}

    def _audit_has_signal(self, signal_id: str) -> bool:
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

    def startup_sync(self) -> None:
        from execution.startup_sync import run_startup_sync
        ok = run_startup_sync()
        if ok:
            logger.info("STARTUP_SYNC: OK")
        else:
            logger.warning("STARTUP_SYNC: FAILED -> system PAUSED (NO TRADE)")

    def _get_last_price(self, symbol: str) -> float:
        t = self.price_feed.fetch_ticker(symbol)
        return float(t["last"])

    def execute_signal(self, signal: Dict[str, Any]) -> None:
        signal_id = str(signal.get("signal_id", "UNKNOWN"))
        verdict = str(signal.get("final_verdict", "")).upper()

        logger.info(f"EXEC_ENTER | id={signal_id} verdict={verdict} MODE={self.mode} ENV_KILL_SWITCH={self.env_kill_switch}")

        if verdict not in ("TRADE", "CLOSE", "NO_TRADE"):
            logger.warning(f"EXEC_REJECT | unknown verdict={verdict} | id={signal_id}")
            return

        if verdict == "NO_TRADE":
            log_event("NO_TRADE", f"{signal_id} verdict=NO_TRADE")
            return

        # 1) contract gates
        if signal.get("certified_signal") is not True:
            log_event("REJECT_NOT_CERTIFIED", f"{signal_id} certified_signal=false")
            return

        mode_allowed = signal.get("mode_allowed") or {}
        if self.mode == "DEMO" and not bool(mode_allowed.get("demo", False)):
            log_event("REJECT_MODE_NOT_ALLOWED", f"{signal_id} mode=DEMO")
            return
        if self.mode in ("LIVE", "TESTNET") and not bool(mode_allowed.get("live", False)):
            log_event("REJECT_MODE_NOT_ALLOWED", f"{signal_id} mode={self.mode}")
            return

        # 2) system gates
        state = self._load_system_state()
        db_status = str(state.get("status") or "").upper()
        db_kill_switch = bool(state.get("kill_switch"))
        startup_sync_ok = bool(state.get("startup_sync_ok"))

        if self.env_kill_switch or db_kill_switch:
            logger.warning(f"EXEC_BLOCKED | KILL_SWITCH=ON | id={signal_id}")
            log_event("EXEC_BLOCKED_KILL_SWITCH", f"{signal_id} env={self.env_kill_switch} db={db_kill_switch}")
            return

        if not startup_sync_ok or db_status not in ("ACTIVE", "RUNNING"):
            logger.warning(f"EXEC_BLOCKED | system not ACTIVE/synced | id={signal_id} status={db_status} startup_sync_ok={startup_sync_ok}")
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

        log_event("SIGNAL_SEEN", f"{signal_id} verdict={verdict} mode={self.mode}")

        # 4) parse payload
        execution = signal.get("execution") or {}
        symbol = execution.get("symbol")
        direction = str(execution.get("direction", "")).upper()
        entry = execution.get("entry") or {}
        entry_type = str(entry.get("type", "")).upper()

        position_size = execution.get("position_size")
        quote_amount = execution.get("quote_amount")

        if not symbol:
            log_event("REJECT_BAD_PAYLOAD", f"{signal_id} missing symbol")
            return

        # Spot safety: we only allow LONG (buy then sell). SHORT is not supported safely here.
        if direction not in ("LONG",):
            logger.warning(f"EXEC_REJECT | unsupported direction for SPOT={direction} | id={signal_id}")
            log_event("REJECT_BAD_PAYLOAD", f"{signal_id} unsupported direction={direction}")
            return

        if entry_type != "MARKET":
            log_event("REJECT_BAD_PAYLOAD", f"{signal_id} unsupported entry={entry_type}")
            return

        if quote_amount is None and position_size is None:
            log_event("REJECT_BAD_PAYLOAD", f"{signal_id} missing quote_amount/position_size")
            return

        # 5) execute
        if self.mode == "DEMO":
            try:
                last_price = self._get_last_price(symbol)
                base_size = float(position_size) if position_size is not None else float(quote_amount) / float(last_price)

                resp = simulate_market_entry(symbol=symbol, side=direction, size=base_size, price=last_price)
                bal = get_balance()

                logger.info(f"EXEC_DEMO_OK | id={signal_id} resp={resp} wallet_balance={bal}")
                log_event("TRADE_EXECUTED", f"{signal_id} DEMO {symbol} {direction} size={base_size} price={last_price}")
                return
            except Exception as e:
                logger.exception(f"EXEC_DEMO_ERROR | id={signal_id} err={e}")
                log_event("EXEC_DEMO_ERROR", f"{signal_id} err={e}")
                return

        # LIVE/TESTNET
        if self.exchange is None:
            logger.warning(f"EXEC_BLOCKED | exchange client not wired | id={signal_id}")
            log_event("EXEC_BLOCKED_NO_EXCHANGE", f"{signal_id} exchange=None")
            return

        try:
            # Prefer quote_amount if provided (safer with small USDT)
            if quote_amount is None:
                # fallback: convert base size to quote by last price (still will place market buy by quote)
                last = self.exchange.fetch_last_price(symbol)
                quote_amount = float(position_size) * float(last)

            quote_amount = float(quote_amount)

            # 1) BUY market by quote (so you control USDT spend)
            buy = self.exchange.place_market_buy_by_quote(symbol=symbol, quote_amount=quote_amount)
            logger.info(f"EXEC_LIVE_BUY_OK | id={signal_id} symbol={symbol} quote={quote_amount} resp={buy}")
            log_event("TRADE_EXECUTED", f"{signal_id} {self.mode} BUY {symbol} quote={quote_amount}")

            # 2) Place LIMIT SELL as take-profit to show an OPEN ORDER on Binance
            last_price = self.exchange.fetch_last_price(symbol)
            tp_price = float(last_price) * (1.0 + (self.tp_pct / 100.0))

            # Extract filled base amount (best-effort)
            filled_base = None
            try:
                filled_base = float(buy.get("filled") or 0.0)  # sometimes ccxt provides filled in base units
            except Exception:
                filled_base = 0.0

            # If filled_base not provided, estimate from quote/price
            if not filled_base or filled_base <= 0:
                filled_base = float(quote_amount) / float(last_price)

            sell = self.exchange.place_limit_sell_amount(symbol=symbol, base_amount=filled_base, price=tp_price)
            logger.info(f"EXEC_LIVE_SELL_LIMIT_OK | id={signal_id} symbol={symbol} amount={filled_base} tp_price={tp_price} resp={sell}")
            log_event("ORDER_PLACED", f"{signal_id} {self.mode} SELL_LIMIT {symbol} amount={filled_base} price={tp_price}")

        except Exception as e:
            logger.exception(f"EXEC_LIVE_ERROR | id={signal_id} err={e}")
            update_system_state(status="PAUSED", startup_sync_ok=False)
            log_event("EXEC_LIVE_ERROR", f"{signal_id} err={e}")
            return
