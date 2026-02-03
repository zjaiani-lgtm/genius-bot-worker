# execution/execution_engine.py
import os
import logging
from typing import Any, Dict

import ccxt

from execution.db.repository import (
    get_system_state,
    log_event,
    list_active_oco_links,
    create_oco_link,
    set_oco_status,
)

from execution.virtual_wallet import (
    simulate_market_entry,
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

        # price feed (public)
        self.price_feed = ccxt.binance({"enableRateLimit": True})

        self.exchange = None
        if self.mode in ("LIVE", "TESTNET"):
            from execution.exchange_client import BinanceSpotClient
            self.exchange = BinanceSpotClient()

        self.state_debug = os.getenv("STATE_DEBUG", "false").lower() == "true"

        self.tp_pct = float(os.getenv("TP_PCT", "0.30"))
        self.sl_pct = float(os.getenv("SL_PCT", "1.00"))
        self.sl_limit_gap_pct = float(os.getenv("SL_LIMIT_GAP_PCT", "0.10"))

        self.sell_buffer = float(os.getenv("SELL_BUFFER", "0.999"))
        self.sell_retry_buffer = float(os.getenv("SELL_RETRY_BUFFER", "0.995"))

    def _load_system_state(self) -> Dict[str, Any]:
        raw = get_system_state()
        if self.state_debug:
            logger.info(f"SYSTEM_STATE_RAW | type={type(raw)} value={raw}")

        # tuple: (id, status, startup_sync_ok, kill_switch, updated_at)
        if isinstance(raw, (list, tuple)):
            status = raw[1] if len(raw) > 1 else ""
            sync = raw[2] if len(raw) > 2 else 0
            kill = raw[3] if len(raw) > 3 else 0
            return {
                "status": str(status or "").upper(),
                "startup_sync_ok": _to_bool01(sync),
                "kill_switch": _to_bool01(kill),
            }

        if isinstance(raw, dict):
            return {
                "status": str(raw.get("status") or "").upper(),
                "startup_sync_ok": _to_bool01(raw.get("startup_sync_ok")),
                "kill_switch": _to_bool01(raw.get("kill_switch")),
            }

        return {"status": "", "startup_sync_ok": False, "kill_switch": False}

    def _get_last_price(self, symbol: str) -> float:
        t = self.price_feed.fetch_ticker(symbol)
        return float(t["last"])

    # ----------------------------
    # OCO reconcile (synthetic)
    # ----------------------------
    def reconcile_oco(self) -> None:
        if self.mode not in ("LIVE", "TESTNET"):
            return
        if self.exchange is None:
            return

        rows = list_active_oco_links(limit=50)
        if not rows:
            return

        for r in rows:
            (
                link_id, signal_id, symbol, base_asset,
                tp_order_id, sl_order_id,
                tp_price, sl_stop_price, sl_limit_price,
                amount, status, created_at, updated_at
            ) = r

            try:
                tp = self.exchange.fetch_order(tp_order_id, symbol)
                sl = self.exchange.fetch_order(sl_order_id, symbol)

                tp_status = str(tp.get("status") or "").lower()
                sl_status = str(sl.get("status") or "").lower()

                logger.info(
                    f"OCO_RECONCILE | link={link_id} id={signal_id} symbol={symbol} "
                    f"tp={tp_order_id}:{tp_status} sl={sl_order_id}:{sl_status}"
                )

                # If TP filled -> cancel SL
                if tp_status in ("closed", "filled"):
                    try:
                        self.exchange.cancel_order(sl_order_id, symbol)
                        logger.info(f"OCO_RESOLVE | TP_FILLED -> cancel SL | link={link_id} tp={tp_order_id} sl={sl_order_id}")
                    except Exception as e:
                        logger.warning(f"OCO_RESOLVE_WARN | TP_FILLED cancel SL failed | link={link_id} err={e}")
                    set_oco_status(link_id, "CLOSED_TP")
                    log_event("OCO_CLOSED", f"{signal_id} TP_FILLED tp={tp_order_id} sl={sl_order_id}")
                    continue

                # If SL filled -> cancel TP
                if sl_status in ("closed", "filled"):
                    try:
                        self.exchange.cancel_order(tp_order_id, symbol)
                        logger.info(f"OCO_RESOLVE | SL_FILLED -> cancel TP | link={link_id} tp={tp_order_id} sl={sl_order_id}")
                    except Exception as e:
                        logger.warning(f"OCO_RESOLVE_WARN | SL_FILLED cancel TP failed | link={link_id} err={e}")
                    set_oco_status(link_id, "CLOSED_SL")
                    log_event("OCO_CLOSED", f"{signal_id} SL_FILLED tp={tp_order_id} sl={sl_order_id}")
                    continue

            except Exception as e:
                logger.warning(f"OCO_RECONCILE_FAIL | link={link_id} symbol={symbol} err={e}")

    # ----------------------------
    # Main execution
    # ----------------------------
    def execute_signal(self, signal: Dict[str, Any]) -> None:
        signal_id = str(signal.get("signal_id", "UNKNOWN"))
        verdict = str(signal.get("final_verdict", "")).upper()

        logger.info(f"EXEC_ENTER | id={signal_id} verdict={verdict} MODE={self.mode} ENV_KILL_SWITCH={self.env_kill_switch}")

        # gates
        state = self._load_system_state()
        db_status = str(state.get("status") or "").upper()
        db_kill = bool(state.get("kill_switch"))
        sync_ok = bool(state.get("startup_sync_ok"))

        if self.env_kill_switch or db_kill:
            logger.warning(f"EXEC_BLOCKED | KILL_SWITCH=ON | id={signal_id}")
            log_event("EXEC_BLOCKED_KILL_SWITCH", f"{signal_id}")
            return

        if not sync_ok or db_status not in ("ACTIVE", "RUNNING"):
            logger.warning(f"EXEC_BLOCKED | system not ACTIVE/synced | id={signal_id} status={db_status} sync_ok={sync_ok}")
            log_event("EXEC_BLOCKED_SYSTEM_STATE", f"{signal_id} status={db_status} sync_ok={sync_ok}")
            return

        if self.mode == "LIVE" and not self.live_confirmation:
            logger.warning(f"EXEC_BLOCKED | LIVE_CONFIRMATION=OFF | id={signal_id}")
            log_event("EXEC_BLOCKED_LIVE_CONFIRMATION", f"{signal_id}")
            return

        if signal.get("certified_signal") is not True:
            log_event("REJECT_NOT_CERTIFIED", f"{signal_id}")
            return

        execution = signal.get("execution") or {}
        symbol = execution.get("symbol")
        direction = str(execution.get("direction", "")).upper()
        entry = execution.get("entry") or {}
        entry_type = str(entry.get("type", "")).upper()

        position_size = execution.get("position_size")
        quote_amount = execution.get("quote_amount")

        if not symbol or direction != "LONG" or entry_type != "MARKET":
            logger.warning(f"EXEC_REJECT | bad payload | id={signal_id} symbol={symbol} dir={direction} entry={entry_type}")
            log_event("REJECT_BAD_PAYLOAD", f"{signal_id}")
            return

        # DEMO
        if self.mode == "DEMO":
            last_price = self._get_last_price(symbol)
            base_size = float(position_size) if position_size is not None else float(quote_amount) / float(last_price)
            resp = simulate_market_entry(symbol=symbol, side=direction, size=base_size, price=last_price)
            log_event("TRADE_EXECUTED", f"{signal_id} DEMO {symbol} size={base_size} price={last_price}")
            logger.info(f"EXEC_DEMO_OK | id={signal_id} resp={resp}")
            return

        # LIVE/TESTNET
        if self.exchange is None:
            log_event("EXEC_BLOCKED_NO_EXCHANGE", f"{signal_id}")
            logger.warning(f"EXEC_BLOCKED | exchange client not wired | id={signal_id}")
            return

        # NOTE: LIVE-ზე შეცდომაზე ნუ ვპაუზავთ მთლიან სისტემას.
        try:
            # compute quote if missing
            if quote_amount is None:
                last = self.exchange.fetch_last_price(symbol)
                quote_amount = float(position_size) * float(last)
            quote_amount = float(quote_amount)

            # BUY by quote
            buy = self.exchange.place_market_buy_by_quote(symbol=symbol, quote_amount=quote_amount)

            # derive buy price
            buy_avg = float(buy.get("average") or buy.get("price") or 0.0) or self.exchange.fetch_last_price(symbol)

            logger.info(
                f"EXEC_LIVE_BUY_OK | id={signal_id} symbol={symbol} quote={quote_amount} "
                f"avg={buy_avg} order_id={buy.get('id')}"
            )
            log_event("TRADE_EXECUTED", f"{signal_id} LIVE BUY {symbol} quote={quote_amount} avg={buy_avg} order_id={buy.get('id')}")

            base_asset = symbol.split("/")[0].upper()

            # free balance (fee-safe)
            free_base = float(self.exchange.fetch_balance_free(base_asset))

            # choose sell amount with buffer (flooring happens inside exchange_client)
            sell_amount = self.exchange.floor_amount(symbol, free_base * self.sell_buffer)
            if sell_amount <= 0:
                sell_amount = self.exchange.floor_amount(symbol, free_base * self.sell_retry_buffer)

            # If still <=0 → do NOT crash / do NOT pause. Just log and exit.
            if sell_amount <= 0:
                msg = f"OCO_SKIP_NO_FREE_BASE | id={signal_id} free_{base_asset}={free_base}"
                logger.warning(msg)
                log_event("OCO_SKIP_NO_FREE_BASE", msg)
                return

            # TP + SL prices (floor tickSize in exchange_client)
            tp_price = self.exchange.floor_price(symbol, buy_avg * (1.0 + (self.tp_pct / 100.0)))
            sl_stop_price = self.exchange.floor_price(symbol, buy_avg * (1.0 - (self.sl_pct / 100.0)))
            sl_limit_price = self.exchange.floor_price(symbol, sl_stop_price * (1.0 - (self.sl_limit_gap_pct / 100.0)))

            logger.info(
                f"OCO_PREP | id={signal_id} free_{base_asset}={free_base} "
                f"sell_amount={sell_amount} tp={tp_price} sl_stop={sl_stop_price} sl_limit={sl_limit_price}"
            )

            # Place TP LIMIT
            tp = self.exchange.place_limit_sell_amount(symbol=symbol, base_amount=sell_amount, price=tp_price)
            logger.info(f"OCO_TP_OK | id={signal_id} tp_order_id={tp.get('id')} price={tp_price} amount={sell_amount}")
            log_event("ORDER_PLACED", f"{signal_id} TP_LIMIT {symbol} id={tp.get('id')} price={tp_price} amount={sell_amount}")

            # Place SL STOP-LOSS-LIMIT
            sl = None
            try:
                sl = self.exchange.place_stop_loss_limit_sell(
                    symbol=symbol,
                    base_amount=sell_amount,
                    stop_price=sl_stop_price,
                    limit_price=sl_limit_price,
                )
                logger.info(
                    f"OCO_SL_OK | id={signal_id} sl_order_id={sl.get('id')} stop={sl_stop_price} "
                    f"limit={sl_limit_price} amount={sell_amount}"
                )
                log_event("ORDER_PLACED", f"{signal_id} SL_STOP_LIMIT {symbol} id={sl.get('id')} stop={sl_stop_price} limit={sl_limit_price} amount={sell_amount}")

            except Exception as e:
                # If SL fails, cancel TP for safety (avoid naked TP-only)
                logger.warning(f"OCO_SL_FAIL | id={signal_id} err={e} -> cancel TP")
                try:
                    self.exchange.cancel_order(tp.get("id"), symbol)
                except Exception as ee:
                    logger.warning(f"OCO_SL_FAIL_CANCEL_TP_WARN | id={signal_id} err={ee}")

                # Mark as NEEDS_RETRY in DB (optional: create link with missing sl? better: just log)
                log_event("OCO_NEEDS_RETRY", f"{signal_id} SL_FAIL err={e}")
                return

            # Save synthetic OCO link
            create_oco_link(
                signal_id=signal_id,
                symbol=symbol,
                base_asset=base_asset,
                tp_order_id=str(tp.get("id")),
                sl_order_id=str(sl.get("id")),
                tp_price=float(tp_price),
                sl_stop_price=float(sl_stop_price),
                sl_limit_price=float(sl_limit_price),
                amount=float(sell_amount),
            )

            logger.info(f"OCO_ARMED | id={signal_id} symbol={symbol} tp={tp.get('id')} sl={sl.get('id')}")
            log_event("OCO_ARMED", f"{signal_id} symbol={symbol} tp={tp.get('id')} sl={sl.get('id')} amount={sell_amount}")

        except Exception as e:
            logger.exception(f"EXEC_LIVE_ERROR | id={signal_id} err={e}")
            log_event("EXEC_LIVE_ERROR", f"{signal_id} err={e}")
            return
