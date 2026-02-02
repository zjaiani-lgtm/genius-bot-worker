# execution/execution_engine.py
import os
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger("gbm")


class ExecutionEngine:
    def __init__(self):
        self.mode = os.getenv("MODE", "DEMO").upper()  # DEMO | TESTNET | LIVE
        self.kill_switch = os.getenv("KILL_SWITCH", "false").lower() == "true"
        self.live_confirmation = os.getenv("LIVE_CONFIRMATION", "false").lower() == "true"

        # TODO: if you have DB/client, wire them here
        # from execution.exchange_client import BinanceSpotClient
        # self.exchange = BinanceSpotClient()

    def startup_sync(self) -> None:
        # placeholder: you already have a working startup_sync and audit logs
        logger.info("STARTUP_SYNC: OK")

    def execute_signal(self, signal: Dict[str, Any]) -> None:
        # ✅ LOG #2 — confirms we entered execute_signal
        logger.info(
            f"EXEC_ENTER | id={signal.get('signal_id')} "
            f"verdict={signal.get('final_verdict')} "
            f"KILL_SWITCH={self.kill_switch} MODE={self.mode}"
        )

        verdict = str(signal.get("final_verdict", "")).upper()

        # safety: only TRADE/CLOSE supported in this reference
        if verdict not in ("TRADE", "CLOSE", "NO_TRADE"):
            logger.warning(f"EXEC_REJECT | unknown verdict={verdict} | id={signal.get('signal_id')}")
            return

        if verdict == "NO_TRADE":
            logger.info(f"EXEC_NO_TRADE | id={signal.get('signal_id')}")
            return

        # ✅ LOG #3 — kill-switch gate
        if self.kill_switch:
            logger.warning(f"EXEC_BLOCKED | KILL_SWITCH=ON | id={signal.get('signal_id')}")
            return

        # extra LIVE gate
        if self.mode == "LIVE" and not self.live_confirmation:
            logger.warning(f"EXEC_BLOCKED | LIVE_CONFIRMATION=OFF | id={signal.get('signal_id')}")
            return

        # parse execution payload
        execution = signal.get("execution") or {}
        symbol = execution.get("symbol")
        direction = str(execution.get("direction", "")).upper()
        entry = execution.get("entry") or {}
        entry_type = str(entry.get("type", "")).upper()

        quote_amount = execution.get("quote_amount")  # e.g. 5 USDT
        position_size = execution.get("position_size")  # e.g. 0.0001 BTC (alternative)

        # validation
        if not symbol or direction not in ("LONG", "SHORT"):
            logger.warning(f"EXEC_REJECT | missing/invalid symbol/direction | id={signal.get('signal_id')} symbol={symbol} dir={direction}")
            return

        if entry_type not in ("MARKET",):
            logger.warning(f"EXEC_REJECT | unsupported entry.type={entry_type} | id={signal.get('signal_id')}")
            return

        if quote_amount is None and position_size is None:
            logger.warning(f"EXEC_REJECT | missing quote_amount or position_size | id={signal.get('signal_id')}")
            return

        # decide side
        side = "buy" if direction == "LONG" else "sell"

        logger.info(
            f"EXEC_PARSED | id={signal.get('signal_id')} symbol={symbol} side={side} "
            f"quote_amount={quote_amount} position_size={position_size} entry={entry_type}"
        )

        # --- PLACEHOLDER for real execution ---
        # Here you call BinanceSpotClient / ccxt create_order
        #
        # if quote_amount is not None:
        #     # Option A: createOrder with quoteOrderQty (Binance specific)
        #     # or compute amount using ticker price then round to precision.
        #     pass
        # else:
        #     # Option B: create market order with amount (base asset)
        #     pass
        #
        # For now, log only:
        logger.info(f"EXEC_DRYRUN | would execute {entry_type} {side} on {symbol} | id={signal.get('signal_id')}")
