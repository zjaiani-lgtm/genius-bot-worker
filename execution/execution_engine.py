# execution/execution_engine.py

from execution.logger import log_info, log_warning
from execution.db.repository import (
    open_position,
    log_event,
    get_open_positions,
    has_executed_signal,
)
from execution.virtual_wallet import simulate_market_entry


def execute_signal(signal: dict) -> None:
    """
    Execution rules (v1):
    - Only TRADE proceeds
    - Must be certified
    - Idempotent: one signal_id -> one execution
    - DEMO simulation only (Virtual Wallet)
    - One open position per symbol (secondary safety)
    - Write to DB only after FILLED
    """

    signal_id = signal.get("signal_id")
    verdict = signal.get("final_verdict")

    # ---- Hard gates ----
    if verdict != "TRADE":
        log_info(f"Engine: verdict={verdict} -> NO EXECUTION | id={signal_id}")
        return

    if signal.get("certified_signal") is not True:
        log_warning(f"Engine: not certified -> NO EXECUTION | id={signal_id}")
        return

    # ---- Idempotency gate ----
    if has_executed_signal(signal_id):
        log_info(f"Engine: signal already executed -> IGNORE | id={signal_id}")
        return

    execution = signal.get("execution") or {}
    symbol = execution.get("symbol")
    direction = (execution.get("direction") or "").upper()  # LONG / SHORT
    position_size = execution.get("position_size")
    entry = (execution.get("entry") or {})
    entry_price = entry.get("price")

    if not symbol or direction not in ("LONG", "SHORT") or position_size is None:
        log_warning(f"Engine: missing/invalid execution fields -> NO EXECUTION | id={signal_id}")
        return

    # ---- Secondary safety: one open position per symbol ----
    for p in get_open_positions():
        # positions columns: (id, symbol, side, size, entry_price, status, opened_at, closed_at, pnl)
        if p[1] == symbol:
            log_warning(f"Engine: open position exists for {symbol} -> BLOCK | id={signal_id}")
            return

    side = "LONG" if direction == "LONG" else "SHORT"

    # ---- DEMO fill simulation ----
    try:
        fill = simulate_market_entry(
            symbol=symbol,
            side=side,
            size=float(position_size),
            price=float(entry_price) if entry_price is not None else None,
        )
    except Exception as e:
        log_warning(f"Engine: demo simulation error -> NO DB WRITE | id={signal_id} err={e}")
        log_event("ORDER_FAILED", f"id={signal_id} reason=DEMO_SIM_ERROR err={e}")
        return

    if fill.get("status") != "FILLED":
        log_warning(f"Engine: fill status != FILLED -> NO DB WRITE | id={signal_id}")
        log_event("ORDER_FAILED", f"id={signal_id} reason=DEMO_NOT_FILLED")
        return

    # ---- DB write (v1) ----
    open_position(
        symbol=symbol,
        side=side,
        size=float(position_size),
        entry_price=float(fill["price"]),
    )

    # Mark this signal as executed (this is what has_executed_signal checks)
    log_event(
        "TRADE_EXECUTED_DEMO",
        f"id={signal_id} {symbol} {side} size={position_size} price={fill['price']}"
    )

    log_info(
        f"DEMO TRADE WRITTEN TO DB | id={signal_id} | {symbol} {side} size={position_size} price={fill['price']}"
    )
