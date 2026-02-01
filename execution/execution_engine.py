# execution/execution_engine.py

from execution.logger import log_info, log_warning
from execution.db.repository import open_position, log_event, get_open_positions
from execution.virtual_wallet import simulate_market_entry

def execute_signal(signal: dict) -> None:
    """
    Execution rules (v1):
    - Only TRADE proceeds
    - Only DEMO simulation (no real exchange)
    - One open position per symbol (basic protection)
    - Write to DB after 'FILLED'
    """
    signal_id = signal.get("signal_id")
    verdict = signal.get("final_verdict")

    # Safety gate
    if verdict != "TRADE":
        log_info(f"Engine: verdict is {verdict} -> NO EXECUTION | id={signal_id}")
        return

    if signal.get("certified_signal") is not True:
        log_warning(f"Engine: signal not certified -> NO EXECUTION | id={signal_id}")
        return

    execution = signal.get("execution") or {}
    symbol = execution.get("symbol")
    direction = execution.get("direction")  # LONG / SHORT
    position_size = execution.get("position_size")
    entry = (execution.get("entry") or {})
    entry_price = entry.get("price")  # for DEMO we require a price in v1 (simple)

    if not symbol or not direction or not position_size:
        log_warning(f"Engine: missing execution fields -> NO EXECUTION | id={signal_id}")
        return

    # Basic 1-position-per-symbol protection
    open_positions = get_open_positions()
    for p in open_positions:
        # positions table columns: (id, symbol, side, size, entry_price, status, opened_at, closed_at, pnl)
        if p[1] == symbol:
            log_warning(f"Engine: open position exists for {symbol} -> BLOCK | id={signal_id}")
            return

    # Map direction to side string for DB
    side = "LONG" if direction.upper() == "LONG" else "SHORT"

    # DEMO: simulate fill
    fill = simulate_market_entry(
        symbol=symbol,
        side=side,
        size=float(position_size),
        price=float(entry_price) if entry_price is not None else None,
    )

    if fill.get("status") != "FILLED":
        log_warning(f"Engine: demo fill not FILLED -> NO DB WRITE | id={signal_id}")
        log_event("ORDER_FAILED", f"id={signal_id} reason=DEMO_NOT_FILLED")
        return

    # DB write (atomic enough for v1)
    open_position(
        symbol=symbol,
        side=side,
        size=float(position_size),
        entry_price=float(fill["price"]),
    )

    log_event("TRADE_EXECUTED_DEMO", f"id={signal_id} {symbol} {side} size={position_size} price={fill['price']}")
    log_info(f"DEMO TRADE WRITTEN TO DB | id={signal_id} | {symbol} {side} size={position_size} price={fill['price']}")

