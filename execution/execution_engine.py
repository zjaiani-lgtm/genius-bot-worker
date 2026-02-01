# execution/execution_engine.py

from execution.logger import log_info, log_warning
from execution.db.repository import (
    open_position,
    close_position,
    log_event,
    get_open_positions,
    get_latest_open_position,
    has_executed_signal,
)
from execution.virtual_wallet import simulate_market_entry, simulate_market_close


def _calc_pnl(side: str, entry_price: float, close_price: float, size: float) -> float:
    """
    Simplified DEMO PnL:
      LONG  pnl = (close - entry) * size
      SHORT pnl = (entry - close) * size
    """
    if side == "LONG":
        return (close_price - entry_price) * size
    return (entry_price - close_price) * size


def execute_signal(signal: dict) -> None:
    """
    Supported verdicts:
      - TRADE (opens position)
      - CLOSE (closes position)

    Gates:
      - certified_signal must be true for TRADE
      - idempotent by signal_id (TRADE_EXECUTED_DEMO / POSITION_CLOSED_DEMO)
    """

    signal_id = signal.get("signal_id")
    verdict = (signal.get("final_verdict") or "").upper()

    # Idempotency gate (applies to both TRADE and CLOSE)
    if has_executed_signal(signal_id):
        log_info(f"Engine: signal already executed -> IGNORE | id={signal_id}")
        return

    # -------------------- OPEN (TRADE) --------------------
    if verdict == "TRADE":
        if signal.get("certified_signal") is not True:
            log_warning(f"Engine: not certified -> NO EXECUTION | id={signal_id}")
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

        # Secondary safety: one open position per symbol
        for p in get_open_positions():
            if p[1] == symbol:
                log_warning(f"Engine: open position exists for {symbol} -> BLOCK | id={signal_id}")
                return

        side = "LONG" if direction == "LONG" else "SHORT"

        try:
            fill = simulate_market_entry(
                symbol=symbol,
                side=side,
                size=float(position_size),
                price=float(entry_price) if entry_price is not None else None,
            )
        except Exception as e:
            log_warning(f"Engine: demo entry error -> NO DB WRITE | id={signal_id} err={e}")
            log_event("ORDER_FAILED", f"id={signal_id} reason=DEMO_ENTRY_ERROR err={e}")
            return

        if fill.get("status") != "FILLED":
            log_warning(f"Engine: entry fill status != FILLED -> NO DB WRITE | id={signal_id}")
            log_event("ORDER_FAILED", f"id={signal_id} reason=DEMO_ENTRY_NOT_FILLED")
            return

        open_position(
            symbol=symbol,
            side=side,
            size=float(position_size),
            entry_price=float(fill["price"]),
        )

        log_event(
            "TRADE_EXECUTED_DEMO",
            f"id={signal_id} {symbol} {side} size={position_size} price={fill['price']}"
        )

        log_info(
            f"DEMO TRADE WRITTEN TO DB | id={signal_id} | {symbol} {side} size={position_size} price={fill['price']}"
        )
        return

    # -------------------- CLOSE --------------------
    if verdict == "CLOSE":
        execution = signal.get("execution") or {}
        symbol = execution.get("symbol")

        close = (execution.get("close") or {})
        close_price = close.get("price")

        if not symbol or close_price is None:
            log_warning(f"Engine: CLOSE missing symbol/close.price -> NO EXECUTION | id={signal_id}")
            return

        pos = get_latest_open_position(symbol)
        if not pos:
            log_warning(f"Engine: no OPEN position for {symbol} -> NO CLOSE | id={signal_id}")
            log_event("CLOSE_BLOCKED", f"id={signal_id} reason=NO_OPEN_POSITION symbol={symbol}")
            return

        # pos: (id, symbol, side, size, entry_price, status, opened_at, closed_at, pnl)
        position_id = pos[0]
        side = pos[2]
        size = float(pos[3])
        entry_price = float(pos[4])

        try:
            fill = simulate_market_close(
                symbol=symbol,
                side=side,
                size=size,
                close_price=float(close_price),
            )
        except Exception as e:
            log_warning(f"Engine: demo close error -> NO DB WRITE | id={signal_id} err={e}")
            log_event("ORDER_FAILED", f"id={signal_id} reason=DEMO_CLOSE_ERROR err={e}")
            return

        if fill.get("status") != "FILLED":
            log_warning(f"Engine: close fill status != FILLED -> NO DB WRITE | id={signal_id}")
            log_event("ORDER_FAILED", f"id={signal_id} reason=DEMO_CLOSE_NOT_FILLED")
            return

        pnl = _calc_pnl(side=side, entry_price=entry_price, close_price=float(close_price), size=size)
        close_position(position_id=position_id, close_price=float(close_price), pnl=pnl)

        log_event(
            "POSITION_CLOSED_DEMO",
            f"id={signal_id} pos_id={position_id} {symbol} {side} size={size} entry={entry_price} close={close_price} pnl={pnl}"
        )

        log_info(
            f"DEMO POSITION CLOSED | id={signal_id} | pos_id={position_id} {symbol} {side} pnl={pnl}"
        )
        return

    # -------------------- OTHER --------------------
    log_info(f"Engine: verdict={verdict} unsupported -> NO EXECUTION | id={signal_id}")
