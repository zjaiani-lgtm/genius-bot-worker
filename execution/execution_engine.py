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
    if side == "LONG":
        return (close_price - entry_price) * size
    return (entry_price - close_price) * size


def _normalize_symbol(s: str) -> str:
    return (s or "").strip().upper()


def execute_signal(signal: dict) -> dict:
    """
    Returns dict:
      {
        "handled": bool,
        "outcome": str,
        "reason": str | None
      }

    outcome examples:
      - OPENED
      - CLOSED
      - IGNORE_ALREADY_EXECUTED
      - TRADE_BLOCKED_OPEN_EXISTS
      - CLOSE_BLOCKED_NO_OPEN
      - INVALID_FIELDS
      - UNSUPPORTED_VERDICT
      - ERROR
    """

    signal_id = signal.get("signal_id")
    verdict = _normalize_symbol(signal.get("final_verdict"))

    # Idempotency (both TRADE/CLOSE)
    if has_executed_signal(signal_id):
        return {"handled": True, "outcome": "IGNORE_ALREADY_EXECUTED", "reason": "signal_id_already_executed"}

    # ---------------- TRADE ----------------
    if verdict == "TRADE":
        if signal.get("certified_signal") is not True:
            return {"handled": True, "outcome": "INVALID_FIELDS", "reason": "not_certified"}

        execution = signal.get("execution") or {}
        symbol = execution.get("symbol")
        direction = _normalize_symbol(execution.get("direction"))
        position_size = execution.get("position_size")
        entry = execution.get("entry") or {}
        entry_price = entry.get("price")

        if not symbol or direction not in ("LONG", "SHORT") or position_size is None or entry_price is None:
            return {"handled": True, "outcome": "INVALID_FIELDS", "reason": "missing_trade_fields"}

        symbol_norm = _normalize_symbol(symbol)

        # Secondary safety: one open position per symbol
        for p in get_open_positions():
            # positions raw row: (id, symbol, side, size, entry_price, status, opened_at, closed_at, pnl)
            if _normalize_symbol(p[1]) == symbol_norm:
                log_warning(f"Engine: open position exists for {symbol} -> BLOCK | id={signal_id}")
                return {"handled": True, "outcome": "TRADE_BLOCKED_OPEN_EXISTS", "reason": f"open_exists:{symbol}"}

        side = "LONG" if direction == "LONG" else "SHORT"

        try:
            fill = simulate_market_entry(
                symbol=symbol,
                side=side,
                size=float(position_size),
                price=float(entry_price),
            )
        except Exception as e:
            log_warning(f"Engine: demo entry error | id={signal_id} err={e}")
            log_event("ORDER_FAILED", f"id={signal_id} reason=DEMO_ENTRY_ERROR err={e}")
            return {"handled": True, "outcome": "ERROR", "reason": "demo_entry_error"}

        if fill.get("status") != "FILLED":
            log_event("ORDER_FAILED", f"id={signal_id} reason=DEMO_ENTRY_NOT_FILLED")
            return {"handled": True, "outcome": "ERROR", "reason": "demo_entry_not_filled"}

        open_position(
            symbol=symbol,
            side=side,
            size=float(position_size),
            entry_price=float(fill["price"]),
        )

        log_event(
            "TRADE_EXECUTED_DEMO",
            f"id={signal_id} {symbol} {side} size={position_size} price={fill['price']}",
        )

        log_info(f"DEMO TRADE OPENED | id={signal_id} | {symbol} {side} size={position_size} price={fill['price']}")
        return {"handled": True, "outcome": "OPENED", "reason": None}

    # ---------------- CLOSE ----------------
    if verdict == "CLOSE":
        execution = signal.get("execution") or {}
        symbol = execution.get("symbol")
        close = execution.get("close") or {}
        close_price = close.get("price")

        if not symbol or close_price is None:
            return {"handled": True, "outcome": "INVALID_FIELDS", "reason": "missing_close_fields"}

        symbol_norm = _normalize_symbol(symbol)

        # 1) try direct query
        pos = get_latest_open_position(symbol)

        # 2) fallback: scan open positions with normalization (fixes subtle symbol mismatch)
        if pos is None:
            for p in get_open_positions():
                if _normalize_symbol(p[1]) == symbol_norm:
                    # build pos tuple in same format as get_latest_open_position returns:
                    # (id, symbol, side, size, entry_price, status, opened_at, closed_at, pnl)
                    pos = (p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8])
                    break

        if pos is None:
            log_warning(f"Engine: no OPEN position for {symbol} -> NO CLOSE | id={signal_id}")
            log_event("CLOSE_BLOCKED", f"id={signal_id} reason=NO_OPEN_POSITION symbol={symbol}")
            return {"handled": True, "outcome": "CLOSE_BLOCKED_NO_OPEN", "reason": f"no_open:{symbol}"}

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
            log_warning(f"Engine: demo close error | id={signal_id} err={e}")
            log_event("ORDER_FAILED", f"id={signal_id} reason=DEMO_CLOSE_ERROR err={e}")
            return {"handled": True, "outcome": "ERROR", "reason": "demo_close_error"}

        if fill.get("status") != "FILLED":
            log_event("ORDER_FAILED", f"id={signal_id} reason=DEMO_CLOSE_NOT_FILLED")
            return {"handled": True, "outcome": "ERROR", "reason": "demo_close_not_filled"}

        pnl = _calc_pnl(side=side, entry_price=entry_price, close_price=float(close_price), size=size)

        close_position(position_id=position_id, close_price=float(close_price), pnl=pnl)

        log_event(
            "POSITION_CLOSED_DEMO",
            f"id={signal_id} pos_id={position_id} {symbol} {side} entry={entry_price} close={close_price} size={size} pnl={pnl}",
        )

        log_info(f"DEMO POSITION CLOSED | id={signal_id} | pos_id={position_id} {symbol} {side} pnl={pnl}")
        return {"handled": True, "outcome": "CLOSED", "reason": None}

    # ---------------- OTHER ----------------
    return {"handled": False, "outcome": "UNSUPPORTED_VERDICT", "reason": verdict}
