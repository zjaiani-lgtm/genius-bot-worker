# execution/db/repository.py

from datetime import datetime
from execution.db.db import get_connection

# ---------------- SYSTEM STATE ----------------

def get_system_state():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM system_state WHERE id = 1")
    row = cur.fetchone()
    conn.close()
    return row

def update_system_state(status=None, startup_sync_ok=None, kill_switch=None):
    conn = get_connection()
    cur = conn.cursor()

    fields = []
    values = []

    if status is not None:
        fields.append("status = ?")
        values.append(status)

    if startup_sync_ok is not None:
        fields.append("startup_sync_ok = ?")
        values.append(int(startup_sync_ok))

    if kill_switch is not None:
        fields.append("kill_switch = ?")
        values.append(int(kill_switch))

    fields.append("updated_at = ?")
    values.append(datetime.utcnow().isoformat())

    sql = f"UPDATE system_state SET {', '.join(fields)} WHERE id = 1"
    cur.execute(sql, values)

    conn.commit()
    conn.close()

# ---------------- POSITIONS ----------------

def get_open_positions():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM positions WHERE status = 'OPEN'")
    rows = cur.fetchall()
    conn.close()
    return rows

def get_latest_open_position(symbol: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, symbol, side, size, entry_price, status, opened_at, closed_at, pnl
        FROM positions
        WHERE status = 'OPEN' AND symbol = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (symbol,)
    )
    row = cur.fetchone()
    conn.close()
    return row

def open_position(symbol, side, size, entry_price):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO positions
        (symbol, side, size, entry_price, status, opened_at)
        VALUES (?, ?, ?, ?, 'OPEN', ?)
        """,
        (symbol, side, float(size), float(entry_price), datetime.utcnow().isoformat())
    )
    conn.commit()
    conn.close()

def close_position(position_id: int, close_price: float, pnl: float):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE positions
        SET status='CLOSED', closed_at=?, pnl=?
        WHERE id=?
        """,
        (datetime.utcnow().isoformat(), float(pnl), int(position_id))
    )
    conn.commit()
    conn.close()

# ---------------- AUDIT LOG ----------------

def log_event(event_type, message):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO audit_log (event_type, message, created_at)
        VALUES (?, ?, ?)
        """,
        (event_type, message, datetime.utcnow().isoformat())
    )
    conn.commit()
    conn.close()

def has_executed_signal(signal_id: str) -> bool:
    if not signal_id:
        return False

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM audit_log
        WHERE event_type IN ('TRADE_EXECUTED_DEMO','POSITION_CLOSED_DEMO')
          AND message LIKE ?
        LIMIT 1
        """,
        (f"%id={signal_id}%",)
    )
    row = cur.fetchone()
    conn.close()
    return row is not None

# ---------------- INSPECT HELPERS ----------------

def list_positions(limit: int = 20):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, symbol, side, size, entry_price, status, opened_at, closed_at, pnl
        FROM positions
        ORDER BY id DESC
        LIMIT ?
        """,
        (int(limit),)
    )
    rows = cur.fetchall()
    conn.close()
    return rows

def list_audit_log(limit: int = 30):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, event_type, message, created_at
        FROM audit_log
        ORDER BY id DESC
        LIMIT ?
        """,
        (int(limit),)
    )
    rows = cur.fetchall()
    conn.close()
    return rows
