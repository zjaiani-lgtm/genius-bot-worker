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
    """
    True if we already executed this signal_id.
    We consider a signal executed if there's an audit_log entry:
      event_type = 'TRADE_EXECUTED_DEMO'
      message contains 'id=<signal_id>'
    """
    if not signal_id:
        return False

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM audit_log
        WHERE event_type = 'TRADE_EXECUTED_DEMO'
          AND message LIKE ?
        LIMIT 1
        """,
        (f"%id={signal_id}%",)
    )
    row = cur.fetchone()
    conn.close()
    return row is not None
