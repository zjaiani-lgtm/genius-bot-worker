# execution/db/db.py

import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path("data/genius_bot.db")

def get_connection():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)

def init_db():
    conn = get_connection()
    cur = conn.cursor()

    with open("execution/db/schema.sql", "r") as f:
        conn.executescript(f.read())

    # init system_state if empty
    cur.execute("SELECT COUNT(*) FROM system_state")
    count = cur.fetchone()[0]

    if count == 0:
        cur.execute(
            """
            INSERT INTO system_state
            (id, mode, status, kill_switch, startup_sync_ok, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "DEMO",
                "PAUSED",
                1,
                0,
                datetime.utcnow().isoformat()
            )
        )

    conn.commit()
    conn.close()

