# execution/config.py
import os
from pathlib import Path


def _env_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "y", "on")


# რეჟიმი: DEMO | LIVE
MODE = os.getenv("MODE", "DEMO").strip().upper()
if MODE not in ("DEMO", "LIVE"):
    MODE = "DEMO"

# LIVE-ზე დამატებითი დაცვა
LIVE_CONFIRMATION = _env_bool("LIVE_CONFIRMATION", "false")

# Startup sync gate
STARTUP_SYNC_ENABLED = _env_bool("STARTUP_SYNC_ENABLED", "true")

# DEMO ბალანსი
VIRTUAL_START_BALANCE = float(os.getenv("VIRTUAL_START_BALANCE", "100000"))

# Binance keys (DEMO-ზე შეიძლება ცარიელი იყოს)
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")

# Kill switch (Render-ზე default TRUE უსაფრთხოდ)
KILL_SWITCH = _env_bool("KILL_SWITCH", "true")

# Persistent DB path (Render disk)
# Mount path: /var/data
DB_PATH = Path(os.getenv("DB_PATH", "/var/data/genius_bot.db"))
