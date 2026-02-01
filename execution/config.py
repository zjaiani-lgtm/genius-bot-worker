
# execution/config.py
import os

def _env_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "y", "on")

# რეჟიმი: DEMO | LIVE
MODE = os.getenv("MODE", "DEMO").strip().upper()

# LIVE-ზე დამატებითი დაცვა
LIVE_CONFIRMATION = _env_bool("LIVE_CONFIRMATION", "false")

# DEMO ბალანსი
VIRTUAL_START_BALANCE = float(os.getenv("VIRTUAL_START_BALANCE", "100000"))

# Binance keys (DEMO-ზე შეიძლება dummy იყოს)
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")

# Kill switch (Render-ზე default true იყოს უსაფრთხოდ)
KILL_SWITCH = _env_bool("KILL_SWITCH", "true")
