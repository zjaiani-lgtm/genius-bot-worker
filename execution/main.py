# execution/main.py

import time
from execution.logger import log_info
from execution.config import MODE

from execution.db.db import init_db

def main():
    log_info(f"GENIUS BOT MAN worker starting | MODE={MODE}")

    init_db()
    log_info("DB initialized")

    while True:
        log_info("Worker alive, waiting for SIGNAL_OUTBOX...")
        time.sleep(10)

if __name__ == "__main__":
    main()

