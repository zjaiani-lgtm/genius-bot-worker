# exchange_client.py

import os
import ccxt
import time


class ExchangeClient:
    def __init__(self):
        self.mode = os.getenv("MODE", "DEMO")          # DEMO | TESTNET | LIVE
        self.kill_switch = os.getenv("KILL_SWITCH", "true").lower() == "true"
        self.live_confirmation = os.getenv("LIVE_CONFIRMATION", "false").lower() == "true"

        self.api_key = os.getenv("BINANCE_API_KEY")
        self.api_secret = os.getenv("BINANCE_API_SECRET")

        self.exchange = None

        if self.mode in ("TESTNET", "LIVE"):
            self._init_binance()

    # -----------------------------
    # INIT BINANCE (SPOT)
    # -----------------------------
    def _init_binance(self):
        if not self.api_key or not self.api_secret:
            raise RuntimeError("❌ BINANCE_API_KEY / BINANCE_API_SECRET not set")

        self.exchange = ccxt.binance({
            "apiKey": self.api_key,
            "secret": self.api_secret,
            "enableRateLimit": True,
        })

        # Spot only
        self.exchange.options["defaultType"] = "spot"

        # TESTNET endpoint
        if self.mode == "TESTNET":
            self.exchange.urls["api"] = {
                "public": "https://testnet.binance.vision/api",
                "private": "https://testnet.binance.vision/api",
            }

        # Safety check
        self.exchange.check_required_credentials()

    # -----------------------------
    # BASIC READ METHODS
    # -----------------------------
    def fetch_balance(self):
        if not self.exchange:
            raise RuntimeError("Exchange not initialized")
        return self.exchange.fetch_balance()

    def fetch_open_orders(self, symbol=None):
        return self.exchange.fetch_open_orders(symbol)

    # -----------------------------
    # ORDER EXECUTION
    # -----------------------------
    def create_market_order(self, symbol, side, amount, client_order_id=None):
        """
        side: BUY or SELL
        amount: base asset amount (e.g. BTC amount)
        """

        if self.kill_switch:
            raise RuntimeError("🛑 KILL_SWITCH is ON — execution blocked")

        if self.mode == "LIVE" and not self.live_confirmation:
            raise RuntimeError("⚠️ LIVE_CONFIRMATION=false — LIVE execution blocked")

        if not self.exchange:
            raise RuntimeError("Exchange not initialized")

        params = {}

        if client_order_id:
            params["newClientOrderId"] = client_order_id

        order = self.exchange.create_order(
            symbol=symbol,
            type="market",
            side=side.lower(),
            amount=amount,
            params=params,
        )

        return order

    # -----------------------------
    # CLOSE POSITION (SPOT)
    # -----------------------------
    def close_spot_position(self, symbol, amount, client_order_id=None):
        """
        Spot CLOSE = SELL base asset
        """
        return self.create_market_order(
            symbol=symbol,
            side="SELL",
            amount=amount,
            client_order_id=client_order_id,
        )

