# execution/exchange_client.py

import os
import time
import uuid
from typing import Any, Dict, Optional

import ccxt


class ExchangeClientError(Exception):
    pass


class LiveTradingBlocked(Exception):
    pass


class BinanceSpotClient:
    """
    Binance Spot client supporting TESTNET by overriding REST base URL.
    Key point for TESTNET:
      - Disable fetchCurrencies (it calls SAPI capital/config endpoints and fails on testnet).
    """

    TESTNET_REST_BASE = "https://testnet.binance.vision/api"

    def __init__(self):
        self.mode = os.getenv("MODE", "DEMO").upper()  # DEMO | TESTNET | LIVE
        self.kill_switch = os.getenv("KILL_SWITCH", "false").lower() == "true"
        self.live_confirmation = os.getenv("LIVE_CONFIRMATION", "false").lower() == "true"

        api_key = os.getenv("BINANCE_API_KEY", "").strip()
        api_secret = os.getenv("BINANCE_API_SECRET", "").strip()

        if self.mode in ("TESTNET", "LIVE"):
            if not api_key or not api_secret:
                raise ExchangeClientError("Missing BINANCE_API_KEY / BINANCE_API_SECRET for TESTNET/LIVE")

        self.exchange = ccxt.binance({
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "options": {
                "defaultType": "spot",
                # ✅ CRITICAL: prevent SAPI calls (capital/config/...) triggered by load_markets()
                # Testnet doesn't support ccxt's sandbox SAPI flow -> crashes otherwise.
                "fetchCurrencies": False,
            },
        })

        if self.mode == "TESTNET":
            self._apply_testnet_urls()

        # ✅ load_markets AFTER setting fetchCurrencies False + urls
        self.exchange.load_markets()

    def _apply_testnet_urls(self):
        # Force REST base to official Spot Testnet endpoint
        self.exchange.urls["api"] = {
            "public": self.TESTNET_REST_BASE,
            "private": self.TESTNET_REST_BASE,
        }

    def _require_trade_allowed(self):
        # Even on TESTNET we keep gates to avoid accidental trading.
        if self.kill_switch:
            raise LiveTradingBlocked("KILL_SWITCH=true -> trading blocked")
        if not self.live_confirmation:
            raise LiveTradingBlocked("LIVE_CONFIRMATION=false -> trading blocked")

    # --------- read ----------
    def fetch_balance(self) -> Dict[str, Any]:
        return self.exchange.fetch_balance()

    # --------- trade ----------
    def create_market_buy_by_quote(self, symbol: str, quote_amount: float, client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Spend quote currency amount (e.g. 10 USDT) to buy base (BTC).
        Uses Binance param: quoteOrderQty.
        """
        self._require_trade_allowed()
        cid = client_order_id or self._new_client_order_id("buy")
        params = {"newClientOrderId": cid, "quoteOrderQty": float(quote_amount)}
        return self.exchange.create_order(symbol, "market", "buy", 0, None, params)

    def create_market_sell(self, symbol: str, base_amount: float, client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Sell base amount (e.g. 0.0002 BTC) back to quote (USDT).
        """
        self._require_trade_allowed()
        cid = client_order_id or self._new_client_order_id("sell")
        params = {"newClientOrderId": cid}
        return self.exchange.create_order(symbol, "market", "sell", float(base_amount), None, params)

    # --------- helpers ----------
    @staticmethod
    def _new_client_order_id(side: str) -> str:
        return f"gbm_{side}_{int(time.time())}_{uuid.uuid4().hex[:10]}"
