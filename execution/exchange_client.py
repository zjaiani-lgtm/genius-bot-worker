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
    Binance Spot client with TESTNET support.

    ✅ Fixes:
      - DO NOT overwrite exchange.urls["api"] dict (it contains sapi/wapi etc).
        Overwriting it removes 'sapi' key and causes:
          "binance does not have a testnet/sandbox URL for sapi endpoints"
        (same class of issue as overriding urls['api']=urls['test']). :contentReference[oaicite:1]{index=1}

      - For Spot REST, exchangeInfo is /api/v3/exchangeInfo. :contentReference[oaicite:2]{index=2}
        So TESTNET/DEMO base should be .../api/v3
    """

    MODE_DEMO = "DEMO"
    MODE_TESTNET = "TESTNET"
    MODE_LIVE = "LIVE"

    DEFAULT_TESTNET_REST_BASE = "https://testnet.binance.vision/api/v3"
    DEFAULT_DEMO_REST_BASE = "https://demo-api.binance.com/api/v3"
    DEFAULT_LIVE_REST_BASE = "https://api.binance.com/api/v3"

    def __init__(self):
        self.mode = os.getenv("MODE", self.MODE_DEMO).strip().upper()
        self.kill_switch = os.getenv("KILL_SWITCH", "false").strip().lower() == "true"
        self.live_confirmation = os.getenv("LIVE_CONFIRMATION", "false").strip().lower() == "true"

        api_key = os.getenv("BINANCE_API_KEY", "").strip()
        api_secret = os.getenv("BINANCE_API_SECRET", "").strip()

        if self.mode in (self.MODE_TESTNET, self.MODE_LIVE) and (not api_key or not api_secret):
            raise ExchangeClientError("Missing BINANCE_API_KEY / BINANCE_API_SECRET for TESTNET/LIVE")

        self.exchange = ccxt.binance({
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "options": {
                "defaultType": "spot",
                # ✅ avoid SAPI-heavy currency endpoints
                "fetchCurrencies": False,
                "adjustForTimeDifference": True,
            },
        })

        # 🧯 Defensive: ensure sandbox is NOT enabled even if some other code flips it.
        if hasattr(self.exchange, "set_sandbox_mode"):
            try:
                self.exchange.set_sandbox_mode(False)
            except Exception:
                pass

        if self.mode == self.MODE_TESTNET:
            self._apply_testnet_urls()
        elif self.mode == self.MODE_LIVE:
            self._apply_live_urls()

        # Validate/prepare markets
        try:
            self.exchange.load_markets()
        except Exception as e:
            raise ExchangeClientError(f"load_markets failed | MODE={self.mode} | err={e}") from e

    def _apply_testnet_urls(self):
        """
        ENV:
          BINANCE_TESTNET_REST_BASE=https://testnet.binance.vision/api/v3
          BINANCE_TESTNET_REST_BASE=https://demo-api.binance.com/api/v3
        Optional:
          BINANCE_USE_DEMO=true  (if ENV base not set)
        """
        base = os.getenv("BINANCE_TESTNET_REST_BASE", "").strip()
        if not base:
            use_demo = os.getenv("BINANCE_USE_DEMO", "false").strip().lower() == "true"
            base = self.DEFAULT_DEMO_REST_BASE if use_demo else self.DEFAULT_TESTNET_REST_BASE

        base = base.rstrip("/")

        # ✅ CRITICAL: do NOT overwrite urls["api"] dict — only patch public/private
        if "api" not in self.exchange.urls or not isinstance(self.exchange.urls["api"], dict):
            self.exchange.urls["api"] = {}

        self.exchange.urls["api"]["public"] = base
        self.exchange.urls["api"]["private"] = base

    def _apply_live_urls(self):
        base = os.getenv("BINANCE_LIVE_REST_BASE", self.DEFAULT_LIVE_REST_BASE).strip() or self.DEFAULT_LIVE_REST_BASE
        base = base.rstrip("/")

        if "api" not in self.exchange.urls or not isinstance(self.exchange.urls["api"], dict):
            self.exchange.urls["api"] = {}

        self.exchange.urls["api"]["public"] = base
        self.exchange.urls["api"]["private"] = base

    # --------- gates ----------
    def _require_trade_allowed(self):
        if self.kill_switch:
            raise LiveTradingBlocked("KILL_SWITCH=true -> trading blocked")
        if not self.live_confirmation:
            raise LiveTradingBlocked("LIVE_CONFIRMATION=false -> trading blocked")

    # --------- read ----------
    def fetch_balance(self) -> Dict[str, Any]:
        return self.exchange.fetch_balance()

    def exchange_ping(self) -> Dict[str, Any]:
        try:
            return {"serverTime": self.exchange.fetch_time()}
        except Exception:
            return self.exchange.publicGetPing()

    def health_check(self) -> Dict[str, Any]:
        # public
        t = self.exchange.fetch_ticker("BTC/USDT")
        # private
        self.exchange.fetch_balance()
        return {"ok": True, "last": t.get("last")}

    # --------- trade ----------
    def create_market_buy_by_quote(
        self,
        symbol: str,
        quote_amount: float,
        client_order_id: Optional[str] = None
    ) -> Dict[str, Any]:
        self._require_trade_allowed()
        cid = client_order_id or self._new_client_order_id("buy")
        params = {"newClientOrderId": cid, "quoteOrderQty": float(quote_amount)}

        try:
            return self.exchange.create_order(symbol, "market", "buy", None, None, params)
        except TypeError:
            return self.exchange.create_order(symbol, "market", "buy", 0, None, params)

    def create_market_sell(
        self,
        symbol: str,
        base_amount: float,
        client_order_id: Optional[str] = None
    ) -> Dict[str, Any]:
        self._require_trade_allowed()
        cid = client_order_id or self._new_client_order_id("sell")
        params = {"newClientOrderId": cid}
        return self.exchange.create_order(symbol, "market", "sell", float(base_amount), None, params)

    # --------- helpers ----------
    @staticmethod
    def _new_client_order_id(side: str) -> str:
        return f"gbm_{side}_{int(time.time())}_{uuid.uuid4().hex[:10]}"
