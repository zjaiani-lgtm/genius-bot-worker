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
    Binance Spot client with TESTNET support (ccxt).

    ✅ Correct behavior:
      - DO NOT use set_sandbox_mode(True) for this flow (causes sapi sandbox errors)
      - For ccxt.binance spot, REST base should be .../api/v3
        because ccxt endpoints are relative like "exchangeInfo", "order", etc.
      - Keep fetchCurrencies=False to avoid SAPI calls on testnet/demo.
    """

    MODE_DEMO = "DEMO"
    MODE_TESTNET = "TESTNET"
    MODE_LIVE = "LIVE"

    # ccxt expects /api/v3 base for spot
    DEFAULT_TESTNET_REST_BASE = "https://testnet.binance.vision/api/v3"
    DEFAULT_DEMO_REST_BASE = "https://demo-api.binance.com/api/v3"
    DEFAULT_LIVE_REST_BASE = "https://api.binance.com/api/v3"

    def __init__(self):
        self.mode = os.getenv("MODE", self.MODE_DEMO).strip().upper()  # DEMO | TESTNET | LIVE
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
                # ✅ avoid SAPI (capital/config) which is not supported on spot testnet/demo
                "fetchCurrencies": False,
                "adjustForTimeDifference": True,
            },
        })

        if self.mode == self.MODE_TESTNET:
            self._apply_testnet_urls()
        elif self.mode == self.MODE_LIVE:
            self._apply_live_urls()
        # DEMO mode: you likely won't call ccxt in your engine, but leaving exchange init ok.

        # Validate/prepare markets
        try:
            self.exchange.load_markets()
        except Exception as e:
            raise ExchangeClientError(f"load_markets failed | MODE={self.mode} | err={e}") from e

    # ---------------- URL overrides ----------------

    def _apply_testnet_urls(self):
        """
        ENV options:
          - BINANCE_TESTNET_REST_BASE=https://testnet.binance.vision/api/v3
          - BINANCE_TESTNET_REST_BASE=https://demo-api.binance.com/api/v3
        """
        base = os.getenv("BINANCE_TESTNET_REST_BASE", "").strip()

        if not base:
            # allow choosing demo by flag
            use_demo = os.getenv("BINANCE_USE_DEMO", "false").strip().lower() == "true"
            base = self.DEFAULT_DEMO_REST_BASE if use_demo else self.DEFAULT_TESTNET_REST_BASE

        base = base.rstrip("/")

        # ✅ IMPORTANT: keep /api/v3 (do not strip it)
        self.exchange.urls["api"] = {"public": base, "private": base}

    def _apply_live_urls(self):
        base = os.getenv("BINANCE_LIVE_REST_BASE", self.DEFAULT_LIVE_REST_BASE).strip() or self.DEFAULT_LIVE_REST_BASE
        base = base.rstrip("/")
        self.exchange.urls["api"] = {"public": base, "private": base}

    # ---------------- gates ----------------

    def _require_trade_allowed(self):
        if self.kill_switch:
            raise LiveTradingBlocked("KILL_SWITCH=true -> trading blocked")
        if not self.live_confirmation:
            raise LiveTradingBlocked("LIVE_CONFIRMATION=false -> trading blocked")

    # ---------------- health / read ----------------

    def health_check(self) -> Dict[str, Any]:
        """
        Startup sync gate:
          - public: ticker
          - private: balance
        """
        try:
            t = self.exchange.fetch_ticker("BTC/USDT")
        except Exception as e:
            raise ExchangeClientError(f"health_check ticker failed | err={e}") from e

        try:
            self.exchange.fetch_balance()
        except Exception as e:
            raise ExchangeClientError(f"health_check balance failed | err={e}") from e

        return {"ok": True, "ticker_last": t.get("last")}

    def fetch_balance(self) -> Dict[str, Any]:
        try:
            return self.exchange.fetch_balance()
        except Exception as e:
            raise ExchangeClientError(f"fetch_balance failed | err={e}") from e

    def exchange_ping(self) -> Dict[str, Any]:
        try:
            return {"serverTime": self.exchange.fetch_time()}
        except Exception:
            try:
                return self.exchange.publicGetPing()
            except Exception as e:
                raise ExchangeClientError(f"exchange_ping failed | err={e}") from e

    # ---------------- trade ----------------

    def create_market_buy_by_quote(
        self,
        symbol: str,
        quote_amount: float,
        client_order_id: Optional[str] = None
    ) -> Dict[str, Any]:
        self._require_trade_allowed()

        if quote_amount <= 0:
            raise ExchangeClientError("quote_amount must be > 0")

        cid = client_order_id or self._new_client_order_id("buy")
        params = {"newClientOrderId": cid, "quoteOrderQty": float(quote_amount)}

        try:
            return self.exchange.create_order(symbol, "market", "buy", None, None, params)
        except TypeError:
            return self.exchange.create_order(symbol, "market", "buy", 0, None, params)
        except Exception as e:
            raise ExchangeClientError(f"market buy failed | {symbol} | err={e}") from e

    def create_market_sell(
        self,
        symbol: str,
        base_amount: float,
        client_order_id: Optional[str] = None
    ) -> Dict[str, Any]:
        self._require_trade_allowed()

        if base_amount <= 0:
            raise ExchangeClientError("base_amount must be > 0")

        cid = client_order_id or self._new_client_order_id("sell")
        params = {"newClientOrderId": cid}

        try:
            return self.exchange.create_order(symbol, "market", "sell", float(base_amount), None, params)
        except Exception as e:
            raise ExchangeClientError(f"market sell failed | {symbol} | err={e}") from e

    # ---------------- helpers ----------------

    @staticmethod
    def _new_client_order_id(side: str) -> str:
        return f"gbm_{side}_{int(time.time())}_{uuid.uuid4().hex[:10]}"
