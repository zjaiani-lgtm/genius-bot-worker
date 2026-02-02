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

    ✅ Key rules:
      - Never overwrite exchange.urls["api"] entirely (keep sapi/wapi keys).
      - In TESTNET, patch only urls["api"]["public/private"].
      - Do NOT call load_markets() inside __init__ (avoid startup auth side-effects).
      - Provide two-phase startup checks:
          1) public endpoints (no API key)
          2) private endpoints (API key required)
    """

    MODE_DEMO = "DEMO"
    MODE_TESTNET = "TESTNET"
    MODE_LIVE = "LIVE"

    # Default bases (spot REST v3)
    DEFAULT_TESTNET_REST_BASE = "https://testnet.binance.vision/api/v3"
    DEFAULT_DEMO_REST_BASE = "https://demo-api.binance.com/api/v3"
    DEFAULT_LIVE_REST_BASE = "https://api.binance.com/api/v3"

    def __init__(self):
        self.mode = os.getenv("MODE", self.MODE_DEMO).strip().upper()
        self.kill_switch = os.getenv("KILL_SWITCH", "false").strip().lower() == "true"
        self.live_confirmation = os.getenv("LIVE_CONFIRMATION", "false").strip().lower() == "true"

        api_key = os.getenv("BINANCE_API_KEY", "").strip()
        api_secret = os.getenv("BINANCE_API_SECRET", "").strip()

        # DEMO mode (your own virtual wallet) may run without keys.
        if self.mode in (self.MODE_TESTNET, self.MODE_LIVE) and (not api_key or not api_secret):
            raise ExchangeClientError("Missing BINANCE_API_KEY / BINANCE_API_SECRET for TESTNET/LIVE")

        self.exchange = ccxt.binance({
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "options": {
                "defaultType": "spot",
                # IMPORTANT: prevents ccxt from calling SAPI-heavy currency endpoints.
                "fetchCurrencies": False,
                "adjustForTimeDifference": True,
            },
        })

        # Defensive: ensure sandbox is OFF (some codebases accidentally flip it)
        if hasattr(self.exchange, "set_sandbox_mode"):
            try:
                self.exchange.set_sandbox_mode(False)
            except Exception:
                pass

        if self.mode == self.MODE_TESTNET:
            self._apply_testnet_urls()
        elif self.mode == self.MODE_LIVE:
            self._apply_live_urls()

        # Do NOT load markets here. We'll do it lazily.
        self._markets_loaded = False

    # ---------------- URL overrides ----------------

    def _apply_testnet_urls(self) -> None:
        """
        ENV:
          BINANCE_TESTNET_REST_BASE=https://testnet.binance.vision/api/v3
          BINANCE_TESTNET_REST_BASE=https://demo-api.binance.com/api/v3

        Optional:
          BINANCE_USE_DEMO=true (if base not set)
        """
        base = os.getenv("BINANCE_TESTNET_REST_BASE", "").strip()
        if not base:
            use_demo = os.getenv("BINANCE_USE_DEMO", "false").strip().lower() == "true"
            base = self.DEFAULT_DEMO_REST_BASE if use_demo else self.DEFAULT_TESTNET_REST_BASE

        base = base.rstrip("/")

        # ✅ CRITICAL: do NOT overwrite urls["api"] dict — patch only public/private
        if "api" not in self.exchange.urls or not isinstance(self.exchange.urls["api"], dict):
            self.exchange.urls["api"] = {}

        self.exchange.urls["api"]["public"] = base
        self.exchange.urls["api"]["private"] = base

    def _apply_live_urls(self) -> None:
        base = os.getenv("BINANCE_LIVE_REST_BASE", self.DEFAULT_LIVE_REST_BASE).strip() or self.DEFAULT_LIVE_REST_BASE
        base = base.rstrip("/")

        if "api" not in self.exchange.urls or not isinstance(self.exchange.urls["api"], dict):
            self.exchange.urls["api"] = {}

        self.exchange.urls["api"]["public"] = base
        self.exchange.urls["api"]["private"] = base

    # ---------------- markets ----------------

    def ensure_markets_loaded(self) -> None:
        """
        Lazy-load markets only when needed.
        This avoids startup failing due to auth-related side effects in some ccxt/binance combos.
        """
        if self._markets_loaded:
            return
        try:
            self.exchange.load_markets()
            self._markets_loaded = True
        except Exception as e:
            raise ExchangeClientError(f"load_markets failed | MODE={self.mode} | err={e}") from e

    # ---------------- gates ----------------

    def _require_trade_allowed(self) -> None:
        if self.kill_switch:
            raise LiveTradingBlocked("KILL_SWITCH=true -> trading blocked")
        if not self.live_confirmation:
            raise LiveTradingBlocked("LIVE_CONFIRMATION=false -> trading blocked")

    # ---------------- startup checks ----------------

    def public_health_check(self) -> Dict[str, Any]:
        """
        Public-only connectivity test (NO API KEY required).
        This is the check that should pass first in STARTUP_SYNC.
        """
        try:
            # /api/v3/ping
            self.exchange.publicGetPing()

            # /api/v3/exchangeInfo (returns symbols list)
            info = self.exchange.publicGetExchangeInfo()
            symbols_count = len(info.get("symbols", [])) if isinstance(info, dict) else None

            return {"ok": True, "symbols": symbols_count}
        except Exception as e:
            raise ExchangeClientError(f"public_health_check failed | err={e}") from e

    def private_health_check(self) -> Dict[str, Any]:
        """
        Private connectivity test (API KEY required).
        This is where invalid key/secret will show up (-2008 etc).
        """
        try:
            bal = self.exchange.fetch_balance()
            # Keep response small
            total = bal.get("total", {}) if isinstance(bal, dict) else {}
            return {"ok": True, "has_total": bool(total)}
        except Exception as e:
            raise ExchangeClientError(f"private_health_check failed | err={e}") from e

    def health_check(self) -> Dict[str, Any]:
        """
        Combined check (optional). Useful when you want "EXCHANGE_OK" in one call.
        """
        pub = self.public_health_check()
        priv = self.private_health_check()
        return {"ok": True, "public": pub, "private": priv}

    # ---------------- read ----------------

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
        """
        Spend QUOTE amount (e.g. 10 USDT) to buy BASE (BTC).
        Uses Binance param: quoteOrderQty.
        """
        self._require_trade_allowed()

        if quote_amount <= 0:
            raise ExchangeClientError("quote_amount must be > 0")

        # Ensure markets are available (symbol formatting, etc.)
        self.ensure_markets_loaded()

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
        """
        Sell BASE amount (e.g. 0.0002 BTC) back to QUOTE (USDT).
        """
        self._require_trade_allowed()

        if base_amount <= 0:
            raise ExchangeClientError("base_amount must be > 0")

        self.ensure_markets_loaded()

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
