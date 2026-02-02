# execution/exchange_client.py

import os
import time
import uuid
from typing import Any, Dict, Optional, Tuple

import ccxt


class ExchangeClientError(Exception):
    pass


class LiveTradingBlocked(Exception):
    pass


class BinanceSpotClient:
    """
    Binance Spot client with TESTNET support (ccxt).

    Goals:
      - Stable init (no private calls in __init__)
      - Patch only urls["api"]["public/private"] (do NOT overwrite dict)
      - Two-phase health checks: public-first then private
      - Lazy market loading for when you actually need symbols / trading
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

        self.api_key = os.getenv("BINANCE_API_KEY", "").strip()
        self.api_secret = os.getenv("BINANCE_API_SECRET", "").strip()

        # In DEMO mode (your virtual wallet) keys are optional.
        if self.mode in (self.MODE_TESTNET, self.MODE_LIVE) and (not self.api_key or not self.api_secret):
            raise ExchangeClientError("Missing BINANCE_API_KEY / BINANCE_API_SECRET for TESTNET/LIVE")

        self.exchange = ccxt.binance({
            "apiKey": self.api_key,
            "secret": self.api_secret,
            "enableRateLimit": True,
            "options": {
                "defaultType": "spot",
                # prevents ccxt from calling some SAPI-heavy currency endpoints
                "fetchCurrencies": False,
                "adjustForTimeDifference": True,
            },
        })

        # defensive: sandbox OFF
        if hasattr(self.exchange, "set_sandbox_mode"):
            try:
                self.exchange.set_sandbox_mode(False)
            except Exception:
                pass

        if self.mode == self.MODE_TESTNET:
            self._apply_testnet_urls()
        elif self.mode == self.MODE_LIVE:
            self._apply_live_urls()

        self._markets_loaded = False

    # ---------------- URL overrides ----------------

    def _apply_testnet_urls(self) -> None:
        """
        ENV:
          BINANCE_TESTNET_REST_BASE=https://testnet.binance.vision/api/v3
          BINANCE_TESTNET_REST_BASE=https://demo-api.binance.com/api/v3
        Optional:
          BINANCE_USE_DEMO=true (if ENV base not set)
        """
        base = os.getenv("BINANCE_TESTNET_REST_BASE", "").strip()
        if not base:
            use_demo = os.getenv("BINANCE_USE_DEMO", "false").strip().lower() == "true"
            base = self.DEFAULT_DEMO_REST_BASE if use_demo else self.DEFAULT_TESTNET_REST_BASE

        base = base.rstrip("/")

        # patch only public/private, keep rest of dict (sapi/wapi etc)
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

    # ---------------- diagnostics ----------------

    def diagnostics(self) -> Dict[str, Any]:
        """
        Safe debug payload (never prints full secrets).
        """
        base_public = None
        try:
            base_public = self.exchange.urls.get("api", {}).get("public")
        except Exception:
            pass

        key_prefix = self.api_key[:6] + "..." if self.api_key else None
        return {
            "mode": self.mode,
            "base_public": base_public,
            "key_len": len(self.api_key),
            "secret_len": len(self.api_secret),
            "key_prefix": key_prefix,
        }

    # ---------------- markets ----------------

    def ensure_markets_loaded(self) -> None:
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

    # ---------------- health checks ----------------

    def public_health_check(self) -> Dict[str, Any]:
        """
        No API key required.
        Confirms endpoint correctness.
        """
        try:
            self.exchange.publicGetPing()
            info = self.exchange.publicGetExchangeInfo()
            symbols_count = len(info.get("symbols", [])) if isinstance(info, dict) else None
            return {"ok": True, "symbols": symbols_count}
        except Exception as e:
            raise ExchangeClientError(f"public_health_check failed | err={e}") from e

    def private_health_check(self) -> Dict[str, Any]:
        """
        Requires valid API key/secret for the selected environment.
        """
        try:
            bal = self.exchange.fetch_balance()
            free = bal.get("free", {}) if isinstance(bal, dict) else {}
            return {"ok": True, "free_keys": list(free.keys())[:5]}
        except Exception as e:
            raise ExchangeClientError(f"fetch_balance failed | err={e}") from e

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
        self._require_trade_allowed()

        if quote_amount <= 0:
            raise ExchangeClientError("quote_amount must be > 0")

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
