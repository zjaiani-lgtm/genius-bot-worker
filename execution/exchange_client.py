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

    Key points:
      - TESTNET requires correct REST base. ccxt expects spot v3 routes,
        so default TESTNET base is: https://testnet.binance.vision/api/v3
      - Disable fetchCurrencies because it triggers SAPI calls (capital/config)
        which are not available on testnet and will crash load_markets().
      - Keep safety gates even on TESTNET:
          KILL_SWITCH=true  -> block trades
          LIVE_CONFIRMATION=false -> block trades
    """

    DEFAULT_TESTNET_REST_BASE = "https://testnet.binance.vision/api/v3"

    def __init__(self):
        self.mode = os.getenv("MODE", "DEMO").strip().upper()  # DEMO | TESTNET | LIVE
        self.kill_switch = os.getenv("KILL_SWITCH", "false").strip().lower() == "true"
        self.live_confirmation = os.getenv("LIVE_CONFIRMATION", "false").strip().lower() == "true"

        api_key = os.getenv("BINANCE_API_KEY", "").strip()
        api_secret = os.getenv("BINANCE_API_SECRET", "").strip()

        if self.mode in ("TESTNET", "LIVE") and (not api_key or not api_secret):
            raise ExchangeClientError("Missing BINANCE_API_KEY / BINANCE_API_SECRET for TESTNET/LIVE")

        self.exchange = ccxt.binance({
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "options": {
                "defaultType": "spot",
                # ✅ CRITICAL: avoid SAPI calls which break testnet
                "fetchCurrencies": False,
            },
        })

        if self.mode == "TESTNET":
            self._apply_testnet_urls()

        # Validate/prepare markets. (Safe now because fetchCurrencies is False)
        self.exchange.load_markets()

    def _apply_testnet_urls(self):
        """
        IMPORTANT:
        ccxt builds paths assuming spot v3 routes for exchangeInfo, etc.
        So the base should include /api/v3.

        You can override via ENV:
          BINANCE_TESTNET_REST_BASE=https://testnet.binance.vision/api/v3
        """
        base = os.getenv("BINANCE_TESTNET_REST_BASE", self.DEFAULT_TESTNET_REST_BASE).strip()
        if not base:
            base = self.DEFAULT_TESTNET_REST_BASE

        # Ensure no trailing slash
        base = base.rstrip("/")

        # ccxt uses 'public' and 'private' for spot REST
        self.exchange.urls["api"] = {
            "public": base,
            "private": base,
        }

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
        """
        Lightweight connectivity test (public endpoint).
        """
        # Some ccxt versions implement fetch_time for binance; fallback to public ping if needed.
        try:
            return {"serverTime": self.exchange.fetch_time()}
        except Exception:
            # raw public endpoint (binance has /api/v3/ping)
            return self.exchange.publicGetPing()

    # --------- trade ----------
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
        cid = client_order_id or self._new_client_order_id("buy")
        params = {"newClientOrderId": cid, "quoteOrderQty": float(quote_amount)}
        # amount ignored when quoteOrderQty exists; pass 0 safely
        return self.exchange.create_order(symbol, "market", "buy", 0, None, params)

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
        cid = client_order_id or self._new_client_order_id("sell")
        params = {"newClientOrderId": cid}
        return self.exchange.create_order(symbol, "market", "sell", float(base_amount), None, params)

    # --------- helpers ----------
    @staticmethod
    def _new_client_order_id(side: str) -> str:
        # keep reasonably short
        return f"gbm_{side}_{int(time.time())}_{uuid.uuid4().hex[:10]}"
