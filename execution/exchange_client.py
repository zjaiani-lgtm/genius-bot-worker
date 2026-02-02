# execution/exchange_client.py
import os
import logging
from typing import Any, Dict, Optional

import ccxt

logger = logging.getLogger("gbm")


class ExchangeClientError(Exception):
    pass


class LiveTradingBlocked(Exception):
    pass


class BinanceSpotClient:
    """
    Binance Spot client supporting:
      - DEMO: blocked (should not be called)
      - TESTNET: uses testnet REST base URL
      - LIVE: uses production REST base URL

    Safety gates:
      - KILL_SWITCH=true blocks
      - LIVE_CONFIRMATION=true required for LIVE
      - MAX_QUOTE_PER_TRADE caps risk in USDT
    """

    TESTNET_REST_BASE = "https://testnet.binance.vision/api"

    def __init__(self):
        self.mode = os.getenv("MODE", "DEMO").upper()  # DEMO | TESTNET | LIVE
        self.kill_switch = os.getenv("KILL_SWITCH", "false").lower() == "true"
        self.live_confirmation = os.getenv("LIVE_CONFIRMATION", "false").lower() == "true"

        self.max_quote_per_trade = float(os.getenv("MAX_QUOTE_PER_TRADE", "10"))  # risk cap
        self.symbol_whitelist = set(
            s.strip().upper()
            for s in os.getenv("SYMBOL_WHITELIST", "BTC/USDT").split(",")
            if s.strip()
        )

        api_key = os.getenv("BINANCE_API_KEY", "").strip()
        api_secret = os.getenv("BINANCE_API_SECRET", "").strip()

        if self.mode in ("LIVE", "TESTNET"):
            if not api_key or not api_secret:
                raise ExchangeClientError("Missing BINANCE_API_KEY / BINANCE_API_SECRET for LIVE/TESTNET.")

        # Spot exchange (ccxt)
        self.exchange = ccxt.binance({
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
        })

        # TESTNET override
        if self.mode == "TESTNET":
            self.exchange.urls["api"] = {
                "public": self.TESTNET_REST_BASE,
                "private": self.TESTNET_REST_BASE,
            }
            # On Binance testnet, some SAPI endpoints fail; avoid fetchCurrencies auto-calls.
            self.exchange.options["fetchCurrencies"] = False

    def _guard(self, symbol: str, quote_amount: Optional[float] = None) -> None:
        if self.kill_switch:
            raise LiveTradingBlocked("KILL_SWITCH is ON.")
        if self.mode == "LIVE" and not self.live_confirmation:
            raise LiveTradingBlocked("LIVE_CONFIRMATION is OFF.")
        if self.mode == "DEMO":
            raise LiveTradingBlocked("MODE=DEMO -> exchange client must not execute real orders.")
        if symbol and symbol.upper() not in self.symbol_whitelist:
            raise LiveTradingBlocked(f"Symbol not allowed by whitelist: {symbol}.")
        if quote_amount is not None and quote_amount > self.max_quote_per_trade:
            raise LiveTradingBlocked(
                f"quote_amount {quote_amount} exceeds MAX_QUOTE_PER_TRADE={self.max_quote_per_trade}"
            )

    def fetch_last_price(self, symbol: str) -> float:
        t = self.exchange.fetch_ticker(symbol)
        return float(t["last"])

    def fetch_free_balance(self, asset: str) -> float:
        bal = self.exchange.fetch_balance()
        free = bal.get("free", {}) or {}
        return float(free.get(asset, 0.0) or 0.0)

    def place_market_buy_by_quote(self, symbol: str, quote_amount: float) -> Dict[str, Any]:
        """
        Market BUY using quote amount (USDT).
        Uses 'quoteOrderQty' (Binance supports it). ccxt passes params.
        """
        self._guard(symbol, quote_amount=quote_amount)

        try:
            params = {"quoteOrderQty": float(quote_amount)}
            order = self.exchange.create_order(symbol, "market", "buy", None, None, params)
            return order
        except Exception as e:
            raise ExchangeClientError(f"Market buy failed: {e}")

    def place_market_sell_amount(self, symbol: str, base_amount: float) -> Dict[str, Any]:
        """
        Market SELL using base amount (e.g. BTC amount).
        """
        self._guard(symbol)

        try:
            amt = float(self.exchange.amount_to_precision(symbol, base_amount))
            order = self.exchange.create_order(symbol, "market", "sell", float(amt), None)
            return order
        except Exception as e:
            raise ExchangeClientError(f"Market sell failed: {e}")

    def place_limit_sell_amount(self, symbol: str, base_amount: float, price: float) -> Dict[str, Any]:
        """
        LIMIT SELL that will likely remain open (if price > current last).
        """
        self._guard(symbol)

        try:
            amt = float(self.exchange.amount_to_precision(symbol, base_amount))
            px = float(self.exchange.price_to_precision(symbol, price))
            order = self.exchange.create_order(symbol, "limit", "sell", float(amt), float(px))
            return order
        except Exception as e:
            raise ExchangeClientError(f"Limit sell failed: {e}")
