# execution/exchange_client.py
import os
import logging
from typing import Any, Dict, Optional

import ccxt
import math

logger = logging.getLogger("gbm")


class ExchangeClientError(Exception):
    pass


class LiveTradingBlocked(Exception):
    pass


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _floor_to_step(value: float, step: float) -> float:
    """
    Floors 'value' to the nearest step grid.
    Important: Binance filters require stepSize exactness.
    """
    value = float(value)
    step = float(step)
    if step <= 0:
        return value
    # small epsilon to avoid float artifacts like 0.000089999999
    n = math.floor((value + 1e-12) / step)
    out = n * step
    # normalize float
    return float(out)


class BinanceSpotClient:
    TESTNET_REST_BASE = "https://testnet.binance.vision/api"

    def __init__(self):
        self.mode = os.getenv("MODE", "DEMO").upper()  # DEMO | TESTNET | LIVE
        self.kill_switch = os.getenv("KILL_SWITCH", "false").lower() == "true"
        self.live_confirmation = os.getenv("LIVE_CONFIRMATION", "false").lower() == "true"

        self.max_quote_per_trade = float(os.getenv("MAX_QUOTE_PER_TRADE", "10"))
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

        self.exchange = ccxt.binance({
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "options": {"defaultType": "spot"},
        })

        if self.mode == "TESTNET":
            self.exchange.urls["api"] = {
                "public": self.TESTNET_REST_BASE,
                "private": self.TESTNET_REST_BASE,
            }
            # testnet usually fails on SAPI endpoints
            self.exchange.options["fetchCurrencies"] = False

        # load markets once
        try:
            self.exchange.load_markets()
        except Exception as e:
            logger.warning(f"LOAD_MARKETS_WARN | err={e}")

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

    def diagnostics(self) -> Dict[str, Any]:
        try:
            bal = self.exchange.fetch_balance()
            sym = next(iter(self.symbol_whitelist)) if self.symbol_whitelist else "BTC/USDT"
            t = self.exchange.fetch_ticker(sym)
            return {
                "mode": self.mode,
                "kill_switch": self.kill_switch,
                "live_confirmation": self.live_confirmation,
                "symbol_probe": sym,
                "last_price": float(t.get("last") or 0.0),
                "usdt_free": float((bal.get("free", {}) or {}).get("USDT", 0.0) or 0.0),
                "ok": True,
            }
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # -------------------------
    # Market filters helpers
    # -------------------------
    def _ensure_markets(self) -> None:
        if not getattr(self.exchange, "markets", None):
            self.exchange.load_markets()

    def get_lot_step_size(self, symbol: str) -> float:
        """
        Binance LOT_SIZE stepSize
        """
        self._ensure_markets()
        m = self.exchange.market(symbol)
        filters = (m.get("info") or {}).get("filters", []) or []
        for f in filters:
            if f.get("filterType") == "LOT_SIZE":
                step = _safe_float(f.get("stepSize"), 0.0)
                if step > 0:
                    return step
        # fallback
        prec = int((m.get("precision", {}) or {}).get("amount", 8))
        return 10 ** (-prec)

    def get_price_tick_size(self, symbol: str) -> float:
        """
        Binance PRICE_FILTER tickSize
        """
        self._ensure_markets()
        m = self.exchange.market(symbol)
        filters = (m.get("info") or {}).get("filters", []) or []
        for f in filters:
            if f.get("filterType") == "PRICE_FILTER":
                tick = _safe_float(f.get("tickSize"), 0.0)
                if tick > 0:
                    return tick
        # fallback
        prec = int((m.get("precision", {}) or {}).get("price", 2))
        return 10 ** (-prec)

    def floor_amount(self, symbol: str, amount: float) -> float:
        step = self.get_lot_step_size(symbol)
        return _floor_to_step(float(amount), float(step))

    def floor_price(self, symbol: str, price: float) -> float:
        tick = self.get_price_tick_size(symbol)
        return _floor_to_step(float(price), float(tick))

    # -------------------------
    # Basic exchange calls
    # -------------------------
    def fetch_last_price(self, symbol: str) -> float:
        t = self.exchange.fetch_ticker(symbol)
        return float(t["last"])

    def fetch_balance_free(self, asset: str) -> float:
        bal = self.exchange.fetch_balance()
        return float((bal.get("free", {}) or {}).get(asset.upper(), 0.0) or 0.0)

    def fetch_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        return self.exchange.fetch_order(str(order_id), symbol)

    def cancel_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        return self.exchange.cancel_order(str(order_id), symbol)

    # -------------------------
    # Trading
    # -------------------------
    def place_market_buy_by_quote(self, symbol: str, quote_amount: float) -> Dict[str, Any]:
        self._guard(symbol, quote_amount=quote_amount)
        try:
            params = {"quoteOrderQty": float(quote_amount)}
            return self.exchange.create_order(symbol, "market", "buy", None, None, params)
        except Exception as e:
            raise ExchangeClientError(f"Market buy failed: {e}")

    def place_limit_sell_amount(self, symbol: str, base_amount: float, price: float) -> Dict[str, Any]:
        self._guard(symbol)
        try:
            amt = self.floor_amount(symbol, base_amount)
            px = self.floor_price(symbol, price)

            if amt <= 0:
                raise ExchangeClientError(f"Limit sell amount <= 0 after floor | raw={base_amount}")
            if px <= 0:
                raise ExchangeClientError(f"Limit sell price <= 0 after floor | raw={price}")

            return self.exchange.create_order(symbol, "limit", "sell", float(amt), float(px))
        except Exception as e:
            raise ExchangeClientError(f"Limit sell failed: {e}")

    def place_stop_loss_limit_sell(
        self,
        symbol: str,
        base_amount: float,
        stop_price: float,
        limit_price: float
    ) -> Dict[str, Any]:
        """
        Binance SPOT Stop-Loss-Limit order.
        We floor both amount and prices to exchange filters.
        """
        self._guard(symbol)
        try:
            amt = self.floor_amount(symbol, base_amount)
            stop_px = self.floor_price(symbol, stop_price)
            limit_px = self.floor_price(symbol, limit_price)

            if amt <= 0:
                raise ExchangeClientError(f"SL amount <= 0 after floor | raw={base_amount}")
            if stop_px <= 0 or limit_px <= 0:
                raise ExchangeClientError(f"SL prices invalid after floor | raw_stop={stop_price} raw_limit={limit_price}")
            if limit_px > stop_px:
                # for sell SL-limit, usually limit <= stop
                logger.warning(f"SL_PRICE_WARN | limit>stop after floor | stop={stop_px} limit={limit_px}")

            params = {"stopPrice": float(stop_px), "timeInForce": "GTC"}

            # ccxt for Binance spot supports 'STOP_LOSS_LIMIT'
            return self.exchange.create_order(symbol, "STOP_LOSS_LIMIT", "sell", float(amt), float(limit_px), params)
        except Exception as e:
            raise ExchangeClientError(f"Stop-loss-limit sell failed: {e}")
