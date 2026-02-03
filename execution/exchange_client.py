# execution/exchange_client.py
import os
import logging
from typing import Any, Dict, Optional
import math

import ccxt

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
    value = float(value)
    step = float(step)
    if step <= 0:
        return value
    n = math.floor((value + 1e-12) / step)
    return float(n * step)


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
            self.exchange.options["fetchCurrencies"] = False

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

    def _ensure_markets(self) -> None:
        if not getattr(self.exchange, "markets", None):
            self.exchange.load_markets()

    def get_lot_step_size(self, symbol: str) -> float:
        self._ensure_markets()
        m = self.exchange.market(symbol)
        filters = (m.get("info") or {}).get("filters", []) or []
        for f in filters:
            if f.get("filterType") == "LOT_SIZE":
                step = _safe_float(f.get("stepSize"), 0.0)
                if step > 0:
                    return step
        prec = int((m.get("precision", {}) or {}).get("amount", 8))
        return 10 ** (-prec)

    def get_price_tick_size(self, symbol: str) -> float:
        self._ensure_markets()
        m = self.exchange.market(symbol)
        filters = (m.get("info") or {}).get("filters", []) or []
        for f in filters:
            if f.get("filterType") == "PRICE_FILTER":
                tick = _safe_float(f.get("tickSize"), 0.0)
                if tick > 0:
                    return tick
        prec = int((m.get("precision", {}) or {}).get("price", 2))
        return 10 ** (-prec)

    def floor_amount(self, symbol: str, amount: float) -> float:
        step = self.get_lot_step_size(symbol)
        return _floor_to_step(float(amount), float(step))

    def floor_price(self, symbol: str, price: float) -> float:
        tick = self.get_price_tick_size(symbol)
        return _floor_to_step(float(price), float(tick))

    # -------------------------
    # Basic calls
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

    def place_oco_sell(
        self,
        symbol: str,
        base_amount: float,
        tp_price: float,
        sl_stop_price: float,
        sl_limit_price: float,
    ) -> Dict[str, Any]:
        """
        Native Binance OCO order (Spot): one request, one balance reserve.
        Endpoint: POST /api/v3/order/oco
        ccxt usually exposes it as privatePostOrderOco (or similar). We try safely.
        """
        self._guard(symbol)

        try:
            amt = self.floor_amount(symbol, base_amount)
            tp_px = self.floor_price(symbol, tp_price)
            stop_px = self.floor_price(symbol, sl_stop_price)
            limit_px = self.floor_price(symbol, sl_limit_price)

            if amt <= 0:
                raise ExchangeClientError(f"OCO amount <= 0 after floor | raw={base_amount}")
            if tp_px <= 0 or stop_px <= 0 or limit_px <= 0:
                raise ExchangeClientError("OCO prices invalid after floor")

            # Binance expects "quantity" (base), "price"(TP), "stopPrice"(SL stop), "stopLimitPrice"(SL limit)
            payload = {
                "symbol": self.exchange.market_id(symbol),  # e.g. BTCUSDT
                "side": "SELL",
                "quantity": amt,
                "price": tp_px,
                "stopPrice": stop_px,
                "stopLimitPrice": limit_px,
                "stopLimitTimeInForce": "GTC",
            }

            # Try ccxt raw methods (varies by version)
            if hasattr(self.exchange, "privatePostOrderOco"):
                res = self.exchange.privatePostOrderOco(payload)
            elif hasattr(self.exchange, "private_post_order_oco"):
                res = self.exchange.private_post_order_oco(payload)
            else:
                # last resort: try through describe / request not available -> fail loudly
                raise ExchangeClientError("ccxt binance: OCO endpoint method not found (privatePostOrderOco).")

            return {"ok": True, "raw": res, "amount": amt, "tp_price": tp_px, "sl_stop": stop_px, "sl_limit": limit_px}

        except Exception as e:
            raise ExchangeClientError(f"OCO sell failed: {e}")
