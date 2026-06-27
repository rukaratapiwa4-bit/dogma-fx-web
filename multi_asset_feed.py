"""
═══════════════════════════════════════════════════════════════════════════════
FOREX TRADING SIGNAL SYSTEM — VERSION 6.0
FILE: multi_asset_feed.py
LAYER: 1 — DATA PERCEPTION LAYER (Multi-Asset Sub-Component)
═══════════════════════════════════════════════════════════════════════════════

PURPOSE:
    Fetches and maintains multi-asset price data used for:
        - Macro environment assessment
        - SMT divergence detection
        - Correlation breakdown detection
        - Crisis early warning
        - Dollar strength/weakness context

ASSETS TRACKED:
    DXY   → US Dollar Index (dollar strength)
    GOLD  → XAU/USD (safe haven / risk sentiment)
    OIL   → WTI Crude (risk on/off, CAD correlate)
    US10Y → 10-Year Treasury Yield (rate expectations)
    VIX   → CBOE Volatility Index (fear — REFERENCE ONLY)
    SP500 → S&P 500 Index (risk sentiment)
    EURUSD→ EUR/USD (correlation check vs DXY)
    GBPUSD→ GBP/USD (correlation check)
    USDJPY→ USD/JPY (risk sentiment / safe haven)

SOURCES:
    Primary   → Yahoo Finance (free, no API key needed)
    Secondary → OANDA (for forex pairs — already connected)
    Fallback  → Stooq CSV endpoint (free)

ARCHITECTURAL RULES:
    - Collects and labels raw price data ONLY
    - NEVER interprets what rising/falling means for trades
    - NEVER labels anything as "bullish" or "bearish"
    - Outputs: price + change + correlation + volatility score
    - All interpretation belongs to Layer 3

OUTPUT:
    {
        "DXY"   : { price, change_pct, volatility_score, ... },
        "GOLD"  : { price, change_pct, volatility_score, ... },
        "VIX"   : { price, change_pct, ... },
        ...
        "correlations" : { "EUR/USD_DXY": -0.92, ... },
        "stress_flags" : { "dxy_spike": bool, "vix_spike": bool, ... },
    }

NOTE ON VIX:
    VIX is REFERENCE ONLY for this forex system.
    Primary forex stress indicators:
        DXY Volatility
        Currency Implied Volatility
        FX Volatility Indices
        Correlation Breakdown
    VIX is a stock market fear index — included as supporting signal only.

═══════════════════════════════════════════════════════════════════════════════
"""

import time
import json
import logging
import threading
import requests
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import Optional
from collections import deque
from dataclasses import dataclass, field

logger = logging.getLogger("MultiAssetFeed")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — ASSET DEFINITIONS
# ═══════════════════════════════════════════════════════════════════════════════

# Yahoo Finance ticker symbols
YAHOO_TICKERS = {
    "DXY"   : "DX-Y.NYB",   # US Dollar Index
    "GOLD"  : "GC=F",       # Gold Futures
    "OIL"   : "CL=F",       # WTI Crude Oil Futures
    "US10Y" : "^TNX",       # 10-Year Treasury Yield
    "VIX"   : "^VIX",       # CBOE Volatility Index
    "SP500" : "^GSPC",      # S&P 500
    "EURUSD": "EURUSD=X",   # EUR/USD
    "GBPUSD": "GBPUSD=X",   # GBP/USD
    "USDJPY": "JPY=X",      # USD/JPY
    "USDCHF": "CHF=X",      # USD/CHF
}

# Stooq fallback tickers
STOOQ_TICKERS = {
    "DXY"   : "DXY",
    "GOLD"  : "XAUUSD",
    "OIL"   : "CL.F",
    "US10Y" : "10USY.B",
    "SP500" : "SPX",
    "EURUSD": "EURUSD",
}

# Asset metadata
ASSET_META = {
    "DXY"   : {"name": "US Dollar Index",        "type": "FX_INDEX",  "unit": "index"},
    "GOLD"  : {"name": "Gold (XAU/USD)",          "type": "COMMODITY", "unit": "USD/oz"},
    "OIL"   : {"name": "WTI Crude Oil",           "type": "COMMODITY", "unit": "USD/bbl"},
    "US10Y" : {"name": "US 10Y Treasury Yield",   "type": "BOND",      "unit": "percent"},
    "VIX"   : {"name": "CBOE Volatility Index",   "type": "VOLATILITY","unit": "index"},
    "SP500" : {"name": "S&P 500 Index",           "type": "EQUITY",    "unit": "index"},
    "EURUSD": {"name": "EUR/USD",                 "type": "FX_PAIR",   "unit": "rate"},
    "GBPUSD": {"name": "GBP/USD",                 "type": "FX_PAIR",   "unit": "rate"},
    "USDJPY": {"name": "USD/JPY",                 "type": "FX_PAIR",   "unit": "rate"},
}

# Stress thresholds — MEASUREMENT ONLY, no trade decisions here
STRESS_THRESHOLDS = {
    "VIX_ELEVATED"      : 20.0,    # VIX above this = elevated
    "VIX_HIGH"          : 30.0,    # VIX above this = high stress
    "VIX_CRISIS"        : 40.0,    # VIX above this = crisis (supporting signal)
    "DXY_DAILY_MOVE"    : 0.5,     # DXY daily move above this % = elevated
    "GOLD_DAILY_MOVE"   : 1.5,     # Gold daily move above this % = stress signal
    "CORRELATION_BREAK" : 0.3,     # Correlation below this magnitude = breakdown
    "VOLATILITY_SPIKE"  : 2.0,     # Price outside N std dev = spike
}

# Expected correlations (approximately)
EXPECTED_CORRELATIONS = {
    ("EUR/USD", "DXY")   : -0.90,   # Strong inverse
    ("GBP/USD", "DXY")   : -0.80,   # Strong inverse
    ("USD/JPY", "DXY")   : +0.70,   # Positive
    ("EUR/USD", "GBP/USD"): +0.85,  # Strong positive (SMT pairs)
    ("GOLD",    "DXY")   : -0.75,   # Inverse
    ("SP500",   "VIX")   : -0.80,   # Strong inverse
}


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — ASSET PRICE RECORD
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class AssetPrice:
    """
    Standardized price record for any asset.
    Measurement only — no interpretation attached.
    """
    symbol          : str
    price           : float
    open_price      : float
    high_price      : float
    low_price       : float
    prev_close      : float
    timestamp_utc   : float
    source          : str

    # Computed on creation
    change_abs      : float = field(init=False)
    change_pct      : float = field(init=False)

    def __post_init__(self):
        if self.prev_close and self.prev_close != 0:
            self.change_abs = self.price - self.prev_close
            self.change_pct = (self.change_abs / self.prev_close) * 100.0
        else:
            self.change_abs = 0.0
            self.change_pct = 0.0

    def to_dict(self) -> dict:
        meta = ASSET_META.get(self.symbol, {})
        return {
            "symbol"        : self.symbol,
            "name"          : meta.get("name", self.symbol),
            "type"          : meta.get("type", "UNKNOWN"),
            "unit"          : meta.get("unit", ""),
            "price"         : self.price,
            "open"          : self.open_price,
            "high"          : self.high_price,
            "low"           : self.low_price,
            "prev_close"    : self.prev_close,
            "change_abs"    : self.change_abs,
            "change_pct"    : self.change_pct,
            "timestamp_utc" : self.timestamp_utc,
            "datetime_utc"  : datetime.fromtimestamp(
                                  self.timestamp_utc / 1000.0,
                                  tz=timezone.utc
                              ).isoformat(),
            "source"        : self.source,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — YAHOO FINANCE FETCHER
# ═══════════════════════════════════════════════════════════════════════════════

class YahooFinanceFetcher:
    """
    Fetches price data from Yahoo Finance v8 JSON API.
    No API key required — completely free.

    Endpoints used:
        Quote:   https://query1.finance.yahoo.com/v8/finance/chart/{symbol}
        Summary: https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}
    """

    CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    HEADERS   = {
        "User-Agent"     : "Mozilla/5.0 (compatible; MarketData/1.0)",
        "Accept"         : "application/json",
        "Accept-Language": "en-US,en;q=0.9",
    }

    @classmethod
    def fetch_quote(cls, symbol_key: str, timeout: int = 10) -> Optional[AssetPrice]:
        """
        Fetch current quote for an asset.

        Args:
            symbol_key : our internal key e.g. "DXY", "GOLD"
        """
        yahoo_ticker = YAHOO_TICKERS.get(symbol_key)
        if not yahoo_ticker:
            logger.warning(f"No Yahoo ticker for {symbol_key}")
            return None

        url = cls.CHART_URL.format(symbol=yahoo_ticker)

        try:
            response = requests.get(
                url,
                headers = cls.HEADERS,
                params  = {"interval": "1d", "range": "5d"},
                timeout = timeout
            )
            response.raise_for_status()
            data = response.json()
            return cls._parse_chart_response(symbol_key, data)

        except requests.RequestException as e:
            logger.warning(f"Yahoo Finance fetch failed [{symbol_key}]: {e}")
            return None
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"Yahoo Finance parse error [{symbol_key}]: {e}")
            return None

    @classmethod
    def fetch_all(cls, symbols: list) -> dict:
        """Fetch multiple assets. Returns dict of symbol → AssetPrice."""
        results = {}
        for symbol in symbols:
            price = cls.fetch_quote(symbol)
            if price:
                results[symbol] = price
                time.sleep(0.2)  # Polite delay between requests
        return results

    @classmethod
    def _parse_chart_response(cls, symbol_key: str,
                               data: dict) -> Optional[AssetPrice]:
        """Parse Yahoo Finance chart API response."""
        try:
            chart    = data["chart"]["result"][0]
            meta     = chart["meta"]
            price    = float(meta.get("regularMarketPrice", 0))
            prev_cls = float(meta.get("previousClose", price))
            open_p   = float(meta.get("regularMarketOpen", price))
            high_p   = float(meta.get("regularMarketDayHigh", price))
            low_p    = float(meta.get("regularMarketDayLow", price))
            ts       = float(meta.get("regularMarketTime", time.time())) * 1000.0

            if price <= 0:
                return None

            return AssetPrice(
                symbol      = symbol_key,
                price       = price,
                open_price  = open_p,
                high_price  = high_p,
                low_price   = low_p,
                prev_close  = prev_cls,
                timestamp_utc = ts,
                source      = "YAHOO_FINANCE",
            )
        except (KeyError, IndexError, TypeError) as e:
            logger.warning(f"Yahoo parse error [{symbol_key}]: {e}")
            return None


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — STOOQ FALLBACK FETCHER
# ═══════════════════════════════════════════════════════════════════════════════

class StooqFetcher:
    """
    Fallback data source — Stooq CSV endpoint.
    Free, no API key, covers most major assets.

    URL: https://stooq.com/q/d/l/?s={symbol}&i=d
    Returns daily OHLC CSV.
    """

    BASE_URL = "https://stooq.com/q/d/l/"
    HEADERS  = {
        "User-Agent": "Mozilla/5.0 (compatible; MarketData/1.0)"
    }

    @classmethod
    def fetch_quote(cls, symbol_key: str,
                    timeout: int = 10) -> Optional[AssetPrice]:
        """Fetch latest daily quote from Stooq."""
        stooq_sym = STOOQ_TICKERS.get(symbol_key)
        if not stooq_sym:
            return None

        try:
            response = requests.get(
                cls.BASE_URL,
                headers = cls.HEADERS,
                params  = {"s": stooq_sym, "i": "d"},
                timeout = timeout
            )
            response.raise_for_status()
            return cls._parse_csv(symbol_key, response.text)

        except requests.RequestException as e:
            logger.warning(f"Stooq fetch failed [{symbol_key}]: {e}")
            return None

    @classmethod
    def _parse_csv(cls, symbol_key: str, csv_text: str) -> Optional[AssetPrice]:
        """Parse Stooq CSV response — last row is most recent."""
        import csv as csv_mod
        from io import StringIO
        try:
            reader = csv_mod.DictReader(StringIO(csv_text))
            rows   = list(reader)
            if not rows:
                return None

            row   = rows[-1]   # Most recent
            row2  = rows[-2] if len(rows) > 1 else row  # Previous

            price     = float(row.get("Close", 0))
            prev_cls  = float(row2.get("Close", price))
            open_p    = float(row.get("Open", price))
            high_p    = float(row.get("High", price))
            low_p     = float(row.get("Low", price))
            date_str  = row.get("Date", "")

            # Parse date
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
                dt = dt.replace(tzinfo=timezone.utc)
                ts_ms = dt.timestamp() * 1000.0
            except ValueError:
                ts_ms = time.time() * 1000.0

            if price <= 0:
                return None

            return AssetPrice(
                symbol      = symbol_key,
                price       = price,
                open_price  = open_p,
                high_price  = high_p,
                low_price   = low_p,
                prev_close  = prev_cls,
                timestamp_utc = ts_ms,
                source      = "STOOQ",
            )
        except Exception as e:
            logger.warning(f"Stooq parse error [{symbol_key}]: {e}")
            return None


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — PRICE HISTORY BUFFER
# ═══════════════════════════════════════════════════════════════════════════════

class PriceHistoryBuffer:
    """
    Maintains rolling price history per asset.
    Used for volatility calculation and correlation computation.
    All values are raw prices — no interpretation.
    """

    def __init__(self, maxlen: int = 100):
        self._prices    : dict[str, deque] = {}
        self._timestamps: dict[str, deque] = {}
        self._maxlen    = maxlen
        self._lock      = threading.Lock()

    def add(self, symbol: str, price: float, timestamp_ms: float):
        with self._lock:
            if symbol not in self._prices:
                self._prices[symbol]     = deque(maxlen=self._maxlen)
                self._timestamps[symbol] = deque(maxlen=self._maxlen)
            self._prices[symbol].append(price)
            self._timestamps[symbol].append(timestamp_ms)

    def get_prices(self, symbol: str) -> list:
        with self._lock:
            return list(self._prices.get(symbol, []))

    def get_returns(self, symbol: str) -> list:
        """Compute percentage returns from price history."""
        prices = self.get_prices(symbol)
        if len(prices) < 2:
            return []
        return [
            (prices[i] - prices[i-1]) / prices[i-1] * 100.0
            for i in range(1, len(prices))
        ]

    def compute_volatility_score(self, symbol: str) -> Optional[float]:
        """
        Compute normalized volatility score (0→100).
        Pure measurement — no threshold judgment here.
        """
        returns = self.get_returns(symbol)
        if len(returns) < 5:
            return None
        arr = np.array(returns)
        std = float(np.std(arr))
        # Normalize: assume max daily std = 3% → score 100
        score = min(100.0, (std / 3.0) * 100.0)
        return score

    def compute_correlation(self, symbol_a: str, symbol_b: str) -> Optional[float]:
        """
        Compute Pearson correlation between two assets.
        Range: -1.0 to +1.0
        Pure measurement — Layer 3 interprets significance.
        """
        returns_a = self.get_returns(symbol_a)
        returns_b = self.get_returns(symbol_b)

        min_len = min(len(returns_a), len(returns_b))
        if min_len < 10:
            return None

        arr_a = np.array(returns_a[-min_len:])
        arr_b = np.array(returns_b[-min_len:])

        try:
            corr_matrix = np.corrcoef(arr_a, arr_b)
            return float(corr_matrix[0, 1])
        except Exception:
            return None


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — STRESS DETECTOR
# ═══════════════════════════════════════════════════════════════════════════════

class StressDetector:
    """
    Detects stress signals across multi-asset landscape.

    RULE:
        This detector MEASURES and LABELS stress signals.
        It NEVER decides whether to trade or activate kill switch.
        Those decisions belong to Layer 3 and Layer 4.

    Stress flags are LABELS:
        True  = signal detected
        False = signal not detected
    """

    def detect(self, prices: dict, history: PriceHistoryBuffer) -> dict:
        """
        Detect stress signals from current prices and history.

        Returns dict of stress flags and scores.
        All are measurements — no trade decisions.
        """
        flags = {}

        # ── VIX level flags (REFERENCE ONLY for forex) ───────────────────
        vix = prices.get("VIX")
        if vix:
            flags["vix_elevated"] = vix.price >= STRESS_THRESHOLDS["VIX_ELEVATED"]
            flags["vix_high"]     = vix.price >= STRESS_THRESHOLDS["VIX_HIGH"]
            flags["vix_crisis"]   = vix.price >= STRESS_THRESHOLDS["VIX_CRISIS"]
            flags["vix_level"]    = vix.price
        else:
            flags["vix_elevated"] = False
            flags["vix_high"]     = False
            flags["vix_crisis"]   = False
            flags["vix_level"]    = None

        # ── DXY volatility (primary forex stress indicator) ───────────────
        dxy = prices.get("DXY")
        if dxy:
            flags["dxy_daily_move_pct"]  = abs(dxy.change_pct)
            flags["dxy_elevated"]        = (
                abs(dxy.change_pct) >= STRESS_THRESHOLDS["DXY_DAILY_MOVE"]
            )
            dxy_vol = history.compute_volatility_score("DXY")
            flags["dxy_volatility_score"] = dxy_vol
            flags["dxy_volatility_spike"] = (
                dxy_vol is not None and dxy_vol >= 70
            )
        else:
            flags["dxy_elevated"]         = False
            flags["dxy_volatility_score"] = None
            flags["dxy_volatility_spike"] = False

        # ── Gold stress signal ────────────────────────────────────────────
        gold = prices.get("GOLD")
        if gold:
            flags["gold_daily_move_pct"] = abs(gold.change_pct)
            flags["gold_stress"]         = (
                abs(gold.change_pct) >= STRESS_THRESHOLDS["GOLD_DAILY_MOVE"]
            )
        else:
            flags["gold_stress"] = False

        # ── Correlation breakdown detection ───────────────────────────────
        corr_eurusd_gbpusd = history.compute_correlation("EURUSD", "GBPUSD")
        flags["corr_eurusd_gbpusd"] = corr_eurusd_gbpusd

        # Correlation breakdown: normally high positive — if low = unusual
        if corr_eurusd_gbpusd is not None:
            flags["corr_breakdown_eur_gbp"] = (
                corr_eurusd_gbpusd < STRESS_THRESHOLDS["CORRELATION_BREAK"]
            )
        else:
            flags["corr_breakdown_eur_gbp"] = False

        corr_eurusd_dxy = history.compute_correlation("EURUSD", "DXY")
        flags["corr_eurusd_dxy"] = corr_eurusd_dxy

        # ── Aggregate stress score (0→100) ────────────────────────────────
        # Pure measurement — weighted sum of stress signals
        stress_components = [
            flags.get("vix_elevated", False),
            flags.get("dxy_elevated", False),
            flags.get("gold_stress", False),
            flags.get("corr_breakdown_eur_gbp", False),
            flags.get("dxy_volatility_spike", False),
        ]
        stress_count = sum(1 for c in stress_components if c)
        flags["aggregate_stress_score"] = (stress_count / len(stress_components)) * 100.0

        # ── Multi-trigger crisis flag ─────────────────────────────────────
        # LABEL ONLY — Layer 4 decides kill switch
        flags["multi_stress_trigger"] = stress_count >= 3
        flags["stress_trigger_count"] = stress_count

        return flags


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — SMT DIVERGENCE DETECTOR
# ═══════════════════════════════════════════════════════════════════════════════

class SMTDivergenceDetector:
    """
    Detects Smart Money Technique (SMT) divergence between correlated pairs.

    SMT Divergence:
        When two historically correlated pairs move in opposite directions
        at a key level — one makes a new high, the other does not.

    This detector MEASURES divergence only.
    What it means for trading is Layer 3's decision.

    Pair groups:
        Group 1: EUR/USD + GBP/USD (both vs USD, normally correlated)
        Group 2: EUR/USD + AUD/USD (risk sentiment)
        Group 3: USD/JPY + DXY (dollar strength)
    """

    CORRELATED_PAIRS = [
        ("EURUSD", "GBPUSD", "USD_PAIRS_GROUP1"),
        ("EURUSD", "AUDUSD", "RISK_SENTIMENT_GROUP"),
        ("USDJPY", "DXY",    "DOLLAR_STRENGTH_GROUP"),
    ]

    def detect(self, history: PriceHistoryBuffer) -> dict:
        """
        Detect SMT divergence across correlated pairs.
        Returns raw divergence measurements.
        """
        results = {}

        for sym_a, sym_b, group in self.CORRELATED_PAIRS:
            prices_a = history.get_prices(sym_a)
            prices_b = history.get_prices(sym_b)

            if len(prices_a) < 20 or len(prices_b) < 20:
                results[group] = {
                    "divergence_detected": False,
                    "reason"            : "INSUFFICIENT_DATA",
                    "correlation"       : None,
                }
                continue

            # Compute current correlation
            correlation = history.compute_correlation(sym_a, sym_b)

            # Check for recent high divergence
            # Compare last N candles: did A make new high but B did not?
            n = 10
            recent_a = prices_a[-n:]
            recent_b = prices_b[-n:]

            max_a      = max(recent_a)
            max_b      = max(recent_b)
            cur_a      = recent_a[-1]
            cur_b      = recent_b[-1]

            # A at recent high, B not at recent high = divergence signal
            a_at_high  = cur_a >= max_a * 0.999
            b_at_high  = cur_b >= max_b * 0.999

            # B at recent low, A not = divergence signal
            min_a      = min(recent_a)
            min_b      = min(recent_b)
            a_at_low   = cur_a <= min_a * 1.001
            b_at_low   = cur_b <= min_b * 1.001

            divergence = (
                (a_at_high and not b_at_high) or
                (b_at_high and not a_at_high) or
                (a_at_low  and not b_at_low)  or
                (b_at_low  and not a_at_low)
            )

            # Correlation breakdown relative to expected
            expected = EXPECTED_CORRELATIONS.get((sym_a, sym_b)) or \
                       EXPECTED_CORRELATIONS.get((sym_b, sym_a))

            corr_deviation = None
            if correlation is not None and expected is not None:
                corr_deviation = abs(correlation - expected)

            results[group] = {
                "divergence_detected": divergence,
                "pair_a"            : sym_a,
                "pair_b"            : sym_b,
                "correlation"       : correlation,
                "expected_corr"     : expected,
                "corr_deviation"    : corr_deviation,
                "a_at_high"         : a_at_high,
                "b_at_high"         : b_at_high,
                "a_at_low"          : a_at_low,
                "b_at_low"          : b_at_low,
            }

        return results


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — MULTI ASSET MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class MultiAssetManager:
    """
    Master manager for all multi-asset data.

    Responsibilities:
        - Fetch prices from Yahoo Finance (primary) / Stooq (fallback)
        - Maintain price history for correlation computation
        - Run stress detection
        - Run SMT divergence detection
        - Provide Layer 1 output package

    Refresh schedule:
        Intraday assets (VIX, DXY, Gold): every 60 seconds
        Daily assets (COT, bonds): every 4 hours

    RULE:
        All output is raw measurement.
        No interpretation, no trade decisions.
        Layer 3 decides what measurements mean.
    """

    INTRADAY_SYMBOLS = ["DXY", "GOLD", "OIL", "VIX", "SP500",
                        "EURUSD", "GBPUSD", "USDJPY"]
    REFRESH_SECONDS  = 60   # Refresh every 60 seconds

    def __init__(self, refresh_seconds: int = 60):
        self._prices         : dict[str, AssetPrice] = {}
        self._history        = PriceHistoryBuffer(maxlen=200)
        self._stress_detector= StressDetector()
        self._smt_detector   = SMTDivergenceDetector()
        self._lock           = threading.Lock()
        self._running        = False
        self._thread         = None
        self._last_refresh   = 0.0
        self._refresh_secs   = refresh_seconds
        self._fetch_errors   : dict[str, int] = {}

        logger.info("MultiAssetManager initialized")

    def start(self):
        """Start background refresh thread."""
        self._running = True
        self._thread  = threading.Thread(
            target = self._refresh_loop,
            daemon = True,
            name   = "MultiAssetThread"
        )
        self._thread.start()
        self._refresh()  # Initial fetch
        logger.info("MultiAssetManager: started")

    def stop(self):
        self._running = False
        logger.info("MultiAssetManager: stopped")

    def _refresh_loop(self):
        while self._running:
            time.sleep(self._refresh_secs)
            self._refresh()

    def _refresh(self):
        """Fetch all asset prices — Yahoo Finance primary, Stooq fallback."""
        logger.info("MultiAsset: refreshing prices...")
        fetched = 0

        for symbol in self.INTRADAY_SYMBOLS:
            # Try Yahoo Finance first
            price = YahooFinanceFetcher.fetch_quote(symbol)

            # Fallback to Stooq if Yahoo fails
            if price is None and symbol in STOOQ_TICKERS:
                price = StooqFetcher.fetch_quote(symbol)

            if price:
                with self._lock:
                    self._prices[symbol] = price
                self._history.add(symbol, price.price, price.timestamp_utc)
                self._fetch_errors[symbol] = 0
                fetched += 1
            else:
                self._fetch_errors[symbol] = self._fetch_errors.get(symbol, 0) + 1
                logger.warning(
                    f"MultiAsset: failed to fetch {symbol} "
                    f"(consecutive failures: {self._fetch_errors[symbol]})"
                )

        self._last_refresh = time.time()
        logger.info(f"MultiAsset: refreshed {fetched}/{len(self.INTRADAY_SYMBOLS)} assets")

    def inject_mock_prices(self, mock_prices: dict):
        """Inject mock prices for testing — bypasses API calls."""
        with self._lock:
            for symbol, price in mock_prices.items():
                self._prices[symbol] = price
                self._history.add(symbol, price.price, price.timestamp_utc)

    # ── Layer 1 Output Interface ─────────────────────────────────────────────

    def get_layer1_package(self) -> dict:
        """
        Return Layer 1 multi-asset package.
        Consumed by Layer 1 / Layer 2.
        All raw measurements — no interpretation.
        """
        with self._lock:
            prices_snapshot = dict(self._prices)

        # Run stress detection
        stress_flags = self._stress_detector.detect(
            prices_snapshot, self._history
        )

        # Run SMT divergence detection
        smt_signals = self._smt_detector.detect(self._history)

        # Correlation matrix
        correlations = self._compute_correlation_matrix()

        # Volatility scores per asset
        volatility_scores = {
            sym: self._history.compute_volatility_score(sym)
            for sym in self.INTRADAY_SYMBOLS
        }

        return {
            # ── Raw prices ──────────────────────────────────────────────
            "prices"            : {
                sym: price.to_dict()
                for sym, price in prices_snapshot.items()
            },

            # ── Volatility scores (0→100, pure measurement) ─────────────
            "volatility_scores" : volatility_scores,

            # ── Correlation matrix ───────────────────────────────────────
            "correlations"      : correlations,

            # ── Stress flags (LABELS — not trade decisions) ─────────────
            "stress_flags"      : stress_flags,

            # ── SMT divergence signals ───────────────────────────────────
            "smt_signals"       : smt_signals,

            # ── Summary scores (pure measurement) ───────────────────────
            "aggregate_stress"  : stress_flags.get("aggregate_stress_score", 0),
            "multi_stress"      : stress_flags.get("multi_stress_trigger", False),
            "stress_count"      : stress_flags.get("stress_trigger_count", 0),

            # ── Metadata ────────────────────────────────────────────────
            "last_refresh_utc"  : datetime.fromtimestamp(
                                      self._last_refresh,
                                      tz=timezone.utc
                                  ).isoformat() if self._last_refresh else None,
            "fetch_errors"      : dict(self._fetch_errors),
            "assets_available"  : list(prices_snapshot.keys()),

            # ── Data notes (for Layer 3 confidence adjustment) ───────────
            # These are LABELS — Layer 3 adjusts confidence based on them
            "vix_note"          : "REFERENCE_ONLY_FOREX",
            "data_source"       : "YAHOO_FINANCE+STOOQ",
        }

    def get_price(self, symbol: str) -> Optional[AssetPrice]:
        with self._lock:
            return self._prices.get(symbol)

    def get_dxy_price(self) -> Optional[float]:
        p = self.get_price("DXY")
        return p.price if p else None

    def get_vix_level(self) -> Optional[float]:
        p = self.get_price("VIX")
        return p.price if p else None

    def get_gold_price(self) -> Optional[float]:
        p = self.get_price("GOLD")
        return p.price if p else None

    def _compute_correlation_matrix(self) -> dict:
        """Compute pairwise correlations for all tracked pairs."""
        symbols = self.INTRADAY_SYMBOLS
        matrix  = {}
        for i, sym_a in enumerate(symbols):
            for sym_b in symbols[i+1:]:
                corr = self._history.compute_correlation(sym_a, sym_b)
                if corr is not None:
                    key = f"{sym_a}_{sym_b}"
                    matrix[key] = round(corr, 4)
        return matrix


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 9 — SELF TEST
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 70)
    print("MULTI-ASSET FEED — SELF TEST")
    print("=" * 70)

    manager = MultiAssetManager(refresh_seconds=60)

    # Inject mock prices (network not available in test)
    print("\n[1] Injecting mock asset prices...")
    now_ms   = time.time() * 1000
    mock_prices = {
        "DXY"   : AssetPrice("DXY",    103.50, 103.20, 103.80, 103.10, 103.00, now_ms, "MOCK"),
        "GOLD"  : AssetPrice("GOLD",  2050.0,  2040.0, 2060.0, 2035.0, 2020.0, now_ms, "MOCK"),
        "OIL"   : AssetPrice("OIL",    78.50,   77.80,  79.00,  77.50,  77.00, now_ms, "MOCK"),
        "VIX"   : AssetPrice("VIX",    22.50,   21.00,  23.00,  20.50,  19.00, now_ms, "MOCK"),
        "SP500" : AssetPrice("SP500", 4800.0,  4780.0, 4820.0, 4770.0, 4750.0, now_ms, "MOCK"),
        "EURUSD": AssetPrice("EURUSD",  1.0850,  1.0830, 1.0870, 1.0820, 1.0820, now_ms, "MOCK"),
        "GBPUSD": AssetPrice("GBPUSD",  1.2710,  1.2690, 1.2730, 1.2680, 1.2680, now_ms, "MOCK"),
        "USDJPY": AssetPrice("USDJPY", 148.50,  148.20, 148.80, 148.00, 148.20, now_ms, "MOCK"),
    }
    manager.inject_mock_prices(mock_prices)

    # Build price history with simulated data
    print("[2] Building price history for correlation/volatility...")
    import random
    base_prices = {
        "DXY"   : 103.0,
        "GOLD"  : 2040.0,
        "EURUSD": 1.0820,
        "GBPUSD": 1.2680,
        "USDJPY": 148.20,
        "VIX"   : 19.0,
    }

    for i in range(50):
        ts = now_ms - ((50 - i) * 60000)  # 1 min apart
        for sym, base in base_prices.items():
            noise = random.gauss(0, base * 0.001)
            price = base + noise
            manager._history.add(sym, price, ts)

    print("[3] Getting Layer 1 package...")
    pkg = manager.get_layer1_package()

    print(f"\n    Assets available : {pkg['assets_available']}")
    print(f"    Aggregate stress : {pkg['aggregate_stress']:.1f}/100")
    print(f"    Multi-stress     : {pkg['multi_stress']}")
    print(f"    Stress count     : {pkg['stress_count']}")

    print("\n[4] Asset prices:")
    for sym, data in pkg["prices"].items():
        print(f"    {sym:8s}: {data['price']:>10.4f} "
              f"({data['change_pct']:+.2f}%) [{data['source']}]")

    print("\n[5] Stress flags:")
    flags = pkg["stress_flags"]
    print(f"    VIX elevated  : {flags.get('vix_elevated')} ({flags.get('vix_level')})")
    print(f"    VIX high      : {flags.get('vix_high')}")
    print(f"    DXY elevated  : {flags.get('dxy_elevated')}")
    print(f"    Gold stress   : {flags.get('gold_stress')}")
    print(f"    Corr breakdown: {flags.get('corr_breakdown_eur_gbp')}")

    print("\n[6] Volatility scores:")
    for sym, score in pkg["volatility_scores"].items():
        if score is not None:
            print(f"    {sym:8s}: {score:.1f}/100")

    print("\n[7] Correlations computed:")
    for pair, corr in list(pkg["correlations"].items())[:5]:
        print(f"    {pair}: {corr:.4f}")

    print("\n[8] SMT divergence signals:")
    for group, signal in pkg["smt_signals"].items():
        div = signal.get("divergence_detected", False)
        corr = signal.get("correlation")
        print(f"    {group}: divergence={div} corr={corr:.4f if corr else 'N/A'}")

    print("\n[9] Impact classification:")
    print(f"    VIX note: {pkg['vix_note']}")
    print(f"    Source  : {pkg['data_source']}")

    print("\n" + "=" * 70)
    print("MULTI-ASSET FEED SELF TEST COMPLETE")
    print("All outputs are raw measurements — Layer 3 interprets")
    print("VIX = REFERENCE ONLY (not primary forex stress indicator)")
    print("=" * 70)
