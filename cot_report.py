"""
═══════════════════════════════════════════════════════════════════════════════
FOREX TRADING SIGNAL SYSTEM — VERSION 6.0
FILE: cot_report.py
LAYER: 1 — DATA PERCEPTION LAYER (COT Report Sub-Component)
═══════════════════════════════════════════════════════════════════════════════

PURPOSE:
    Fetches and parses CFTC Commitment of Traders (COT) reports.
    Provides institutional positioning data for forex pairs.

SOURCE:
    CFTC (Commodity Futures Trading Commission) — FREE, public data
    URL: https://www.cftc.gov/dea/newcot/

REPORT TYPES:
    Legacy COT   → Futures only, commercial vs non-commercial
    Disaggregated→ More detailed breakdown
    TFF          → Traders in Financial Futures (most useful for forex)

    WE USE: TFF (Traders in Financial Futures)
    WHY:    Separates dealers, asset managers, leveraged funds
            Leveraged funds = most relevant for forex trend

RELEASE SCHEDULE:
    Every Friday ~3:30pm ET
    Reports current Tuesday's positions
    ~3 day delay from measurement to release

FOREX CONTRACTS TRACKED (CME):
    EUR/USD → Euro FX (6E)
    GBP/USD → British Pound (6B)
    USD/JPY → Japanese Yen (6J)
    USD/CHF → Swiss Franc (6S)
    AUD/USD → Australian Dollar (6A)
    USD/CAD → Canadian Dollar (6C)
    NZD/USD → New Zealand Dollar (6N)

ARCHITECTURAL RULES:
    - Collects and labels COT data ONLY
    - NEVER interprets what positioning means for trades
    - Outputs: raw long/short counts + net position + change + score
    - All interpretation belongs to Layer 3
    - Data is weekly — label it as TIER_2 (delayed, semi-institutional)

OUTPUT:
    {
        "EUR": {
            "net_position"     : int,
            "net_change"       : int,
            "long_pct"         : float,
            "short_pct"        : float,
            "positioning_score": float (0→100),
            "extreme_flag"     : bool,
            "report_date"      : str,
            "weeks_since"      : int,
        },
        ...
    }

═══════════════════════════════════════════════════════════════════════════════
"""

import io
import csv
import time
import zipfile
import logging
import threading
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional
from dataclasses import dataclass, field
from collections import deque

logger = logging.getLogger("COTReport")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — CONTRACT DEFINITIONS
# ═══════════════════════════════════════════════════════════════════════════════

# CFTC contract names for forex futures
# Used to filter relevant rows from COT CSV
CFTC_CONTRACT_NAMES = {
    "EUR"   : "EURO FX - CHICAGO MERCANTILE EXCHANGE",
    "GBP"   : "BRITISH POUND STERLING - CHICAGO MERCANTILE EXCHANGE",
    "JPY"   : "JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE",
    "CHF"   : "SWISS FRANC - CHICAGO MERCANTILE EXCHANGE",
    "AUD"   : "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "CAD"   : "CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "NZD"   : "NEW ZEALAND DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "DXY"   : "U.S. DOLLAR INDEX - ICE FUTURES U.S.",
}

# Map currency → forex pairs it affects
CURRENCY_TO_PAIRS = {
    "EUR": ["EUR/USD", "EUR/GBP", "EUR/JPY"],
    "GBP": ["GBP/USD", "EUR/GBP"],
    "JPY": ["USD/JPY", "EUR/JPY"],
    "CHF": ["USD/CHF"],
    "AUD": ["AUD/USD"],
    "CAD": ["USD/CAD"],
    "NZD": ["NZD/USD"],
    "DXY": ["EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF",
            "AUD/USD", "USD/CAD", "NZD/USD"],
}

# CFTC data sources
CFTC_BASE_URL = "https://www.cftc.gov/dea/newcot/"

# TFF (Traders in Financial Futures) report URLs
CFTC_TFF_URLS = {
    "current"  : f"{CFTC_BASE_URL}FinFutWk.txt",      # Current week
    "historical": f"{CFTC_BASE_URL}f_year.zip",        # Full year ZIP
}

# Legacy COT report URLs
CFTC_LEGACY_URLS = {
    "current"  : f"{CFTC_BASE_URL}deacot.zip",
    "historical": f"{CFTC_BASE_URL}dea_fut_xls_{{}}.zip",
}

# Extreme positioning thresholds
# MEASUREMENT only — Layer 3 decides what extreme means
EXTREME_NET_THRESHOLD = 0.75  # Net position > 75% of 52-week range = extreme


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — COT DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class COTRecord:
    """
    Single week's COT data for one currency/contract.

    Tracks both legacy (commercial/non-commercial) and
    TFF (dealer/asset manager/leveraged fund) breakdowns.

    RULE: Raw positioning data only — no interpretation attached.
    """
    currency        : str
    contract_name   : str
    report_date     : str           # "YYYY-MM-DD"
    timestamp_utc   : float         # Unix ms of report date

    # ── Non-commercial (speculative) positions ──
    noncomm_long    : int = 0
    noncomm_short   : int = 0
    noncomm_spread  : int = 0

    # ── Commercial (hedger) positions ──
    comm_long       : int = 0
    comm_short      : int = 0

    # ── TFF breakdown (when available) ──
    dealer_long     : int = 0
    dealer_short    : int = 0
    asset_mgr_long  : int = 0
    asset_mgr_short : int = 0
    lev_fund_long   : int = 0   # Leveraged funds = most relevant for trend
    lev_fund_short  : int = 0

    # ── Open interest ──
    open_interest   : int = 0

    # ── Computed on creation ──
    net_noncomm     : int = field(init=False)
    net_lev_fund    : int = field(init=False)
    total_long      : int = field(init=False)
    total_short     : int = field(init=False)
    long_pct        : float = field(init=False)
    short_pct       : float = field(init=False)

    def __post_init__(self):
        self.net_noncomm  = self.noncomm_long  - self.noncomm_short
        self.net_lev_fund = self.lev_fund_long - self.lev_fund_short
        self.total_long   = self.noncomm_long  + self.comm_long
        self.total_short  = self.noncomm_short + self.comm_short
        total = self.total_long + self.total_short
        if total > 0:
            self.long_pct  = (self.total_long  / total) * 100.0
            self.short_pct = (self.total_short / total) * 100.0
        else:
            self.long_pct  = 50.0
            self.short_pct = 50.0

    def to_dict(self) -> dict:
        return {
            "currency"       : self.currency,
            "contract"       : self.contract_name,
            "report_date"    : self.report_date,
            "timestamp_utc"  : self.timestamp_utc,
            "noncomm_long"   : self.noncomm_long,
            "noncomm_short"  : self.noncomm_short,
            "net_noncomm"    : self.net_noncomm,
            "lev_fund_long"  : self.lev_fund_long,
            "lev_fund_short" : self.lev_fund_short,
            "net_lev_fund"   : self.net_lev_fund,
            "open_interest"  : self.open_interest,
            "long_pct"       : round(self.long_pct, 2),
            "short_pct"      : round(self.short_pct, 2),
        }


@dataclass
class COTAnalysis:
    """
    Computed COT analysis for a currency — pure measurements.
    Layer 3 decides what these measurements mean for trading.
    """
    currency            : str
    latest_record       : COTRecord
    previous_record     : Optional[COTRecord]

    # Computed
    net_position        : int   = field(init=False)
    net_change          : int   = field(init=False)
    positioning_score   : float = field(init=False)  # 0→100
    extreme_flag        : bool  = field(init=False)
    weeks_of_data       : int   = field(init=False)
    trend_direction     : str   = field(init=False)  # INCREASING/DECREASING/STABLE

    def __post_init__(self):
        self.net_position = self.latest_record.net_noncomm
        self.net_change   = (
            self.latest_record.net_noncomm -
            self.previous_record.net_noncomm
        ) if self.previous_record else 0
        self.weeks_of_data    = 1
        self.extreme_flag     = False
        self.positioning_score = 50.0
        self.trend_direction  = "STABLE"

    def to_dict(self) -> dict:
        return {
            "currency"          : self.currency,
            "net_position"      : self.net_position,
            "net_change"        : self.net_change,
            "positioning_score" : round(self.positioning_score, 2),
            "extreme_flag"      : self.extreme_flag,
            "trend_direction"   : self.trend_direction,
            "long_pct"          : round(self.latest_record.long_pct, 2),
            "short_pct"         : round(self.latest_record.short_pct, 2),
            "report_date"       : self.latest_record.report_date,
            "lev_fund_net"      : self.latest_record.net_lev_fund,
            "open_interest"     : self.latest_record.open_interest,
            "affected_pairs"    : CURRENCY_TO_PAIRS.get(self.currency, []),
            "data_tier"         : "TIER_2",  # Weekly delayed
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — CFTC TFF PARSER
# ═══════════════════════════════════════════════════════════════════════════════

class CFTCTFFParser:
    """
    Parses CFTC Traders in Financial Futures (TFF) report.

    TFF CSV columns (partial):
        Market_and_Exchange_Names
        As_of_Date_in_Form_YYMMDD
        Open_Interest_All
        Dealer_Positions_Long_All
        Dealer_Positions_Short_All
        Asset_Mgr_Positions_Long_All
        Asset_Mgr_Positions_Short_All
        Lev_Money_Positions_Long_All
        Lev_Money_Positions_Short_All
        Other_Rept_Positions_Long_All
        Other_Rept_Positions_Short_All
    """

    @staticmethod
    def parse_txt(content: str) -> list:
        """
        Parse TFF report text (CSV format).
        Returns list of COTRecord objects.
        """
        records = []
        try:
            reader = csv.DictReader(io.StringIO(content))
            for row in reader:
                record = CFTCTFFParser._parse_row(row)
                if record:
                    records.append(record)
        except Exception as e:
            logger.error(f"TFF parse error: {e}")
        return records

    @staticmethod
    def _parse_row(row: dict) -> Optional[COTRecord]:
        """Parse single TFF CSV row."""
        try:
            name = row.get(
                "Market_and_Exchange_Names", ""
            ).strip().upper()

            # Find matching currency
            currency = None
            for curr, contract in CFTC_CONTRACT_NAMES.items():
                if contract.upper() in name or name in contract.upper():
                    currency = curr
                    break

            if not currency:
                return None

            # Parse date (YYMMDD format)
            date_raw  = row.get("As_of_Date_in_Form_YYMMDD", "").strip()
            report_dt = CFTCTFFParser._parse_date(date_raw)
            if not report_dt:
                return None

            def _int(key: str) -> int:
                val = row.get(key, "0").strip().replace(",", "")
                try:
                    return int(float(val)) if val else 0
                except ValueError:
                    return 0

            return COTRecord(
                currency        = currency,
                contract_name   = name,
                report_date     = report_dt.strftime("%Y-%m-%d"),
                timestamp_utc   = report_dt.timestamp() * 1000.0,
                dealer_long     = _int("Dealer_Positions_Long_All"),
                dealer_short    = _int("Dealer_Positions_Short_All"),
                asset_mgr_long  = _int("Asset_Mgr_Positions_Long_All"),
                asset_mgr_short = _int("Asset_Mgr_Positions_Short_All"),
                lev_fund_long   = _int("Lev_Money_Positions_Long_All"),
                lev_fund_short  = _int("Lev_Money_Positions_Short_All"),
                # Use leveraged fund as non-commercial proxy
                noncomm_long    = _int("Lev_Money_Positions_Long_All"),
                noncomm_short   = _int("Lev_Money_Positions_Short_All"),
                open_interest   = _int("Open_Interest_All"),
            )

        except Exception as e:
            logger.debug(f"TFF row parse error: {e}")
            return None

    @staticmethod
    def _parse_date(date_str: str) -> Optional[datetime]:
        """Parse CFTC date formats."""
        for fmt in ["%y%m%d", "%Y%m%d", "%m/%d/%Y", "%Y-%m-%d"]:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — LEGACY COT PARSER
# ═══════════════════════════════════════════════════════════════════════════════

class CFTCLegacyParser:
    """
    Parses CFTC Legacy COT report (futures only).
    Fallback when TFF is unavailable.

    Legacy CSV key columns:
        Market_and_Exchange_Names
        As_of_Date_in_Form_YYMMDD
        Open_Interest_All
        NonComm_Positions_Long_All
        NonComm_Positions_Short_All
        NonComm_Positions_Spread_All
        Comm_Positions_Long_All
        Comm_Positions_Short_All
    """

    @staticmethod
    def parse_txt(content: str) -> list:
        records = []
        try:
            reader = csv.DictReader(io.StringIO(content))
            for row in reader:
                record = CFTCLegacyParser._parse_row(row)
                if record:
                    records.append(record)
        except Exception as e:
            logger.error(f"Legacy COT parse error: {e}")
        return records

    @staticmethod
    def _parse_row(row: dict) -> Optional[COTRecord]:
        try:
            name = row.get(
                "Market_and_Exchange_Names", ""
            ).strip().upper()

            currency = None
            for curr, contract in CFTC_CONTRACT_NAMES.items():
                if contract.upper() in name or name in contract.upper():
                    currency = curr
                    break

            if not currency:
                return None

            date_raw  = row.get("As_of_Date_in_Form_YYMMDD", "").strip()
            report_dt = CFTCTFFParser._parse_date(date_raw)
            if not report_dt:
                return None

            def _int(key: str) -> int:
                val = row.get(key, "0").strip().replace(",", "")
                try:
                    return int(float(val)) if val else 0
                except ValueError:
                    return 0

            return COTRecord(
                currency       = currency,
                contract_name  = name,
                report_date    = report_dt.strftime("%Y-%m-%d"),
                timestamp_utc  = report_dt.timestamp() * 1000.0,
                noncomm_long   = _int("NonComm_Positions_Long_All"),
                noncomm_short  = _int("NonComm_Positions_Short_All"),
                noncomm_spread = _int("NonComm_Positions_Spread_All"),
                comm_long      = _int("Comm_Positions_Long_All"),
                comm_short     = _int("Comm_Positions_Short_All"),
                open_interest  = _int("Open_Interest_All"),
            )

        except Exception as e:
            logger.debug(f"Legacy row parse error: {e}")
            return None


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — COT HISTORY STORE
# ═══════════════════════════════════════════════════════════════════════════════

class COTHistoryStore:
    """
    Maintains rolling history of COT records per currency.
    Used for positioning score, extreme detection, trend direction.
    All computations are pure measurements.
    """

    def __init__(self, history_weeks: int = 52):
        self._history    : dict[str, deque] = {}
        self._history_len = history_weeks
        self._lock        = threading.Lock()

    def add_records(self, records: list):
        """Add new records to history."""
        with self._lock:
            for record in records:
                curr = record.currency
                if curr not in self._history:
                    self._history[curr] = deque(maxlen=self._history_len)

                # Only add if newer than latest
                existing = self._history[curr]
                if not existing or record.timestamp_utc > existing[-1].timestamp_utc:
                    existing.append(record)
                    logger.info(
                        f"COT: added {curr} record "
                        f"date={record.report_date}"
                    )

    def get_latest(self, currency: str) -> Optional[COTRecord]:
        with self._lock:
            hist = self._history.get(currency, [])
            return hist[-1] if hist else None

    def get_previous(self, currency: str) -> Optional[COTRecord]:
        with self._lock:
            hist = self._history.get(currency, [])
            return hist[-2] if len(hist) >= 2 else None

    def get_history(self, currency: str, weeks: int = 52) -> list:
        with self._lock:
            hist = self._history.get(currency, [])
            return list(hist)[-weeks:]

    def compute_positioning_score(self, currency: str) -> float:
        """
        Compute positioning score (0→100) based on 52-week range.

        Score interpretation (Layer 3 assigns meaning):
            0   = max historical short (extreme short)
            50  = neutral
            100 = max historical long (extreme long)

        RULE: Pure measurement — Layer 3 decides what score means.
        """
        history = self.get_history(currency, 52)
        if len(history) < 4:
            return 50.0  # Not enough data — return neutral

        net_positions = [r.net_noncomm for r in history]
        latest_net    = net_positions[-1]
        min_net       = min(net_positions)
        max_net       = max(net_positions)

        if max_net == min_net:
            return 50.0

        score = (latest_net - min_net) / (max_net - min_net) * 100.0
        return round(max(0.0, min(100.0, score)), 2)

    def compute_extreme_flag(self, currency: str) -> bool:
        """
        Flag if positioning is at extreme vs 52-week range.
        LABEL only — Layer 3 decides significance.
        """
        score = self.compute_positioning_score(currency)
        return score >= 75.0 or score <= 25.0

    def compute_trend_direction(self, currency: str,
                                lookback: int = 4) -> str:
        """
        Measure direction of positioning change over N weeks.
        Returns: INCREASING / DECREASING / STABLE
        LABEL only — no directional trade implication here.
        """
        history = self.get_history(currency, lookback + 1)
        if len(history) < 3:
            return "STABLE"

        net_positions = [r.net_noncomm for r in history]
        recent_avg    = sum(net_positions[-2:]) / 2
        older_avg     = sum(net_positions[:-2]) / max(len(net_positions) - 2, 1)
        delta         = recent_avg - older_avg

        # Threshold: 5% of open interest
        latest = history[-1]
        threshold = latest.open_interest * 0.05 if latest.open_interest > 0 else 1000

        if delta > threshold:
            return "INCREASING"
        elif delta < -threshold:
            return "DECREASING"
        return "STABLE"

    def get_currencies_available(self) -> list:
        with self._lock:
            return list(self._history.keys())


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — CFTC FETCHER
# ═══════════════════════════════════════════════════════════════════════════════

class CFTCFetcher:
    """
    Fetches COT report files from CFTC website.
    Handles both TXT and ZIP formats.
    Tries TFF first, falls back to Legacy.
    """

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (compatible; COTFetcher/1.0)"
    }

    @classmethod
    def fetch_current_tff(cls, timeout: int = 30) -> list:
        """Fetch current week TFF report."""
        try:
            response = requests.get(
                CFTC_TFF_URLS["current"],
                headers=cls.HEADERS,
                timeout=timeout
            )
            response.raise_for_status()
            records = CFTCTFFParser.parse_txt(response.text)
            logger.info(f"CFTC TFF current: {len(records)} records fetched")
            return records
        except requests.RequestException as e:
            logger.warning(f"CFTC TFF fetch failed: {e}")
            return []

    @classmethod
    def fetch_historical_tff(cls, timeout: int = 60) -> list:
        """Fetch full year TFF report from ZIP."""
        try:
            response = requests.get(
                CFTC_TFF_URLS["historical"],
                headers=cls.HEADERS,
                timeout=timeout,
                stream=True
            )
            response.raise_for_status()

            # Parse ZIP
            zip_data = io.BytesIO(response.content)
            records  = []

            with zipfile.ZipFile(zip_data) as zf:
                for name in zf.namelist():
                    if name.endswith(".txt") or name.endswith(".csv"):
                        content = zf.read(name).decode("utf-8", errors="ignore")
                        records.extend(CFTCTFFParser.parse_txt(content))

            logger.info(f"CFTC TFF historical: {len(records)} records")
            return records

        except requests.RequestException as e:
            logger.warning(f"CFTC TFF historical fetch failed: {e}")
            return []
        except zipfile.BadZipFile as e:
            logger.warning(f"CFTC ZIP parse error: {e}")
            return []

    @classmethod
    def fetch_current_legacy(cls, timeout: int = 30) -> list:
        """Fetch current week Legacy COT report — fallback."""
        try:
            response = requests.get(
                CFTC_LEGACY_URLS["current"],
                headers=cls.HEADERS,
                timeout=timeout
            )
            response.raise_for_status()

            zip_data = io.BytesIO(response.content)
            records  = []

            with zipfile.ZipFile(zip_data) as zf:
                for name in zf.namelist():
                    if name.lower().endswith(".txt"):
                        content = zf.read(name).decode("utf-8", errors="ignore")
                        records.extend(CFTCLegacyParser.parse_txt(content))

            logger.info(f"CFTC Legacy: {len(records)} records")
            return records

        except Exception as e:
            logger.warning(f"CFTC Legacy fetch failed: {e}")
            return []


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — COT MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class COTManager:
    """
    Master manager for COT report data.

    Responsibilities:
        - Fetch CFTC reports on schedule (weekly)
        - Maintain positioning history per currency
        - Compute positioning scores and flags
        - Provide Layer 1 output package

    Refresh schedule:
        Weekly — Friday after 3:30pm ET (report release time)
        Also refreshes on startup to get latest available

    RULE:
        All output is raw measurement and labels.
        No interpretation, no trade decisions.
        Layer 3 decides what positioning means.
    """

    def __init__(self, refresh_hours: float = 6.0):
        self._store         = COTHistoryStore(history_weeks=52)
        self._fetcher       = CFTCFetcher()
        self._lock          = threading.Lock()
        self._running       = False
        self._thread        = None
        self._last_refresh  = 0.0
        self._refresh_hours = refresh_hours
        self._data_age_days = None  # How old is our data

        logger.info("COTManager initialized")

    def start(self):
        """Start background refresh thread."""
        self._running = True
        self._thread  = threading.Thread(
            target = self._refresh_loop,
            daemon = True,
            name   = "COTRefreshThread"
        )
        self._thread.start()
        self._refresh()
        logger.info("COTManager: started")

    def stop(self):
        self._running = False
        logger.info("COTManager: stopped")

    def _refresh_loop(self):
        while self._running:
            time.sleep(3600)  # Check every hour
            elapsed = time.time() - self._last_refresh
            if elapsed >= self._refresh_hours * 3600:
                self._refresh()

    def _refresh(self):
        """Fetch latest COT data — TFF primary, Legacy fallback."""
        logger.info("COT: refreshing...")

        # Try TFF first
        records = self._fetcher.fetch_current_tff()

        # Fallback to legacy
        if not records:
            logger.info("COT: TFF failed, trying Legacy...")
            records = self._fetcher.fetch_current_legacy()

        if records:
            self._store.add_records(records)
            self._last_refresh = time.time()

            # Calculate data age
            currencies = self._store.get_currencies_available()
            if currencies:
                latest = self._store.get_latest(currencies[0])
                if latest:
                    age_ms = time.time() * 1000 - latest.timestamp_utc
                    self._data_age_days = age_ms / (86400 * 1000)

            logger.info(
                f"COT: refresh complete — "
                f"{len(records)} records | "
                f"currencies={self._store.get_currencies_available()}"
            )
        else:
            logger.warning("COT: no data fetched from any source")

    def load_mock_data(self, records: list):
        """Load mock COT data for testing."""
        self._store.add_records(records)
        self._last_refresh = time.time()

    # ── Layer 1 Output Interface ─────────────────────────────────────────────

    def get_layer1_package(self) -> dict:
        """
        Return Layer 1 COT package.
        Raw measurements and labels only.
        Layer 3 assigns meaning to positioning data.
        """
        currencies    = self._store.get_currencies_available()
        cot_data      = {}
        extreme_flags = []

        for currency in currencies:
            latest   = self._store.get_latest(currency)
            previous = self._store.get_previous(currency)

            if not latest:
                continue

            analysis = COTAnalysis(
                currency        = currency,
                latest_record   = latest,
                previous_record = previous,
            )

            # Compute measurements
            analysis.positioning_score = self._store.compute_positioning_score(currency)
            analysis.extreme_flag      = self._store.compute_extreme_flag(currency)
            analysis.trend_direction   = self._store.compute_trend_direction(currency)

            if previous:
                analysis.net_change    = latest.net_noncomm - previous.net_noncomm
                analysis.weeks_of_data = len(self._store.get_history(currency))

            if analysis.extreme_flag:
                extreme_flags.append(currency)

            cot_data[currency] = analysis.to_dict()

        # Data freshness assessment
        data_fresh     = self._data_age_days is not None and self._data_age_days <= 7
        data_age_label = "CURRENT" if data_fresh else "STALE"

        return {
            # ── Per-currency positioning data ────────────────────────────
            "positioning"           : cot_data,

            # ── Extreme positioning flags ────────────────────────────────
            "extreme_currencies"    : extreme_flags,
            "any_extreme"           : len(extreme_flags) > 0,

            # ── Data freshness ───────────────────────────────────────────
            "data_age_days"         : self._data_age_days,
            "data_freshness"        : data_age_label,
            "last_refresh_utc"      : datetime.fromtimestamp(
                                          self._last_refresh,
                                          tz=timezone.utc
                                      ).isoformat() if self._last_refresh else None,

            # ── Data tier label ──────────────────────────────────────────
            # TIER_2: weekly delayed data — Layer 3 applies confidence
            "data_tier"             : "TIER_2",
            "data_source"           : "CFTC_COT",
            "release_schedule"      : "WEEKLY_FRIDAY_1530ET",
            "measurement_delay"     : "3_DAYS_FROM_TUESDAY",

            # ── Currencies available ─────────────────────────────────────
            "currencies_available"  : currencies,
        }

    def get_currency_positioning(self, currency: str) -> Optional[dict]:
        """Get positioning data for a specific currency."""
        latest   = self._store.get_latest(currency)
        previous = self._store.get_previous(currency)

        if not latest:
            return None

        analysis = COTAnalysis(
            currency        = currency,
            latest_record   = latest,
            previous_record = previous,
        )
        analysis.positioning_score = self._store.compute_positioning_score(currency)
        analysis.extreme_flag      = self._store.compute_extreme_flag(currency)
        analysis.trend_direction   = self._store.compute_trend_direction(currency)

        return analysis.to_dict()

    def get_pairs_with_extreme_positioning(self) -> list:
        """
        Return list of pairs where at least one currency
        shows extreme positioning.
        LABEL only — Layer 3 decides trade implication.
        """
        extreme_pairs = []
        currencies    = self._store.get_currencies_available()

        for curr in currencies:
            if self._store.compute_extreme_flag(curr):
                affected = CURRENCY_TO_PAIRS.get(curr, [])
                extreme_pairs.extend(affected)

        return list(set(extreme_pairs))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — SELF TEST
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 70)
    print("COT REPORT — SELF TEST")
    print("=" * 70)

    manager = COTManager()

    # Build mock COT history (52 weeks)
    print("\n[1] Building 52-week mock COT history...")
    import random

    mock_records = []
    base_date    = datetime(2023, 1, 6, tzinfo=timezone.utc)

    # Simulate EUR positioning history
    eur_net = 50000
    for week in range(52):
        dt      = base_date + timedelta(weeks=week)
        eur_net = eur_net + random.randint(-10000, 10000)
        eur_net = max(-200000, min(200000, eur_net))

        long_  = max(0, 200000 + eur_net // 2)
        short_ = max(0, 200000 - eur_net // 2)

        mock_records.append(COTRecord(
            currency      = "EUR",
            contract_name = CFTC_CONTRACT_NAMES["EUR"],
            report_date   = dt.strftime("%Y-%m-%d"),
            timestamp_utc = dt.timestamp() * 1000.0,
            noncomm_long  = long_,
            noncomm_short = short_,
            lev_fund_long = long_,
            lev_fund_short= short_,
            open_interest = long_ + short_,
        ))

    # Add GBP records
    gbp_net = -30000
    for week in range(52):
        dt      = base_date + timedelta(weeks=week)
        gbp_net = gbp_net + random.randint(-8000, 8000)
        gbp_net = max(-150000, min(150000, gbp_net))

        long_  = max(0, 100000 + gbp_net // 2)
        short_ = max(0, 100000 - gbp_net // 2)

        mock_records.append(COTRecord(
            currency      = "GBP",
            contract_name = CFTC_CONTRACT_NAMES["GBP"],
            report_date   = dt.strftime("%Y-%m-%d"),
            timestamp_utc = dt.timestamp() * 1000.0,
            noncomm_long  = long_,
            noncomm_short = short_,
            lev_fund_long = long_,
            lev_fund_short= short_,
            open_interest = long_ + short_,
        ))

    manager.load_mock_data(mock_records)
    print(f"    Loaded {len(mock_records)} mock records")

    print("\n[2] EUR positioning analysis:")
    eur = manager.get_currency_positioning("EUR")
    if eur:
        print(f"    Net position    : {eur['net_position']:,}")
        print(f"    Net change      : {eur['net_change']:,}")
        print(f"    Positioning score: {eur['positioning_score']:.1f}/100")
        print(f"    Extreme flag    : {eur['extreme_flag']}")
        print(f"    Trend direction : {eur['trend_direction']}")
        print(f"    Long %          : {eur['long_pct']:.1f}%")
        print(f"    Short %         : {eur['short_pct']:.1f}%")
        print(f"    Data tier       : {eur['data_tier']}")

    print("\n[3] GBP positioning analysis:")
    gbp = manager.get_currency_positioning("GBP")
    if gbp:
        print(f"    Net position    : {gbp['net_position']:,}")
        print(f"    Positioning score: {gbp['positioning_score']:.1f}/100")
        print(f"    Extreme flag    : {gbp['extreme_flag']}")

    print("\n[4] Layer 1 package:")
    pkg = manager.get_layer1_package()
    print(f"    Currencies available : {pkg['currencies_available']}")
    print(f"    Extreme currencies   : {pkg['extreme_currencies']}")
    print(f"    Any extreme          : {pkg['any_extreme']}")
    print(f"    Data tier            : {pkg['data_tier']}")
    print(f"    Data source          : {pkg['data_source']}")
    print(f"    Release schedule     : {pkg['release_schedule']}")
    print(f"    Measurement delay    : {pkg['measurement_delay']}")

    print("\n[5] Pairs with extreme positioning:")
    extreme_pairs = manager.get_pairs_with_extreme_positioning()
    print(f"    {extreme_pairs if extreme_pairs else 'None currently'}")

    print("\n[6] Positioning score interpretation (Layer 3 assigns meaning):")
    print("    Score 0-25  : Historically extreme short positioning")
    print("    Score 25-45 : Below average long positioning")
    print("    Score 45-55 : Neutral positioning")
    print("    Score 55-75 : Above average long positioning")
    print("    Score 75-100: Historically extreme long positioning")
    print("    NOTE: These are labels only — Layer 3 decides trade implication")

    print("\n[7] Data freshness:")
    print(f"    Data age    : {pkg['data_age_days']:.1f} days" if pkg['data_age_days'] else "    Data age    : Unknown")
    print(f"    Freshness   : {pkg['data_freshness']}")
    print("    NOTE: COT is weekly data — 3 day delay from Tuesday measurement")

    print("\n" + "=" * 70)
    print("COT REPORT SELF TEST COMPLETE")
    print("TIER_2 label confirmed — weekly delayed data")
    print("All outputs are raw measurements — Layer 3 interprets")
    print("=" * 70)
