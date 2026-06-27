"""
═══════════════════════════════════════════════════════════════════════════════
FOREX TRADING SIGNAL SYSTEM — VERSION 6.0
FILE: economic_calendar.py
LAYER: 1 — DATA PERCEPTION LAYER (Economic Calendar Sub-Component)
═══════════════════════════════════════════════════════════════════════════════

PURPOSE:
    Fetches, parses, and maintains the economic event calendar.
    Provides event impact scores and timing windows to the pipeline.
    Drives NULL_TIME decisions for pre/post news windows.

SOURCES (in priority order):
    1. OANDA v20 calendar endpoint (if API key available)
    2. Forex Factory XML feed (free, public)
    3. Investing.com scraper (fallback)
    4. Manual CSV override (always available)

ARCHITECTURAL RULES:
    - This file COLLECTS and LABELS events only
    - It NEVER decides whether to trade
    - It outputs: event list + impact scores + timing windows
    - NULL_TIME decisions belong to Layer 3
    - All times stored in UTC

OUTPUT FORMAT (fed to Layer 1 package):
    {
        "upcoming_events"     : [EventRecord, ...],
        "active_window"       : bool,
        "window_type"         : "PRE_NEWS" | "POST_NEWS" | "CLEAR",
        "minutes_to_next"     : float,
        "highest_impact_next" : "HIGH" | "MEDIUM" | "LOW" | "NONE",
        "affected_pairs"      : ["EUR/USD", ...],
        "blackout_pairs"      : ["EUR/USD", ...],
    }

EVENT IMPACT LEVELS:
    HIGH   → NFP, CPI, Fed Rate Decision, GDP, ECB, BOE, BOJ decisions
    MEDIUM → PMI, Retail Sales, Trade Balance, Consumer Confidence
    LOW    → Minor sentiment surveys, secondary releases

TIMING WINDOWS:
    PRE_NEWS  → 30 minutes before HIGH impact event
    POST_NEWS → 15 minutes after HIGH impact event
               (until structure reforms — detected externally)

═══════════════════════════════════════════════════════════════════════════════
"""

import time
import json
import logging
import threading
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path

logger = logging.getLogger("EconomicCalendar")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — EVENT DEFINITIONS
# ═══════════════════════════════════════════════════════════════════════════════

class ImpactLevel:
    HIGH   = "HIGH"
    MEDIUM = "MEDIUM"
    LOW    = "LOW"
    NONE   = "NONE"


class WindowType:
    PRE_NEWS  = "PRE_NEWS"
    POST_NEWS = "POST_NEWS"
    CLEAR     = "CLEAR"


# Currency → affected forex pairs mapping
CURRENCY_PAIR_MAP = {
    "USD": ["EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "AUD/USD", "USD/CAD", "NZD/USD"],
    "EUR": ["EUR/USD", "EUR/GBP", "EUR/JPY"],
    "GBP": ["GBP/USD", "EUR/GBP"],
    "JPY": ["USD/JPY", "EUR/JPY"],
    "CHF": ["USD/CHF"],
    "AUD": ["AUD/USD"],
    "CAD": ["USD/CAD"],
    "NZD": ["NZD/USD"],
}

# High impact event keywords — used for classification
HIGH_IMPACT_KEYWORDS = [
    "non-farm", "nonfarm", "nfp",
    "federal funds rate", "fed rate", "fomc",
    "interest rate decision",
    "cpi", "consumer price index",
    "gdp", "gross domestic product",
    "ecb", "boe", "boj", "rba", "rbnz", "boc",
    "monetary policy",
    "unemployment rate",
    "retail sales",
    "pce",
    "ism manufacturing",
    "ism non-manufacturing",
    "trade balance",
    "manufacturing pmi",
    "services pmi",
]

MEDIUM_IMPACT_KEYWORDS = [
    "pmi", "purchasing managers",
    "consumer confidence",
    "industrial production",
    "housing starts",
    "building permits",
    "durable goods",
    "jobless claims",
    "existing home sales",
    "new home sales",
    "producer price",
    "ppi",
    "current account",
]

# Pre/post news window durations in minutes
PRE_NEWS_WINDOW_MINUTES  = {
    ImpactLevel.HIGH  : 30,
    ImpactLevel.MEDIUM: 15,
    ImpactLevel.LOW   : 0,
}

POST_NEWS_WINDOW_MINUTES = {
    ImpactLevel.HIGH  : 15,
    ImpactLevel.MEDIUM: 5,
    ImpactLevel.LOW   : 0,
}


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — EVENT DATA STRUCTURE
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class EconomicEvent:
    """
    Standardized economic event record.
    Every event regardless of source is normalized into this structure.
    """
    event_id        : str
    title           : str
    currency        : str                  # e.g. "USD", "EUR"
    impact          : str                  # ImpactLevel
    timestamp_utc   : float                # Unix ms — event scheduled time
    actual          : Optional[str] = None # Actual value (if released)
    forecast        : Optional[str] = None # Consensus forecast
    previous        : Optional[str] = None # Previous reading
    source          : str = "UNKNOWN"      # Where this came from
    affected_pairs  : list = field(default_factory=list)

    def __post_init__(self):
        if not self.affected_pairs:
            self.affected_pairs = CURRENCY_PAIR_MAP.get(self.currency.upper(), [])

    @property
    def is_released(self) -> bool:
        return self.actual is not None

    @property
    def datetime_utc(self) -> datetime:
        return datetime.fromtimestamp(
            self.timestamp_utc / 1000.0,
            tz=timezone.utc
        )

    def minutes_until(self) -> float:
        """Minutes from now until this event."""
        now_ms = time.time() * 1000
        return (self.timestamp_utc - now_ms) / 60000.0

    def minutes_since(self) -> float:
        """Minutes since this event occurred."""
        now_ms = time.time() * 1000
        return (now_ms - self.timestamp_utc) / 60000.0

    def to_dict(self) -> dict:
        return {
            "event_id"      : self.event_id,
            "title"         : self.title,
            "currency"      : self.currency,
            "impact"        : self.impact,
            "timestamp_utc" : self.timestamp_utc,
            "datetime_utc"  : self.datetime_utc.isoformat(),
            "actual"        : self.actual,
            "forecast"      : self.forecast,
            "previous"      : self.previous,
            "source"        : self.source,
            "affected_pairs": self.affected_pairs,
            "is_released"   : self.is_released,
            "minutes_until" : self.minutes_until(),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — IMPACT CLASSIFIER
# ═══════════════════════════════════════════════════════════════════════════════

class ImpactClassifier:
    """
    Classifies event impact level from title text.
    Used when source does not provide explicit impact rating.

    RULE: This is a MEASUREMENT — not a trade decision.
    Layer 3 decides what to do with impact levels.
    """

    @staticmethod
    def classify(title: str, source_impact: Optional[str] = None) -> str:
        """
        Classify impact level from event title.

        Priority:
            1. Source-provided impact (if reliable)
            2. Keyword matching
            3. Default LOW
        """
        # If source provides explicit HIGH — trust it
        if source_impact and source_impact.upper() in ("HIGH", "3", "RED"):
            return ImpactLevel.HIGH
        if source_impact and source_impact.upper() in ("MEDIUM", "2", "ORANGE"):
            return ImpactLevel.MEDIUM

        title_lower = title.lower()

        for kw in HIGH_IMPACT_KEYWORDS:
            if kw in title_lower:
                return ImpactLevel.HIGH

        for kw in MEDIUM_IMPACT_KEYWORDS:
            if kw in title_lower:
                return ImpactLevel.MEDIUM

        return ImpactLevel.LOW


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — FOREX FACTORY PARSER
# ═══════════════════════════════════════════════════════════════════════════════

class ForexFactoryParser:
    """
    Parses Forex Factory XML calendar feed.

    Forex Factory XML feed:
        URL: https://nfs.faireconomy.media/ff_calendar_thisweek.xml
        Also: ff_calendar_nextweek.xml

    XML structure:
        <weeklyevents>
          <event>
            <title>Non-Farm Employment Change</title>
            <country>USD</country>
            <date>Jan 15, 2024</date>
            <time>8:30am</time>
            <impact>High</impact>
            <forecast>180K</forecast>
            <previous>216K</previous>
            <actual></actual>
          </event>
        </weeklyevents>

    RULE: Parser LABELS events only — no trade decisions made here.
    """

    FEED_URL_THIS_WEEK = "https://nfs.faireconomy.media/ff_calendar_thisweek.xml"
    FEED_URL_NEXT_WEEK = "https://nfs.faireconomy.media/ff_calendar_nextweek.xml"

    @staticmethod
    def fetch_and_parse(url: str, timeout: int = 10) -> list:
        """
        Fetch and parse Forex Factory XML feed.
        Returns list of EconomicEvent objects.
        """
        try:
            response = requests.get(url, timeout=timeout, headers={
                "User-Agent": "Mozilla/5.0 (compatible; ForexCalendar/1.0)"
            })
            response.raise_for_status()
            return ForexFactoryParser.parse_xml(response.text)
        except requests.RequestException as e:
            logger.warning(f"ForexFactory fetch failed: {e}")
            return []

    @staticmethod
    def parse_xml(xml_text: str) -> list:
        """Parse XML string into EconomicEvent list."""
        events = []
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            logger.error(f"ForexFactory XML parse error: {e}")
            return []

        for i, event_el in enumerate(root.findall("event")):
            try:
                title    = event_el.findtext("title", "").strip()
                country  = event_el.findtext("country", "").strip().upper()
                date_str = event_el.findtext("date", "").strip()
                time_str = event_el.findtext("time", "").strip()
                impact   = event_el.findtext("impact", "").strip()
                forecast = event_el.findtext("forecast", "").strip() or None
                previous = event_el.findtext("previous", "").strip() or None
                actual   = event_el.findtext("actual", "").strip() or None

                if not title or not country:
                    continue

                timestamp_utc = ForexFactoryParser._parse_datetime(
                    date_str, time_str
                )
                if timestamp_utc is None:
                    continue

                classified_impact = ImpactClassifier.classify(title, impact)

                event = EconomicEvent(
                    event_id      = f"FF_{i}_{int(timestamp_utc)}",
                    title         = title,
                    currency      = country,
                    impact        = classified_impact,
                    timestamp_utc = timestamp_utc,
                    actual        = actual,
                    forecast      = forecast,
                    previous      = previous,
                    source        = "FOREX_FACTORY",
                )
                events.append(event)

            except Exception as e:
                logger.warning(f"ForexFactory event parse error: {e}")
                continue

        logger.info(f"ForexFactory: parsed {len(events)} events")
        return events

    @staticmethod
    def _parse_datetime(date_str: str, time_str: str) -> Optional[float]:
        """
        Parse Forex Factory date + time into Unix ms UTC.
        Format: "Jan 15, 2024" + "8:30am"
        """
        try:
            # Combine and parse
            dt_str = f"{date_str} {time_str}"

            # Handle various time formats
            for fmt in [
                "%b %d, %Y %I:%M%p",
                "%b %d, %Y %I:%M %p",
                "%b %d, %Y",  # All-day events
            ]:
                try:
                    dt = datetime.strptime(dt_str.strip(), fmt)
                    # Assume ET (UTC-5 or UTC-4) — convert to UTC
                    dt = dt.replace(tzinfo=timezone.utc)
                    # Add 5 hours for ET→UTC approximation
                    dt = dt + timedelta(hours=5)
                    return dt.timestamp() * 1000.0
                except ValueError:
                    continue
            return None
        except Exception:
            return None


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — OANDA CALENDAR PARSER
# ═══════════════════════════════════════════════════════════════════════════════

class OANDACalendarParser:
    """
    Fetches economic calendar from OANDA v20 REST API.

    OANDA calendar endpoint:
        GET /v3/instruments/{instrument}/calendarEvents
        period: -604800 to 604800 (seconds, ±1 week)

    OANDA provides impact as 1 (low) to 3 (high).
    """

    BASE_URL     = "https://api-fxpractice.oanda.com"
    CALENDAR_PATH= "/v3/instruments/{instrument}/calendarEvents"

    def __init__(self, api_key: str, practice: bool = True):
        self.api_key  = api_key
        self.base_url = (
            "https://api-fxpractice.oanda.com"
            if practice else
            "https://api-fxtrade.oanda.com"
        )
        self.headers  = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type" : "application/json",
        }

    def fetch_events(self, instrument: str = "EUR_USD",
                     period: int = 604800) -> list:
        """
        Fetch calendar events for an instrument.

        Args:
            instrument : OANDA format e.g. "EUR_USD"
            period     : seconds (604800 = 1 week ahead)
        """
        url    = f"{self.base_url}/v3/instruments/{instrument}/calendarEvents"
        params = {"period": period}

        try:
            response = requests.get(
                url, headers=self.headers,
                params=params, timeout=10
            )
            response.raise_for_status()
            data = response.json()
            return self._parse_oanda_events(data)

        except requests.RequestException as e:
            logger.warning(f"OANDA calendar fetch failed: {e}")
            return []

    def _parse_oanda_events(self, data: list) -> list:
        """Parse OANDA calendar JSON response."""
        events = []
        for i, item in enumerate(data):
            try:
                title    = item.get("title", "")
                currency = item.get("unit", "")
                ts_s     = item.get("timestamp", 0)
                impact_n = item.get("impact", 1)
                forecast = str(item.get("forecast", "")) or None
                previous = str(item.get("previous", "")) or None
                actual   = str(item.get("actual", ""))   or None

                impact_map = {1: "LOW", 2: "MEDIUM", 3: "HIGH"}
                impact_str = impact_map.get(impact_n, "LOW")
                classified = ImpactClassifier.classify(title, impact_str)

                event = EconomicEvent(
                    event_id      = f"OANDA_{i}_{ts_s}",
                    title         = title,
                    currency      = currency,
                    impact        = classified,
                    timestamp_utc = float(ts_s) * 1000.0,
                    actual        = actual,
                    forecast      = forecast,
                    previous      = previous,
                    source        = "OANDA_CALENDAR",
                )
                events.append(event)
            except Exception as e:
                logger.warning(f"OANDA event parse error: {e}")
                continue

        logger.info(f"OANDA Calendar: parsed {len(events)} events")
        return events


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — CSV OVERRIDE LOADER
# ═══════════════════════════════════════════════════════════════════════════════

class CSVCalendarLoader:
    """
    Load economic events from a manual CSV file.
    This is always available — fallback when APIs fail.

    CSV format:
        date,time_utc,currency,title,impact,forecast,previous
        2024-01-15,13:30:00,USD,Non-Farm Payrolls,HIGH,180K,216K
    """

    @staticmethod
    def load(filepath: Path) -> list:
        """Load events from CSV file."""
        import csv
        events = []

        if not filepath.exists():
            logger.warning(f"Calendar CSV not found: {filepath}")
            return []

        try:
            with open(filepath, "r") as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    try:
                        dt_str = f"{row['date']} {row['time_utc']}"
                        dt     = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                        dt     = dt.replace(tzinfo=timezone.utc)
                        ts_ms  = dt.timestamp() * 1000.0

                        impact = ImpactClassifier.classify(
                            row.get("title", ""),
                            row.get("impact", "LOW")
                        )

                        event = EconomicEvent(
                            event_id      = f"CSV_{i}_{int(ts_ms)}",
                            title         = row.get("title", ""),
                            currency      = row.get("currency", "USD").upper(),
                            impact        = impact,
                            timestamp_utc = ts_ms,
                            forecast      = row.get("forecast") or None,
                            previous      = row.get("previous") or None,
                            actual        = row.get("actual")   or None,
                            source        = "CSV_MANUAL",
                        )
                        events.append(event)
                    except (KeyError, ValueError) as e:
                        logger.warning(f"CSV row error: {row} — {e}")
                        continue

            logger.info(f"CSV Calendar: loaded {len(events)} events")
        except OSError as e:
            logger.error(f"CSV load failed: {e}")

        return events


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — EVENT STORE
# ═══════════════════════════════════════════════════════════════════════════════

class EventStore:
    """
    Thread-safe store for economic events.
    Manages deduplication, sorting, and expiry of past events.
    """

    def __init__(self, history_hours: int = 4):
        self._events          : list = []
        self._lock            = threading.Lock()
        self._history_hours   = history_hours  # Keep N hours of past events

    def update(self, new_events: list):
        """Add new events, deduplicate, sort by timestamp."""
        with self._lock:
            # Merge — deduplicate by title + timestamp
            existing_keys = {
                (e.title, int(e.timestamp_utc / 60000))
                for e in self._events
            }
            added = 0
            for event in new_events:
                key = (event.title, int(event.timestamp_utc / 60000))
                if key not in existing_keys:
                    self._events.append(event)
                    existing_keys.add(key)
                    added += 1

            # Sort by timestamp
            self._events.sort(key=lambda e: e.timestamp_utc)

            # Expire old events
            cutoff_ms = (time.time() - self._history_hours * 3600) * 1000
            self._events = [
                e for e in self._events
                if e.timestamp_utc > cutoff_ms
            ]

            if added > 0:
                logger.info(f"EventStore: added {added} events, total={len(self._events)}")

    def get_upcoming(self, hours_ahead: float = 24.0) -> list:
        """Get events scheduled in the next N hours."""
        now_ms    = time.time() * 1000
        cutoff_ms = now_ms + hours_ahead * 3600 * 1000
        with self._lock:
            return [
                e for e in self._events
                if now_ms <= e.timestamp_utc <= cutoff_ms
            ]

    def get_recent(self, hours_back: float = 1.0) -> list:
        """Get events that occurred in the last N hours."""
        now_ms    = time.time() * 1000
        cutoff_ms = now_ms - hours_back * 3600 * 1000
        with self._lock:
            return [
                e for e in self._events
                if cutoff_ms <= e.timestamp_utc <= now_ms
            ]

    def get_all(self) -> list:
        with self._lock:
            return list(self._events)

    def update_actual(self, event_id: str, actual: str):
        """Mark an event as released with actual value."""
        with self._lock:
            for event in self._events:
                if event.event_id == event_id:
                    event.actual = actual
                    break


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — WINDOW CALCULATOR
# ═══════════════════════════════════════════════════════════════════════════════

class WindowCalculator:
    """
    Calculates pre/post news windows for each moment in time.

    RULE:
        This calculator LABELS windows only.
        It does NOT decide whether to trade.
        NULL_TIME decisions belong to Layer 3.

    Output per pair:
        window_type      : PRE_NEWS | POST_NEWS | CLEAR
        minutes_to_next  : float
        event            : EconomicEvent | None
        blackout         : bool (True = Layer 3 should consider NULL_TIME)
    """

    @staticmethod
    def calculate(events: list, pair: str) -> dict:
        """
        Calculate current window state for a specific pair.

        Returns window assessment for this pair right now.
        """
        now_ms = time.time() * 1000

        # Filter events that affect this pair
        relevant = [
            e for e in events
            if pair in e.affected_pairs or not e.affected_pairs
        ]

        # Sort by timestamp
        relevant.sort(key=lambda e: e.timestamp_utc)

        # Check for active pre-news window
        for event in relevant:
            if event.impact == ImpactLevel.LOW:
                continue

            pre_window_ms  = PRE_NEWS_WINDOW_MINUTES[event.impact] * 60 * 1000
            post_window_ms = POST_NEWS_WINDOW_MINUTES[event.impact] * 60 * 1000

            time_to_event = event.timestamp_utc - now_ms
            time_since    = now_ms - event.timestamp_utc

            # Pre-news window: event is upcoming and within pre-window
            if 0 < time_to_event <= pre_window_ms:
                return {
                    "window_type"    : WindowType.PRE_NEWS,
                    "event"          : event.to_dict(),
                    "impact"         : event.impact,
                    "minutes_to_event": time_to_event / 60000,
                    "blackout"       : event.impact == ImpactLevel.HIGH,
                    "pair"           : pair,
                }

            # Post-news window: event just happened
            if 0 < time_since <= post_window_ms:
                return {
                    "window_type"    : WindowType.POST_NEWS,
                    "event"          : event.to_dict(),
                    "impact"         : event.impact,
                    "minutes_since"  : time_since / 60000,
                    "blackout"       : event.impact == ImpactLevel.HIGH,
                    "pair"           : pair,
                }

        # Find next upcoming event
        upcoming = [
            e for e in relevant
            if e.timestamp_utc > now_ms
        ]

        next_event     = upcoming[0] if upcoming else None
        minutes_to_next = next_event.minutes_until() if next_event else None

        return {
            "window_type"     : WindowType.CLEAR,
            "event"           : next_event.to_dict() if next_event else None,
            "impact"          : next_event.impact if next_event else ImpactLevel.NONE,
            "minutes_to_next" : minutes_to_next,
            "blackout"        : False,
            "pair"            : pair,
        }

    @staticmethod
    def get_blackout_pairs(events: list) -> list:
        """
        Return list of pairs currently in a high-impact blackout window.
        LABEL only — Layer 3 enforces NULL_TIME.
        """
        blackout = []
        pairs_to_check = [
            "EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF",
            "AUD/USD", "USD/CAD", "NZD/USD", "EUR/GBP", "EUR/JPY"
        ]
        for pair in pairs_to_check:
            result = WindowCalculator.calculate(events, pair)
            if result.get("blackout"):
                blackout.append(pair)
        return blackout


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 9 — CALENDAR MANAGER (MAIN CLASS)
# ═══════════════════════════════════════════════════════════════════════════════

class EconomicCalendarManager:
    """
    Master manager for all economic calendar data.

    Responsibilities:
        - Fetch events from multiple sources with fallback
        - Maintain event store with deduplication
        - Calculate window states per pair
        - Refresh on schedule
        - Provide Layer 1 output package

    Refresh schedule:
        - Full refresh: every 6 hours
        - Rapid check: every 60 seconds (check for releases)

    RULE:
        This manager outputs measurements and labels.
        All NULL decisions belong to Layer 3.
    """

    def __init__(self,
                 api_key        : Optional[str] = None,
                 oanda_practice : bool = True,
                 csv_path       : Optional[Path] = None,
                 refresh_hours  : float = 6.0):

        self.api_key        = api_key
        self.oanda_practice = oanda_practice
        self.csv_path       = csv_path
        self.refresh_hours  = refresh_hours

        self._store         = EventStore()
        self._last_refresh  = 0.0
        self._lock          = threading.Lock()
        self._thread        = None
        self._running       = False

        # Parsers
        self._ff_parser     = ForexFactoryParser()
        self._oanda_parser  = OANDACalendarParser(api_key, oanda_practice) \
                              if api_key else None

        logger.info("EconomicCalendarManager initialized")

    def start(self):
        """Start background refresh thread."""
        self._running = True
        self._thread  = threading.Thread(
            target  = self._refresh_loop,
            daemon  = True,
            name    = "CalendarRefreshThread"
        )
        self._thread.start()
        # Initial fetch
        self._refresh()
        logger.info("EconomicCalendarManager: started")

    def stop(self):
        self._running = False
        logger.info("EconomicCalendarManager: stopped")

    def _refresh_loop(self):
        """Background loop — refresh every N hours."""
        while self._running:
            time.sleep(60)  # Check every minute
            elapsed = time.time() - self._last_refresh
            if elapsed >= self.refresh_hours * 3600:
                self._refresh()

    def _refresh(self):
        """Fetch events from all available sources."""
        logger.info("EconomicCalendar: refreshing...")
        all_events = []

        # Source 1: OANDA calendar (if API key available)
        if self._oanda_parser:
            try:
                oanda_events = self._oanda_parser.fetch_events()
                all_events.extend(oanda_events)
                logger.info(f"OANDA calendar: {len(oanda_events)} events")
            except Exception as e:
                logger.warning(f"OANDA calendar failed: {e}")

        # Source 2: Forex Factory XML feed
        try:
            ff_this = self._ff_parser.fetch_and_parse(
                ForexFactoryParser.FEED_URL_THIS_WEEK
            )
            ff_next = self._ff_parser.fetch_and_parse(
                ForexFactoryParser.FEED_URL_NEXT_WEEK
            )
            all_events.extend(ff_this)
            all_events.extend(ff_next)
        except Exception as e:
            logger.warning(f"ForexFactory failed: {e}")

        # Source 3: CSV override (always try if path provided)
        if self.csv_path:
            try:
                csv_events = CSVCalendarLoader.load(self.csv_path)
                all_events.extend(csv_events)
            except Exception as e:
                logger.warning(f"CSV calendar failed: {e}")

        # Update store
        self._store.update(all_events)
        self._last_refresh = time.time()
        logger.info(
            f"EconomicCalendar: refresh complete — "
            f"{len(all_events)} events fetched"
        )

    def force_refresh(self):
        """Force immediate refresh."""
        self._refresh()

    def load_mock_events(self, events: list):
        """Load mock events for testing — bypasses API calls."""
        self._store.update(events)

    # ── Layer 1 Output Interface ─────────────────────────────────────────────

    def get_layer1_package(self, pair: str = None) -> dict:
        """
        Return Layer 1 calendar package.
        This is the output consumed by Layer 1 / Layer 2.

        Contains measurements and labels only.
        No trade decisions.
        """
        upcoming = self._store.get_upcoming(hours_ahead=24)
        recent   = self._store.get_recent(hours_back=1)
        blackout = WindowCalculator.get_blackout_pairs(
            upcoming + recent
        )

        # High impact upcoming events
        high_upcoming = [
            e for e in upcoming
            if e.impact == ImpactLevel.HIGH
        ]

        # Pair-specific window
        pair_window = None
        if pair:
            all_relevant = upcoming + recent
            pair_window  = WindowCalculator.calculate(all_relevant, pair)

        # Next high impact event
        next_high = high_upcoming[0] if high_upcoming else None

        return {
            # ── Event lists ────────────────────────────────────────────
            "upcoming_events"       : [e.to_dict() for e in upcoming[:20]],
            "recent_events"         : [e.to_dict() for e in recent],
            "high_impact_upcoming"  : [e.to_dict() for e in high_upcoming[:5]],

            # ── Window state ───────────────────────────────────────────
            "pair_window"           : pair_window,
            "blackout_pairs"        : blackout,
            "active_blackout"       : len(blackout) > 0,

            # ── Next event summary ─────────────────────────────────────
            "next_high_impact"      : next_high.to_dict() if next_high else None,
            "minutes_to_next_high"  : next_high.minutes_until() if next_high else None,
            "highest_impact_next4h" : self._highest_impact_next_hours(upcoming, 4),

            # ── Metadata ───────────────────────────────────────────────
            "last_refresh_utc"      : datetime.fromtimestamp(
                                          self._last_refresh,
                                          tz=timezone.utc
                                      ).isoformat() if self._last_refresh else None,
            "total_events_stored"   : len(self._store.get_all()),

            # ── LABEL (Layer 3 uses this for NULL_TIME decisions) ──────
            # Layer 3 decides what to do — this file only labels
            "calendar_data_source"  : "FOREX_FACTORY+OANDA+CSV",
        }

    def get_window_state(self, pair: str) -> dict:
        """Get current window state for a specific pair."""
        all_events = (
            self._store.get_upcoming(hours_ahead=2) +
            self._store.get_recent(hours_back=0.5)
        )
        return WindowCalculator.calculate(all_events, pair)

    def is_high_impact_window(self, pair: str) -> bool:
        """
        Quick check — is this pair in a high impact window right now?
        LABEL only — Layer 3 enforces NULL_TIME.
        """
        window = self.get_window_state(pair)
        return (
            window.get("blackout", False) and
            window.get("impact") == ImpactLevel.HIGH
        )

    @staticmethod
    def _highest_impact_next_hours(events: list, hours: float) -> str:
        """Return highest impact level in next N hours."""
        now_ms    = time.time() * 1000
        cutoff_ms = now_ms + hours * 3600 * 1000
        upcoming  = [e for e in events if e.timestamp_utc <= cutoff_ms]
        if any(e.impact == ImpactLevel.HIGH for e in upcoming):
            return ImpactLevel.HIGH
        if any(e.impact == ImpactLevel.MEDIUM for e in upcoming):
            return ImpactLevel.MEDIUM
        return ImpactLevel.NONE


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 10 — SELF TEST
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 70)
    print("ECONOMIC CALENDAR — SELF TEST")
    print("=" * 70)

    cal = EconomicCalendarManager(api_key=None)

    # Inject mock events
    now_ms = time.time() * 1000

    mock_events = [
        EconomicEvent(
            event_id      = "MOCK_001",
            title         = "Non-Farm Payrolls",
            currency      = "USD",
            impact        = ImpactLevel.HIGH,
            timestamp_utc = now_ms + (25 * 60 * 1000),  # 25 min from now
            forecast      = "180K",
            previous      = "216K",
            source        = "MOCK",
        ),
        EconomicEvent(
            event_id      = "MOCK_002",
            title         = "ECB Rate Decision",
            currency      = "EUR",
            impact        = ImpactLevel.HIGH,
            timestamp_utc = now_ms + (4 * 3600 * 1000),  # 4h from now
            forecast      = "4.50%",
            previous      = "4.50%",
            source        = "MOCK",
        ),
        EconomicEvent(
            event_id      = "MOCK_003",
            title         = "German PMI",
            currency      = "EUR",
            impact        = ImpactLevel.MEDIUM,
            timestamp_utc = now_ms - (10 * 60 * 1000),  # 10 min ago
            actual        = "47.3",
            forecast      = "47.0",
            previous      = "46.8",
            source        = "MOCK",
        ),
    ]

    cal.load_mock_events(mock_events)

    print("\n[1] EUR/USD window state:")
    window = cal.get_window_state("EUR/USD")
    print(f"    Window type : {window['window_type']}")
    print(f"    Impact      : {window['impact']}")
    print(f"    Blackout    : {window['blackout']}")
    if window.get("minutes_to_event"):
        print(f"    Minutes to  : {window['minutes_to_event']:.1f} min")

    print("\n[2] USD/JPY window state:")
    window2 = cal.get_window_state("USD/JPY")
    print(f"    Window type : {window2['window_type']}")
    print(f"    Blackout    : {window2['blackout']}")

    print("\n[3] Layer 1 package:")
    pkg = cal.get_layer1_package("EUR/USD")
    print(f"    Upcoming events      : {len(pkg['upcoming_events'])}")
    print(f"    High impact upcoming : {len(pkg['high_impact_upcoming'])}")
    print(f"    Active blackout      : {pkg['active_blackout']}")
    print(f"    Blackout pairs       : {pkg['blackout_pairs']}")
    print(f"    Next high impact     : {pkg['next_high_impact']['title'] if pkg['next_high_impact'] else 'None'}")
    print(f"    Minutes to next HIGH : {pkg['minutes_to_next_high']:.1f}" if pkg['minutes_to_next_high'] else "    Minutes to next HIGH : None")
    print(f"    Highest next 4h      : {pkg['highest_impact_next4h']}")

    print("\n[4] Impact classification test:")
    tests = [
        ("Non-Farm Employment Change", None),
        ("Federal Funds Rate", None),
        ("German PMI Manufacturing", None),
        ("French Consumer Confidence", None),
        ("Minor speech", None),
    ]
    for title, src_impact in tests:
        classified = ImpactClassifier.classify(title, src_impact)
        print(f"    '{title}' → {classified}")

    print("\n[5] High impact window check:")
    print(f"    EUR/USD high impact : {cal.is_high_impact_window('EUR/USD')}")
    print(f"    EUR/JPY high impact : {cal.is_high_impact_window('EUR/JPY')}")

    print("\n" + "=" * 70)
    print("ECONOMIC CALENDAR SELF TEST COMPLETE")
    print("Layer 1 package ready — NULL_TIME decisions belong to Layer 3")
    print("=" * 70)
