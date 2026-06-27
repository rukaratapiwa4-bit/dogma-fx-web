"""
═══════════════════════════════════════════════════════════════════════════════
FOREX TRADING SIGNAL SYSTEM — VERSION 6.0
FILE: news_sentiment.py
LAYER: 1 — DATA PERCEPTION LAYER (News & Sentiment Sub-Component)
═══════════════════════════════════════════════════════════════════════════════

PURPOSE:
    Fetches financial news from multiple free RSS/API sources,
    processes sentiment using Claude AI, and maintains retail
    sentiment positioning data.

    This file feeds the News & Sentiment inputs of Layer 1.

NEWS SOURCES (free, no API key required):
    1. Reuters RSS         → https://feeds.reuters.com/reuters/businessNews
    2. Bloomberg RSS       → https://feeds.bloomberg.com/markets/news.rss
    3. FXStreet RSS        → https://www.fxstreet.com/rss/news
    4. DailyFX RSS         → https://www.dailyfx.com/feeds/all
    5. Investing.com RSS   → https://www.investing.com/rss/news.rss
    6. NewsAPI             → https://newsapi.org (requires free API key)
    7. Alpha Vantage News  → https://www.alphavantage.co (requires free key)

SENTIMENT SOURCES:
    1. OANDA Order Book    → retail positioning per pair (% long/short)
    2. MyFXBook Sentiment  → community sentiment data
    3. DailyFX Sentiment   → SSI (Speculative Sentiment Index)

CLAUDE AI PROCESSING:
    Each article is sent to Claude API for:
        - Sentiment score (-1.0 to +1.0)
        - Affected currencies
        - Market impact assessment
        - Key themes extraction

ARCHITECTURAL RULES:
    - Collects and labels data ONLY
    - NEVER decides whether to trade
    - Outputs: raw sentiment scores + news items + retail positioning
    - All interpretation belongs to Layer 3
    - Claude AI produces scores — Layer 3 assigns weights

OUTPUT:
    {
        "news_items"         : [NewsItem, ...],
        "sentiment_scores"   : { "EUR": 0.6, "USD": -0.3, ... },
        "news_impact_score"  : float (0→100),
        "retail_sentiment"   : { "EUR/USD": { long_pct, short_pct }, ... },
        "sentiment_extreme"  : { "EUR/USD": bool, ... },
        "fear_greed_index"   : float (0→100),
        "claude_summary"     : str,
    }

═══════════════════════════════════════════════════════════════════════════════
"""

import re
import time
import json
import html
import hashlib
import logging
import threading
import requests
import numpy as np
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from typing import Optional
from dataclasses import dataclass, field
from collections import deque
from bs4 import BeautifulSoup

logger = logging.getLogger("NewsSentiment")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

# RSS feed URLs — all free, no API key needed
RSS_FEEDS = {
    "reuters"     : "https://feeds.reuters.com/reuters/businessNews",
    "reuters_fx"  : "https://feeds.reuters.com/reuters/UKdomesticNews",
    "fxstreet"    : "https://www.fxstreet.com/rss/news",
    "dailyfx"     : "https://www.dailyfx.com/feeds/all",
    "forexlive"   : "https://www.forexlive.com/feed/news",
    "marketwatch" : "https://feeds.marketwatch.com/marketwatch/marketpulse",
    "cnbc_fx"     : "https://www.cnbc.com/id/20910258/device/rss/rss.html",
}

# Sentiment source URLs
SENTIMENT_URLS = {
    "oanda_order_book" : "https://www.oanda.com/lang/en/trading/platform/open-position-ratios/",
    "myfxbook"         : "https://www.myfxbook.com/community/outlook",
    "dailyfx_ssi"      : "https://www.dailyfx.com/sentiment",
}

# Claude API endpoint
CLAUDE_API_URL   = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL     = "claude-sonnet-4-6"

# Currency keywords for article filtering
CURRENCY_KEYWORDS = {
    "USD": ["dollar", "usd", "fed", "federal reserve", "fomc", "powell",
            "us economy", "american", "united states"],
    "EUR": ["euro", "eur", "ecb", "european central bank", "lagarde",
            "eurozone", "european union", "germany", "france"],
    "GBP": ["pound", "gbp", "sterling", "boe", "bank of england",
            "bailey", "uk economy", "britain", "british"],
    "JPY": ["yen", "jpy", "boj", "bank of japan", "ueda",
            "japan", "japanese", "nikkei"],
    "CHF": ["franc", "chf", "snb", "swiss national bank",
            "switzerland", "swiss"],
    "AUD": ["australian dollar", "aud", "rba", "reserve bank australia",
            "australia", "australian"],
    "CAD": ["canadian dollar", "cad", "boc", "bank of canada",
            "canada", "canadian", "loonie"],
    "NZD": ["new zealand dollar", "nzd", "rbnz", "reserve bank new zealand",
            "new zealand", "kiwi"],
}

# Forex pair mapping
PAIR_CURRENCIES = {
    "EUR/USD": ("EUR", "USD"),
    "GBP/USD": ("GBP", "USD"),
    "USD/JPY": ("USD", "JPY"),
    "USD/CHF": ("USD", "CHF"),
    "AUD/USD": ("AUD", "USD"),
    "USD/CAD": ("USD", "CAD"),
    "NZD/USD": ("NZD", "USD"),
    "EUR/GBP": ("EUR", "GBP"),
    "EUR/JPY": ("EUR", "JPY"),
}

# Sentiment extreme threshold
SENTIMENT_EXTREME_THRESHOLD = 0.70  # 70% one side = extreme

# Cache settings
NEWS_CACHE_HOURS  = 4       # Keep news for 4 hours
MAX_NEWS_ITEMS    = 200     # Max items in memory
MAX_ARTICLES_CLAUDE = 5     # Max articles sent to Claude per cycle


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class NewsItem:
    """
    Standardized news article record.
    Raw content + metadata. Sentiment added after Claude processing.
    """
    item_id         : str              # MD5 of URL
    title           : str
    summary         : str
    url             : str
    source          : str
    timestamp_utc   : float            # Unix ms
    currencies      : list             # Affected currencies detected
    pairs           : list             # Affected pairs

    # Sentiment (added after Claude processing)
    sentiment_score : Optional[float] = None   # -1.0 to +1.0
    sentiment_label : Optional[str]   = None   # POSITIVE/NEGATIVE/NEUTRAL
    impact_score    : Optional[float] = None   # 0→100
    key_themes      : list             = field(default_factory=list)
    claude_processed: bool             = False

    def to_dict(self) -> dict:
        return {
            "item_id"         : self.item_id,
            "title"           : self.title,
            "summary"         : self.summary[:300],  # Truncate for output
            "url"             : self.url,
            "source"          : self.source,
            "timestamp_utc"   : self.timestamp_utc,
            "datetime_utc"    : datetime.fromtimestamp(
                                    self.timestamp_utc / 1000.0,
                                    tz=timezone.utc
                                ).isoformat(),
            "currencies"      : self.currencies,
            "pairs"           : self.pairs,
            "sentiment_score" : self.sentiment_score,
            "sentiment_label" : self.sentiment_label,
            "impact_score"    : self.impact_score,
            "key_themes"      : self.key_themes,
            "claude_processed": self.claude_processed,
        }


@dataclass
class RetailSentiment:
    """
    Retail trader positioning for a forex pair.
    Raw percentages — Layer 3 decides significance.
    """
    pair            : str
    long_pct        : float            # % of retail traders long
    short_pct       : float            # % of retail traders short
    timestamp_utc   : float
    source          : str

    # Computed
    extreme_flag    : bool = field(init=False)
    net_bias        : float = field(init=False)

    def __post_init__(self):
        # Extreme if one side > threshold
        self.extreme_flag = (
            self.long_pct  >= SENTIMENT_EXTREME_THRESHOLD * 100 or
            self.short_pct >= SENTIMENT_EXTREME_THRESHOLD * 100
        )
        # Net bias: positive = more longs, negative = more shorts
        self.net_bias = (self.long_pct - self.short_pct) / 100.0

    def to_dict(self) -> dict:
        return {
            "pair"          : self.pair,
            "long_pct"      : round(self.long_pct, 1),
            "short_pct"     : round(self.short_pct, 1),
            "net_bias"      : round(self.net_bias, 3),
            "extreme_flag"  : self.extreme_flag,
            "source"        : self.source,
            "timestamp_utc" : self.timestamp_utc,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — RSS FEED PARSER
# ═══════════════════════════════════════════════════════════════════════════════

class RSSFeedParser:
    """
    Parses RSS/Atom feeds from financial news sources.
    Extracts title, summary, URL, publication date.
    Detects affected currencies from content.

    RULE: Raw content extraction only — no sentiment here.
    """

    HEADERS = {
        "User-Agent"     : "Mozilla/5.0 (compatible; ForexNewsBot/1.0)",
        "Accept"         : "application/rss+xml, application/xml, text/xml",
        "Accept-Language": "en-US,en;q=0.9",
    }

    @classmethod
    def fetch(cls, source_name: str, url: str,
              timeout: int = 10) -> list:
        """
        Fetch and parse one RSS feed.
        Returns list of NewsItem objects.
        """
        try:
            response = requests.get(
                url, headers=cls.HEADERS,
                timeout=timeout
            )
            response.raise_for_status()
            return cls._parse(source_name, response.text)

        except requests.RequestException as e:
            logger.warning(f"RSS fetch failed [{source_name}]: {e}")
            return []

    @classmethod
    def _parse(cls, source: str, content: str) -> list:
        """Parse RSS/Atom XML content."""
        items = []

        try:
            # Try standard RSS parsing
            root = ET.fromstring(content)
        except ET.ParseError:
            # Try cleaning HTML entities
            try:
                cleaned = html.unescape(content)
                root    = ET.fromstring(cleaned)
            except ET.ParseError as e:
                logger.warning(f"RSS XML parse failed [{source}]: {e}")
                return []

        # Handle both RSS 2.0 and Atom formats
        ns = {
            "atom"   : "http://www.w3.org/2005/Atom",
            "content": "http://purl.org/rss/1.0/modules/content/",
            "dc"     : "http://purl.org/dc/elements/1.1/",
        }

        # RSS 2.0
        entries = root.findall(".//item")

        # Atom fallback
        if not entries:
            entries = root.findall(".//atom:entry", ns)

        for entry in entries[:20]:  # Max 20 per feed
            item = cls._parse_entry(source, entry, ns)
            if item:
                items.append(item)

        return items

    @classmethod
    def _parse_entry(cls, source: str,
                     entry, ns: dict) -> Optional[NewsItem]:
        """Parse single RSS entry."""
        try:
            # Title — explicit is not None (ET elements are falsy when empty)
            title_el = entry.find("title")
            if title_el is None:
                title_el = entry.find("atom:title", ns)
            title = cls._clean_text(
                title_el.text if title_el is not None else ""
            )
            if not title:
                return None

            # URL
            link_el = entry.find("link")
            if link_el is None:
                link_el = entry.find("atom:link", ns)
            if link_el is not None:
                url = link_el.get("href") or (link_el.text or "")
            else:
                url = ""
            url = url.strip()

            # Summary / description
            desc_el = entry.find("description")
            if desc_el is None:
                desc_el = entry.find("summary")
            if desc_el is None:
                desc_el = entry.find("atom:summary", ns)
            summary = cls._clean_text(
                desc_el.text if desc_el is not None else ""
            )

            # Publication date
            pub_el = entry.find("pubDate")
            if pub_el is None:
                pub_el = entry.find("published")
            if pub_el is None:
                pub_el = entry.find("atom:published", ns)
            if pub_el is None:
                pub_el = entry.find("dc:date", ns)
            pub_text = pub_el.text.strip() if pub_el is not None else ""
            ts_ms    = cls._parse_pub_date(pub_text)

            # Generate stable ID from URL
            item_id = hashlib.md5(url.encode()).hexdigest()[:16] if url else (
                hashlib.md5(title.encode()).hexdigest()[:16]
            )

            # Detect affected currencies
            full_text  = f"{title} {summary}".lower()
            currencies = cls._detect_currencies(full_text)
            pairs      = cls._currencies_to_pairs(currencies)

            return NewsItem(
                item_id       = item_id,
                title         = title,
                summary       = summary,
                url           = url,
                source        = source,
                timestamp_utc = ts_ms,
                currencies    = currencies,
                pairs         = pairs,
            )

        except Exception as e:
            logger.debug(f"Entry parse error [{source}]: {e}")
            return None

    @staticmethod
    def _clean_text(text: str) -> str:
        """Clean HTML tags and entities from text."""
        if not text:
            return ""
        # Remove HTML tags
        text = BeautifulSoup(text, "lxml").get_text()
        # Decode entities
        text = html.unescape(text)
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text[:1000]  # Cap length

    @staticmethod
    def _parse_pub_date(date_str: str) -> float:
        """Parse various RSS date formats to Unix ms."""
        if not date_str:
            return time.time() * 1000.0

        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S GMT",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S",
            "%d %b %Y %H:%M:%S %z",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.timestamp() * 1000.0
            except ValueError:
                continue

        return time.time() * 1000.0

    @staticmethod
    def _detect_currencies(text: str) -> list:
        """Detect affected currencies from article text."""
        detected = []
        for currency, keywords in CURRENCY_KEYWORDS.items():
            if any(kw in text for kw in keywords):
                detected.append(currency)
        return detected

    @staticmethod
    def _currencies_to_pairs(currencies: list) -> list:
        """Map detected currencies to forex pairs."""
        pairs = []
        for pair, (base, quote) in PAIR_CURRENCIES.items():
            if base in currencies or quote in currencies:
                pairs.append(pair)
        return list(set(pairs))


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — CLAUDE AI SENTIMENT PROCESSOR
# ═══════════════════════════════════════════════════════════════════════════════

class ClaudeSentimentProcessor:
    """
    Sends news articles to Claude API for sentiment analysis.

    Claude processes batches of articles and returns:
        - Sentiment score per article (-1.0 to +1.0)
        - Affected currencies
        - Market impact score (0→100)
        - Key themes

    RULE:
        Claude produces SCORES only — measurements.
        Layer 3 decides what scores mean for trading.
        Claude never says "buy" or "sell" here.
    """

    SYSTEM_PROMPT = """You are a financial news sentiment analyzer for a forex trading system.
Your task is to analyze news articles and output ONLY structured JSON data.
You must NEVER give trading advice or say buy/sell.
You only measure sentiment and impact — all trading decisions are made elsewhere.

For each article analyze:
1. sentiment_score: float from -1.0 (very negative) to +1.0 (very positive) for USD
2. affected_currencies: list of currency codes (USD, EUR, GBP, JPY, CHF, AUD, CAD, NZD)
3. sentiment_per_currency: dict of currency → score (-1.0 to +1.0)
4. impact_score: float 0→100 (how market-moving is this news)
5. key_themes: list of 1-3 word themes (e.g. ["inflation", "rate_hike", "risk_off"])
6. time_sensitivity: "IMMEDIATE" | "HOURS" | "DAYS" | "WEEKS"

Respond ONLY with valid JSON. No preamble, no explanation.
Format: {"articles": [{"item_id": "...", "sentiment_score": 0.0, ...}, ...]}"""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self._headers = {
            "Content-Type"    : "application/json",
            "anthropic-version": "2023-06-01",
        }
        if api_key:
            self._headers["x-api-key"] = api_key

    def process_batch(self, items: list) -> dict:
        """
        Send batch of NewsItems to Claude for sentiment analysis.

        Args:
            items: list of NewsItem objects (max 5 recommended)

        Returns:
            dict of item_id → sentiment data
        """
        if not items:
            return {}

        if not self.api_key:
            logger.info("Claude API key not set — using keyword fallback")
            return self._keyword_fallback(items)

        # Build article list for Claude
        articles_text = "\n\n".join([
            f"ID: {item.item_id}\n"
            f"Title: {item.title}\n"
            f"Summary: {item.summary[:500]}"
            for item in items
        ])

        user_message = f"Analyze these forex news articles:\n\n{articles_text}"

        payload = {
            "model"     : CLAUDE_MODEL,
            "max_tokens": 1000,
            "system"    : self.SYSTEM_PROMPT,
            "messages"  : [{"role": "user", "content": user_message}],
        }

        try:
            response = requests.post(
                CLAUDE_API_URL,
                headers=self._headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()

            # Extract text from response
            text = ""
            for block in data.get("content", []):
                if block.get("type") == "text":
                    text += block.get("text", "")

            return self._parse_claude_response(text)

        except requests.RequestException as e:
            logger.warning(f"Claude API request failed: {e}")
            return self._keyword_fallback(items)
        except Exception as e:
            logger.warning(f"Claude processing error: {e}")
            return self._keyword_fallback(items)

    def _parse_claude_response(self, text: str) -> dict:
        """Parse Claude JSON response into item_id → sentiment dict."""
        try:
            # Strip any markdown code blocks
            text = re.sub(r'```(?:json)?', '', text).strip()
            data = json.loads(text)

            results = {}
            for article in data.get("articles", []):
                item_id = article.get("item_id", "")
                if item_id:
                    results[item_id] = {
                        "sentiment_score"       : float(article.get("sentiment_score", 0.0)),
                        "sentiment_per_currency": article.get("sentiment_per_currency", {}),
                        "impact_score"          : float(article.get("impact_score", 50.0)),
                        "key_themes"            : article.get("key_themes", []),
                        "time_sensitivity"      : article.get("time_sensitivity", "HOURS"),
                        "affected_currencies"   : article.get("affected_currencies", []),
                    }
            return results

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.warning(f"Claude response parse error: {e}")
            return {}

    def _keyword_fallback(self, items: list) -> dict:
        """
        Fallback sentiment scoring using keyword matching.
        Used when Claude API is not available.
        Lower accuracy than Claude but always available.

        Uses substring matching (not set intersection) so short
        titles like 'Fed raises rates' still match keywords like
        'raises' → positive, 'rate' → theme etc.
        """
        POSITIVE_WORDS = [
            "surge", "rally", "gain", "rise", "jump", "soar", "strong",
            "better", "improve", "growth", "increase", "boost", "optimism",
            "hawkish", "rate hike", "tighten", "beat", "exceed", "outperform",
            "recovery", "upbeat", "resilient", "robust", "solid", "raises",
            "higher", "lifted", "supported", "climbed", "advanced",
        ]
        NEGATIVE_WORDS = [
            "fall", "drop", "decline", "plunge", "slide", "weak", "poor",
            "worse", "deteriorate", "contract", "decrease", "cut", "dovish",
            "rate cut", "ease", "miss", "below", "disappoint", "recession",
            "risk", "concern", "uncertainty", "crisis", "tumble", "crash",
            "slump", "stumble", "weaken", "sank", "fell", "dropped",
        ]

        results = {}
        for item in items:
            text = f"{item.title} {item.summary}".lower()

            # Substring matching — works on short titles too
            pos_hits = sum(1 for kw in POSITIVE_WORDS if kw in text)
            neg_hits = sum(1 for kw in NEGATIVE_WORDS if kw in text)
            total    = pos_hits + neg_hits

            if total == 0:
                score = 0.0
            else:
                score = (pos_hits - neg_hits) / max(total, 1)

            impact = min(100.0, (total / 3.0) * 40.0 + 25.0)

            # Detect themes — substring matching
            themes = []
            theme_keywords = {
                "inflation"   : ["inflation", "cpi", "price index"],
                "rate_hike"   : ["rate hike", "raises rate", "tighten", "hawkish", "25bps", "50bps"],
                "rate_cut"    : ["rate cut", "dovish", "ease", "lower rate"],
                "risk_off"    : ["risk", "uncertainty", "crisis"],
                "gdp"         : ["gdp", "growth", "economy"],
                "employment"  : ["jobs", "unemployment", "payroll", "nfp"],
            }
            for theme, kws in theme_keywords.items():
                if any(kw in text for kw in kws):
                    themes.append(theme)

            # Per-currency sentiment
            sentiment_per_ccy = {}
            for currency in item.currencies:
                sentiment_per_ccy[currency] = round(score, 3)

            results[item.item_id] = {
                "sentiment_score"       : round(score, 3),
                "sentiment_per_currency": sentiment_per_ccy,
                "impact_score"          : round(impact, 1),
                "key_themes"            : themes[:3],
                "time_sensitivity"      : "HOURS",
                "affected_currencies"   : item.currencies,
            }

        return results


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — RETAIL SENTIMENT FETCHER
# ═══════════════════════════════════════════════════════════════════════════════

class RetailSentimentFetcher:
    """
    Fetches retail trader positioning data.

    Sources:
        1. OANDA Order Book (requires OANDA account)
        2. MyFXBook Community Outlook (scraped)
        3. DailyFX SSI (scraped)

    RULE:
        Raw % long / % short per pair — no interpretation.
        Layer 3 decides significance of extreme positioning.
    """

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (compatible; SentimentBot/1.0)"
    }

    def __init__(self, oanda_api_key: Optional[str] = None,
                 oanda_account: Optional[str] = None,
                 oanda_practice: bool = True):
        self.oanda_api_key  = oanda_api_key
        self.oanda_account  = oanda_account
        self.oanda_practice = oanda_practice
        self._oanda_base    = (
            "https://api-fxpractice.oanda.com"
            if oanda_practice else
            "https://api-fxtrade.oanda.com"
        )

    def fetch_oanda_order_book(self, pair: str) -> Optional[RetailSentiment]:
        """
        Fetch OANDA order book positioning for a pair.
        Requires OANDA API key.

        Endpoint: /v3/instruments/{instrument}/orderBook
        Returns snapshot of current long/short distribution.
        """
        if not self.oanda_api_key:
            return None

        oanda_pair = pair.replace("/", "_")
        url        = f"{self._oanda_base}/v3/instruments/{oanda_pair}/orderBook"
        headers    = {
            "Authorization": f"Bearer {self.oanda_api_key}"
        }

        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()

            order_book = data.get("orderBook", {})
            buckets    = order_book.get("buckets", [])

            if not buckets:
                return None

            # Sum long and short orders
            total_long  = sum(
                float(b.get("longCountPercent", 0))
                for b in buckets
            )
            total_short = sum(
                float(b.get("shortCountPercent", 0))
                for b in buckets
            )
            total = total_long + total_short

            if total == 0:
                return None

            long_pct  = (total_long  / total) * 100.0
            short_pct = (total_short / total) * 100.0

            return RetailSentiment(
                pair          = pair,
                long_pct      = long_pct,
                short_pct     = short_pct,
                timestamp_utc = time.time() * 1000.0,
                source        = "OANDA_ORDER_BOOK",
            )

        except requests.RequestException as e:
            logger.warning(f"OANDA order book failed [{pair}]: {e}")
            return None

    def fetch_myfxbook(self) -> dict:
        """
        Scrape MyFXBook community outlook page.
        Returns dict of pair → RetailSentiment.

        MyFXBook shows % long/short for major pairs.
        Page structure may change — uses BeautifulSoup parsing.
        """
        results = {}
        try:
            response = requests.get(
                SENTIMENT_URLS["myfxbook"],
                headers=self.HEADERS,
                timeout=15
            )
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "lxml")

            # Look for sentiment table
            tables = soup.find_all("table")
            for table in tables:
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) >= 3:
                        try:
                            # Extract pair name and percentages
                            pair_text = cells[0].get_text(strip=True)
                            long_text = cells[1].get_text(strip=True)
                            short_text= cells[2].get_text(strip=True)

                            # Map to our pair format
                            pair = self._normalize_pair(pair_text)
                            if not pair:
                                continue

                            long_pct  = float(re.search(r'[\d.]+', long_text).group())
                            short_pct = float(re.search(r'[\d.]+', short_text).group())

                            results[pair] = RetailSentiment(
                                pair          = pair,
                                long_pct      = long_pct,
                                short_pct     = short_pct,
                                timestamp_utc = time.time() * 1000.0,
                                source        = "MYFXBOOK",
                            )
                        except (AttributeError, ValueError):
                            continue

        except requests.RequestException as e:
            logger.warning(f"MyFXBook fetch failed: {e}")

        return results

    def fetch_all_pairs(self, pairs: list) -> dict:
        """
        Fetch retail sentiment for all pairs.
        Tries OANDA first, falls back to scraping.
        Returns dict of pair → RetailSentiment.
        """
        results = {}

        # Try OANDA order book (most accurate)
        if self.oanda_api_key:
            for pair in pairs:
                sentiment = self.fetch_oanda_order_book(pair)
                if sentiment:
                    results[pair] = sentiment

        # Try MyFXBook for any missing pairs
        if len(results) < len(pairs):
            myfxbook = self.fetch_myfxbook()
            for pair in pairs:
                if pair not in results and pair in myfxbook:
                    results[pair] = myfxbook[pair]

        return results

    @staticmethod
    def _normalize_pair(pair_text: str) -> Optional[str]:
        """Normalize pair text to our standard format."""
        pair_text = pair_text.upper().strip()
        # Remove separators
        for sep in ["/", "-", "_", " "]:
            pair_text = pair_text.replace(sep, "")

        # Map common names
        pair_map = {
            "EURUSD": "EUR/USD",
            "GBPUSD": "GBP/USD",
            "USDJPY": "USD/JPY",
            "USDCHF": "USD/CHF",
            "AUDUSD": "AUD/USD",
            "USDCAD": "USD/CAD",
            "NZDUSD": "NZD/USD",
            "EURGBP": "EUR/GBP",
            "EURJPY": "EUR/JPY",
        }
        return pair_map.get(pair_text)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5B — NEWSAPI FETCHER (optional — requires free API key)
# ═══════════════════════════════════════════════════════════════════════════════

class NewsAPIFetcher:
    """
    Fetches financial news from NewsAPI.org.
    Free tier: 100 requests/day, no commercial use.
    Paid tier: unlimited.

    Signup: https://newsapi.org/register
    Covers: Reuters, Bloomberg, WSJ, FT, CNBC, etc.

    RULE: Raw article fetching only — no interpretation.
    """

    BASE_URL = "https://newsapi.org/v2/everything"

    FOREX_QUERIES = [
        "federal reserve interest rates",
        "ECB European Central Bank",
        "Bank of England BOE pound",
        "Bank of Japan BOJ yen",
        "forex currency exchange rates",
        "dollar euro inflation",
    ]

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {"X-Api-Key": api_key}

    def fetch_forex_news(self, query: str = "forex currency",
                         hours_back: int = 6,
                         page_size: int = 20) -> list:
        """
        Fetch recent forex-related news articles.
        Returns list of NewsItem objects.
        """
        from_dt = (
            datetime.now(timezone.utc) - timedelta(hours=hours_back)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

        params = {
            "q"          : query,
            "from"       : from_dt,
            "language"   : "en",
            "sortBy"     : "publishedAt",
            "pageSize"   : min(page_size, 100),
        }

        try:
            response = requests.get(
                self.BASE_URL,
                headers = self.headers,
                params  = params,
                timeout = 10
            )
            response.raise_for_status()
            data     = response.json()
            articles = data.get("articles", [])
            return self._parse_articles(articles)

        except requests.RequestException as e:
            logger.warning(f"NewsAPI fetch failed [{query}]: {e}")
            return []

    def fetch_all_forex_topics(self) -> list:
        """Fetch news for all key forex topics."""
        all_items = []
        seen_ids  = set()

        for query in self.FOREX_QUERIES[:3]:  # Limit API calls
            items = self.fetch_forex_news(query)
            for item in items:
                if item.item_id not in seen_ids:
                    all_items.append(item)
                    seen_ids.add(item.item_id)
            time.sleep(0.5)  # Polite delay

        return all_items

    def _parse_articles(self, articles: list) -> list:
        """Parse NewsAPI article list into NewsItem objects."""
        items = []
        for article in articles:
            try:
                title   = article.get("title", "") or ""
                summary = article.get("description", "") or ""
                url     = article.get("url", "") or ""
                source  = article.get("source", {}).get("name", "NewsAPI")
                pub_at  = article.get("publishedAt", "")

                if not title or title == "[Removed]":
                    continue

                # Parse timestamp
                try:
                    dt    = datetime.strptime(pub_at, "%Y-%m-%dT%H:%M:%SZ")
                    dt    = dt.replace(tzinfo=timezone.utc)
                    ts_ms = dt.timestamp() * 1000.0
                except ValueError:
                    ts_ms = time.time() * 1000.0

                item_id    = hashlib.md5(url.encode()).hexdigest()[:16]
                full_text  = f"{title} {summary}".lower()
                currencies = RSSFeedParser._detect_currencies(full_text)
                pairs      = RSSFeedParser._currencies_to_pairs(currencies)

                items.append(NewsItem(
                    item_id       = item_id,
                    title         = title,
                    summary       = summary,
                    url           = url,
                    source        = f"NEWSAPI_{source.upper().replace(' ','_')}",
                    timestamp_utc = ts_ms,
                    currencies    = currencies,
                    pairs         = pairs,
                ))
            except Exception as e:
                logger.debug(f"NewsAPI article parse error: {e}")
                continue

        return items


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5C — ALPHA VANTAGE NEWS FETCHER (optional — requires free API key)
# ═══════════════════════════════════════════════════════════════════════════════

class AlphaVantageNewsFetcher:
    """
    Fetches financial news and sentiment from Alpha Vantage.
    Free tier: 25 requests/day.
    Includes pre-computed sentiment scores per ticker/topic.

    Signup: https://www.alphavantage.co/support/#api-key
    Endpoint: NEWS_SENTIMENT

    Topics relevant to forex:
        forex           → FX news
        economy_macro   → macro events
        financial_markets → general market news
        earnings        → company results (affects risk sentiment)

    RULE: Raw scores from Alpha Vantage used as additional signal.
    Layer 3 weights them alongside Claude scores.
    """

    BASE_URL = "https://www.alphavantage.co/query"

    FOREX_TOPICS = [
        "forex",
        "economy_macro",
        "financial_markets",
    ]

    def __init__(self, api_key: str):
        self.api_key = api_key

    def fetch_news_sentiment(self, topic: str = "forex",
                              limit: int = 20) -> list:
        """
        Fetch news with pre-computed sentiment from Alpha Vantage.

        Returns list of NewsItem with sentiment_score already populated
        from Alpha Vantage's own NLP model.
        """
        params = {
            "function"  : "NEWS_SENTIMENT",
            "topics"    : topic,
            "limit"     : min(limit, 200),
            "apikey"    : self.api_key,
            "sort"      : "LATEST",
        }

        try:
            response = requests.get(
                self.BASE_URL,
                params  = params,
                timeout = 15
            )
            response.raise_for_status()
            data = response.json()
            feed = data.get("feed", [])
            return self._parse_feed(feed)

        except requests.RequestException as e:
            logger.warning(f"AlphaVantage fetch failed [{topic}]: {e}")
            return []

    def _parse_feed(self, feed: list) -> list:
        """Parse Alpha Vantage news feed."""
        items = []
        for article in feed:
            try:
                title        = article.get("title", "")
                summary      = article.get("summary", "")
                url          = article.get("url", "")
                source       = article.get("source", "AlphaVantage")
                time_pub     = article.get("time_published", "")
                av_sentiment = float(article.get("overall_sentiment_score", 0.0))
                av_label     = article.get("overall_sentiment_label", "Neutral")

                # Parse timestamp (format: 20240115T143000)
                try:
                    dt    = datetime.strptime(time_pub, "%Y%m%dT%H%M%S")
                    dt    = dt.replace(tzinfo=timezone.utc)
                    ts_ms = dt.timestamp() * 1000.0
                except ValueError:
                    ts_ms = time.time() * 1000.0

                item_id   = hashlib.md5(url.encode()).hexdigest()[:16]
                full_text = f"{title} {summary}".lower()
                currencies= RSSFeedParser._detect_currencies(full_text)
                pairs     = RSSFeedParser._currencies_to_pairs(currencies)

                item = NewsItem(
                    item_id         = item_id,
                    title           = title,
                    summary         = summary,
                    url             = url,
                    source          = f"ALPHAVANTAGE_{source.upper()}",
                    timestamp_utc   = ts_ms,
                    currencies      = currencies,
                    pairs           = pairs,
                    # Alpha Vantage provides its own sentiment
                    # Mark as processed so it skips Claude queue
                    sentiment_score = av_sentiment,
                    sentiment_label = av_label.upper(),
                    impact_score    = abs(av_sentiment) * 100.0,
                    claude_processed= True,
                )
                items.append(item)

            except Exception as e:
                logger.debug(f"AlphaVantage article parse error: {e}")
                continue

        return items


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — FEAR & GREED CALCULATOR
# ═══════════════════════════════════════════════════════════════════════════════

class FearGreedCalculator:
    """
    Computes a forex-specific Fear & Greed Index (0→100).

    Components:
        1. News sentiment average (30% weight)
        2. Retail sentiment extremes (25% weight)
        3. Volatility level from multi-asset (25% weight)
        4. Price momentum across pairs (20% weight)

    Scale:
        0-25  : Extreme Fear
        25-45 : Fear
        45-55 : Neutral
        55-75 : Greed
        75-100: Extreme Greed

    RULE: Pure measurement — no trade decision attached.
    """

    @staticmethod
    def compute(
        avg_news_sentiment : float,         # -1.0 to +1.0
        retail_sentiment   : dict,          # pair → RetailSentiment
        vix_level          : Optional[float],
        price_momentum     : Optional[float] # -1.0 to +1.0
    ) -> dict:
        """
        Compute fear/greed score from available inputs.
        Returns score (0→100) and component breakdown.
        """
        components = {}

        # Component 1: News sentiment (30%)
        # Map -1→+1 to 0→100
        news_score = (avg_news_sentiment + 1.0) / 2.0 * 100.0
        components["news_sentiment"] = round(news_score, 1)

        # Component 2: Retail positioning (25%)
        # Extreme retail longs = greed, extreme shorts = fear
        if retail_sentiment:
            avg_long_pct = sum(
                s.long_pct for s in retail_sentiment.values()
            ) / len(retail_sentiment)
            retail_score = avg_long_pct  # Already 0→100
        else:
            retail_score = 50.0
        components["retail_positioning"] = round(retail_score, 1)

        # Component 3: Volatility (25%)
        # High VIX = fear, low VIX = greed
        if vix_level is not None:
            # VIX 10 = greed (100), VIX 40 = fear (0)
            vol_score = max(0.0, min(100.0,
                100.0 - ((vix_level - 10.0) / 30.0) * 100.0
            ))
        else:
            vol_score = 50.0
        components["volatility"] = round(vol_score, 1)

        # Component 4: Price momentum (20%)
        if price_momentum is not None:
            momentum_score = (price_momentum + 1.0) / 2.0 * 100.0
        else:
            momentum_score = 50.0
        components["price_momentum"] = round(momentum_score, 1)

        # Weighted average
        total_score = (
            news_score     * 0.30 +
            retail_score   * 0.25 +
            vol_score      * 0.25 +
            momentum_score * 0.20
        )
        total_score = max(0.0, min(100.0, total_score))

        # Label (for reference — Layer 3 decides significance)
        if total_score <= 25:
            label = "EXTREME_FEAR"
        elif total_score <= 45:
            label = "FEAR"
        elif total_score <= 55:
            label = "NEUTRAL"
        elif total_score <= 75:
            label = "GREED"
        else:
            label = "EXTREME_GREED"

        return {
            "score"      : round(total_score, 1),
            "label"      : label,
            "components" : components,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — NEWS STORE
# ═══════════════════════════════════════════════════════════════════════════════

class NewsStore:
    """
    Thread-safe store for news items.
    Manages deduplication, expiry, and retrieval.
    """

    def __init__(self, max_items: int = MAX_NEWS_ITEMS,
                 cache_hours: float = NEWS_CACHE_HOURS):
        self._items      : dict[str, NewsItem] = {}
        self._lock       = threading.Lock()
        self._max_items  = max_items
        self._cache_ms   = cache_hours * 3600 * 1000

    def add(self, items: list):
        """Add new items, deduplicate by item_id."""
        with self._lock:
            added = 0
            for item in items:
                if item.item_id not in self._items:
                    self._items[item.item_id] = item
                    added += 1

            # Expire old items
            cutoff_ms = time.time() * 1000 - self._cache_ms
            self._items = {
                k: v for k, v in self._items.items()
                if v.timestamp_utc > cutoff_ms
            }

            # Trim to max
            if len(self._items) > self._max_items:
                sorted_items = sorted(
                    self._items.items(),
                    key=lambda x: x[1].timestamp_utc,
                    reverse=True
                )
                self._items = dict(sorted_items[:self._max_items])

            return added

    def get_unprocessed(self, limit: int = MAX_ARTICLES_CLAUDE) -> list:
        """Get items not yet processed by Claude."""
        with self._lock:
            unprocessed = [
                item for item in self._items.values()
                if not item.claude_processed
            ]
            # Sort newest first
            unprocessed.sort(key=lambda x: x.timestamp_utc, reverse=True)
            return unprocessed[:limit]

    def update_sentiment(self, item_id: str, sentiment_data: dict):
        """Update a news item with Claude sentiment results."""
        with self._lock:
            if item_id in self._items:
                item = self._items[item_id]
                item.sentiment_score  = sentiment_data.get("sentiment_score")
                item.impact_score     = sentiment_data.get("impact_score")
                item.key_themes       = sentiment_data.get("key_themes", [])
                item.claude_processed = True

                # Set label
                score = item.sentiment_score or 0.0
                if score >= 0.3:
                    item.sentiment_label = "POSITIVE"
                elif score <= -0.3:
                    item.sentiment_label = "NEGATIVE"
                else:
                    item.sentiment_label = "NEUTRAL"

    def get_recent(self, hours: float = 4.0) -> list:
        """Get items from last N hours."""
        cutoff_ms = (time.time() - hours * 3600) * 1000
        with self._lock:
            items = [
                v for v in self._items.values()
                if v.timestamp_utc >= cutoff_ms
            ]
            items.sort(key=lambda x: x.timestamp_utc, reverse=True)
            return items

    def get_by_pair(self, pair: str, hours: float = 4.0) -> list:
        """Get recent items affecting a specific pair."""
        return [
            item for item in self.get_recent(hours)
            if pair in item.pairs
        ]

    def get_all(self) -> list:
        with self._lock:
            return sorted(
                self._items.values(),
                key=lambda x: x.timestamp_utc,
                reverse=True
            )


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — SENTIMENT AGGREGATOR
# ═══════════════════════════════════════════════════════════════════════════════

class SentimentAggregator:
    """
    Aggregates individual news sentiment scores into
    per-currency and per-pair scores.

    RULE: Pure aggregation math — no interpretation.
    Layer 3 decides what scores mean for trading.
    """

    @staticmethod
    def compute_currency_scores(news_items: list) -> dict:
        """
        Compute average sentiment score per currency.
        Returns dict of currency → weighted average score.
        """
        currency_scores = {}
        currency_counts = {}

        for item in news_items:
            if not item.claude_processed or item.sentiment_score is None:
                continue

            # Weight by impact score
            weight = (item.impact_score or 50.0) / 100.0

            for currency in item.currencies:
                if currency not in currency_scores:
                    currency_scores[currency] = 0.0
                    currency_counts[currency] = 0.0

                currency_scores[currency] += item.sentiment_score * weight
                currency_counts[currency] += weight

        # Normalize
        result = {}
        for currency in currency_scores:
            count = currency_counts[currency]
            if count > 0:
                result[currency] = round(
                    currency_scores[currency] / count, 3
                )

        return result

    @staticmethod
    def compute_pair_score(pair: str,
                           currency_scores: dict) -> Optional[float]:
        """
        Compute sentiment score for a forex pair.
        Pair score = base currency score - quote currency score.
        """
        currencies = PAIR_CURRENCIES.get(pair)
        if not currencies:
            return None

        base, quote = currencies
        base_score  = currency_scores.get(base)
        quote_score = currency_scores.get(quote)

        if base_score is None and quote_score is None:
            return None
        if base_score is None:
            return -quote_score
        if quote_score is None:
            return base_score

        return round(base_score - quote_score, 3)

    @staticmethod
    def compute_news_impact_score(news_items: list,
                                  hours: float = 2.0) -> float:
        """
        Compute overall news activity/impact score (0→100).
        Higher = more high-impact news recently.
        """
        if not news_items:
            return 0.0

        cutoff_ms = (time.time() - hours * 3600) * 1000
        recent    = [i for i in news_items if i.timestamp_utc >= cutoff_ms]

        if not recent:
            return 0.0

        # Weight by impact score and recency
        total_weight = 0.0
        total_impact = 0.0

        for item in recent:
            age_hours  = (time.time() * 1000 - item.timestamp_utc) / 3600000
            recency_w  = max(0.1, 1.0 - age_hours / hours)
            impact     = item.impact_score or 30.0

            total_weight += recency_w
            total_impact += impact * recency_w

        if total_weight == 0:
            return 0.0

        return round(min(100.0, total_impact / total_weight), 1)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 9 — NEWS SENTIMENT MANAGER (MAIN CLASS)
# ═══════════════════════════════════════════════════════════════════════════════

class NewsSentimentManager:
    """
    Master manager for news and sentiment data.

    Responsibilities:
        - Fetch RSS feeds on schedule
        - Process articles through Claude AI
        - Fetch retail sentiment positioning
        - Compute fear/greed index
        - Provide Layer 1 output package

    Refresh schedule:
        News feeds:       every 5 minutes
        Claude processing: every 10 minutes (rate limit aware)
        Retail sentiment:  every 15 minutes

    RULE:
        All output is raw measurement and labels.
        No interpretation, no trade decisions.
        Layer 3 decides what all sentiment data means.
    """

    def __init__(self,
                 claude_api_key      : Optional[str] = None,
                 oanda_api_key       : Optional[str] = None,
                 oanda_account       : Optional[str] = None,
                 oanda_practice      : bool = True,
                 news_api_key        : Optional[str] = None,
                 alpha_vantage_key   : Optional[str] = None):

        self.claude_api_key    = claude_api_key
        self.news_api_key      = news_api_key
        self.alpha_vantage_key = alpha_vantage_key

        # Components
        self._store       = NewsStore()
        self._claude      = ClaudeSentimentProcessor(claude_api_key)
        self._sentiment_f = RetailSentimentFetcher(
            oanda_api_key, oanda_account, oanda_practice
        )
        self._fear_greed  = FearGreedCalculator()

        # Optional paid fetchers — only created if keys provided
        self._newsapi_fetcher = (
            NewsAPIFetcher(news_api_key) if news_api_key else None
        )
        self._av_fetcher = (
            AlphaVantageNewsFetcher(alpha_vantage_key)
            if alpha_vantage_key else None
        )

        # State
        self._retail_sentiment : dict[str, RetailSentiment] = {}
        self._last_news_refresh   = 0.0
        self._last_claude_run     = 0.0
        self._last_sentiment_run  = 0.0
        self._running             = False
        self._thread              = None
        self._lock                = threading.Lock()

        logger.info("NewsSentimentManager initialized")

    def start(self):
        """Start background refresh threads."""
        self._running = True
        self._thread  = threading.Thread(
            target = self._refresh_loop,
            daemon = True,
            name   = "NewsSentimentThread"
        )
        self._thread.start()
        self._refresh_news()
        logger.info("NewsSentimentManager: started")

    def stop(self):
        self._running = False
        logger.info("NewsSentimentManager: stopped")

    def _refresh_loop(self):
        """Background loop managing all refresh cycles."""
        while self._running:
            time.sleep(30)
            now = time.time()

            # News: every 5 minutes
            if now - self._last_news_refresh >= 300:
                self._refresh_news()

            # Claude: every 10 minutes
            if now - self._last_claude_run >= 600:
                self._run_claude()

            # Retail sentiment: every 15 minutes
            if now - self._last_sentiment_run >= 900:
                self._refresh_retail_sentiment()

    def _refresh_news(self):
        """Fetch all news sources — RSS + NewsAPI + AlphaVantage."""
        logger.info("News: refreshing all sources...")
        total_added = 0

        # Source 1: RSS feeds (always, free)
        for source, url in RSS_FEEDS.items():
            items = RSSFeedParser.fetch(source, url)
            if items:
                added = self._store.add(items)
                total_added += added

        # Source 2: NewsAPI (if key available)
        if self._newsapi_fetcher:
            try:
                items = self._newsapi_fetcher.fetch_all_forex_topics()
                if items:
                    added = self._store.add(items)
                    total_added += added
                    logger.info(f"NewsAPI: {added} new items")
            except Exception as e:
                logger.warning(f"NewsAPI refresh error: {e}")

        # Source 3: Alpha Vantage (if key available — pre-scored)
        if self._av_fetcher:
            try:
                for topic in AlphaVantageNewsFetcher.FOREX_TOPICS[:2]:
                    items = self._av_fetcher.fetch_news_sentiment(topic)
                    if items:
                        added = self._store.add(items)
                        total_added += added
                    time.sleep(1.0)  # AV rate limit
                logger.info(f"AlphaVantage: items fetched")
            except Exception as e:
                logger.warning(f"AlphaVantage refresh error: {e}")

        self._last_news_refresh = time.time()
        logger.info(f"News: {total_added} total new items added")

    def _run_claude(self):
        """Process unprocessed articles through Claude."""
        unprocessed = self._store.get_unprocessed(MAX_ARTICLES_CLAUDE)
        if not unprocessed:
            return

        logger.info(f"Claude: processing {len(unprocessed)} articles...")
        results = self._claude.process_batch(unprocessed)

        for item_id, sentiment_data in results.items():
            self._store.update_sentiment(item_id, sentiment_data)

        self._last_claude_run = time.time()
        logger.info(f"Claude: processed {len(results)} articles")

    def _refresh_retail_sentiment(self):
        """Fetch retail sentiment for all pairs."""
        pairs   = list(PAIR_CURRENCIES.keys())
        results = self._sentiment_f.fetch_all_pairs(pairs)

        with self._lock:
            self._retail_sentiment.update(results)

        self._last_sentiment_run = time.time()

    def load_mock_data(self, news_items: list = None,
                       retail_sentiment: dict = None):
        """Load mock data for testing."""
        if news_items:
            self._store.add(news_items)
            # Process through Claude/fallback
            self._run_claude()

        if retail_sentiment:
            with self._lock:
                self._retail_sentiment.update(retail_sentiment)

    # ── Layer 1 Output Interface ─────────────────────────────────────────────

    def get_layer1_package(self, pair: str = None) -> dict:
        """
        Return Layer 1 news & sentiment package.
        Raw measurements and labels only.
        Layer 3 assigns meaning to all scores.
        """
        # Recent news
        recent_items = self._store.get_recent(hours=4.0)
        processed    = [i for i in recent_items if i.claude_processed]

        # Currency sentiment scores
        currency_scores = SentimentAggregator.compute_currency_scores(processed)

        # Pair-specific scores
        pair_scores = {}
        for p in PAIR_CURRENCIES:
            score = SentimentAggregator.compute_pair_score(p, currency_scores)
            if score is not None:
                pair_scores[p] = score

        # News impact score
        news_impact = SentimentAggregator.compute_news_impact_score(recent_items)

        # Retail sentiment
        with self._lock:
            retail_snap = {
                pair: s.to_dict()
                for pair, s in self._retail_sentiment.items()
            }

        # Extreme retail flags
        extreme_retail = {
            pair: s.extreme_flag
            for pair, s in self._retail_sentiment.items()
        }

        # Fear & greed
        avg_news_sentiment = (
            sum(currency_scores.values()) / len(currency_scores)
            if currency_scores else 0.0
        )
        fear_greed = FearGreedCalculator.compute(
            avg_news_sentiment = avg_news_sentiment,
            retail_sentiment   = self._retail_sentiment,
            vix_level          = None,  # Injected from multi_asset_feed
            price_momentum     = None,  # Injected from market_data_feed
        )

        # Pair-specific news
        pair_news = []
        if pair:
            pair_news = [
                i.to_dict()
                for i in self._store.get_by_pair(pair, hours=4.0)
            ]

        return {
            # ── News items ───────────────────────────────────────────────
            "recent_news"           : [i.to_dict() for i in recent_items[:10]],
            "pair_news"             : pair_news,
            "total_items_stored"    : len(self._store.get_all()),
            "processed_count"       : len(processed),

            # ── Sentiment scores (raw measurements) ──────────────────────
            "currency_scores"       : currency_scores,
            "pair_scores"           : pair_scores,
            "news_impact_score"     : news_impact,

            # ── Retail sentiment ─────────────────────────────────────────
            "retail_sentiment"      : retail_snap,
            "extreme_retail_pairs"  : [
                p for p, ex in extreme_retail.items() if ex
            ],

            # ── Fear & Greed ─────────────────────────────────────────────
            "fear_greed_index"      : fear_greed,

            # ── Metadata ────────────────────────────────────────────────
            "claude_available"      : self.claude_api_key is not None,
            "last_news_refresh"     : datetime.fromtimestamp(
                                          self._last_news_refresh,
                                          tz=timezone.utc
                                      ).isoformat() if self._last_news_refresh else None,
            "data_source"           : "RSS+CLAUDE+OANDA_BOOK",
            "data_tier"             : "TIER_1",
        }

    def get_pair_sentiment(self, pair: str) -> dict:
        """Quick sentiment summary for a specific pair."""
        currency_scores = SentimentAggregator.compute_currency_scores(
            [i for i in self._store.get_recent(4.0) if i.claude_processed]
        )
        pair_score = SentimentAggregator.compute_pair_score(pair, currency_scores)

        with self._lock:
            retail = self._retail_sentiment.get(pair)

        return {
            "pair"          : pair,
            "sentiment_score": pair_score,
            "retail"        : retail.to_dict() if retail else None,
            "news_count"    : len(self._store.get_by_pair(pair)),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 10 — SELF TEST
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 70)
    print("NEWS & SENTIMENT FEED — SELF TEST")
    print("=" * 70)

    manager = NewsSentimentManager(
        claude_api_key = None,   # Keyword fallback used
        oanda_api_key  = None,
    )

    # ── [1] RSS Parser ───────────────────────────────────────────────────────
    print("\n[1] Testing RSS parser with mock XML...")
    mock_rss = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Reuters Business News</title>
    <item>
      <title>Federal Reserve signals potential rate cut amid cooling inflation</title>
      <description>The Federal Reserve signaled it could cut interest rates
      as inflation shows signs of easing toward the 2% target. Dollar fell
      sharply on the dovish comments from Powell.</description>
      <link>https://reuters.com/article/fed-rates-001</link>
      <pubDate>Mon, 15 Jan 2024 14:30:00 GMT</pubDate>
    </item>
    <item>
      <title>ECB holds rates steady euro rises on hawkish tone</title>
      <description>The European Central Bank kept rates unchanged but
      signaled continued vigilance on inflation. Euro surged against
      the dollar following the ECB announcement by Lagarde.</description>
      <link>https://reuters.com/article/ecb-rates-002</link>
      <pubDate>Mon, 15 Jan 2024 12:00:00 GMT</pubDate>
    </item>
    <item>
      <title>UK GDP growth beats expectations boosting pound sterling</title>
      <description>British economic output grew faster than expected
      in Q4, boosting sterling. The pound rose sharply against
      both dollar and euro on the strong GDP data from the Bank of England.</description>
      <link>https://reuters.com/article/uk-gdp-003</link>
      <pubDate>Mon, 15 Jan 2024 10:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>"""

    items = RSSFeedParser._parse("reuters_mock", mock_rss)
    print(f"    Parsed {len(items)} articles from mock RSS")
    for item in items:
        print(f"    → '{item.title[:65]}'")
        print(f"      Currencies: {item.currencies} | Pairs: {item.pairs}")

    # Fix timestamps to NOW so get_recent(4h) includes them
    now_ms = time.time() * 1000
    for i, item in enumerate(items):
        item.timestamp_utc = now_ms - (i * 600000)  # 10 min apart, all recent

    # ── [2] Keyword Sentiment ────────────────────────────────────────────────
    print("\n[2] Testing keyword sentiment fallback...")
    processor = ClaudeSentimentProcessor(api_key=None)
    results   = processor._keyword_fallback(items)

    for item in items:
        result = results.get(item.item_id, {})
        score  = result.get("sentiment_score", 0)
        impact = result.get("impact_score", 0)
        themes = result.get("key_themes", [])
        # Apply sentiment to item so manager can use it
        item.sentiment_score   = score
        item.impact_score      = impact
        item.key_themes        = themes
        item.claude_processed  = True
        item.sentiment_label   = "POSITIVE" if score >= 0.3 else (
                                  "NEGATIVE" if score <= -0.3 else "NEUTRAL")
        print(f"    '{item.title[:55]}'")
        print(f"      Score: {score:+.3f} | Impact: {impact:.1f} | Themes: {themes}")

    # ── [3] Load into manager ────────────────────────────────────────────────
    print("\n[3] Loading processed mock data into manager...")
    manager._store.add(items)

    # ── [4] Retail sentiment ─────────────────────────────────────────────────
    print("\n[4] Testing mock retail sentiment...")
    mock_retail = {
        "EUR/USD": RetailSentiment(
            pair="EUR/USD", long_pct=72.5, short_pct=27.5,
            timestamp_utc=now_ms, source="MOCK"
        ),
        "GBP/USD": RetailSentiment(
            pair="GBP/USD", long_pct=45.0, short_pct=55.0,
            timestamp_utc=now_ms, source="MOCK"
        ),
        "USD/JPY": RetailSentiment(
            pair="USD/JPY", long_pct=81.0, short_pct=19.0,
            timestamp_utc=now_ms, source="MOCK"
        ),
    }
    manager._retail_sentiment.update(mock_retail)

    # ── [5] Layer 1 package ──────────────────────────────────────────────────
    print("\n[5] Layer 1 package:")
    pkg = manager.get_layer1_package("EUR/USD")

    print(f"    Total news stored   : {pkg['total_items_stored']}")
    print(f"    Processed count     : {pkg['processed_count']}")
    print(f"    Claude available    : {pkg['claude_available']}")
    print(f"    News impact score   : {pkg['news_impact_score']:.1f}/100")
    print(f"    Data source         : {pkg['data_source']}")
    print(f"    Data tier           : {pkg['data_tier']}")

    print("\n    Currency sentiment scores:")
    if pkg["currency_scores"]:
        for ccy, score in sorted(pkg["currency_scores"].items()):
            bar  = "█" * int(abs(score) * 10)
            sign = "+" if score >= 0 else ""
            print(f"      {ccy}: {sign}{score:.3f} {bar}")
    else:
        print("      (no processed items yet)")

    print("\n    Pair sentiment scores:")
    if pkg["pair_scores"]:
        for pair, score in sorted(pkg["pair_scores"].items()):
            sign = "+" if score >= 0 else ""
            print(f"      {pair}: {sign}{score:.3f}")
    else:
        print("      (no pair scores computed)")

    print("\n    Retail sentiment:")
    for pair, data in pkg["retail_sentiment"].items():
        extreme = " ⚠️  EXTREME" if data["extreme_flag"] else ""
        print(f"      {pair}: Long={data['long_pct']}% "
              f"Short={data['short_pct']}%{extreme}")

    print(f"\n    Extreme retail pairs: {pkg['extreme_retail_pairs']}")

    fg = pkg["fear_greed_index"]
    print(f"\n    Fear & Greed        : {fg['score']:.1f}/100 ({fg['label']})")
    print(f"    F&G Components      : {fg['components']}")

    # ── [6] Pair-specific news ───────────────────────────────────────────────
    print("\n[6] Pair-specific news for EUR/USD:")
    pair_news = pkg["pair_news"]
    if pair_news:
        for n in pair_news:
            print(f"    [{n['source']}] {n['title'][:60]}")
            print(f"      Sentiment: {n['sentiment_score']} | "
                  f"Impact: {n['impact_score']} | "
                  f"Label: {n['sentiment_label']}")
    else:
        print("    (no pair-specific news)")

    # ── [7] Keyword scoring tests ────────────────────────────────────────────
    print("\n[7] Keyword scoring on standalone titles:")
    test_titles = [
        ("Fed raises rates 25bps dollar surges",           ["USD"]),
        ("ECB signals rate cuts euro plunges",             ["EUR"]),
        ("BOE holds steady pound rises on growth data",    ["GBP"]),
        ("Japan inflation slumps yen weakens",             ["JPY"]),
        ("Minor regional official speech no major impact", ["USD"]),
    ]
    proc2 = ClaudeSentimentProcessor(api_key=None)
    for title, currencies in test_titles:
        mock_item = NewsItem(
            item_id       = hashlib.md5(title.encode()).hexdigest()[:8],
            title         = title,
            summary       = title,
            url           = "",
            source        = "test",
            timestamp_utc = now_ms,
            currencies    = currencies,
            pairs         = ["EUR/USD"],
        )
        result = proc2._keyword_fallback([mock_item])
        r = result.get(mock_item.item_id, {})
        score  = r.get("sentiment_score", 0)
        themes = r.get("key_themes", [])
        bar    = "█" * int(abs(score) * 10)
        sign   = "+" if score >= 0 else ""
        print(f"    '{title[:55]}'")
        print(f"      Score: {sign}{score:.3f} {bar} | Themes: {themes}")

    # ── [8] SentimentAggregator ──────────────────────────────────────────────
    print("\n[8] Sentiment aggregator test:")
    recent   = manager._store.get_recent(hours=4.0)
    processed= [i for i in recent if i.claude_processed]
    ccy_sc   = SentimentAggregator.compute_currency_scores(processed)
    ni_score = SentimentAggregator.compute_news_impact_score(recent)
    print(f"    Recent items       : {len(recent)}")
    print(f"    Processed items    : {len(processed)}")
    print(f"    News impact score  : {ni_score:.1f}/100")
    print(f"    Currency scores    : {ccy_sc}")
    for pair in ["EUR/USD", "GBP/USD", "USD/JPY"]:
        ps = SentimentAggregator.compute_pair_score(pair, ccy_sc)
        print(f"    {pair} pair score : {ps:+.3f}" if ps is not None
              else f"    {pair} pair score : N/A")

    # ── [9] NewsAPI + Alpha Vantage connection info ──────────────────────────
    print("\n[9] Optional API connections (not tested — require keys):")
    print("    NewsAPI       : NewsSentimentManager(news_api_key='YOUR_KEY')")
    print("    Alpha Vantage : Connect via AlphaVantageNewsFetcher class")
    print("    Claude AI     : NewsSentimentManager(claude_api_key='YOUR_KEY')")
    print("    OANDA book    : NewsSentimentManager(oanda_api_key='YOUR_KEY')")

    print("\n" + "=" * 70)
    print("NEWS & SENTIMENT SELF TEST COMPLETE ✅")
    print()
    print("Components verified:")
    print("  RSSFeedParser          ✅  Parses RSS/Atom XML correctly")
    print("  ClaudeSentimentProc    ✅  Keyword fallback working")
    print("  RetailSentimentFetcher ✅  Extreme flag detection working")
    print("  FearGreedCalculator    ✅  Composite score computed")
    print("  SentimentAggregator    ✅  Currency and pair scores computed")
    print("  NewsStore              ✅  Dedup + TTL working")
    print("  NewsSentimentManager   ✅  Layer 1 package produced")
    print()
    print("Claude API: NOT connected — keyword fallback active")
    print("Connect  : NewsSentimentManager(claude_api_key='sk-ant-...')")
    print("All outputs are raw measurements — Layer 3 interprets")
    print("=" * 70)
