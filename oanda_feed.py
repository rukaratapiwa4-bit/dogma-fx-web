"""
═══════════════════════════════════════════════════════════════════════════════
FOREX TRADING SIGNAL SYSTEM — VERSION 6.0
FILE: oanda_feed.py
LAYER: 1 — DATA PERCEPTION LAYER (Live Feed Sub-Component)
═══════════════════════════════════════════════════════════════════════════════

PURPOSE:
    OANDA v20 REST/Streaming API connector.
    Connects to OANDA WebSocket price stream and feeds raw ticks
    into the MarketDataPipeline.

    This file CONNECTS and DELIVERS data only.
    It NEVER interprets, weights, or judges any data.

SOURCE TAG: LIVE_SOURCE (OANDA)

OANDA v20 API:
    Streaming endpoint: /v3/accounts/{accountID}/pricing/stream
    Instruments:        EUR_USD, GBP_USD, USD_JPY etc.
    Tick fields:        bids[], asks[], tradeable, time

RULES:
    - Every tick tagged LIVE_SOURCE
    - Latency measured from OANDA timestamp to local receive time
    - Feed outages reported to pipeline immediately
    - Reconnection handled automatically with backoff
    - API key never logged

═══════════════════════════════════════════════════════════════════════════════
"""

import time
import json
import logging
import threading
import requests
from typing import Optional
from market_data_feed import (
    MarketDataPipeline,
    FeedManager,
    DataSource,
    FeedHealthState,
)

logger = logging.getLogger("OANDAFeed")


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

# OANDA v20 endpoints
OANDA_STREAM_URL_LIVE    = "https://stream-fxtrade.oanda.com"
OANDA_STREAM_URL_PRACTICE= "https://stream-fxpractice.oanda.com"
OANDA_REST_URL_LIVE      = "https://api-fxtrade.oanda.com"
OANDA_REST_URL_PRACTICE  = "https://api-fxpractice.oanda.com"

# Reconnection settings
MAX_RECONNECT_ATTEMPTS   = 10
RECONNECT_BACKOFF_BASE   = 2.0   # seconds
RECONNECT_BACKOFF_MAX    = 60.0  # seconds

# Instrument name mapping
# System uses: EUR/USD — OANDA uses: EUR_USD
INSTRUMENT_MAP = {
    "EUR/USD" : "EUR_USD",
    "GBP/USD" : "GBP_USD",
    "USD/JPY" : "USD_JPY",
    "USD/CHF" : "USD_CHF",
    "AUD/USD" : "AUD_USD",
    "USD/CAD" : "USD_CAD",
    "NZD/USD" : "NZD_USD",
    "EUR/GBP" : "EUR_GBP",
    "EUR/JPY" : "EUR_JPY",
}

INSTRUMENT_MAP_REVERSE = {v: k for k, v in INSTRUMENT_MAP.items()}


# ═══════════════════════════════════════════════════════════════════════════════
# OANDA TICK PARSER
# ═══════════════════════════════════════════════════════════════════════════════

class OANDATickParser:
    """
    Parses raw OANDA streaming JSON into standardized tick fields.

    OANDA streaming format:
    {
        "type"       : "PRICE",
        "time"       : "2024-01-15T10:30:00.123456789Z",
        "instrument" : "EUR_USD",
        "bids"       : [{"price": "1.08500", "liquidity": 10000000}],
        "asks"       : [{"price": "1.08503", "liquidity": 10000000}],
        "tradeable"  : true
    }

    Also handles:
        "HEARTBEAT" → feed alive signal (not a tick)
        "DISCONNECT" → feed disconnected
    """

    @staticmethod
    def parse_timestamp(oanda_time: str) -> float:
        """Convert OANDA ISO timestamp to Unix ms."""
        from datetime import datetime, timezone
        try:
            # Remove trailing Z and parse
            oanda_time = oanda_time.rstrip("Z")
            if "." in oanda_time:
                dt = datetime.strptime(oanda_time[:26], "%Y-%m-%dT%H:%M:%S.%f")
            else:
                dt = datetime.strptime(oanda_time, "%Y-%m-%dT%H:%M:%S")
            dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp() * 1000.0  # ms
        except Exception as e:
            logger.error(f"Timestamp parse error: {oanda_time} — {e}")
            return time.time() * 1000.0

    @staticmethod
    def parse(raw_json: dict) -> Optional[dict]:
        """
        Parse OANDA streaming message.

        Returns:
            dict with tick fields, or None if not a tradeable price tick
        """
        msg_type = raw_json.get("type")

        if msg_type == "HEARTBEAT":
            return None  # Feed alive — not a tick

        if msg_type != "PRICE":
            return None

        if not raw_json.get("tradeable", False):
            return None  # Market closed or non-tradeable

        instrument_oanda = raw_json.get("instrument", "")
        instrument = INSTRUMENT_MAP_REVERSE.get(instrument_oanda)
        if not instrument:
            logger.warning(f"Unknown OANDA instrument: {instrument_oanda}")
            return None

        bids = raw_json.get("bids", [])
        asks = raw_json.get("asks", [])

        if not bids or not asks:
            return None

        # Best bid and ask (first in list = best price)
        bid        = float(bids[0]["price"])
        ask        = float(asks[0]["price"])
        bid_volume = float(bids[0].get("liquidity", 0))
        ask_volume = float(asks[0].get("liquidity", 0))

        timestamp_utc = OANDATickParser.parse_timestamp(raw_json.get("time", ""))

        return {
            "instrument"   : instrument,
            "timestamp_utc": timestamp_utc,
            "bid"          : bid,
            "ask"          : ask,
            "bid_volume"   : bid_volume / 1_000_000,  # Normalize to millions
            "ask_volume"   : ask_volume / 1_000_000,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# OANDA STREAMING FEED
# ═══════════════════════════════════════════════════════════════════════════════

class OANDAStreamingFeed:
    """
    Connects to OANDA v20 streaming API and feeds ticks into pipeline.

    Connection flow:
        1. Open HTTP streaming connection to OANDA
        2. Read newline-delimited JSON messages
        3. Parse each message with OANDATickParser
        4. Feed valid ticks into FeedManager
        5. Measure latency per tick
        6. Reconnect automatically on disconnect

    RULE:
        This class delivers data only.
        All quality control happens in the pipeline.
        This class does NOT filter or modify prices.
    """

    def __init__(self,
                 api_key      : str,
                 account_id   : str,
                 feed_manager : FeedManager,
                 instruments  : list,
                 practice     : bool = True):

        self.api_key      = api_key
        self.account_id   = account_id
        self.feed_manager = feed_manager
        self.instruments  = instruments
        self.practice     = practice

        self._stream_url  = OANDA_STREAM_URL_PRACTICE if practice else OANDA_STREAM_URL_LIVE
        self._running     = False
        self._thread      = None
        self._reconnect_count = 0

        # Map instruments to OANDA format
        self._oanda_instruments = ",".join([
            INSTRUMENT_MAP.get(i, i.replace("/", "_"))
            for i in instruments
        ])

        logger.info(
            f"OANDAFeed initialized: "
            f"{'practice' if practice else 'live'} | "
            f"instruments={instruments}"
        )

    def start(self):
        """Start the streaming feed in a background thread."""
        self._running = True
        self._thread  = threading.Thread(
            target=self._stream_loop,
            daemon=True,
            name="OANDAStreamThread"
        )
        self._thread.start()
        logger.info("OANDAFeed: streaming started")

    def stop(self):
        """Stop the streaming feed."""
        self._running = False
        logger.info("OANDAFeed: streaming stopped")

    def _stream_loop(self):
        """Main streaming loop with automatic reconnection."""
        while self._running:
            try:
                self._connect_and_stream()
                self._reconnect_count = 0  # Reset on clean exit
            except Exception as e:
                if not self._running:
                    break
                self._reconnect_count += 1
                backoff = min(
                    RECONNECT_BACKOFF_BASE ** self._reconnect_count,
                    RECONNECT_BACKOFF_MAX
                )
                logger.error(
                    f"OANDAFeed: connection error — {e} | "
                    f"reconnect {self._reconnect_count}/{MAX_RECONNECT_ATTEMPTS} "
                    f"in {backoff:.1f}s"
                )
                for instr in self.instruments:
                    self.feed_manager.on_feed_outage(instr, str(e))

                if self._reconnect_count >= MAX_RECONNECT_ATTEMPTS:
                    logger.critical("OANDAFeed: max reconnects reached — stopping")
                    self._running = False
                    break

                time.sleep(backoff)

    def _connect_and_stream(self):
        """Open streaming connection and process messages."""
        url     = f"{self._stream_url}/v3/accounts/{self.account_id}/pricing/stream"
        headers = {
            "Authorization" : f"Bearer {self.api_key}",
            "Accept-Datetime-Format": "RFC3339",
        }
        params  = {"instruments": self._oanda_instruments}

        logger.info(f"OANDAFeed: connecting to {url}")

        with requests.get(
            url,
            headers=headers,
            params=params,
            stream=True,
            timeout=30
        ) as response:

            if response.status_code != 200:
                raise ConnectionError(
                    f"OANDA stream returned {response.status_code}: "
                    f"{response.text[:200]}"
                )

            logger.info("OANDAFeed: stream connected")

            for line in response.iter_lines():
                if not self._running:
                    break
                if not line:
                    continue

                receive_time = time.time() * 1000.0  # ms

                try:
                    raw = json.loads(line.decode("utf-8"))
                except json.JSONDecodeError as e:
                    logger.warning(f"OANDAFeed: JSON parse error — {e}")
                    continue

                tick_data = OANDATickParser.parse(raw)
                if tick_data is None:
                    continue  # Heartbeat or non-price message

                # Measure latency: receive time vs OANDA timestamp
                latency_ms = receive_time - tick_data["timestamp_utc"]

                # Feed into pipeline
                self.feed_manager.on_tick(
                    instrument    = tick_data["instrument"],
                    timestamp_utc = tick_data["timestamp_utc"],
                    bid           = tick_data["bid"],
                    ask           = tick_data["ask"],
                    bid_volume    = tick_data["bid_volume"],
                    ask_volume    = tick_data["ask_volume"],
                    latency_ms    = latency_ms,
                )


# ═══════════════════════════════════════════════════════════════════════════════
# OANDA REST — HISTORICAL PRICE FETCHER
# ═══════════════════════════════════════════════════════════════════════════════

class OANDAHistoricalFetcher:
    """
    Fetches historical candles from OANDA v20 REST API.
    Used for system initialization and backfilling gaps.

    OANDA candles endpoint:
        GET /v3/instruments/{instrument}/candles
        granularity: S5, S10, S15, S30, M1, M2, M4, M5,
                     M10, M15, M30, H1, H2, H3, H4, H6,
                     H8, H12, D, W, M
        count: max 5000 per request
        price: "BA" = bid + ask

    NOTE:
        Historical data from OANDA is still LIVE_SOURCE
        (same broker, same data lineage as live feed)
        Dukascopy historical = HISTORICAL_SOURCE (different source)
    """

    GRANULARITY_MAP = {
        "1s"  : "S5",    # Closest available — 5s minimum from OANDA REST
        "5s"  : "S5",
        "15s" : "S15",
        "1m"  : "M1",
        "5m"  : "M5",
        "15m" : "M15",
        "1h"  : "H1",
        "4h"  : "H4",
        "1d"  : "D",
    }

    def __init__(self, api_key: str, practice: bool = True):
        self.api_key    = api_key
        self._base_url  = OANDA_REST_URL_PRACTICE if practice else OANDA_REST_URL_LIVE
        self._headers   = {
            "Authorization"         : f"Bearer {api_key}",
            "Accept-Datetime-Format": "RFC3339",
        }

    def fetch_candles(self,
                      instrument : str,
                      timeframe  : str,
                      count      : int = 500) -> list:
        """
        Fetch historical OHLC candles.

        Returns:
            List of bar dicts with bid/ask OHLC + volume + timestamp
        """
        oanda_instrument = INSTRUMENT_MAP.get(
            instrument,
            instrument.replace("/", "_")
        )
        granularity = self.GRANULARITY_MAP.get(timeframe, "M1")

        url = f"{self._base_url}/v3/instruments/{oanda_instrument}/candles"
        params = {
            "granularity" : granularity,
            "count"       : min(count, 5000),
            "price"       : "BA",  # Bid + Ask
        }

        try:
            response = requests.get(
                url,
                headers=self._headers,
                params=params,
                timeout=10
            )
            response.raise_for_status()
            data     = response.json()
            candles  = data.get("candles", [])

            bars = []
            for c in candles:
                if not c.get("complete", False):
                    continue  # Skip incomplete (live) bar
                bid  = c.get("bid", {})
                ask  = c.get("ask", {})
                bars.append({
                    "timestamp_utc": OANDATickParser.parse_timestamp(c["time"]),
                    "timeframe"    : timeframe,
                    "source"       : DataSource.LIVE_SOURCE.value,
                    "bid_open"     : float(bid.get("o", 0)),
                    "bid_high"     : float(bid.get("h", 0)),
                    "bid_low"      : float(bid.get("l", 0)),
                    "bid_close"    : float(bid.get("c", 0)),
                    "ask_open"     : float(ask.get("o", 0)),
                    "ask_high"     : float(ask.get("h", 0)),
                    "ask_low"      : float(ask.get("l", 0)),
                    "ask_close"    : float(ask.get("c", 0)),
                    "volume"       : int(c.get("volume", 0)),
                })

            logger.info(
                f"OANDA historical: {instrument} {timeframe} "
                f"— {len(bars)} bars fetched"
            )
            return bars

        except requests.RequestException as e:
            logger.error(f"OANDA historical fetch failed: {e}")
            return []


# ═══════════════════════════════════════════════════════════════════════════════
# SELF TEST
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    """
    Self test — simulates OANDA message parsing without live connection.
    """
    print("=" * 70)
    print("OANDA FEED — PARSER SELF TEST")
    print("=" * 70)

    # Simulate OANDA streaming messages
    test_messages = [
        # Valid price tick
        {
            "type"       : "PRICE",
            "time"       : "2024-01-15T10:30:00.123456789Z",
            "instrument" : "EUR_USD",
            "bids"       : [{"price": "1.08500", "liquidity": 10000000}],
            "asks"       : [{"price": "1.08503", "liquidity": 10000000}],
            "tradeable"  : True,
        },
        # Heartbeat — should return None
        {
            "type": "HEARTBEAT",
            "time": "2024-01-15T10:30:05.000Z",
        },
        # Non-tradeable — should return None
        {
            "type"       : "PRICE",
            "time"       : "2024-01-15T10:30:01.000Z",
            "instrument" : "EUR_USD",
            "bids"       : [{"price": "1.08501", "liquidity": 0}],
            "asks"       : [{"price": "1.08504", "liquidity": 0}],
            "tradeable"  : False,
        },
        # GBP/USD tick
        {
            "type"       : "PRICE",
            "time"       : "2024-01-15T10:30:02.000Z",
            "instrument" : "GBP_USD",
            "bids"       : [{"price": "1.27100", "liquidity": 5000000}],
            "asks"       : [{"price": "1.27104", "liquidity": 5000000}],
            "tradeable"  : True,
        },
    ]

    print("\n[1] Testing OANDATickParser:")
    for i, msg in enumerate(test_messages):
        result = OANDATickParser.parse(msg)
        print(f"    Message {i+1} ({msg['type']}): "
              f"{'PARSED → ' + str(result) if result else 'SKIPPED (expected)'}")

    print("\n[2] Testing pipeline integration:")
    from market_data_feed import create_feed_system
    feed_mgr, layer1 = create_feed_system(
        instruments=["EUR/USD", "GBP/USD"],
        source=DataSource.LIVE_SOURCE
    )

    # Simulate parsed ticks entering pipeline
    import time
    base_ts = time.time() * 1000

    feed_mgr.on_tick("EUR/USD", base_ts,        1.08500, 1.08503, 10.0, 10.0, 25.0)
    feed_mgr.on_tick("EUR/USD", base_ts + 250,  1.08502, 1.08505, 10.0, 10.0, 28.0)
    feed_mgr.on_tick("GBP/USD", base_ts,        1.27100, 1.27104, 5.0,  5.0,  30.0)
    feed_mgr.on_tick("GBP/USD", base_ts + 250,  1.27103, 1.27107, 5.0,  5.0,  32.0)

    for instr in ["EUR/USD", "GBP/USD"]:
        pkg = layer1.get_layer1_package(instr)
        print(f"    {instr}: bid={pkg['latest_bid']:.5f} "
              f"ask={pkg['latest_ask']:.5f} "
              f"health={pkg['feed_health']}")

    print("\n" + "=" * 70)
    print("OANDA FEED SELF TEST COMPLETE")
    print("=" * 70)
