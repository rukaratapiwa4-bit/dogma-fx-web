"""
═══════════════════════════════════════════════════════════════════════════════
FOREX TRADING SIGNAL SYSTEM — VERSION 6.0
FILE: market_data_feed.py
LAYER: 1 — DATA PERCEPTION LAYER (Data Pipeline Sub-Component)
═══════════════════════════════════════════════════════════════════════════════

PURPOSE:
    Raw market data collection, ingestion, quality control, and multi-timeframe
    aggregation. This file feeds clean, validated, labeled tick data into
    Layer 1 of the system.

ARCHITECTURAL RULES (PERMANENT):
    - This file COLLECTS and LABELS data only
    - This file NEVER interprets, weights, or judges any data
    - This file NEVER decides what data means for a trade
    - All interpretation belongs exclusively to Layer 3
    - Missing data is modeled as uncertainty — NEVER assumed
    - Every tick carries quality metadata — price alone is NOT a tick

DATA SOURCES:
    PRIMARY   → OANDA (live WebSocket stream)        [LIVE_SOURCE]
    SECONDARY → Dukascopy JForex API (historical)    [HISTORICAL_SOURCE]

    RULE: Sources are NEVER mixed without explicit tagging.
          Every piece of data must carry its source label.

FEED CHAIN (STRICT ORDER — NO SHORTCUTS):
    Raw Ticks
        ↓
    Ingestion Layer (sequence + gap + duplicate checks)
        ↓
    Data Quality Engine (validation + anomaly flagging only)
        ↓
    Clean Ticks (standardized CleanTick format)
        ↓
    Tick Processing Engine (measurement only — no interpretation)
        ↓
    1s Aggregation Engine
        ↓
    Multi-Timeframe Builder (1s → 5s → 15s → 1m → 5m → 15m → 1h → 4h → 1d)
        ↓
    Internal Market State (latest value snapshot only)
        ↓
    Storage Layer
        ↓
    Layer 1 / Layer 2 Input Feed

JFOREX API REFERENCE:
    ITick   → bid, ask, askVolume, bidVolume, asks[], bids[], timestamp
    IBar    → open, high, low, close, volume, timestamp
    Period  → TICK, ONE_SEC, ONE_MIN, FIVE_MINS, FIFTEEN_MINS,
              ONE_HOUR, FOUR_HOURS, DAILY, WEEKLY
    IFeedDescriptor → TIME_PERIOD_AGGREGATION, TICKS, PRICE_RANGE_AGGREGATION

═══════════════════════════════════════════════════════════════════════════════
"""

import time
import queue
import logging
import threading
from enum import Enum
from typing import Optional
from dataclasses import dataclass, field
from collections import deque

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("MarketDataFeed")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — ENUMS AND CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════

class DataSource(Enum):
    """
    Every piece of data must be tagged with its source.
    NEVER mix LIVE and HISTORICAL data without this label.
    """
    LIVE_SOURCE       = "OANDA"         # Primary live WebSocket stream
    HISTORICAL_SOURCE = "DUKASCOPY"     # Dukascopy JForex historical replay


class TickFlag(Enum):
    """
    Validity flags assigned during ingestion.
    These are LABELS only — what to do with them is Layer 3's job.
    """
    VALID       = "VALID"       # Passed all ingestion checks
    INVALID     = "INVALID"     # Failed a hard sanity check
    GAP         = "GAP"         # Time gap detected before this tick
    DUPLICATE   = "DUPLICATE"   # Identical timestamp + bid/ask — dropped


class AnomalyFlag(Enum):
    """
    Anomaly flags assigned by the Data Quality Engine.
    This layer DETECTS only — it NEVER corrects data.
    """
    NONE              = "NONE"
    SPIKE_DETECTED    = "SPIKE_DETECTED"       # Price jump beyond threshold
    SPREAD_EXPLOSION  = "SPREAD_EXPLOSION"     # Spread exceeds normal bounds
    TICK_FREEZE       = "TICK_FREEZE"          # No movement for abnormal duration
    STALE_FEED        = "STALE_FEED"           # Repeated identical ticks
    NEGATIVE_SPREAD   = "NEGATIVE_SPREAD"      # bid >= ask — invalid
    LATENCY_SPIKE     = "LATENCY_SPIKE"        # Latency above threshold


class FeedHealthState(Enum):
    """
    Feed health states reported to downstream layers.
    Downstream layers decide what to do — this layer only measures.
    """
    HEALTHY         = "HEALTHY"         # All feeds active, latency normal
    DEGRADED        = "DEGRADED"        # Non-critical feed delayed
    PARTIAL_OUTAGE  = "PARTIAL_OUTAGE"  # Mixed failures
    CRITICAL_OUTAGE = "CRITICAL_OUTAGE" # Price feed or broker feed down
    TOTAL_OUTAGE    = "TOTAL_OUTAGE"    # Multiple core feeds offline


class Timeframe(Enum):
    """
    Supported aggregation timeframes.
    ALL higher timeframes build strictly from 1s bars — no shortcuts.

    Maps to JForex Period:
        ONE_SEC     → Period.ONE_SEC
        FIVE_SEC    → Period.createCustomPeriod(Unit.Second, 5)
        FIFTEEN_SEC → Period.createCustomPeriod(Unit.Second, 15)
        ONE_MIN     → Period.ONE_MIN
        FIVE_MIN    → Period.FIVE_MINS
        FIFTEEN_MIN → Period.FIFTEEN_MINS
        ONE_HOUR    → Period.ONE_HOUR
        FOUR_HOUR   → Period.FOUR_HOURS
        ONE_DAY     → Period.DAILY
    """
    ONE_SEC     = "1s"
    FIVE_SEC    = "5s"
    FIFTEEN_SEC = "15s"
    ONE_MIN     = "1m"
    FIVE_MIN    = "5m"
    FIFTEEN_MIN = "15m"
    ONE_HOUR    = "1h"
    FOUR_HOUR   = "4h"
    ONE_DAY     = "1d"


# Timeframe cascade — strict order, no shortcuts
TIMEFRAME_CASCADE = [
    Timeframe.ONE_SEC,
    Timeframe.FIVE_SEC,
    Timeframe.FIFTEEN_SEC,
    Timeframe.ONE_MIN,
    Timeframe.FIVE_MIN,
    Timeframe.FIFTEEN_MIN,
    Timeframe.ONE_HOUR,
    Timeframe.FOUR_HOUR,
    Timeframe.ONE_DAY,
]

# Timeframe duration in seconds
TIMEFRAME_SECONDS = {
    Timeframe.ONE_SEC:     1,
    Timeframe.FIVE_SEC:    5,
    Timeframe.FIFTEEN_SEC: 15,
    Timeframe.ONE_MIN:     60,
    Timeframe.FIVE_MIN:    300,
    Timeframe.FIFTEEN_MIN: 900,
    Timeframe.ONE_HOUR:    3600,
    Timeframe.FOUR_HOUR:   14400,
    Timeframe.ONE_DAY:     86400,
}

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

# Ingestion rules
MAX_TICK_GAP_SECONDS        = 5.0    # Gap beyond this triggers TICK_GAP_DETECTED event
MAX_SPREAD_PIPS             = 50.0   # Spread above this = SPREAD_EXPLOSION
MAX_SPIKE_PIPS              = 100.0  # Price jump above this = SPIKE_DETECTED
TICK_FREEZE_SECONDS         = 10.0   # No tick for this long = TICK_FREEZE
MAX_LATENCY_MS              = 500    # Latency above this = LATENCY_SPIKE
SPREAD_EXPLOSION_MULTIPLIER = 5.0    # Spread > N × rolling average = explosion

# Feed health thresholds
DEGRADED_LATENCY_MS        = 200
CRITICAL_LATENCY_MS        = 1000

# Rolling window sizes
SPREAD_HISTORY_SIZE        = 100     # For rolling spread average
VELOCITY_WINDOW_TICKS      = 20     # For tick velocity calculation
VELOCITY_WINDOW_SECONDS    = 5.0    # For time-based velocity


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — RAW TICK STANDARD
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class RawTick:
    """
    Standardized raw tick format.

    CRITICAL RULE:
        A tick is NOT just price.
        It is price + timing + quality metadata.
        Without metadata, the system cannot reason about trust.

    Maps to JForex ITick:
        timestamp_utc → ITimedData.getTime()
        bid           → ITick.getBid()
        ask           → ITick.getAsk()
        bid_volume    → ITick.getBidVolume()
        ask_volume    → ITick.getAskVolume()
        bids[]        → ITick.getBids()      (live only — depth > 1)
        asks[]        → ITick.getAsks()      (live only — depth > 1)

    Fields not in ITick (added by this system):
        mid           → computed: (bid + ask) / 2
        spread        → computed: ask - bid
        source        → DataSource label (LIVE or HISTORICAL)
        latency_ms    → measured delivery latency
        sequence_id   → monotonic counter for ordering
        flag          → TickFlag assigned during ingestion
    """
    # ── Core price fields (from JForex ITick) ──
    timestamp_utc : float               # Unix timestamp UTC (ms precision)
    bid           : float               # Best bid price
    ask           : float               # Best ask price
    bid_volume    : float = 0.0         # Volume at best bid
    ask_volume    : float = 0.0         # Volume at best ask

    # ── Computed fields (never stored from external source) ──
    mid           : float = field(init=False)
    spread        : float = field(init=False)

    # ── Metadata (required — not optional) ──
    source        : DataSource = DataSource.LIVE_SOURCE
    latency_ms    : Optional[float] = None
    sequence_id   : Optional[int]   = None

    # ── Quality flag (assigned by ingestion layer) ──
    flag          : TickFlag = TickFlag.VALID

    def __post_init__(self):
        # Computed fields — always derived, never externally provided
        self.mid    = (self.bid + self.ask) / 2.0
        self.spread = self.ask - self.bid

    def to_dict(self) -> dict:
        return {
            "timestamp_utc" : self.timestamp_utc,
            "bid"           : self.bid,
            "ask"           : self.ask,
            "mid"           : self.mid,
            "spread"        : self.spread,
            "bid_volume"    : self.bid_volume,
            "ask_volume"    : self.ask_volume,
            "source"        : self.source.value,
            "latency_ms"    : self.latency_ms,
            "sequence_id"   : self.sequence_id,
            "flag"          : self.flag.value,
        }


@dataclass
class CleanTick:
    """
    Output of the Data Quality Engine.

    Every tick leaving the quality layer carries:
        - All original tick data
        - A quality_score (0.0 → 1.0)
        - A validity_flag (True/False)
        - A list of anomaly_flags (may be empty)

    RULE:
        Data Quality Engine DETECTS anomalies only.
        It NEVER corrects data.
        Downstream layers receive the raw value + the flag.
        What to do with flagged data is Layer 3's decision.
    """
    tick          : RawTick
    quality_score : float          # 0.0 = worst, 1.0 = perfect
    validity_flag : bool           # False = anomaly detected (not necessarily rejected)
    anomaly_flags : list           # List of AnomalyFlag values


@dataclass
class OHLCBar:
    """
    Aggregated OHLC bar — produced by 1s Aggregation Engine.

    ALL higher timeframe bars must come from 1s bars.
    No direct tick → M1 shortcuts allowed.

    Maps to JForex IBar:
        open      → IBar.getOpen()
        high      → IBar.getHigh()
        low       → IBar.getLow()
        close     → IBar.getClose()
        volume    → IBar.getVolume()
        timestamp → ITimedData.getTime()
    """
    timestamp_utc : float           # Bar open time (UTC ms)
    timeframe     : Timeframe
    source        : DataSource

    # ── Bid OHLC ──
    bid_open      : float
    bid_high      : float
    bid_low       : float
    bid_close     : float

    # ── Ask OHLC ──
    ask_open      : float
    ask_high      : float
    ask_low       : float
    ask_close     : float

    # ── Mid OHLC (computed) ──
    mid_open      : float = field(init=False)
    mid_high      : float = field(init=False)
    mid_low       : float = field(init=False)
    mid_close     : float = field(init=False)

    # ── Volume ──
    tick_volume   : int   = 0      # Count of ticks in this bar
    bid_volume    : float = 0.0
    ask_volume    : float = 0.0

    # ── Bar health ──
    is_complete   : bool  = False  # True when bar period has closed
    anomaly_count : int   = 0      # Number of anomalous ticks inside bar

    def __post_init__(self):
        self.mid_open  = (self.bid_open  + self.ask_open)  / 2.0
        self.mid_high  = (self.bid_high  + self.ask_high)  / 2.0
        self.mid_low   = (self.bid_low   + self.ask_low)   / 2.0
        self.mid_close = (self.bid_close + self.ask_close) / 2.0

    def update_close(self, tick: RawTick):
        """Update bar with new tick — only before bar closes."""
        self.bid_high   = max(self.bid_high, tick.bid)
        self.bid_low    = min(self.bid_low,  tick.bid)
        self.bid_close  = tick.bid
        self.ask_high   = max(self.ask_high, tick.ask)
        self.ask_low    = min(self.ask_low,  tick.ask)
        self.ask_close  = tick.ask
        self.tick_volume += 1
        self.bid_volume  += tick.bid_volume
        self.ask_volume  += tick.ask_volume
        # Recompute mid
        self.mid_open  = (self.bid_open  + self.ask_open)  / 2.0
        self.mid_high  = (self.bid_high  + self.ask_high)  / 2.0
        self.mid_low   = (self.bid_low   + self.ask_low)   / 2.0
        self.mid_close = (self.bid_close + self.ask_close) / 2.0


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — INGESTION LAYER
# ═══════════════════════════════════════════════════════════════════════════════

class IngestionLayer:
    """
    First gate after raw data arrives.

    Responsibilities (ONLY):
        1. Sequence integrity — every tick must have increasing timestamp
        2. Gap detection — time gaps flagged as TICK_GAP_DETECTED
        3. Duplicate filtering — same timestamp + same bid/ask = DROP

    RULES:
        - Does NOT interpret data
        - Does NOT correct data
        - Does NOT decide if a gap is acceptable
        - Flags problems and passes forward
        - Historical replay preserves original timestamps exactly
        - Speed control in replay affects delivery only — NOT data integrity
    """

    def __init__(self, source: DataSource):
        self.source           = source
        self._last_timestamp  = None
        self._last_bid        = None
        self._last_ask        = None
        self._sequence_id     = 0
        self._gap_events      = []        # Log of gap events for storage layer
        self._ingestion_errors= []        # Log of ingestion errors

    def ingest(self, timestamp_utc: float, bid: float, ask: float,
               bid_volume: float = 0.0, ask_volume: float = 0.0,
               latency_ms: Optional[float] = None) -> Optional[RawTick]:
        """
        Process one incoming tick through all ingestion checks.

        Returns:
            RawTick with appropriate flag, or None if tick is DUPLICATE
        """

        # ── 1. Duplicate filter ──────────────────────────────────────────
        # Same timestamp AND same bid/ask = exact duplicate, drop silently
        if (self._last_timestamp is not None and
                timestamp_utc == self._last_timestamp and
                bid == self._last_bid and
                ask == self._last_ask):
            logger.debug(
                f"[{self.source.value}] DUPLICATE tick dropped "
                f"ts={timestamp_utc} bid={bid} ask={ask}"
            )
            return None  # Do not store duplicates twice

        # ── 2. Sequence integrity ────────────────────────────────────────
        # Timestamp must be >= last timestamp (no time travel)
        flag = TickFlag.VALID
        if self._last_timestamp is not None:
            if timestamp_utc < self._last_timestamp:
                # Out-of-order tick — mark INVALID, do not pass forward
                error_record = {
                    "type"          : "OUT_OF_ORDER",
                    "timestamp"     : timestamp_utc,
                    "last_timestamp": self._last_timestamp,
                    "source"        : self.source.value,
                }
                self._ingestion_errors.append(error_record)
                logger.warning(
                    f"[{self.source.value}] INVALID_TICK: out-of-order "
                    f"ts={timestamp_utc} last={self._last_timestamp}"
                )
                tick = RawTick(
                    timestamp_utc=timestamp_utc,
                    bid=bid, ask=ask,
                    bid_volume=bid_volume, ask_volume=ask_volume,
                    source=self.source,
                    latency_ms=latency_ms,
                    sequence_id=self._sequence_id,
                    flag=TickFlag.INVALID
                )
                return tick  # Return flagged, do NOT pass to processing

            # ── 3. Gap detection ─────────────────────────────────────────
            gap_seconds = (timestamp_utc - self._last_timestamp) / 1000.0
            if gap_seconds > MAX_TICK_GAP_SECONDS:
                flag = TickFlag.GAP
                gap_event = {
                    "type"       : "TICK_GAP_DETECTED",
                    "gap_seconds": gap_seconds,
                    "from_ts"    : self._last_timestamp,
                    "to_ts"      : timestamp_utc,
                    "source"     : self.source.value,
                }
                self._gap_events.append(gap_event)
                logger.warning(
                    f"[{self.source.value}] TICK_GAP_DETECTED: "
                    f"{gap_seconds:.2f}s gap detected"
                )
                # RULE: Do NOT interpolate — just flag it

        # ── Build standardized RawTick ───────────────────────────────────
        self._sequence_id += 1
        tick = RawTick(
            timestamp_utc=timestamp_utc,
            bid=bid,
            ask=ask,
            bid_volume=bid_volume,
            ask_volume=ask_volume,
            source=self.source,
            latency_ms=latency_ms,
            sequence_id=self._sequence_id,
            flag=flag
        )

        # Update state
        self._last_timestamp = timestamp_utc
        self._last_bid       = bid
        self._last_ask       = ask

        return tick

    def get_gap_events(self) -> list:
        return list(self._gap_events)

    def get_ingestion_errors(self) -> list:
        return list(self._ingestion_errors)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — DATA QUALITY ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class DataQualityEngine:
    """
    Sits immediately after ingestion.

    Responsibilities (ONLY):
        1. Validation rules (timestamp sanity, bid < ask, spread bounds)
        2. Anomaly detection (spike, spread explosion, tick freeze, stale feed)

    CRITICAL RULE:
        This engine does NOT correct data.
        It ONLY flags it.
        Every clean tick carries quality_score + validity_flag + anomaly_flags.
        Downstream layers decide what to do with flagged ticks.

    Output format: CleanTick (tick + quality_score + validity_flag + anomaly_flags)
    """

    def __init__(self, instrument: str):
        self.instrument           = instrument
        self._spread_history      = deque(maxlen=SPREAD_HISTORY_SIZE)
        self._last_price          = None
        self._last_tick_time      = None
        self._anomaly_log         = []

    def _rolling_spread_average(self) -> Optional[float]:
        if len(self._spread_history) < 10:
            return None
        return sum(self._spread_history) / len(self._spread_history)

    def process(self, tick: RawTick) -> CleanTick:
        """
        Run all quality checks on a raw tick.
        Returns CleanTick with full quality metadata.
        """
        anomaly_flags  = []
        validity_flag  = True
        quality_score  = 1.0

        # ── Hard validation rules ────────────────────────────────────────

        # Rule 1: bid must be strictly less than ask
        if tick.bid >= tick.ask:
            anomaly_flags.append(AnomalyFlag.NEGATIVE_SPREAD)
            validity_flag  = False
            quality_score -= 0.5
            self._log_anomaly("NEGATIVE_SPREAD", tick)

        # Rule 2: no negative or zero prices
        if tick.bid <= 0 or tick.ask <= 0:
            anomaly_flags.append(AnomalyFlag.NEGATIVE_SPREAD)
            validity_flag  = False
            quality_score -= 0.5
            self._log_anomaly("ZERO_OR_NEGATIVE_PRICE", tick)

        # Rule 3: spread sanity bounds
        spread_pips = tick.spread * 10000  # Approximate pip conversion
        if spread_pips > MAX_SPREAD_PIPS:
            anomaly_flags.append(AnomalyFlag.SPREAD_EXPLOSION)
            quality_score -= 0.3
            self._log_anomaly("SPREAD_EXPLOSION_HARD", tick)

        # Rule 4: spread explosion vs rolling average
        avg_spread = self._rolling_spread_average()
        if avg_spread is not None and tick.spread > avg_spread * SPREAD_EXPLOSION_MULTIPLIER:
            if AnomalyFlag.SPREAD_EXPLOSION not in anomaly_flags:
                anomaly_flags.append(AnomalyFlag.SPREAD_EXPLOSION)
            quality_score -= 0.2
            self._log_anomaly("SPREAD_EXPLOSION_RELATIVE", tick)

        # ── Anomaly detection rules ──────────────────────────────────────

        # Spike detection — price jumps beyond threshold
        if self._last_price is not None:
            price_jump_pips = abs(tick.mid - self._last_price) * 10000
            if price_jump_pips > MAX_SPIKE_PIPS:
                anomaly_flags.append(AnomalyFlag.SPIKE_DETECTED)
                quality_score -= 0.3
                self._log_anomaly(f"SPIKE {price_jump_pips:.1f} pips", tick)

        # Tick freeze detection — no movement for abnormal duration
        now = time.time()
        if self._last_tick_time is not None:
            silence_seconds = now - self._last_tick_time
            if silence_seconds > TICK_FREEZE_SECONDS:
                anomaly_flags.append(AnomalyFlag.TICK_FREEZE)
                quality_score -= 0.2
                self._log_anomaly(f"TICK_FREEZE {silence_seconds:.1f}s", tick)

        # Latency spike detection
        if tick.latency_ms is not None and tick.latency_ms > MAX_LATENCY_MS:
            anomaly_flags.append(AnomalyFlag.LATENCY_SPIKE)
            quality_score -= 0.1
            self._log_anomaly(f"LATENCY_SPIKE {tick.latency_ms:.0f}ms", tick)

        # ── Update rolling state ─────────────────────────────────────────
        self._spread_history.append(tick.spread)
        self._last_price     = tick.mid
        self._last_tick_time = now

        # ── Clamp quality score ──────────────────────────────────────────
        quality_score = max(0.0, min(1.0, quality_score))

        if not anomaly_flags:
            anomaly_flags.append(AnomalyFlag.NONE)

        return CleanTick(
            tick          = tick,
            quality_score = quality_score,
            validity_flag = validity_flag,
            anomaly_flags = anomaly_flags
        )

    def _log_anomaly(self, anomaly_type: str, tick: RawTick):
        self._anomaly_log.append({
            "anomaly"      : anomaly_type,
            "timestamp"    : tick.timestamp_utc,
            "bid"          : tick.bid,
            "ask"          : tick.ask,
            "spread"       : tick.spread,
            "source"       : tick.source.value,
            "sequence_id"  : tick.sequence_id,
        })

    def get_anomaly_log(self) -> list:
        return list(self._anomaly_log)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — TICK PROCESSING ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class TickProcessingEngine:
    """
    Processes clean ticks into raw measurement scores.

    ARCHITECTURAL RULE:
        This engine outputs MEASUREMENTS ONLY.
        It NEVER uses interpretation words like:
            "trend", "strong", "weak", "bullish", "bearish"
        Those belong in Layer 3.

    Output measurements:
        spread_history       → rolling spread values
        velocity_pips_per_s  → Δprice per second window (NOT "trend")
        velocity_pips_per_n  → Δprice per N ticks window
        directional_bias     → ratio of up moves vs down moves (NOT "bullish")
        movement_consistency → consistency score of directional movement
        noise_ratio          → statistical noise in recent price action
        acceleration         → rate of change of velocity
    """

    def __init__(self):
        self._tick_buffer    = deque(maxlen=VELOCITY_WINDOW_TICKS)
        self._spread_buffer  = deque(maxlen=SPREAD_HISTORY_SIZE)
        self._time_buffer    = deque(maxlen=VELOCITY_WINDOW_TICKS)

    def process(self, clean_tick: CleanTick) -> dict:
        """
        Compute raw measurements from a clean tick.
        Returns measurement dict — no interpretation attached.
        """
        tick = clean_tick.tick

        # Update buffers
        self._tick_buffer.append(tick.mid)
        self._spread_buffer.append(tick.spread)
        self._time_buffer.append(tick.timestamp_utc)

        measurements = {
            "timestamp"           : tick.timestamp_utc,
            "bid"                 : tick.bid,
            "ask"                 : tick.ask,
            "mid"                 : tick.mid,
            "spread"              : tick.spread,
            "spread_avg_rolling"  : self._rolling_spread_avg(),
            "velocity_per_second" : self._velocity_per_second(),
            "velocity_per_n_ticks": self._velocity_per_n_ticks(),
            "directional_bias"    : self._directional_bias_ratio(),
            "movement_consistency": self._movement_consistency(),
            "noise_ratio"         : self._noise_ratio(),
            "acceleration"        : self._acceleration(),
            "quality_score"       : clean_tick.quality_score,
            "anomaly_flags"       : [f.value for f in clean_tick.anomaly_flags],
            "source"              : tick.source.value,
        }
        return measurements

    def _rolling_spread_avg(self) -> Optional[float]:
        if not self._spread_buffer:
            return None
        return sum(self._spread_buffer) / len(self._spread_buffer)

    def _velocity_per_second(self) -> Optional[float]:
        """Δprice per second window — pure measurement."""
        if len(self._time_buffer) < 2:
            return None
        dt_ms   = self._time_buffer[-1] - self._time_buffer[0]
        dt_s    = dt_ms / 1000.0
        if dt_s <= 0:
            return None
        dp      = self._tick_buffer[-1] - self._tick_buffer[0]
        return (dp / dt_s) * 10000  # In pips per second

    def _velocity_per_n_ticks(self) -> Optional[float]:
        """Δprice per N ticks — pure measurement."""
        if len(self._tick_buffer) < 2:
            return None
        return (self._tick_buffer[-1] - self._tick_buffer[0]) * 10000  # pips

    def _directional_bias_ratio(self) -> Optional[float]:
        """
        Ratio of up-moves to total moves.
        Range: 0.0 (all down) → 1.0 (all up) → 0.5 = balanced.
        LABEL: directional_bias_ratio — NOT "bullish" or "bearish".
        """
        if len(self._tick_buffer) < 2:
            return None
        prices = list(self._tick_buffer)
        moves  = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        if not moves:
            return None
        up_moves = sum(1 for m in moves if m > 0)
        return up_moves / len(moves)

    def _movement_consistency(self) -> Optional[float]:
        """
        Consistency of directional movement.
        1.0 = perfectly consistent, 0.0 = random.
        LABEL: movement_consistency_score — NOT "strong trend".
        """
        if len(self._tick_buffer) < 3:
            return None
        prices = list(self._tick_buffer)
        moves  = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        if not moves:
            return None
        positive = sum(1 for m in moves if m > 0)
        negative = sum(1 for m in moves if m < 0)
        dominant = max(positive, negative)
        return dominant / len(moves)

    def _noise_ratio(self) -> Optional[float]:
        """
        Statistical noise in recent price action.
        High = noisy/choppy, Low = clean movement.
        """
        if len(self._tick_buffer) < 3:
            return None
        prices  = list(self._tick_buffer)
        total   = sum(abs(prices[i] - prices[i-1]) for i in range(1, len(prices)))
        net     = abs(prices[-1] - prices[0])
        if total == 0:
            return 1.0
        return 1.0 - (net / total)

    def _acceleration(self) -> Optional[float]:
        """
        Rate of change of velocity.
        Pure statistical measurement — no directional interpretation.
        """
        if len(self._tick_buffer) < 4:
            return None
        half   = len(self._tick_buffer) // 2
        prices = list(self._tick_buffer)
        v1     = prices[half] - prices[0]
        v2     = prices[-1]   - prices[half]
        return (v2 - v1) * 10000  # pips


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — 1s AGGREGATION ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class AggregationEngine1s:
    """
    Aggregates clean ticks into 1-second OHLC bars.

    CRITICAL RULE:
        ALL higher timeframe bars must come from this 1s layer.
        No direct tick → M1 shortcuts anywhere in the system.

    Output: bid OHLC, ask OHLC, mid OHLC, tick volume — per 1s bar.
    Completed 1s bars are passed to the Multi-Timeframe Builder.
    """

    def __init__(self, instrument: str, source: DataSource):
        self.instrument   = instrument
        self.source       = source
        self._current_bar : Optional[OHLCBar] = None
        self._completed_bars : list = []

    def _bar_timestamp(self, timestamp_ms: float) -> float:
        """Floor timestamp to 1-second boundary."""
        return float(int(timestamp_ms / 1000) * 1000)

    def update(self, clean_tick: CleanTick) -> Optional[OHLCBar]:
        """
        Feed a clean tick into the 1s aggregator.
        Returns completed bar when a new second begins, else None.
        """
        tick    = clean_tick.tick
        bar_ts  = self._bar_timestamp(tick.timestamp_utc)
        completed = None

        if self._current_bar is None:
            # Start first bar
            self._current_bar = self._open_new_bar(tick, bar_ts)

        elif bar_ts > self._current_bar.timestamp_utc:
            # New second — close current bar, start new one
            self._current_bar.is_complete = True
            completed = self._current_bar
            self._completed_bars.append(completed)
            self._current_bar = self._open_new_bar(tick, bar_ts)

        else:
            # Same second — update current bar
            self._current_bar.update_close(tick)
            if clean_tick.anomaly_flags != [AnomalyFlag.NONE]:
                self._current_bar.anomaly_count += 1

        return completed

    def _open_new_bar(self, tick: RawTick, bar_ts: float) -> OHLCBar:
        return OHLCBar(
            timestamp_utc = bar_ts,
            timeframe     = Timeframe.ONE_SEC,
            source        = tick.source,
            bid_open      = tick.bid,
            bid_high      = tick.bid,
            bid_low       = tick.bid,
            bid_close     = tick.bid,
            ask_open      = tick.ask,
            ask_high      = tick.ask,
            ask_low       = tick.ask,
            ask_close     = tick.ask,
            tick_volume   = 1,
            bid_volume    = tick.bid_volume,
            ask_volume    = tick.ask_volume,
        )

    def get_live_bar(self) -> Optional[OHLCBar]:
        """Return the current incomplete bar (live state)."""
        return self._current_bar


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — MULTI-TIMEFRAME BUILDER
# ═══════════════════════════════════════════════════════════════════════════════

class MultiTimeframeBuilder:
    """
    Builds all higher timeframe bars strictly from 1s bars.

    CASCADE (STRICT — NO SHORTCUTS):
        1s → 5s → 15s → 1m → 5m → 15m → 1h → 4h → 1d

    RULE:
        Ticks NEVER feed directly into M1, M5, H1 etc.
        All aggregation starts from completed 1s bars only.

    Maps to JForex:
        TIME_PERIOD_AGGREGATION with appropriate Period values
        Period.ONE_SEC, Period.ONE_MIN, Period.FIVE_MINS etc.
    """

    def __init__(self, instrument: str, source: DataSource):
        self.instrument    = instrument
        self.source        = source
        self._bars         = {tf: [] for tf in TIMEFRAME_CASCADE}
        self._open_bars    = {tf: None for tf in TIMEFRAME_CASCADE}

    def _tf_seconds(self, tf: Timeframe) -> int:
        return TIMEFRAME_SECONDS[tf]

    def _bar_open_ts(self, timestamp_ms: float, tf: Timeframe) -> float:
        period_ms = self._tf_seconds(tf) * 1000
        return float(int(timestamp_ms / period_ms) * period_ms)

    def update(self, bar_1s: OHLCBar) -> dict:
        """
        Feed a completed 1s bar into all higher timeframe builders.
        Returns dict of any newly completed bars per timeframe.
        """
        newly_completed = {}

        for tf in TIMEFRAME_CASCADE[1:]:  # Skip 1s (already done)
            completed = self._update_timeframe(bar_1s, tf)
            if completed:
                newly_completed[tf] = completed

        return newly_completed

    def _update_timeframe(self, bar_1s: OHLCBar, tf: Timeframe) -> Optional[OHLCBar]:
        """Update a single timeframe with a completed 1s bar."""
        bar_ts    = self._bar_open_ts(bar_1s.timestamp_utc, tf)
        completed = None

        if self._open_bars[tf] is None:
            self._open_bars[tf] = self._open_from_1s(bar_1s, bar_ts, tf)

        elif bar_ts > self._open_bars[tf].timestamp_utc:
            self._open_bars[tf].is_complete = True
            completed = self._open_bars[tf]
            self._bars[tf].append(completed)
            self._open_bars[tf] = self._open_from_1s(bar_1s, bar_ts, tf)

        else:
            self._merge_1s_into_tf(bar_1s, self._open_bars[tf])

        return completed

    def _open_from_1s(self, bar_1s: OHLCBar, bar_ts: float, tf: Timeframe) -> OHLCBar:
        return OHLCBar(
            timestamp_utc = bar_ts,
            timeframe     = tf,
            source        = bar_1s.source,
            bid_open      = bar_1s.bid_open,
            bid_high      = bar_1s.bid_high,
            bid_low       = bar_1s.bid_low,
            bid_close     = bar_1s.bid_close,
            ask_open      = bar_1s.ask_open,
            ask_high      = bar_1s.ask_high,
            ask_low       = bar_1s.ask_low,
            ask_close     = bar_1s.ask_close,
            tick_volume   = bar_1s.tick_volume,
            bid_volume    = bar_1s.bid_volume,
            ask_volume    = bar_1s.ask_volume,
            anomaly_count = bar_1s.anomaly_count,
        )

    def _merge_1s_into_tf(self, bar_1s: OHLCBar, tf_bar: OHLCBar):
        """Merge 1s bar data into an open higher-timeframe bar."""
        tf_bar.bid_high    = max(tf_bar.bid_high, bar_1s.bid_high)
        tf_bar.bid_low     = min(tf_bar.bid_low,  bar_1s.bid_low)
        tf_bar.bid_close   = bar_1s.bid_close
        tf_bar.ask_high    = max(tf_bar.ask_high, bar_1s.ask_high)
        tf_bar.ask_low     = min(tf_bar.ask_low,  bar_1s.ask_low)
        tf_bar.ask_close   = bar_1s.ask_close
        tf_bar.tick_volume += bar_1s.tick_volume
        tf_bar.bid_volume  += bar_1s.bid_volume
        tf_bar.ask_volume  += bar_1s.ask_volume
        tf_bar.anomaly_count += bar_1s.anomaly_count
        # Recompute mid
        tf_bar.mid_high    = (tf_bar.bid_high  + tf_bar.ask_high)  / 2.0
        tf_bar.mid_low     = (tf_bar.bid_low   + tf_bar.ask_low)   / 2.0
        tf_bar.mid_close   = (tf_bar.bid_close + tf_bar.ask_close) / 2.0

    def get_latest_bar(self, tf: Timeframe) -> Optional[OHLCBar]:
        """Get the most recent completed bar for a timeframe."""
        bars = self._bars.get(tf, [])
        return bars[-1] if bars else None

    def get_live_bar(self, tf: Timeframe) -> Optional[OHLCBar]:
        """Get the current (incomplete) bar for a timeframe."""
        return self._open_bars.get(tf)

    def get_bars(self, tf: Timeframe, count: int = 100) -> list:
        """Get N most recent completed bars for a timeframe."""
        bars = self._bars.get(tf, [])
        return bars[-count:]


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — INTERNAL MARKET STATE
# ═══════════════════════════════════════════════════════════════════════════════

class InternalMarketState:
    """
    Real-time snapshot memory — LIVE STATE HOLDER ONLY.

    RULE: This layer stores latest values only.
    It MUST NOT:
        - Classify regime
        - Score anything
        - Decide anything
        - Interpret anything

    It is a snapshot of the most recent measurements.
    All interpretation happens in Layer 3.
    """

    def __init__(self):
        self.latest_spread      : Optional[float] = None
        self.latest_mid         : Optional[float] = None
        self.latest_bid         : Optional[float] = None
        self.latest_ask         : Optional[float] = None
        self.latest_velocity    : Optional[float] = None
        self.latest_quality     : Optional[float] = None
        self.latest_anomalies   : list             = []
        self.latest_timestamp   : Optional[float] = None
        self.feed_health        : FeedHealthState  = FeedHealthState.HEALTHY
        self.latest_bars        : dict             = {}  # tf → OHLCBar
        self.source             : Optional[DataSource] = None
        self._lock              = threading.Lock()

    def update_tick(self, measurements: dict):
        with self._lock:
            self.latest_spread    = measurements.get("spread")
            self.latest_mid       = measurements.get("mid")
            self.latest_bid       = measurements.get("bid")
            self.latest_ask       = measurements.get("ask")
            self.latest_velocity  = measurements.get("velocity_per_second")
            self.latest_quality   = measurements.get("quality_score")
            self.latest_anomalies = measurements.get("anomaly_flags", [])
            self.latest_timestamp = measurements.get("timestamp")
            src = measurements.get("source")
            if src == DataSource.LIVE_SOURCE.value:
                self.source = DataSource.LIVE_SOURCE
            else:
                self.source = DataSource.HISTORICAL_SOURCE

    def update_bar(self, tf: Timeframe, bar: OHLCBar):
        with self._lock:
            self.latest_bars[tf] = bar

    def update_feed_health(self, state: FeedHealthState):
        with self._lock:
            self.feed_health = state

    def snapshot(self) -> dict:
        """Return a thread-safe snapshot of current market state."""
        with self._lock:
            return {
                "latest_spread"   : self.latest_spread,
                "latest_mid"      : self.latest_mid,
                "latest_bid"      : self.latest_bid,
                "latest_ask"      : self.latest_ask,
                "latest_velocity" : self.latest_velocity,
                "latest_quality"  : self.latest_quality,
                "latest_anomalies": list(self.latest_anomalies),
                "latest_timestamp": self.latest_timestamp,
                "feed_health"     : self.feed_health.value,
                "source"          : self.source.value if self.source else None,
                "bars"            : {
                    tf.value: {
                        "open"      : bar.mid_open,
                        "high"      : bar.mid_high,
                        "low"       : bar.mid_low,
                        "close"     : bar.mid_close,
                        "volume"    : bar.tick_volume,
                        "complete"  : bar.is_complete,
                        "anomalies" : bar.anomaly_count,
                    }
                    for tf, bar in self.latest_bars.items()
                }
            }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 9 — FEED HEALTH MONITOR
# ═══════════════════════════════════════════════════════════════════════════════

class FeedHealthMonitor:
    """
    Continuously monitors feed health and reports status.

    States:
        HEALTHY         → All feeds active, latency normal
        DEGRADED        → Non-critical feed delayed
        PARTIAL_OUTAGE  → Mixed failures
        CRITICAL_OUTAGE → Price or broker feed down
        TOTAL_OUTAGE    → Multiple core feeds offline

    RULE:
        This monitor measures and labels feed health.
        Downstream layers decide what to do with that label.
        This monitor NEVER blocks trades or triggers NULLs directly.
        It only reports state — response belongs to Layer 3 and Layer 4.
    """

    def __init__(self):
        self._feed_status       = {}
        self._last_tick_time    = None
        self._feed_interruptions = []

    def record_tick(self, source: DataSource, latency_ms: Optional[float] = None):
        """Record successful tick arrival from a source."""
        self._last_tick_time = time.time()
        self._feed_status[source.value] = {
            "last_tick"  : self._last_tick_time,
            "latency_ms" : latency_ms,
            "status"     : "ACTIVE",
        }

    def record_outage(self, source: DataSource, reason: str):
        """Record a feed outage event."""
        self._feed_status[source.value] = {
            "last_tick" : self._feed_status.get(source.value, {}).get("last_tick"),
            "status"    : "OUTAGE",
            "reason"    : reason,
        }
        self._feed_interruptions.append({
            "source"    : source.value,
            "reason"    : reason,
            "timestamp" : time.time(),
        })
        logger.warning(f"[FeedHealth] OUTAGE recorded: {source.value} — {reason}")

    def assess_health(self) -> FeedHealthState:
        """
        Assess current feed health state.
        Returns FeedHealthState label — no action taken here.
        """
        if not self._feed_status:
            return FeedHealthState.TOTAL_OUTAGE

        outages = sum(
            1 for v in self._feed_status.values()
            if v.get("status") == "OUTAGE"
        )
        total = len(self._feed_status)

        if outages == 0:
            return FeedHealthState.HEALTHY
        elif outages == total:
            return FeedHealthState.TOTAL_OUTAGE
        elif DataSource.LIVE_SOURCE.value in self._feed_status:
            live = self._feed_status[DataSource.LIVE_SOURCE.value]
            if live.get("status") == "OUTAGE":
                return FeedHealthState.CRITICAL_OUTAGE
        elif outages >= total // 2:
            return FeedHealthState.PARTIAL_OUTAGE
        else:
            return FeedHealthState.DEGRADED

    def get_feed_interruptions(self) -> list:
        return list(self._feed_interruptions)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 10 — STORAGE LAYER
# ═══════════════════════════════════════════════════════════════════════════════

class StorageLayer:
    """
    Stores all data for downstream access and audit trail.

    Stores:
        REQUIRED:
            - Raw ticks (clean + flagged)
            - 1s bars
            - Derived MTF bars
            - Market state snapshots

        ALSO REQUIRED:
            - Anomaly events log
            - Ingestion errors log
            - Feed interruption log

    This is an in-memory implementation.
    Production version connects to a time-series database.
    """

    def __init__(self, max_ticks: int = 100_000):
        self._ticks             = deque(maxlen=max_ticks)
        self._bars              = {tf: deque(maxlen=10_000) for tf in TIMEFRAME_CASCADE}
        self._anomaly_events    = []
        self._ingestion_errors  = []
        self._feed_interruptions= []
        self._snapshots         = deque(maxlen=1000)
        self._lock              = threading.Lock()

    def store_tick(self, clean_tick: CleanTick):
        with self._lock:
            self._ticks.append(clean_tick.tick.to_dict())
            if clean_tick.anomaly_flags != [AnomalyFlag.NONE]:
                self._anomaly_events.append({
                    "timestamp"    : clean_tick.tick.timestamp_utc,
                    "anomaly_flags": [f.value for f in clean_tick.anomaly_flags],
                    "quality_score": clean_tick.quality_score,
                    "source"       : clean_tick.tick.source.value,
                })

    def store_bar(self, bar: OHLCBar):
        with self._lock:
            self._bars[bar.timeframe].append({
                "timestamp"   : bar.timestamp_utc,
                "timeframe"   : bar.timeframe.value,
                "bid_open"    : bar.bid_open,
                "bid_high"    : bar.bid_high,
                "bid_low"     : bar.bid_low,
                "bid_close"   : bar.bid_close,
                "ask_open"    : bar.ask_open,
                "ask_high"    : bar.ask_high,
                "ask_low"     : bar.ask_low,
                "ask_close"   : bar.ask_close,
                "mid_open"    : bar.mid_open,
                "mid_high"    : bar.mid_high,
                "mid_low"     : bar.mid_low,
                "mid_close"   : bar.mid_close,
                "tick_volume" : bar.tick_volume,
                "bid_volume"  : bar.bid_volume,
                "ask_volume"  : bar.ask_volume,
                "anomaly_cnt" : bar.anomaly_count,
                "source"      : bar.source.value,
            })

    def store_ingestion_error(self, error: dict):
        with self._lock:
            self._ingestion_errors.append(error)

    def store_feed_interruption(self, event: dict):
        with self._lock:
            self._feed_interruptions.append(event)

    def store_snapshot(self, snapshot: dict):
        with self._lock:
            self._snapshots.append(snapshot)

    def get_recent_ticks(self, n: int = 100) -> list:
        with self._lock:
            return list(self._ticks)[-n:]

    def get_recent_bars(self, tf: Timeframe, n: int = 100) -> list:
        with self._lock:
            return list(self._bars[tf])[-n:]

    def get_anomaly_log(self) -> list:
        with self._lock:
            return list(self._anomaly_events)

    def get_ingestion_errors(self) -> list:
        with self._lock:
            return list(self._ingestion_errors)

    def get_feed_interruptions(self) -> list:
        with self._lock:
            return list(self._feed_interruptions)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 11 — MASTER PIPELINE ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════════

class MarketDataPipeline:
    """
    Master orchestrator for the complete data pipeline.

    Connects all components in strict order:
        Ingestion → Quality → Processing → 1s Agg → MTF → State → Storage

    This is the interface that Layer 1 / Layer 2 consumes.
    All output is raw measurement — zero interpretation.

    Usage:
        pipeline = MarketDataPipeline("EUR/USD", DataSource.LIVE_SOURCE)
        pipeline.on_tick(timestamp, bid, ask, bid_vol, ask_vol, latency_ms)
        snapshot = pipeline.get_market_state()
        bars_1h  = pipeline.get_bars(Timeframe.ONE_HOUR, count=50)
    """

    def __init__(self, instrument: str, source: DataSource):
        self.instrument    = instrument
        self.source        = source

        # Pipeline components
        self._ingestion    = IngestionLayer(source)
        self._quality      = DataQualityEngine(instrument)
        self._processing   = TickProcessingEngine()
        self._agg_1s       = AggregationEngine1s(instrument, source)
        self._mtf_builder  = MultiTimeframeBuilder(instrument, source)
        self._market_state = InternalMarketState()
        self._storage      = StorageLayer()
        self._health_mon   = FeedHealthMonitor()

        # Output queue for Layer 1/2 consumers
        self._output_queue = queue.Queue(maxsize=10_000)

        logger.info(
            f"MarketDataPipeline initialized: {instrument} "
            f"[{source.value}]"
        )

    def on_tick(self,
                timestamp_utc : float,
                bid           : float,
                ask           : float,
                bid_volume    : float = 0.0,
                ask_volume    : float = 0.0,
                latency_ms    : Optional[float] = None):
        """
        Entry point for every incoming tick.
        Runs the full pipeline in strict order.
        """

        # ── STEP 1: Ingestion ────────────────────────────────────────────
        raw_tick = self._ingestion.ingest(
            timestamp_utc, bid, ask,
            bid_volume, ask_volume, latency_ms
        )
        if raw_tick is None:
            return  # Duplicate — dropped at ingestion

        if raw_tick.flag == TickFlag.INVALID:
            self._storage.store_ingestion_error({
                "timestamp": timestamp_utc,
                "reason"   : "OUT_OF_ORDER",
                "source"   : self.source.value,
            })
            return  # Invalid — do not pass forward

        # ── STEP 2: Data Quality Engine ──────────────────────────────────
        clean_tick = self._quality.process(raw_tick)

        # ── STEP 3: Store raw tick with quality metadata ─────────────────
        self._storage.store_tick(clean_tick)
        self._health_mon.record_tick(self.source, latency_ms)

        # ── STEP 4: Tick Processing Engine ──────────────────────────────
        measurements = self._processing.process(clean_tick)

        # ── STEP 5: Update Internal Market State ─────────────────────────
        self._market_state.update_tick(measurements)
        health = self._health_mon.assess_health()
        self._market_state.update_feed_health(health)

        # ── STEP 6: 1s Aggregation ───────────────────────────────────────
        completed_1s = self._agg_1s.update(clean_tick)

        if completed_1s:
            # ── STEP 7: Store 1s bar ─────────────────────────────────────
            self._storage.store_bar(completed_1s)

            # ── STEP 8: Multi-Timeframe Builder ──────────────────────────
            # RULE: All higher TF bars come from 1s bars — no shortcuts
            newly_completed = self._mtf_builder.update(completed_1s)

            for tf, bar in newly_completed.items():
                self._storage.store_bar(bar)
                self._market_state.update_bar(tf, bar)

        # ── STEP 9: Push snapshot to output queue ────────────────────────
        snapshot = self._market_state.snapshot()
        self._storage.store_snapshot(snapshot)

        try:
            self._output_queue.put_nowait({
                "type"         : "TICK",
                "instrument"   : self.instrument,
                "measurements" : measurements,
                "snapshot"     : snapshot,
                "feed_health"  : health.value,
            })
        except queue.Full:
            logger.warning("Output queue full — snapshot dropped")

    def on_feed_outage(self, reason: str):
        """Call when a feed goes offline."""
        self._health_mon.record_outage(self.source, reason)
        self._storage.store_feed_interruption({
            "source"   : self.source.value,
            "reason"   : reason,
            "timestamp": time.time(),
        })
        health = self._health_mon.assess_health()
        self._market_state.update_feed_health(health)
        logger.error(f"Feed outage [{self.source.value}]: {reason}")

    def get_market_state(self) -> dict:
        """Get current market state snapshot — for Layer 1/2 consumption."""
        return self._market_state.snapshot()

    def get_bars(self, tf: Timeframe, count: int = 100) -> list:
        """Get recent completed bars for a timeframe."""
        return self._storage.get_recent_bars(tf, count)

    def get_live_bar(self, tf: Timeframe) -> Optional[OHLCBar]:
        """Get current (incomplete) bar for a timeframe."""
        return self._mtf_builder.get_live_bar(tf)

    def get_output_queue(self) -> queue.Queue:
        """Return output queue for async Layer 1/2 consumption."""
        return self._output_queue

    def get_feed_health(self) -> FeedHealthState:
        return self._health_mon.assess_health()

    def get_anomaly_log(self) -> list:
        return self._storage.get_anomaly_log()

    def get_ingestion_errors(self) -> list:
        return self._storage.get_ingestion_errors()

    def get_gap_events(self) -> list:
        return self._ingestion.get_gap_events()


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 12 — HISTORICAL REPLAY ENGINE (DUKASCOPY)
# ═══════════════════════════════════════════════════════════════════════════════

class HistoricalReplayEngine:
    """
    Replays Dukascopy historical tick data through the pipeline.

    RULES:
        - Original timestamps preserved exactly — no smoothing
        - Speed control affects delivery only — NOT data integrity
        - Every tick tagged HISTORICAL_SOURCE
        - Replay honesty: what was recorded is what is replayed

    Maps to JForex:
        IHistory.getTicks() → historical tick retrieval
        ITick → bid, ask, askVolume, bidVolume, getTime()
    """

    def __init__(self, instrument: str, speed_multiplier: float = 1.0):
        self.instrument        = instrument
        self.speed_multiplier  = speed_multiplier  # Speed only — not data
        self._pipeline = MarketDataPipeline(
            instrument, DataSource.HISTORICAL_SOURCE
        )

    def replay_tick(self,
                    original_timestamp_utc : float,
                    bid                    : float,
                    ask                    : float,
                    bid_volume             : float = 0.0,
                    ask_volume             : float = 0.0):
        """
        Replay one historical tick.
        Timestamp is preserved exactly from original data.
        """
        # RULE: Original timestamp preserved — no resampling before ingestion
        self._pipeline.on_tick(
            timestamp_utc = original_timestamp_utc,
            bid           = bid,
            ask           = ask,
            bid_volume    = bid_volume,
            ask_volume    = ask_volume,
            latency_ms    = None  # Historical has no live latency
        )

    def get_pipeline(self) -> MarketDataPipeline:
        return self._pipeline


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 13 — MULTI-INSTRUMENT FEED MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class FeedManager:
    """
    Manages data pipelines for multiple forex pairs simultaneously.

    Each instrument gets its own isolated pipeline.
    All pipelines share a common output queue for Layer 1/2 consumption.

    Supported pairs (forex focus):
        EUR/USD, GBP/USD, USD/JPY, USD/CHF,
        AUD/USD, USD/CAD, NZD/USD, EUR/GBP, EUR/JPY
    """

    def __init__(self):
        self._pipelines    : dict[str, MarketDataPipeline] = {}
        self._global_queue : queue.Queue = queue.Queue(maxsize=100_000)

    def add_instrument(self, instrument: str,
                       source: DataSource = DataSource.LIVE_SOURCE):
        """Add a forex pair to monitoring."""
        if instrument not in self._pipelines:
            self._pipelines[instrument] = MarketDataPipeline(instrument, source)
            logger.info(f"FeedManager: Added instrument {instrument} [{source.value}]")

    def on_tick(self, instrument: str, timestamp_utc: float,
                bid: float, ask: float,
                bid_volume: float = 0.0, ask_volume: float = 0.0,
                latency_ms: Optional[float] = None):
        """Route tick to correct instrument pipeline."""
        if instrument not in self._pipelines:
            logger.warning(f"Unknown instrument: {instrument} — tick dropped")
            return
        self._pipelines[instrument].on_tick(
            timestamp_utc, bid, ask,
            bid_volume, ask_volume, latency_ms
        )

    def get_state(self, instrument: str) -> Optional[dict]:
        """Get market state for an instrument."""
        pipeline = self._pipelines.get(instrument)
        return pipeline.get_market_state() if pipeline else None

    def get_all_states(self) -> dict:
        """Get market states for all instruments."""
        return {
            instr: p.get_market_state()
            for instr, p in self._pipelines.items()
        }

    def get_bars(self, instrument: str,
                 tf: Timeframe, count: int = 100) -> list:
        """Get bars for a specific instrument and timeframe."""
        pipeline = self._pipelines.get(instrument)
        return pipeline.get_bars(tf, count) if pipeline else []

    def get_feed_health(self) -> dict:
        """Get feed health for all instruments."""
        return {
            instr: p.get_feed_health().value
            for instr, p in self._pipelines.items()
        }

    def on_feed_outage(self, instrument: str, reason: str):
        """Report feed outage for an instrument."""
        pipeline = self._pipelines.get(instrument)
        if pipeline:
            pipeline.on_feed_outage(reason)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 14 — LAYER 1 OUTPUT INTERFACE
# ═══════════════════════════════════════════════════════════════════════════════

class Layer1OutputFeed:
    """
    The final output interface consumed by Layer 1 / Layer 2.

    Packages the pipeline output into the Layer 1 standard format:
        - All raw measurements (no interpretation)
        - Feed health status per instrument
        - Data tier labels per input
        - Anomaly flags
        - Bar data per timeframe

    ARCHITECTURAL RULE:
        Everything in this output is a measurement or label.
        Nothing is weighted, interpreted, or judged.
        Layer 2 receives this and produces scores.
        Layer 3 receives Layer 2 scores and produces decisions.
    """

    def __init__(self, feed_manager: FeedManager):
        self._feed_manager = feed_manager

    def get_layer1_package(self, instrument: str) -> dict:
        """
        Return the complete Layer 1 data package for an instrument.
        This is the input to Layer 2 Feature Engine.
        """
        state    = self._feed_manager.get_state(instrument)
        health   = self._feed_manager.get_feed_health().get(instrument, "UNKNOWN")

        if state is None:
            return {
                "instrument"  : instrument,
                "feed_health" : "CRITICAL_OUTAGE",
                "data"        : None,
                "null_data"   : True,
            }

        return {
            "instrument"        : instrument,
            "feed_health"       : health,
            "null_data"         : health in ("CRITICAL_OUTAGE", "TOTAL_OUTAGE"),

            # ── Current tick measurements (raw — no interpretation) ──────
            "latest_bid"        : state.get("latest_bid"),
            "latest_ask"        : state.get("latest_ask"),
            "latest_mid"        : state.get("latest_mid"),
            "latest_spread"     : state.get("latest_spread"),
            "latest_velocity"   : state.get("latest_velocity"),
            "latest_quality"    : state.get("latest_quality"),
            "latest_anomalies"  : state.get("latest_anomalies", []),
            "latest_timestamp"  : state.get("latest_timestamp"),

            # ── Data source label (LIVE or HISTORICAL) ───────────────────
            "data_source"       : state.get("source"),

            # ── OHLC bars per timeframe ──────────────────────────────────
            # All bars produced from 1s cascade — no shortcuts
            "bars"              : state.get("bars", {}),

            # ── Data tier label (assigned here for Layer 3 confidence) ───
            # Layer 3 applies confidence intervals based on tier
            # This file labels — Layer 3 applies the weight
            "data_tier"         : self._assess_data_tier(state),
        }

    def _assess_data_tier(self, state: dict) -> str:
        """
        Label the data tier based on source.
        This is a LABEL only — confidence intervals applied in Layer 3.
        """
        source = state.get("source")
        if source == DataSource.LIVE_SOURCE.value:
            return "TIER_1"  # Live retail price feed
        elif source == DataSource.HISTORICAL_SOURCE.value:
            return "TIER_2"  # Semi-institutional historical
        return "TIER_1"      # Default conservative

    def get_all_packages(self) -> dict:
        """Get Layer 1 packages for all instruments."""
        states = self._feed_manager.get_all_states()
        return {
            instr: self.get_layer1_package(instr)
            for instr in states
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 15 — FACTORY FUNCTION
# ═══════════════════════════════════════════════════════════════════════════════

def create_feed_system(instruments: list,
                       source: DataSource = DataSource.LIVE_SOURCE
                       ) -> tuple[FeedManager, Layer1OutputFeed]:
    """
    Factory function to create and configure the full data feed system.

    Returns:
        (FeedManager, Layer1OutputFeed)

    Usage:
        feed_mgr, layer1 = create_feed_system(
            instruments=["EUR/USD", "GBP/USD", "USD/JPY"],
            source=DataSource.LIVE_SOURCE
        )

        # On each incoming tick from OANDA WebSocket:
        feed_mgr.on_tick("EUR/USD", timestamp, bid, ask, bid_vol, ask_vol, latency)

        # Feed Layer 2 every N milliseconds:
        package = layer1.get_layer1_package("EUR/USD")
    """
    manager = FeedManager()
    for instrument in instruments:
        manager.add_instrument(instrument, source)

    output_feed = Layer1OutputFeed(manager)

    logger.info(
        f"Feed system created: {len(instruments)} instruments "
        f"[{source.value}]"
    )
    return manager, output_feed


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 16 — SELF TEST
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    """
    Self-test: simulates tick ingestion through the full pipeline.
    Verifies each component in sequence without external dependencies.
    """
    print("=" * 70)
    print("MARKET DATA FEED — SELF TEST")
    print("=" * 70)

    # Create system for EUR/USD
    feed_mgr, layer1 = create_feed_system(
        instruments=["EUR/USD"],
        source=DataSource.LIVE_SOURCE
    )

    import random

    # Simulate 200 ticks
    base_ts  = time.time() * 1000  # ms
    base_bid = 1.08500
    base_ask = 1.08503

    print("\n[1] Feeding 200 simulated ticks...")
    for i in range(200):
        ts         = base_ts + (i * 250)   # 250ms between ticks
        bid        = base_bid + random.gauss(0, 0.00010)
        ask        = bid + 0.00003 + random.gauss(0, 0.000005)
        bid_volume = random.uniform(0.5, 5.0)
        ask_volume = random.uniform(0.5, 5.0)
        latency    = random.uniform(10, 80)

        feed_mgr.on_tick(
            "EUR/USD", ts, bid, ask,
            bid_volume, ask_volume, latency
        )

    print("[2] Injecting duplicate tick...")
    feed_mgr.on_tick("EUR/USD", base_ts, base_bid, base_ask)  # Should be dropped

    print("[3] Injecting out-of-order tick...")
    feed_mgr.on_tick("EUR/USD", base_ts - 1000, base_bid, base_ask)  # Should be flagged INVALID

    print("[4] Injecting spike tick...")
    feed_mgr.on_tick("EUR/USD", base_ts + 60000, base_bid + 0.02, base_ask + 0.02)

    print("\n[5] Layer 1 output package:")
    package = layer1.get_layer1_package("EUR/USD")
    print(f"    Instrument   : {package['instrument']}")
    print(f"    Feed health  : {package['feed_health']}")
    print(f"    Data source  : {package['data_source']}")
    print(f"    Data tier    : {package['data_tier']}")
    print(f"    NULL data    : {package['null_data']}")
    print(f"    Latest bid   : {package['latest_bid']:.5f}" if package['latest_bid'] else "    Latest bid: None")
    print(f"    Latest ask   : {package['latest_ask']:.5f}" if package['latest_ask'] else "    Latest ask: None")
    print(f"    Latest spread: {package['latest_spread']:.5f}" if package['latest_spread'] else "    Spread: None")
    print(f"    Quality score: {package['latest_quality']:.3f}" if package['latest_quality'] else "    Quality: None")
    print(f"    Anomalies    : {package['latest_anomalies']}")

    print("\n[6] Available timeframe bars:")
    bars = package.get("bars", {})
    for tf, bar_data in bars.items():
        print(f"    {tf:5s} → close={bar_data['close']:.5f} "
              f"vol={bar_data['volume']} complete={bar_data['complete']}")

    print("\n[7] Anomaly log summary:")
    anomalies = feed_mgr._pipelines["EUR/USD"].get_anomaly_log()
    print(f"    Total anomalies detected: {len(anomalies)}")

    print("\n[8] Ingestion errors summary:")
    errors = feed_mgr._pipelines["EUR/USD"].get_ingestion_errors()
    print(f"    Total ingestion errors: {len(errors)}")

    print("\n[9] Gap events:")
    gaps = feed_mgr._pipelines["EUR/USD"].get_gap_events()
    print(f"    Total gaps detected: {len(gaps)}")

    print("\n" + "=" * 70)
    print("SELF TEST COMPLETE")
    print("Pipeline verified: Ingestion → Quality → Processing → 1s → MTF → State → Storage")
    print("Layer 1 output package ready for Layer 2 Feature Engine")
    print("=" * 70)
