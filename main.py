"""
═══════════════════════════════════════════════════════════════════════════════
FOREX TRADING SIGNAL SYSTEM — VERSION 6.0
FILE: main.py
LAYER: SYSTEM INTEGRATION — MASTER ORCHESTRATOR
═══════════════════════════════════════════════════════════════════════════════

PURPOSE:
    Wires all 10 layers into one running system.
    Manages startup, shutdown, the main signal loop, and the background
    learning/monitoring threads.

SIGNAL FLOW (per tick cycle):
    Layer 1  → FeedManager + ancillary feeds (price, news, COT, multi-asset,
                                               options, calendar)
        ↓
    Layer 2  → FeatureEngine.compute_all_pairs()
        ↓
    Layer 3  → DecisionEngine.decide_all_pairs()
        ↓
    Gate 1   → RiskControlManager.process()          (Hard Safety)
        ↓
    Gate 4B  → PortfolioExposureManager.process()    (Portfolio)
        ↓
    Layer 5  → ExecutionEngine.execute()              (Fill + PFVW)
        ↓
    Layer 6  → JournalManager.record_trade_open/close/null()
        ↓ (background threads)
    Layer 7  → ValidationEngine (scheduled)
    Layer 8  → LearningLoop (scheduled)
    Layer 9  → ChaosModeEngine (parallel — every tick)
    Layer 10 → OptimizationEngine (every tick on open positions)

THREADING MODEL:
    Main thread      → signal loop (tick → decision → execution)
    Chaos thread     → Layer 9 stress monitoring (every tick)
    Optimization thread → Layer 10 position management (every 30s)
    Learning thread  → Layers 7+8 cycle (every 6 hours)
    Feed threads     → Layer 1 ancillary feeds (each has own schedule)

CONFIGURATION:
    All runtime config lives in SystemConfig dataclass below.
    Paper trading = True by default — set to False for live.

═══════════════════════════════════════════════════════════════════════════════
"""

import sys
import time
import queue
import signal
import logging
import threading
import traceback
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path

# ── Layer 1: Market Data ──────────────────────────────────────────────────────
from market_data_feed import FeedManager, DataSource

# ── Layer 1: Ancillary Feeds ─────────────────────────────────────────────────
from economic_calendar  import EconomicCalendarManager
from news_sentiment     import NewsSentimentManager
from cot_report         import COTManager
from multi_asset_feed   import MultiAssetManager
from options_flow       import OptionsFlowManager

# ── Layer 2: Feature Engine ───────────────────────────────────────────────────
from feature_engine     import FeatureEngine, FeaturePackage

# ── Layer 3: Decision Engine ──────────────────────────────────────────────────
from decision_engine    import DecisionEngine, DecisionOutput, NullType

# ── Layer 4A: Risk Control ────────────────────────────────────────────────────
from risk_control       import RiskControlManager, RiskOutput, RiskState

# ── Layer 4B: Portfolio Exposure ──────────────────────────────────────────────
from portfolio_exposure import PortfolioExposureManager, PortfolioOutput

# ── Layer 5: Execution Engine ─────────────────────────────────────────────────
from execution_engine   import ExecutionEngine, MarketSnapshot

# ── Layer 6: Journal System ───────────────────────────────────────────────────
from journal_system     import JournalManager

# ── Layer 7: Validation Engine ───────────────────────────────────────────────
from validation_engine  import ValidationEngine

# ── Layer 8: Learning Loop ────────────────────────────────────────────────────
from learning_loop      import LearningLoop

# ── Layer 9: Chaos Mode ───────────────────────────────────────────────────────
from chaos_mode         import ChaosModeEngine, StressReading, ChaosPhase

# ── Layer 10: Optimization ───────────────────────────────────────────────────
from optimization_layer import OptimizationEngine, TradeState

# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s",
    datefmt= "%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("SYSTEM")


# ═══════════════════════════════════════════════════════════════════════════════
# PRICE FEED ADAPTER
# Wraps FeedManager to provide get_layer1_package() interface Layer 2 expects
# ═══════════════════════════════════════════════════════════════════════════════

class PriceFeedAdapter:
    """
    Wraps FeedManager and exposes get_layer1_package(pair) so
    FeatureEngine.compute_all_pairs() can call it correctly.
    Mirrors Layer1OutputFeed without requiring the import.
    """
    def __init__(self, feed_manager: FeedManager):
        self._fm = feed_manager

    def get_layer1_package(self, instrument: str) -> dict:
        state  = self._fm.get_state(instrument)
        health = self._fm.get_feed_health().get(instrument, "UNKNOWN")
        if state is None:
            return {
                "instrument": instrument,
                "feed_health": "CRITICAL_OUTAGE",
                "null_data"  : True,
            }
        return {
            "instrument"        : instrument,
            "feed_health"       : health,
            "null_data"         : health in ("CRITICAL_OUTAGE", "TOTAL_OUTAGE"),
            "latest_bid"        : state.get("latest_bid"),
            "latest_ask"        : state.get("latest_ask"),
            "latest_mid"        : state.get("latest_mid"),
            "latest_spread"     : state.get("latest_spread"),
            "latest_velocity"   : state.get("latest_velocity"),
            "latest_quality"    : state.get("latest_quality"),
            "latest_anomalies"  : state.get("latest_anomalies", []),
            "latest_timestamp"  : state.get("latest_timestamp"),
            "data_source"       : state.get("source"),
            "bars"              : state.get("bars", {}),
            "data_tier"         : "TIER_1" if health == "HEALTHY" else "TIER_2",
            # Also expose bid/ask at top level for MarketSnapshot builder
            "bid"               : state.get("latest_bid"),
            "ask"               : state.get("latest_ask"),
            "timestamp_utc"     : state.get("latest_timestamp"),
            "active_session"    : state.get("active_session", "OVERLAP"),
        }

    def get_state(self, instrument: str) -> Optional[dict]:
        """Pass-through for direct state access."""
        return self._fm.get_state(instrument)

    def get_feed_health(self) -> dict:
        return self._fm.get_feed_health()

    def on_tick(self, instrument: str, timestamp_utc: float,
                bid: float, ask: float,
                bid_volume: float = 0.0, ask_volume: float = 0.0,
                latency_ms: Optional[float] = None):
        self._fm.on_tick(instrument, timestamp_utc, bid, ask,
                         bid_volume, ask_volume, latency_ms)

    def add_instrument(self, instrument: str, source):
        self._fm.add_instrument(instrument, source)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — SYSTEM CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class SystemConfig:
    """
    All runtime configuration in one place.
    Change values here — nothing else needs touching.
    """
    # ── Account ───────────────────────────────────────────────────────────────
    account_balance         : float = 10_000.0
    paper_trading           : bool  = True      # False = live orders

    # ── Pairs to trade ────────────────────────────────────────────────────────
    pairs: list = field(default_factory=lambda: [
        "EUR/USD", "GBP/USD", "USD/JPY",
        "AUD/USD", "USD/CAD", "NZD/USD",
    ])

    # ── API keys (set via env or replace here) ────────────────────────────────
    oanda_api_key           : Optional[str] = None   # OANDA v20 API key
    oanda_account_id        : Optional[str] = None   # OANDA account ID
    oanda_practice          : bool  = True            # True = practice account
    anthropic_api_key       : Optional[str] = None   # For Claude news processing

    # ── Data feed settings ────────────────────────────────────────────────────
    tick_interval_ms        : int   = 500    # Min ms between signal evaluations
    multi_asset_refresh_s   : int   = 60     # Multi-asset refresh interval
    news_refresh_min        : int   = 5      # News feed refresh
    cot_refresh_h           : float = 6.0    # COT refresh (weekly data)
    options_refresh_min     : int   = 30     # Options/vol refresh

    # ── Learning cycle ────────────────────────────────────────────────────────
    learning_cycle_hours    : float = 6.0    # How often Layer 7/8 runs

    # ── Optimization cycle ────────────────────────────────────────────────────
    optimization_interval_s : int   = 30     # How often Layer 10 evaluates

    # ── Journal ───────────────────────────────────────────────────────────────
    journal_db_path         : str   = "journal.db"

    # ── Logging ───────────────────────────────────────────────────────────────
    log_level               : str   = "INFO"

    def load_from_env(self):
        """Load API keys from environment variables."""
        import os
        self.oanda_api_key      = os.getenv("OANDA_API_KEY",      self.oanda_api_key)
        self.oanda_account_id   = os.getenv("OANDA_ACCOUNT_ID",   self.oanda_account_id)
        self.anthropic_api_key  = os.getenv("ANTHROPIC_API_KEY",  self.anthropic_api_key)
        oanda_env               = os.getenv("OANDA_PRACTICE", "true").lower()
        self.oanda_practice     = oanda_env != "false"
        paper_env               = os.getenv("PAPER_TRADING", "true").lower()
        self.paper_trading      = paper_env != "false"
        balance_env             = os.getenv("ACCOUNT_BALANCE")
        if balance_env:
            self.account_balance = float(balance_env)
        return self


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — LAYER 1 FEED BUNDLE
# ═══════════════════════════════════════════════════════════════════════════════

class Layer1FeedBundle:
    """
    Initialises and manages all Layer 1 data sources.

    Contains:
        price_feed    → FeedManager (tick data per pair)
        calendar      → EconomicCalendarManager
        news          → NewsSentimentManager
        cot           → COTManager
        multi_asset   → MultiAssetManager
        options       → OptionsFlowManager

    All managers run on independent background threads.
    get_snapshot() returns a merged Layer 1 package for Layer 2 consumption.
    """

    def __init__(self, cfg: SystemConfig):
        self._cfg = cfg
        logger.info("Initialising Layer 1 feeds...")

        # ── Price feed (per pair) ─────────────────────────────────────────────
        _raw_feed = FeedManager()
        for pair in cfg.pairs:
            _raw_feed.add_instrument(pair, DataSource.LIVE_SOURCE)

        # PriceFeedAdapter wraps FeedManager — single interface for all layers
        self.price_feed    = PriceFeedAdapter(_raw_feed)
        self.price_feed_l1 = self.price_feed   # alias — Layer 2 uses this

        # ── Economic Calendar ──────────────────────────────────────────────────
        self.calendar = EconomicCalendarManager(
            api_key        = cfg.oanda_api_key,
            oanda_practice = cfg.oanda_practice,
        )

        # ── News & Sentiment ───────────────────────────────────────────────────
        self.news = NewsSentimentManager(
            claude_api_key = cfg.anthropic_api_key,
            oanda_api_key  = cfg.oanda_api_key,
            oanda_account  = cfg.oanda_account_id,
            oanda_practice = cfg.oanda_practice,
        )

        # ── COT Report ────────────────────────────────────────────────────────
        self.cot = COTManager(refresh_hours=cfg.cot_refresh_h)

        # ── Multi-Asset (DXY, Gold, VIX etc.) ────────────────────────────────
        self.multi_asset = MultiAssetManager(
            refresh_seconds = cfg.multi_asset_refresh_s,
        )

        # ── Options & Volatility Flow ─────────────────────────────────────────
        self.options = OptionsFlowManager(
            oanda_api_key  = cfg.oanda_api_key,
            oanda_practice = cfg.oanda_practice,
        )

        logger.info("Layer 1 feeds initialised ✅")

    def start(self):
        """Start all background feed threads."""
        logger.info("Starting Layer 1 feed threads...")
        self.calendar.start()
        self.news.start()
        self.cot.start()
        self.multi_asset.start()
        self.options.start()
        logger.info("All Layer 1 feed threads running ✅")

    def stop(self):
        """Stop all background feed threads."""
        logger.info("Stopping Layer 1 feed threads...")
        try: self.calendar.stop()
        except Exception: pass
        try: self.news.stop()
        except Exception: pass
        try: self.cot.stop()
        except Exception: pass
        try: self.multi_asset.stop()
        except Exception: pass
        try: self.options.stop()
        except Exception: pass
        logger.info("Layer 1 feeds stopped ✅")

    def on_tick(self, pair: str, ts: float,
                bid: float, ask: float,
                bid_vol: float = 0.0, ask_vol: float = 0.0,
                latency_ms: Optional[float] = None):
        """Route a live tick into the price feed pipeline."""
        self.price_feed.on_tick(pair, ts, bid, ask, bid_vol, ask_vol, latency_ms)

    def get_snapshot(self, pair: str) -> dict:
        """
        Merge all Layer 1 packages into one dict for Layer 2.

        Returns:
            {
                "price"      : dict   (market state from FeedManager)
                "calendar"   : dict   (economic event window)
                "news"       : dict   (news + sentiment scores)
                "cot"        : dict   (COT positioning scores)
                "multi_asset": dict   (DXY, VIX, Gold, correlations)
                "options"    : dict   (IV surfaces, flow scores)
            }
        """
        return {
            "price"      : self.price_feed_l1.get_layer1_package(pair) or {},
            "calendar"   : self.calendar.get_layer1_package(pair) or {},
            "news"       : self.news.get_layer1_package(pair) or {},
            "cot"        : self.cot.get_layer1_package() or {},
            "multi_asset": self.multi_asset.get_layer1_package() or {},
            "options"    : self.options.get_layer1_package() or {},
        }

    def get_feed_health(self) -> dict:
        """Aggregate feed health across all sources."""
        return {
            "price_feed" : {p: self.price_feed.get_state(p) for p in self._cfg.pairs},
            "calendar"   : getattr(self.calendar,   "_health", "UNKNOWN"),
            "news"       : getattr(self.news,        "_health", "UNKNOWN"),
            "cot"        : getattr(self.cot,         "_health", "UNKNOWN"),
            "multi_asset": getattr(self.multi_asset, "_health", "UNKNOWN"),
            "options"    : getattr(self.options,     "_health", "UNKNOWN"),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — BACKGROUND THREAD MANAGERS
# ═══════════════════════════════════════════════════════════════════════════════

class LearningThread:
    """
    Background thread: runs Layers 7 + 8 on schedule.
    Does not block the signal loop.
    Frequency: every cfg.learning_cycle_hours.
    """

    def __init__(self, journal: JournalManager,
                 validation : ValidationEngine,
                 learning   : LearningLoop,
                 interval_h : float = 6.0):
        self._journal    = journal
        self._validation = validation
        self._learning   = learning
        self._interval   = interval_h * 3600
        self._thread     : Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def start(self):
        self._thread = threading.Thread(
            target = self._run, daemon = True, name = "LearningThread"
        )
        self._thread.start()
        logger.info(f"LearningThread started (interval={self._interval/3600:.1f}h)")

    def stop(self):
        self._stop_event.set()

    def _run(self):
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=self._interval)
            if self._stop_event.is_set():
                break
            try:
                self._cycle()
            except Exception as e:
                logger.error(f"LearningThread error: {e}")
                logger.debug(traceback.format_exc())

    def _cycle(self):
        logger.info("═══ Learning cycle starting (Layers 7 + 8) ═══")
        counts = self._journal.get_total_counts()
        logger.info(
            f"Journal: {counts['total_trades']} trades | "
            f"{counts['total_nulls']} NULLs | "
            f"NULL rate={counts['null_rate']:.1f}%"
        )

        # Pull history
        trades = self._journal._storage.query_trades()
        nulls  = self._journal._storage.query_nulls()

        if not trades:
            logger.info("LearningThread: no trades yet — skipping cycle")
            return

        # Layer 7: validate all regimes
        validation_results = self._validation.validate_all_regimes(trades)
        approved_regimes   = [r for r, v in validation_results.items() if v.get("gate_passed")]
        logger.info(
            f"Layer 7 complete: {len(approved_regimes)}/{len(validation_results)} "
            f"regimes passed gate"
        )

        # Layer 8: run learning cycle
        result = self._learning.run_cycle(trades, nulls, regime="ALL")
        logger.info(
            f"Layer 8 complete: {result.proposals_approved} approved | "
            f"{result.proposals_rejected} rejected | "
            f"EV delta=${result.ev_delta or 0:.2f}"
        )

        # Save performance snapshot
        self._journal.save_performance_snapshot()
        logger.info("═══ Learning cycle complete ═══")


class OptimizationThread:
    """
    Background thread: runs Layer 10 on all open positions.
    Frequency: every cfg.optimization_interval_s seconds.
    """

    def __init__(self, optimizer    : OptimizationEngine,
                 execution          : 'ExecutionEngine',
                 journal            : JournalManager,
                 chaos_engine       : ChaosModeEngine,
                 interval_s         : int = 30):
        self._optimizer  = optimizer
        self._execution  = execution
        self._journal    = journal
        self._chaos      = chaos_engine
        self._interval   = interval_s
        self._thread     : Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def start(self):
        self._thread = threading.Thread(
            target = self._run, daemon = True, name = "OptimizationThread"
        )
        self._thread.start()
        logger.info(f"OptimizationThread started (interval={self._interval}s)")

    def stop(self):
        self._stop_event.set()

    def _run(self):
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=self._interval)
            if self._stop_event.is_set():
                break
            try:
                self._evaluate()
            except Exception as e:
                logger.error(f"OptimizationThread error: {e}")

    def _evaluate(self):
        """Evaluate all active trades through Layer 10."""
        chaos_phase = self._chaos.get_state()
        active      = self._execution._active_trades if hasattr(
            self._execution, "_active_trades"
        ) else {}

        if not active:
            return

        for trade_id, trade_record in list(active.items()):
            try:
                state = self._build_trade_state(trade_record, chaos_phase)
                decision = self._optimizer.evaluate(state, chaos_phase)

                if decision.action != "HOLD":
                    logger.info(
                        f"[Layer10] {trade_id} | {decision.action} | "
                        f"{decision.reasoning[:60]}"
                    )
                    self._apply_decision(decision, trade_record)

            except Exception as e:
                logger.error(f"OptimizationThread: error on {trade_id}: {e}")

    def _build_trade_state(self, trade_record, chaos_phase: str) -> TradeState:
        """Convert ExecutionEngine TradeRecord to OptimizationEngine TradeState."""
        tr = trade_record
        return TradeState(
            trade_id           = tr.trade_id,
            pair               = tr.pair,
            direction          = tr.direction,
            entry_price        = tr.entry_price,
            current_price      = getattr(tr, "current_price", tr.entry_price),
            stop_loss          = tr.stop_loss,
            take_profit        = tr.take_profit,
            lot_size           = tr.lot_size,
            atr_pips           = getattr(tr, "atr_pips", 8.5),
            current_atr_pips   = getattr(tr, "current_atr_pips",
                                          getattr(tr, "atr_pips", 8.5)),
            risk_amount        = getattr(tr, "risk_amount", 100.0),
            pfvw_active        = getattr(tr, "pfvw_active", True),
            hold_hours         = tr.age_hours() if hasattr(tr, "age_hours") else 0.0,
            regime             = getattr(tr, "regime", "TREND"),
            mtf_score_current  = getattr(tr, "mtf_score_current",
                                          getattr(tr, "mtf_score_at_entry", 70.0)),
            mtf_score_at_entry = getattr(tr, "mtf_score_at_entry", 70.0),
            unrealized_pnl     = getattr(tr, "unrealized_pnl", 0.0),
            unrealized_pips    = getattr(tr, "unrealized_pips", 0.0),
            current_rr         = getattr(tr, "current_rr", 0.0),
            chaos_active       = (chaos_phase in {
                ChaosPhase.CHAOS_ACTIVE, ChaosPhase.COOLDOWN
            }),
        )

    def _apply_decision(self, decision, trade_record):
        """
        Apply Layer 10 decision to the execution engine.
        Layer 10 decisions are advisory — ExecutionEngine executes them.
        """
        action = decision.action

        if action == "FULL_EXIT":
            logger.info(
                f"[Layer10→Layer5] FULL_EXIT {decision.trade_id} | "
                f"{decision.exit_reason}"
            )
            # ExecutionEngine would handle this via its trade manager
            # Calling close if method available
            if hasattr(self._execution, "close_trade"):
                self._execution.close_trade(
                    decision.trade_id,
                    reason=decision.exit_reason or "LAYER10_EXIT"
                )

        elif action in ("TRAIL_STOP", "MOVE_TO_BREAKEVEN", "TIGHTEN_STOP"):
            if decision.new_stop_loss and hasattr(self._execution, "update_stop"):
                self._execution.update_stop(
                    decision.trade_id,
                    decision.new_stop_loss
                )

        elif action == "PARTIAL_EXIT":
            if hasattr(self._execution, "partial_close"):
                self._execution.partial_close(
                    decision.trade_id,
                    fraction=decision.exit_fraction
                )


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — SIGNAL PROCESSOR
# ═══════════════════════════════════════════════════════════════════════════════

class SignalProcessor:
    """
    Runs the full signal pipeline for one tick cycle:
        Layer 2 → Layer 3 → Gate 4A → Gate 4B → Layer 5 → Layer 6

    Called by the main loop on every tick (throttled by tick_interval_ms).
    """

    def __init__(self,
                 feeds       : Layer1FeedBundle,
                 feature_eng : FeatureEngine,
                 decision_eng: DecisionEngine,
                 risk_ctrl   : RiskControlManager,
                 portfolio   : PortfolioExposureManager,
                 execution   : ExecutionEngine,
                 journal     : JournalManager,
                 chaos       : ChaosModeEngine,
                 cfg         : SystemConfig):
        self._feeds    = feeds
        self._l2       = feature_eng
        self._l3       = decision_eng
        self._l4a      = risk_ctrl
        self._l4b      = portfolio
        self._l5       = execution
        self._l6       = journal
        self._chaos    = chaos
        self._cfg      = cfg
        self._cycle    = 0

    def run_cycle(self, pairs: list):
        """
        Run one full signal cycle across all pairs.
        """
        self._cycle += 1
        chaos_state = self._chaos.get_state()

        # Layer 9 blocks new signals in CHAOS + COOLDOWN
        if not self._chaos.is_signals_active():
            logger.debug(
                f"Cycle {self._cycle}: signals suspended "
                f"(chaos phase={chaos_state})"
            )
            return

        # ── Layer 2: Feature Engine ───────────────────────────────────────────
        try:
            # Sanitize calendar — pair_window must be dict, never None
            _cal_raw = self._feeds.calendar.get_layer1_package() or {}
            if _cal_raw.get("pair_window") is None:
                _cal_raw["pair_window"] = {}

            feature_packages = self._l2.compute_all_pairs(
                pairs              = pairs,
                layer1_price_mgr   = self._feeds.price_feed_l1,
                layer1_calendar    = _cal_raw,
                layer1_multi_asset = self._feeds.multi_asset.get_layer1_package() or {},
                layer1_cot         = self._feeds.cot.get_layer1_package() or {},
                layer1_news        = self._feeds.news.get_layer1_package() or {},
                layer1_options     = self._feeds.options.get_layer1_package() or {},
            )
        except Exception as e:
            logger.error(f"Layer 2 error: {e}")
            logger.error(traceback.format_exc())
            return

        if not feature_packages:
            return

        # Filter out pairs where feature computation returned None
        feature_packages = {
            pair: fp for pair, fp in feature_packages.items()
            if fp is not None
        }

        if not feature_packages:
            return

        # ── Layer 9: Stress reading from feature packages ─────────────────────
        self._update_chaos_monitor(feature_packages)

        # ── Layer 3: Decision Engine ──────────────────────────────────────────
        try:
            decisions = self._l3.decide_all_pairs(feature_packages)
        except Exception as e:
            logger.error(f"Layer 3 error: {e}")
            return

        # ── Process each pair decision ────────────────────────────────────────
        for pair, decision in decisions.items():
            try:
                self._process_pair(
                    pair, decision, feature_packages.get(pair), chaos_state
                )
            except Exception as e:
                logger.error(f"Pair {pair} processing error: {e}")
                logger.debug(traceback.format_exc())

    def _process_pair(self, pair: str, decision: DecisionOutput,
                       fp: FeaturePackage, chaos_state: str):
        """
        Process one pair from Layer 3 decision through to execution or NULL log.
        """
        # ── NULL path: log and return ─────────────────────────────────────────
        if decision.primary_null is not None:
            self._l6.record_null(
                pair        = pair,
                decision    = decision.to_dict(),
                feature_pkg = fp.to_dict() if fp else {},
            )
            logger.debug(
                f"{pair}: NULL_{decision.primary_null} | "
                f"{decision.null_reason[:60] if decision.null_reason else ''}"
            )
            return

        # ── Signal path: Gate 4A → 4B → Layer 5 ──────────────────────────────
        logger.info(
            f"{pair}: SIGNAL {decision.direction} | "
            f"prob={decision.trade_probability:.2f} | "
            f"conf={decision.confidence_score:.0f} | "
            f"regime={decision.regime_tag}"
        )

        # Gate 4A — Risk Control
        try:
            risk_output = self._l4a.process(
                decision        = decision,
                feature_package = fp,
            )
        except Exception as e:
            logger.error(f"Layer 4A error on {pair}: {e}")
            return

        if risk_output.risk_state == RiskState.BLOCKED:
            self._l6.record_null(
                pair        = pair,
                decision    = {**decision.to_dict(),
                               "primary_null": "NULL_RISK",
                               "null_reason" : "Layer 4A block"},
                feature_pkg = fp.to_dict() if fp else {},
            )
            logger.info(f"{pair}: NULL_RISK — Layer 4A blocked")
            return

        # Layer 9: check risk cap
        risk_cap = self._chaos.get_risk_cap()
        if risk_cap < risk_output.risk_pct:
            risk_output.risk_pct  = risk_cap
            risk_output.lot_size  = self._l4a._lot_calc.calculate(
                self._l4a._account.balance,
                risk_cap / 100,
                risk_output.atr_pips,
                fp.pair if fp else pair,
            ).lot_size if hasattr(self._l4a, '_lot_calc') else risk_output.lot_size
            logger.info(
                f"{pair}: Risk cap applied by Layer 9: "
                f"{risk_cap}% (chaos phase={chaos_state})"
            )

        # Gate 4B — Portfolio Exposure
        try:
            portfolio_output = self._l4b.process(
                risk_output     = risk_output,
                decision        = decision,
                feature_package = fp,
            )
        except Exception as e:
            logger.error(f"Layer 4B error on {pair}: {e}")
            return

        if portfolio_output.null_issued:
            self._l6.record_null(
                pair        = pair,
                decision    = {**decision.to_dict(),
                               "primary_null": "NULL_RISK",
                               "null_reason" : "Layer 4B portfolio block"},
                feature_pkg = fp.to_dict() if fp else {},
            )
            logger.info(f"{pair}: NULL_RISK — Layer 4B blocked (portfolio)")
            return

        # Layer 5 — Execution
        market = self._get_market_snapshot(pair, fp)
        if market is None:
            logger.warning(f"{pair}: no market snapshot — execution skipped")
            return

        try:
            exec_result = self._l5.execute(
                portfolio_output  = portfolio_output,
                decision          = decision,
                market            = market,
                entry_price       = market.mid(),
                atr_pips          = risk_output.atr_pips,
            )
        except Exception as e:
            logger.error(f"Layer 5 error on {pair}: {e}")
            return

        if not exec_result or exec_result.get("status") != "FILLED":
            logger.info(
                f"{pair}: execution not filled | "
                f"status={exec_result.get('status') if exec_result else 'None'}"
            )
            return

        # Layer 6 — Journal trade open
        try:
            self._l6.record_trade_open(
                trade_id         = exec_result.get("trade_id", ""),
                pair             = pair,
                direction        = decision.direction or "",
                decision         = decision.to_dict(),
                feature_pkg      = fp.to_dict() if fp else {},
                risk_output      = risk_output.to_dict() if hasattr(risk_output, "to_dict") else {},
                portfolio_output = portfolio_output.to_dict() if hasattr(portfolio_output, "to_dict") else {},
                execution_result = exec_result,
            )
        except Exception as e:
            logger.error(f"Layer 6 journal error on {pair}: {e}")

        logger.info(
            f"✅ {pair} {decision.direction} OPENED | "
            f"trade_id={exec_result.get('trade_id')} | "
            f"entry={exec_result.get('fill_price', 'N/A')}"
        )

    def _update_chaos_monitor(self, feature_packages: dict):
        """
        Build a StressReading from current feature packages and feed Layer 9.
        """
        try:
            # Aggregate across all pairs
            max_spread_ratio = 1.0
            multi_null_count = 0
            for pair, fp in feature_packages.items():
                if fp:
                    spread_r = getattr(fp, "spread_ratio", 1.0) or 1.0
                    max_spread_ratio = max(max_spread_ratio, spread_r)

            # Multi-asset data for VIX + DXY
            multi = self._feeds.multi_asset.get_layer1_package() or {}
            vix   = multi.get("vix_level", 15.0) or 15.0
            dxy_m = multi.get("dxy_move_pct", 0.0) or 0.0
            corr_break   = multi.get("correlation_breakdown", False)
            liq_collapse = multi.get("liquidity_collapsed", False)
            iv_distorted = multi.get("iv_surface_distorted", False)

            reading = StressReading(
                timestamp_utc         = time.time() * 1000,
                spread_ratio          = max_spread_ratio,
                vix_level             = vix,
                dxy_move_pct          = dxy_m,
                correlation_breakdown = corr_break,
                iv_surface_distorted  = iv_distorted,
                liquidity_collapsed   = liq_collapse,
                null_types_firing     = multi_null_count,
            )
            self._chaos.assess_market(reading)
        except Exception as e:
            logger.debug(f"Chaos monitor update error: {e}")

    def _get_market_snapshot(self, pair: str,
                              fp) -> Optional[MarketSnapshot]:
        """Build MarketSnapshot from current feed state."""
        try:
            state = self._feeds.price_feed.get_state(pair)
            if not state:
                return None
            bid = state.get("bid") or state.get("last_bid")
            ask = state.get("ask") or state.get("last_ask")
            if not bid or not ask:
                return None
            pip_size   = 0.01 if "JPY" in pair else 0.0001
            spread_pips= round((ask - bid) / pip_size, 2)
            avg_spread = spread_pips  # fallback — use current as average
            spread_ratio = 1.0
            return MarketSnapshot(
                pair            = pair,
                bid             = bid,
                ask             = ask,
                spread_pips     = spread_pips,
                avg_spread_pips = avg_spread,
                latency_ms      = state.get("latency_ms"),
                session         = state.get("active_session", "OVERLAP"),
                spread_ratio    = spread_ratio,
                timestamp_utc   = state.get("timestamp_utc", time.time() * 1000),
                feed_health     = state.get("feed_health", "HEALTHY"),
            )
        except Exception as e:
            logger.debug(f"MarketSnapshot error {pair}: {e}")
            return None


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — OANDA LIVE FEED CONNECTOR
# ═══════════════════════════════════════════════════════════════════════════════

class OANDALiveFeedConnector:
    """
    Connects OANDA streaming feed to FeedManager.
    Spawns one streaming thread per currency pair batch.
    Falls back to poll-based fetch if streaming unavailable.
    """

    def __init__(self, feeds: Layer1FeedBundle, cfg: SystemConfig):
        self._feeds   = feeds
        self._cfg     = cfg
        self._streams = []

    def start(self):
        if not self._cfg.oanda_api_key:
            logger.warning(
                "No OANDA API key — price feed will use mock/paper data. "
                "Set OANDA_API_KEY env var for live data."
            )
            self._start_mock_feed()
            return

        try:
            from oanda_feed import OANDAStreamingFeed
            for pair in self._cfg.pairs:
                oanda_instrument = pair.replace("/", "_")
                stream = OANDAStreamingFeed(
                    api_key    = self._cfg.oanda_api_key,
                    account_id = self._cfg.oanda_account_id or "",
                    instrument = oanda_instrument,
                    practice   = self._cfg.oanda_practice,
                    on_tick    = lambda ts, bid, ask, p=pair: (
                        self._feeds.on_tick(p, ts, bid, ask)
                    ),
                )
                stream.start()
                self._streams.append(stream)
                logger.info(f"OANDA stream started: {pair}")
        except Exception as e:
            logger.error(f"OANDA stream failed: {e} — falling back to mock")
            self._start_mock_feed()

    def stop(self):
        for stream in self._streams:
            try: stream.stop()
            except Exception: pass

    def _start_mock_feed(self):
        """
        Paper/mock tick generator — used when no live feed available.
        Generates realistic synthetic ticks for testing.
        """
        import random
        BASE_PRICES = {
            "EUR/USD": 1.0850, "GBP/USD": 1.2700, "USD/JPY": 148.50,
            "AUD/USD": 0.6500, "USD/CAD": 1.3600, "NZD/USD": 0.5950,
        }

        def _mock_loop():
            prices = dict(BASE_PRICES)
            rng    = random.Random()
            while not _stop.is_set():
                for pair in self._cfg.pairs:
                    pip   = 0.01 if "JPY" in pair else 0.0001
                    move  = rng.gauss(0, pip * 3)
                    prices[pair] = max(0.1, prices.get(pair, 1.0) + move)
                    bid   = prices[pair]
                    ask   = bid + pip * rng.uniform(0.8, 2.5)
                    self._feeds.on_tick(
                        pair, time.time() * 1000, bid, ask
                    )
                time.sleep(self._cfg.tick_interval_ms / 1000)

        _stop  = threading.Event()
        thread = threading.Thread(
            target=_mock_loop, daemon=True, name="MockFeed"
        )
        thread.start()
        self._streams.append(type("_MockStream", (), {
            "stop": lambda self=None: _stop.set()
        })())
        logger.info("Mock tick feed started (paper trading mode)")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — TRADE CLOSE HANDLER
# ═══════════════════════════════════════════════════════════════════════════════

class TradeCloseHandler:
    """
    Listens for trade close events from Layer 5 and journals them to Layer 6.
    Registers as a callback on the ExecutionEngine.
    """

    def __init__(self, journal: JournalManager,
                 decision_eng: DecisionEngine,
                 chaos: ChaosModeEngine):
        self._journal = journal
        self._l3      = decision_eng
        self._chaos   = chaos

    def on_trade_closed(self, trade_id   : str,
                         pair            : str,
                         exit_price      : float,
                         exit_reason     : str,
                         realized_pnl    : float,
                         pnl_pips        : float,
                         actual_rr       : float,
                         trade_status    : str,
                         hold_hours      : float,
                         regime_at_exit  : Optional[str] = None,
                         mtf_at_exit     : Optional[float] = None):
        """Called by Layer 5 when a trade closes."""
        try:
            self._journal.record_trade_close(
                trade_id       = trade_id,
                exit_price     = exit_price,
                exit_reason    = exit_reason,
                realized_pnl   = realized_pnl,
                pnl_pips       = pnl_pips,
                actual_rr      = actual_rr,
                trade_status   = trade_status,
                hold_hours     = hold_hours,
                regime_at_exit = regime_at_exit,
                mtf_at_exit    = mtf_at_exit,
            )

            # Feed outcome back to Layer 3 EV engine
            if regime_at_exit:
                self._l3.record_trade_outcome(
                    pair   = pair,
                    regime = regime_at_exit,
                    pnl    = realized_pnl,
                    won    = realized_pnl > 0,
                )

            logger.info(
                f"Trade closed & journaled: {trade_id} | "
                f"{pair} | PnL=${realized_pnl:.2f} | "
                f"RR={actual_rr:.2f} | {exit_reason}"
            )
        except Exception as e:
            logger.error(f"TradeCloseHandler error: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — TRADING SYSTEM (MASTER CLASS)
# ═══════════════════════════════════════════════════════════════════════════════

class TradingSystem:
    """
    The complete assembled system.

    Instantiates every layer, wires them together, and runs the main loop.

    Usage:
        system = TradingSystem(cfg)
        system.start()         # blocking — runs until Ctrl+C or shutdown()
        system.shutdown()      # graceful stop from another thread
    """

    def __init__(self, cfg: SystemConfig):
        self._cfg      = cfg
        self._running  = False
        self._shutdown = threading.Event()

        logger.info("═" * 60)
        logger.info("FOREX TRADING SIGNAL SYSTEM v6.0 — INITIALISING")
        logger.info("═" * 60)
        logger.info(f"Account balance : ${cfg.account_balance:,.2f}")
        logger.info(f"Paper trading   : {cfg.paper_trading}")
        logger.info(f"Pairs           : {', '.join(cfg.pairs)}")

        # ── Layer 1 ───────────────────────────────────────────────────────────
        self.feeds      = Layer1FeedBundle(cfg)

        # ── Layer 2 ───────────────────────────────────────────────────────────
        self.feature_eng= FeatureEngine()

        # ── Layer 3 ───────────────────────────────────────────────────────────
        self.decision   = DecisionEngine()

        # ── Layer 4A ──────────────────────────────────────────────────────────
        self.risk_ctrl  = RiskControlManager(
            initial_balance = cfg.account_balance
        )

        # ── Layer 4B ──────────────────────────────────────────────────────────
        self.portfolio  = PortfolioExposureManager(
            account_balance = cfg.account_balance
        )

        # ── Layer 5 ───────────────────────────────────────────────────────────
        self.execution  = ExecutionEngine(
            paper_trading   = cfg.paper_trading,
            account_balance = cfg.account_balance,
        )

        # ── Layer 6 ───────────────────────────────────────────────────────────
        self.journal    = JournalManager(db_path=cfg.journal_db_path)

        # ── Layer 7 ───────────────────────────────────────────────────────────
        self.validation = ValidationEngine()

        # ── Layer 8 ───────────────────────────────────────────────────────────
        self.learning   = LearningLoop(validation_engine=self.validation)

        # ── Layer 9 ───────────────────────────────────────────────────────────
        self.chaos      = ChaosModeEngine()

        # ── Layer 10 ──────────────────────────────────────────────────────────
        self.optimizer  = OptimizationEngine()

        # ── Signal processor ──────────────────────────────────────────────────
        self.signal_proc = SignalProcessor(
            feeds        = self.feeds,
            feature_eng  = self.feature_eng,
            decision_eng = self.decision,
            risk_ctrl    = self.risk_ctrl,
            portfolio    = self.portfolio,
            execution    = self.execution,
            journal      = self.journal,
            chaos        = self.chaos,
            cfg          = cfg,
        )

        # ── Live feed connector ───────────────────────────────────────────────
        self.live_feed  = OANDALiveFeedConnector(self.feeds, cfg)

        # ── Trade close handler ───────────────────────────────────────────────
        self.close_handler = TradeCloseHandler(
            journal      = self.journal,
            decision_eng = self.decision,
            chaos        = self.chaos,
        )

        # ── Background threads ────────────────────────────────────────────────
        self.learning_thread = LearningThread(
            journal    = self.journal,
            validation = self.validation,
            learning   = self.learning,
            interval_h = cfg.learning_cycle_hours,
        )
        self.opt_thread = OptimizationThread(
            optimizer   = self.optimizer,
            execution   = self.execution,
            journal     = self.journal,
            chaos_engine= self.chaos,
            interval_s  = cfg.optimization_interval_s,
        )

        # Wire trade close callback into Layer 5
        if hasattr(self.execution, "set_close_callback"):
            self.execution.set_close_callback(self.close_handler.on_trade_closed)

        logger.info("All layers initialised ✅")

    def start(self):
        """Start the system. Blocks until shutdown() is called or Ctrl+C."""
        # Install signal handlers
        signal.signal(signal.SIGINT,  self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        logger.info("Starting all subsystems...")

        # Start Layer 1 feed threads
        self.feeds.start()

        # Start live price feed
        self.live_feed.start()

        # Start background threads
        self.learning_thread.start()
        self.opt_thread.start()

        self._running = True
        logger.info("═" * 60)
        logger.info("SYSTEM RUNNING — press Ctrl+C to stop")
        logger.info("═" * 60)

        self._main_loop()

    def shutdown(self, reason: str = "MANUAL"):
        """Graceful shutdown from any thread."""
        if self._running:
            logger.info(f"Shutdown requested: {reason}")
            self._shutdown.set()

    def _main_loop(self):
        """
        Main signal loop.
        Runs until shutdown event is set.
        Throttled by tick_interval_ms.
        """
        interval_s = self._cfg.tick_interval_ms / 1000.0
        cycle      = 0

        while not self._shutdown.is_set():
            try:
                cycle += 1
                t_start = time.time()

                # Run one full signal cycle
                self.signal_proc.run_cycle(self._cfg.pairs)

                # Throttle to avoid hammering Layer 2
                elapsed = time.time() - t_start
                sleep_t = max(0.0, interval_s - elapsed)
                if sleep_t > 0:
                    self._shutdown.wait(timeout=sleep_t)

            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Main loop error (cycle {cycle}): {e}")
                logger.debug(traceback.format_exc())
                time.sleep(1.0)   # Brief pause before retry

        self._teardown()

    def _teardown(self):
        """Graceful shutdown of all components."""
        logger.info("Shutting down...")
        self._running = False

        # Stop background threads
        self.learning_thread.stop()
        self.opt_thread.stop()

        # Stop feeds
        self.live_feed.stop()
        self.feeds.stop()

        # Final journal snapshot
        try:
            self.journal.save_performance_snapshot()
            counts = self.journal.get_total_counts()
            logger.info(
                f"Final journal: "
                f"{counts['total_trades']} trades | "
                f"{counts['total_nulls']} NULLs"
            )
        except Exception:
            pass

        logger.info("═" * 60)
        logger.info("SYSTEM SHUTDOWN COMPLETE")
        logger.info("═" * 60)

    def _handle_signal(self, signum, frame):
        """OS signal handler for Ctrl+C / SIGTERM."""
        self.shutdown(reason=f"OS_SIGNAL_{signum}")

    def get_status(self) -> dict:
        """Return current system status snapshot."""
        chaos_status = self.chaos.get_full_status()
        journal_counts = self.journal.get_total_counts()
        return {
            "running"          : self._running,
            "pairs"            : self._cfg.pairs,
            "paper_trading"    : self._cfg.paper_trading,
            "chaos_phase"      : chaos_status["state"],
            "trading_allowed"  : chaos_status["trading_allowed"],
            "risk_cap_pct"     : chaos_status["risk_cap_pct"],
            "total_trades"     : journal_counts["total_trades"],
            "total_nulls"      : journal_counts["total_nulls"],
            "null_rate_pct"    : journal_counts["null_rate"],
            "account_balance"  : self._cfg.account_balance,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    """
    System entry point.

    Usage:
        python main.py                          # paper trading, defaults
        PAPER_TRADING=false python main.py      # live trading
        OANDA_API_KEY=xxx python main.py        # with live data feed
        ACCOUNT_BALANCE=50000 python main.py    # custom balance
    """
    # Load config from environment
    cfg = SystemConfig().load_from_env()

    # Set log level
    logging.getLogger().setLevel(getattr(logging, cfg.log_level, logging.INFO))

    # Print startup banner
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║     FOREX TRADING SIGNAL SYSTEM — VERSION 6.0           ║")
    print("║     10-Layer Architecture | Evidence-Driven             ║")
    print("╠══════════════════════════════════════════════════════════╣")
    print(f"║  Mode    : {'PAPER TRADING' if cfg.paper_trading else '⚠️  LIVE TRADING':52s}║")
    print(f"║  Balance : ${cfg.account_balance:>10,.2f}                                  ║")
    print(f"║  Pairs   : {', '.join(cfg.pairs):<48s}║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()

    # Build and start
    system = TradingSystem(cfg)
    system.start()   # Blocks until Ctrl+C


if __name__ == "__main__":
    main()
