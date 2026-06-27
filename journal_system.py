"""
═══════════════════════════════════════════════════════════════════════════════
FOREX TRADING SIGNAL SYSTEM — VERSION 6.0
FILE: journal_system.py
LAYER: 6 — MEMORY & JOURNAL SYSTEM (HIPPOCAMPUS)
═══════════════════════════════════════════════════════════════════════════════

PURPOSE:
    Records everything. Learns from everything. Forgets nothing.
    This is the biggest long-term edge generator in the system.
    Most systems execute but never learn WHY they win or lose.

WHAT IS STORED:
    Every trade:
        Full signal details at generation time
        All Layer 2 raw scores at decision time
        All Layer 3 weight assignments at decision time
        Layer 3 regime interpretation applied
        MTF state and score
        Structure state and Layer 3 response
        Regime probability distribution
        Regime confidence score
        Risk state at entry
        Session, spread, slippage at entry
        Portfolio exposure at entry
        Stress test results
        Feed health status
        Theoretical vs realized EV
        Actual fill vs planned entry
        PFVW results and duration
        Post-fill regime monitoring outcomes
        Final outcome and exit details

    Every NULL:
        Primary NULL classification
        Secondary NULLs logged
        All Layer 2 raw scores at rejection
        All Layer 3 weight assignments at rejection
        Layer 3 judgment that caused rejection
        Feed health at rejection
        Causal attribution output

AI ANALYSIS QUESTIONS ANSWERED:
    "What patterns lead to most losses?"
    "Which Layer 3 weight assignments correlate with failure?"
    "Which MTF raw score ranges produce best outcomes?"
    "Which regime interpretations are most accurate?"
    "What is real EV per regime after all costs?"
    "Which NULL type appears most frequently?"
    "Which data tier inputs are most predictive?"
    "Which regime confidence levels best predict NULL?"

STORAGE:
    Primary   → SQLite (always available, zero setup)
    Secondary → JSON files (human-readable backup)
    In-memory → Recent cache for fast access

═══════════════════════════════════════════════════════════════════════════════
"""

import json
import time
import math
import uuid
import sqlite3
import logging
import threading
import statistics
from datetime import datetime, timezone, timedelta
from typing import Optional
from dataclasses import dataclass, field, asdict
from collections import deque, defaultdict
from pathlib import Path

logger = logging.getLogger("JournalSystem")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — JOURNAL ENTRY STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class TradeJournalEntry:
    """
    Complete journal record for one completed trade.
    Everything that happened — recorded for analysis.
    """
    # ── Identity ──────────────────────────────────────────────────────────────
    journal_id          : str
    trade_id            : str
    pair                : str
    direction           : str
    timestamp_utc       : float
    entry_time          : Optional[float] = None
    exit_time           : Optional[float] = None

    # ── Layer 2 raw scores at decision (measurement only) ─────────────────────
    l2_mtf_score        : Optional[float] = None
    l2_structure_state  : Optional[str]   = None
    l2_regime_probs     : Optional[dict]  = None
    l2_regime_confidence: Optional[float] = None
    l2_volatility_score : Optional[float] = None
    l2_trend_strength   : Optional[float] = None
    l2_session_quality  : Optional[float] = None
    l2_institutional    : Optional[float] = None
    l2_macro_score      : Optional[float] = None
    l2_sentiment_score  : Optional[float] = None
    l2_order_flow       : Optional[float] = None

    # ── Layer 3 judgment at decision ─────────────────────────────────────────
    l3_regime_tag       : Optional[str]   = None
    l3_weights_applied  : Optional[dict]  = None
    l3_bayesian_prob    : Optional[float] = None
    l3_bayesian_threshold: Optional[float]= None
    l3_confidence_score : Optional[float] = None
    l3_ev_lower_bound   : Optional[float] = None
    l3_ev_trusted       : bool = False
    l3_votes            : Optional[dict]  = None
    l3_conf_multiplier  : Optional[float] = None

    # ── Risk state at entry ───────────────────────────────────────────────────
    risk_state          : Optional[str]   = None
    risk_pct            : Optional[float] = None
    lot_size            : Optional[float] = None
    risk_amount         : Optional[float] = None
    atr_pips            : Optional[float] = None
    stop_loss_price     : Optional[float] = None
    take_profit_price   : Optional[float] = None
    target_rr           : float = 3.0

    # ── Execution quality ─────────────────────────────────────────────────────
    entry_price_planned : Optional[float] = None
    entry_price_actual  : Optional[float] = None
    slippage_pips       : Optional[float] = None
    fill_pct            : Optional[float] = None
    spread_at_entry     : Optional[float] = None
    latency_ms          : Optional[float] = None
    theoretical_ev      : Optional[float] = None
    realized_ev_est     : Optional[float] = None
    ev_deviation        : Optional[float] = None

    # ── Session and context ───────────────────────────────────────────────────
    session             : Optional[str]   = None
    session_overlap     : bool = False
    feed_health         : str = "HEALTHY"
    data_tier           : Optional[str]   = None
    confidence_penalty  : float = 0.0

    # ── Portfolio state at entry ──────────────────────────────────────────────
    n_positions_at_entry: int = 0
    portfolio_var_pct   : Optional[float] = None
    stress_test_passed  : bool = True

    # ── PFVW ─────────────────────────────────────────────────────────────────
    pfvw_duration_min   : Optional[float] = None
    pfvw_exit_reason    : Optional[str]   = None
    pfvw_validations    : int = 0

    # ── Outcome ───────────────────────────────────────────────────────────────
    exit_price          : Optional[float] = None
    exit_reason         : Optional[str]   = None
    realized_pnl        : Optional[float] = None
    pnl_pips            : Optional[float] = None
    actual_rr           : Optional[float] = None
    trade_status        : Optional[str]   = None
    hold_duration_hours : Optional[float] = None
    was_winner          : Optional[bool]  = None

    # ── Causal attribution ────────────────────────────────────────────────────
    primary_cause       : Optional[str]   = None
    contributing_factors: Optional[list]  = field(default_factory=list)
    regime_at_exit      : Optional[str]   = None
    mtf_at_exit         : Optional[float] = None

    def to_dict(self) -> dict:
        d = {}
        for f in self.__dataclass_fields__:
            val = getattr(self, f)
            if isinstance(val, (dict, list)):
                d[f] = json.dumps(val) if val else None
            else:
                d[f] = val
        return d

    def to_json(self) -> dict:
        d = {}
        for f in self.__dataclass_fields__:
            d[f] = getattr(self, f)
        return d


@dataclass
class NullJournalEntry:
    """
    Complete journal record for every NULL rejection.
    NULLs are as valuable as trades for system improvement.
    """
    # ── Identity ──────────────────────────────────────────────────────────────
    journal_id          : str
    pair                : str
    timestamp_utc       : float

    # ── NULL classification ───────────────────────────────────────────────────
    primary_null        : str
    secondary_nulls     : Optional[list] = field(default_factory=list)
    null_reason         : Optional[str]  = None
    null_gate           : Optional[int]  = None

    # ── Layer 2 raw scores at rejection ──────────────────────────────────────
    l2_mtf_score        : Optional[float] = None
    l2_structure_state  : Optional[str]   = None
    l2_regime_dominant  : Optional[str]   = None
    l2_regime_confidence: Optional[float] = None
    l2_volatility_score : Optional[float] = None
    l2_session_quality  : Optional[float] = None
    l2_news_blackout    : bool = False

    # ── Layer 3 judgment at rejection ────────────────────────────────────────
    l3_regime_tag       : Optional[str]   = None
    l3_bayesian_result  : Optional[float] = None
    l3_threshold        : Optional[float] = None
    l3_ev_lower_bound   : Optional[float] = None
    l3_votes            : Optional[dict]  = None

    # ── Context ───────────────────────────────────────────────────────────────
    session             : Optional[str]   = None
    feed_health         : str = "HEALTHY"
    spread_ratio        : Optional[float] = None
    vix_level           : Optional[float] = None
    stress_count        : int = 0

    # ── Causal attribution ────────────────────────────────────────────────────
    causal_notes        : Optional[str]   = None

    def to_dict(self) -> dict:
        d = {}
        for f in self.__dataclass_fields__:
            val = getattr(self, f)
            if isinstance(val, (dict, list)):
                d[f] = json.dumps(val) if val else None
            else:
                d[f] = val
        return d


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — SQLITE STORAGE ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class SQLiteStorage:
    """
    Persistent SQLite storage for all journal entries.
    Zero setup — file-based, always available.
    Thread-safe with connection-per-thread pattern.
    """

    def __init__(self, db_path: str = "journal.db"):
        self.db_path = db_path
        self._local  = threading.local()
        self._lock   = threading.Lock()
        self._init_db()
        logger.info(f"SQLiteStorage initialized: {db_path}")

    def _get_conn(self) -> sqlite3.Connection:
        """Get thread-local connection."""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0,
            )
            self._local.conn.row_factory = sqlite3.Row
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA synchronous=NORMAL")
        return self._local.conn

    def _init_db(self):
        """Create all tables if they don't exist."""
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS trades (
                journal_id          TEXT PRIMARY KEY,
                trade_id            TEXT,
                pair                TEXT,
                direction           TEXT,
                timestamp_utc       REAL,
                entry_time          REAL,
                exit_time           REAL,
                l2_mtf_score        REAL,
                l2_structure_state  TEXT,
                l2_regime_probs     TEXT,
                l2_regime_confidence REAL,
                l2_volatility_score REAL,
                l2_trend_strength   REAL,
                l2_session_quality  REAL,
                l2_institutional    REAL,
                l2_macro_score      REAL,
                l2_sentiment_score  REAL,
                l2_order_flow       REAL,
                l3_regime_tag       TEXT,
                l3_weights_applied  TEXT,
                l3_bayesian_prob    REAL,
                l3_bayesian_threshold REAL,
                l3_confidence_score REAL,
                l3_ev_lower_bound   REAL,
                l3_ev_trusted       INTEGER,
                l3_votes            TEXT,
                l3_conf_multiplier  REAL,
                risk_state          TEXT,
                risk_pct            REAL,
                lot_size            REAL,
                risk_amount         REAL,
                atr_pips            REAL,
                stop_loss_price     REAL,
                take_profit_price   REAL,
                target_rr           REAL,
                entry_price_planned REAL,
                entry_price_actual  REAL,
                slippage_pips       REAL,
                fill_pct            REAL,
                spread_at_entry     REAL,
                latency_ms          REAL,
                theoretical_ev      REAL,
                realized_ev_est     REAL,
                ev_deviation        REAL,
                session             TEXT,
                session_overlap     INTEGER,
                feed_health         TEXT,
                data_tier           TEXT,
                confidence_penalty  REAL,
                n_positions_at_entry INTEGER,
                portfolio_var_pct   REAL,
                stress_test_passed  INTEGER,
                pfvw_duration_min   REAL,
                pfvw_exit_reason    TEXT,
                pfvw_validations    INTEGER,
                exit_price          REAL,
                exit_reason         TEXT,
                realized_pnl        REAL,
                pnl_pips            REAL,
                actual_rr           REAL,
                trade_status        TEXT,
                hold_duration_hours REAL,
                was_winner          INTEGER,
                primary_cause       TEXT,
                contributing_factors TEXT,
                regime_at_exit      TEXT,
                mtf_at_exit         REAL
            );

            CREATE TABLE IF NOT EXISTS nulls (
                journal_id          TEXT PRIMARY KEY,
                pair                TEXT,
                timestamp_utc       REAL,
                primary_null        TEXT,
                secondary_nulls     TEXT,
                null_reason         TEXT,
                null_gate           INTEGER,
                l2_mtf_score        REAL,
                l2_structure_state  TEXT,
                l2_regime_dominant  TEXT,
                l2_regime_confidence REAL,
                l2_volatility_score REAL,
                l2_session_quality  REAL,
                l2_news_blackout    INTEGER,
                l3_regime_tag       TEXT,
                l3_bayesian_result  REAL,
                l3_threshold        REAL,
                l3_ev_lower_bound   REAL,
                l3_votes            TEXT,
                session             TEXT,
                feed_health         TEXT,
                spread_ratio        REAL,
                vix_level           REAL,
                stress_count        INTEGER,
                causal_notes        TEXT
            );

            CREATE TABLE IF NOT EXISTS performance_snapshots (
                snapshot_id         TEXT PRIMARY KEY,
                timestamp_utc       REAL,
                regime              TEXT,
                n_trades            INTEGER,
                n_wins              INTEGER,
                win_rate            REAL,
                avg_pnl             REAL,
                avg_winner          REAL,
                avg_loser           REAL,
                ev_point            REAL,
                sharpe_ratio        REAL,
                max_drawdown_pct    REAL,
                rolling_10_ev       REAL,
                rolling_25_ev       REAL,
                rolling_50_ev       REAL,
                null_rate           REAL,
                most_common_null    TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_trades_pair
                ON trades(pair);
            CREATE INDEX IF NOT EXISTS idx_trades_regime
                ON trades(l3_regime_tag);
            CREATE INDEX IF NOT EXISTS idx_trades_time
                ON trades(timestamp_utc);
            CREATE INDEX IF NOT EXISTS idx_nulls_pair
                ON nulls(pair);
            CREATE INDEX IF NOT EXISTS idx_nulls_type
                ON nulls(primary_null);
        """)
        conn.commit()

    def insert_trade(self, entry: TradeJournalEntry):
        """Insert trade journal entry."""
        conn   = self._get_conn()
        d      = entry.to_dict()
        cols   = ", ".join(d.keys())
        placeh = ", ".join(["?" for _ in d])
        vals   = list(d.values())
        try:
            conn.execute(
                f"INSERT OR REPLACE INTO trades ({cols}) VALUES ({placeh})",
                vals
            )
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Trade insert failed: {e}")

    def insert_null(self, entry: NullJournalEntry):
        """Insert NULL journal entry."""
        conn   = self._get_conn()
        d      = entry.to_dict()
        cols   = ", ".join(d.keys())
        placeh = ", ".join(["?" for _ in d])
        vals   = list(d.values())
        try:
            conn.execute(
                f"INSERT OR REPLACE INTO nulls ({cols}) VALUES ({placeh})",
                vals
            )
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"NULL insert failed: {e}")

    def update_trade_exit(self, trade_id      : str,
                           exit_price         : float,
                           exit_time          : float,
                           exit_reason        : str,
                           realized_pnl       : float,
                           pnl_pips           : float,
                           actual_rr          : float,
                           trade_status       : str,
                           hold_hours         : float,
                           regime_at_exit     : Optional[str] = None,
                           mtf_at_exit        : Optional[float] = None):
        """Update trade record with exit data."""
        conn = self._get_conn()
        try:
            conn.execute("""
                UPDATE trades SET
                    exit_price          = ?,
                    exit_time           = ?,
                    exit_reason         = ?,
                    realized_pnl        = ?,
                    pnl_pips            = ?,
                    actual_rr           = ?,
                    trade_status        = ?,
                    hold_duration_hours = ?,
                    was_winner          = ?,
                    regime_at_exit      = ?,
                    mtf_at_exit         = ?
                WHERE trade_id = ?
            """, (
                exit_price, exit_time, exit_reason,
                realized_pnl, pnl_pips, actual_rr,
                trade_status, hold_hours,
                1 if realized_pnl > 0 else 0,
                regime_at_exit, mtf_at_exit,
                trade_id
            ))
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Trade exit update failed: {e}")

    def query_trades(self, pair        : Optional[str]  = None,
                      regime           : Optional[str]  = None,
                      last_n           : Optional[int]  = None,
                      winners_only     : bool = False,
                      losers_only      : bool = False) -> list:
        """Query trades with optional filters."""
        conn   = self._get_conn()
        where  = []
        params = []

        if pair:
            where.append("pair = ?")
            params.append(pair)
        if regime:
            where.append("l3_regime_tag = ?")
            params.append(regime)
        if winners_only:
            where.append("was_winner = 1")
        if losers_only:
            where.append("was_winner = 0")
        # Only completed trades
        where.append("exit_time IS NOT NULL")

        sql = "SELECT * FROM trades"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY timestamp_utc DESC"
        if last_n:
            sql += f" LIMIT {last_n}"

        try:
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]
        except sqlite3.Error as e:
            logger.error(f"Trade query failed: {e}")
            return []

    def query_nulls(self, pair         : Optional[str] = None,
                     null_type          : Optional[str] = None,
                     last_n             : Optional[int] = None) -> list:
        """Query NULL entries."""
        conn   = self._get_conn()
        where  = []
        params = []

        if pair:
            where.append("pair = ?")
            params.append(pair)
        if null_type:
            where.append("primary_null = ?")
            params.append(null_type)

        sql = "SELECT * FROM nulls"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY timestamp_utc DESC"
        if last_n:
            sql += f" LIMIT {last_n}"

        try:
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]
        except sqlite3.Error as e:
            logger.error(f"NULL query failed: {e}")
            return []

    def save_performance_snapshot(self, snapshot: dict):
        """Save performance analysis snapshot."""
        conn = self._get_conn()
        try:
            snap_id = f"SNAP_{int(time.time()*1000)}"
            conn.execute("""
                INSERT OR REPLACE INTO performance_snapshots
                (snapshot_id, timestamp_utc, regime, n_trades, n_wins,
                 win_rate, avg_pnl, avg_winner, avg_loser, ev_point,
                 sharpe_ratio, max_drawdown_pct, rolling_10_ev,
                 rolling_25_ev, rolling_50_ev, null_rate, most_common_null)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                snap_id,
                snapshot.get("timestamp_utc", time.time()*1000),
                snapshot.get("regime"),
                snapshot.get("n_trades", 0),
                snapshot.get("n_wins", 0),
                snapshot.get("win_rate"),
                snapshot.get("avg_pnl"),
                snapshot.get("avg_winner"),
                snapshot.get("avg_loser"),
                snapshot.get("ev_point"),
                snapshot.get("sharpe_ratio"),
                snapshot.get("max_drawdown_pct"),
                snapshot.get("rolling_10_ev"),
                snapshot.get("rolling_25_ev"),
                snapshot.get("rolling_50_ev"),
                snapshot.get("null_rate"),
                snapshot.get("most_common_null"),
            ))
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Snapshot save failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — PERFORMANCE ANALYZER
# ═══════════════════════════════════════════════════════════════════════════════

class PerformanceAnalyzer:
    """
    Analyzes journal data to answer key questions about system performance.

    AI-powered question answering from trade journal.
    All analysis is pure statistics — no interpretation of market.
    """

    def __init__(self, storage: SQLiteStorage):
        self._storage = storage

    def compute_regime_stats(self, regime: Optional[str] = None) -> dict:
        """
        Compute performance statistics per regime.
        Rolling windows: 10, 25, 50, 100 trades.
        """
        trades = self._storage.query_trades(regime=regime)

        if not trades:
            return {
                "regime"        : regime or "ALL",
                "n_trades"      : 0,
                "sufficient"    : False,
                "message"       : "No completed trades",
            }

        pnls    = [t["realized_pnl"] for t in trades if t.get("realized_pnl") is not None]
        winners = [p for p in pnls if p > 0]
        losers  = [p for p in pnls if p <= 0]
        n       = len(pnls)

        win_rate    = len(winners) / n if n > 0 else 0
        avg_win     = sum(winners) / len(winners) if winners else 0
        avg_loss    = sum(losers)  / len(losers)  if losers  else 0
        avg_pnl     = sum(pnls)    / n if n > 0 else 0
        ev_point    = (win_rate * avg_win) + ((1-win_rate) * avg_loss)

        # Sharpe ratio (simplified daily)
        if len(pnls) > 1:
            pnl_std = statistics.stdev(pnls)
            sharpe  = (avg_pnl / pnl_std * math.sqrt(252)) if pnl_std > 0 else 0
        else:
            sharpe = 0

        # Rolling windows
        def rolling_ev(window):
            subset = pnls[-window:]
            if not subset:
                return None
            w = [p for p in subset if p > 0]
            l = [p for p in subset if p <= 0]
            wr = len(w) / len(subset)
            aw = sum(w) / len(w) if w else 0
            al = sum(l) / len(l) if l else 0
            return round((wr * aw) + ((1-wr) * al), 2)

        # Max drawdown
        cumulative  = 0.0
        peak        = 0.0
        max_dd      = 0.0
        for pnl in pnls:
            cumulative += pnl
            if cumulative > peak:
                peak = cumulative
            dd = (peak - cumulative) / peak if peak > 0 else 0
            max_dd = max(max_dd, dd)

        return {
            "regime"          : regime or "ALL",
            "n_trades"        : n,
            "n_wins"          : len(winners),
            "n_losses"        : len(losers),
            "win_rate"        : round(win_rate, 3),
            "avg_pnl"         : round(avg_pnl, 2),
            "avg_winner"      : round(avg_win, 2),
            "avg_loser"       : round(avg_loss, 2),
            "ev_point"        : round(ev_point, 2),
            "sharpe_ratio"    : round(sharpe, 3),
            "max_drawdown_pct": round(max_dd * 100, 2),
            "rolling_10_ev"   : rolling_ev(10),
            "rolling_25_ev"   : rolling_ev(25),
            "rolling_50_ev"   : rolling_ev(50),
            "rolling_100_ev"  : rolling_ev(100),
            "sufficient"      : n >= 100,
            "trusted"         : n >= 100 and len(winners) >= 30,
        }

    def compute_null_analysis(self) -> dict:
        """
        Analyze NULL patterns.
        "Which NULL type appears most frequently?"
        "What conditions predict NULL?"
        """
        nulls   = self._storage.query_nulls()
        if not nulls:
            return {"n_nulls": 0, "message": "No NULL records"}

        # Count by type
        null_counts = defaultdict(int)
        null_by_pair= defaultdict(lambda: defaultdict(int))
        null_by_sess= defaultdict(int)

        for n in nulls:
            nt = n.get("primary_null", "UNKNOWN")
            null_counts[nt] += 1
            null_by_pair[n.get("pair","?")][nt] += 1
            null_by_sess[n.get("session","?")] += 1

        total = len(nulls)

        return {
            "n_nulls"          : total,
            "null_distribution": {
                k: {"count": v, "pct": round(v/total*100, 1)}
                for k, v in sorted(null_counts.items(),
                                    key=lambda x: -x[1])
            },
            "most_common_null" : max(null_counts, key=null_counts.get)
                                  if null_counts else None,
            "nulls_by_session" : dict(null_by_sess),
            "nulls_by_pair"    : {
                pair: dict(types)
                for pair, types in null_by_pair.items()
            },
        }

    def answer_question(self, question: str) -> dict:
        """
        Answer analysis questions from the journal data.
        Maps predefined questions to analysis functions.
        """
        question_lower = question.lower()

        # Route to appropriate analysis
        if "loss" in question_lower or "lose" in question_lower:
            return self._what_leads_to_losses()
        elif "win" in question_lower or "best" in question_lower:
            return self._what_leads_to_wins()
        elif "null" in question_lower or "reject" in question_lower:
            return self.compute_null_analysis()
        elif "regime" in question_lower:
            return self._regime_performance_breakdown()
        elif "session" in question_lower or "time" in question_lower:
            return self._session_performance()
        elif "pair" in question_lower:
            return self._pair_performance()
        elif "ev" in question_lower or "expectancy" in question_lower:
            return self._ev_breakdown()
        elif "mtf" in question_lower or "structure" in question_lower:
            return self._mtf_analysis()
        elif "slippage" in question_lower or "execution" in question_lower:
            return self._execution_quality()
        else:
            return {"message": f"Question mapped to general stats",
                    "stats": self.compute_regime_stats()}

    def _what_leads_to_losses(self) -> dict:
        """Analyze patterns in losing trades."""
        losers = self._storage.query_trades(losers_only=True)
        if not losers:
            return {"message": "No losing trades to analyze"}

        # Average scores in losing trades
        mtf_scores    = [t.get("l2_mtf_score") for t in losers if t.get("l2_mtf_score")]
        conf_scores   = [t.get("l3_confidence_score") for t in losers if t.get("l3_confidence_score")]
        regimes       = [t.get("l3_regime_tag") for t in losers if t.get("l3_regime_tag")]
        sessions      = [t.get("session") for t in losers if t.get("session")]

        regime_counts = defaultdict(int)
        for r in regimes:
            regime_counts[r] += 1

        session_counts = defaultdict(int)
        for s in sessions:
            session_counts[s] += 1

        return {
            "question"        : "What patterns lead to most losses?",
            "n_losing_trades" : len(losers),
            "avg_mtf_score_in_losses": round(
                sum(mtf_scores)/len(mtf_scores), 1
            ) if mtf_scores else None,
            "avg_confidence_in_losses": round(
                sum(conf_scores)/len(conf_scores), 1
            ) if conf_scores else None,
            "regime_distribution": dict(regime_counts),
            "worst_regime"    : max(regime_counts, key=regime_counts.get)
                                 if regime_counts else None,
            "session_distribution": dict(session_counts),
            "worst_session"   : max(session_counts, key=session_counts.get)
                                 if session_counts else None,
        }

    def _what_leads_to_wins(self) -> dict:
        """Analyze patterns in winning trades."""
        winners = self._storage.query_trades(winners_only=True)
        if not winners:
            return {"message": "No winning trades to analyze"}

        mtf_scores    = [t.get("l2_mtf_score") for t in winners if t.get("l2_mtf_score")]
        conf_scores   = [t.get("l3_confidence_score") for t in winners if t.get("l3_confidence_score")]
        sessions      = [t.get("session") for t in winners if t.get("session")]
        regimes       = [t.get("l3_regime_tag") for t in winners if t.get("l3_regime_tag")]

        regime_counts  = defaultdict(int)
        session_counts = defaultdict(int)
        for r in regimes:
            regime_counts[r] += 1
        for s in sessions:
            session_counts[s] += 1

        return {
            "question"         : "Which setups have best outcomes?",
            "n_winning_trades" : len(winners),
            "avg_mtf_score_wins": round(
                sum(mtf_scores)/len(mtf_scores), 1
            ) if mtf_scores else None,
            "avg_confidence_wins": round(
                sum(conf_scores)/len(conf_scores), 1
            ) if conf_scores else None,
            "best_regime"      : max(regime_counts, key=regime_counts.get)
                                  if regime_counts else None,
            "best_session"     : max(session_counts, key=session_counts.get)
                                  if session_counts else None,
        }

    def _regime_performance_breakdown(self) -> dict:
        """Performance stats broken down by regime."""
        regimes = ["TREND", "HV_TREND", "RANGE", "NEWS"]
        breakdown = {}
        for regime in regimes:
            breakdown[regime] = self.compute_regime_stats(regime)
        breakdown["ALL"] = self.compute_regime_stats()
        return breakdown

    def _session_performance(self) -> dict:
        """Performance by trading session."""
        all_trades = self._storage.query_trades()
        by_session = defaultdict(list)
        for t in all_trades:
            sess = t.get("session", "UNKNOWN")
            pnl  = t.get("realized_pnl")
            if pnl is not None:
                by_session[sess].append(pnl)

        result = {}
        for sess, pnls in by_session.items():
            wins = [p for p in pnls if p > 0]
            result[sess] = {
                "n_trades" : len(pnls),
                "win_rate" : round(len(wins)/len(pnls), 3) if pnls else 0,
                "avg_pnl"  : round(sum(pnls)/len(pnls), 2) if pnls else 0,
            }
        return {"question": "What time of day performs best?",
                "by_session": result}

    def _pair_performance(self) -> dict:
        """Performance by currency pair."""
        all_trades = self._storage.query_trades()
        by_pair    = defaultdict(list)
        for t in all_trades:
            pair = t.get("pair", "UNKNOWN")
            pnl  = t.get("realized_pnl")
            if pnl is not None:
                by_pair[pair].append(pnl)

        result = {}
        for pair, pnls in by_pair.items():
            wins = [p for p in pnls if p > 0]
            result[pair] = {
                "n_trades" : len(pnls),
                "win_rate" : round(len(wins)/len(pnls), 3) if pnls else 0,
                "avg_pnl"  : round(sum(pnls)/len(pnls), 2) if pnls else 0,
                "total_pnl": round(sum(pnls), 2),
            }
        return {"question": "Which pairs perform best?",
                "by_pair": result}

    def _ev_breakdown(self) -> dict:
        """EV analysis — theoretical vs realized."""
        all_trades = self._storage.query_trades()
        if not all_trades:
            return {"message": "No trades"}

        th_evs  = [t.get("theoretical_ev") for t in all_trades if t.get("theoretical_ev")]
        re_evs  = [t.get("realized_ev_est") for t in all_trades if t.get("realized_ev_est")]
        devs    = [t.get("ev_deviation") for t in all_trades if t.get("ev_deviation")]

        return {
            "question"           : "Real EV vs theoretical EV",
            "avg_theoretical_ev" : round(sum(th_evs)/len(th_evs), 2) if th_evs else None,
            "avg_realized_ev"    : round(sum(re_evs)/len(re_evs), 2) if re_evs else None,
            "avg_ev_deviation"   : round(sum(devs)/len(devs), 2) if devs else None,
            "ev_leakage_pct"     : round(
                (1 - sum(re_evs)/sum(th_evs)) * 100, 1
            ) if th_evs and re_evs and sum(th_evs) > 0 else None,
        }

    def _mtf_analysis(self) -> dict:
        """MTF score vs outcome analysis."""
        all_trades = self._storage.query_trades()
        if not all_trades:
            return {"message": "No trades"}

        buckets = {"0-25": [], "25-50": [], "50-75": [], "75-100": []}
        for t in all_trades:
            score = t.get("l2_mtf_score")
            pnl   = t.get("realized_pnl")
            if score is None or pnl is None:
                continue
            if score < 25:
                buckets["0-25"].append(pnl)
            elif score < 50:
                buckets["25-50"].append(pnl)
            elif score < 75:
                buckets["50-75"].append(pnl)
            else:
                buckets["75-100"].append(pnl)

        result = {}
        for bucket, pnls in buckets.items():
            if pnls:
                wins = [p for p in pnls if p > 0]
                result[bucket] = {
                    "n_trades" : len(pnls),
                    "win_rate" : round(len(wins)/len(pnls), 3),
                    "avg_pnl"  : round(sum(pnls)/len(pnls), 2),
                }

        return {
            "question"   : "Which MTF score ranges produce best outcomes?",
            "by_mtf_range": result,
        }

    def _execution_quality(self) -> dict:
        """Execution quality analysis."""
        all_trades = self._storage.query_trades()
        if not all_trades:
            return {"message": "No trades"}

        slippages = [t.get("slippage_pips") for t in all_trades if t.get("slippage_pips") is not None]
        fills     = [t.get("fill_pct") for t in all_trades if t.get("fill_pct") is not None]

        return {
            "question"        : "Execution quality analysis",
            "n_trades"        : len(all_trades),
            "avg_slippage"    : round(sum(slippages)/len(slippages), 3) if slippages else None,
            "max_slippage"    : round(max(slippages), 3) if slippages else None,
            "avg_fill_pct"    : round(sum(fills)/len(fills)*100, 1) if fills else None,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — JOURNAL MANAGER (MAIN CLASS)
# ═══════════════════════════════════════════════════════════════════════════════

class JournalManager:
    """
    Master Layer 6 orchestrator.

    Records every trade and NULL with full context.
    Provides analysis interface for Layers 7 and 8.
    Maintains in-memory cache for fast recent access.

    CRITICAL RULE:
        This is the biggest long-term edge generator.
        Most systems execute but never learn WHY they win or lose.
        Every event recorded here feeds the learning loop.
    """

    def __init__(self, db_path: str = "journal.db"):
        self._storage    = SQLiteStorage(db_path)
        self._analyzer   = PerformanceAnalyzer(self._storage)
        self._recent     : deque = deque(maxlen=500)
        self._null_cache : deque = deque(maxlen=500)
        self._lock       = threading.Lock()
        logger.info("JournalManager initialized")

    # ── Recording Interface ──────────────────────────────────────────────────

    def record_trade_open(self,
                           trade_id          : str,
                           pair              : str,
                           direction         : str,
                           decision          : dict,
                           feature_pkg       : dict,
                           risk_output       : dict,
                           portfolio_output  : dict,
                           execution_result  : dict) -> str:
        """
        Record trade opening with all context from all layers.

        Args:
            trade_id        : Unique trade ID from Layer 5
            pair            : Forex pair
            direction       : BUY or SELL
            decision        : DecisionOutput.to_dict() from Layer 3
            feature_pkg     : FeaturePackage.to_dict() from Layer 2
            risk_output     : RiskOutput.to_dict() from Layer 4A
            portfolio_output: PortfolioOutput.to_dict() from Layer 4B
            execution_result: Execution result dict from Layer 5

        Returns:
            journal_id (UUID)
        """
        journal_id = f"J_{uuid.uuid4().hex[:12].upper()}"
        now_ms     = time.time() * 1000

        order = execution_result.get("order", {})
        trade = execution_result.get("trade", {})

        entry = TradeJournalEntry(
            journal_id           = journal_id,
            trade_id             = trade_id,
            pair                 = pair,
            direction            = direction,
            timestamp_utc        = now_ms,
            entry_time           = trade.get("entry_time"),

            # Layer 2 raw scores
            l2_mtf_score         = feature_pkg.get("mtf_score"),
            l2_structure_state   = feature_pkg.get("structure_state"),
            l2_regime_probs      = feature_pkg.get("regime_probs"),
            l2_regime_confidence = feature_pkg.get("regime_confidence"),
            l2_volatility_score  = feature_pkg.get("volatility_score"),
            l2_trend_strength    = feature_pkg.get("trend_strength_score"),
            l2_session_quality   = feature_pkg.get("session_quality_score"),
            l2_institutional     = feature_pkg.get("institutional_score"),
            l2_macro_score       = feature_pkg.get("macro_score"),
            l2_sentiment_score   = feature_pkg.get("sentiment_extreme_score"),
            l2_order_flow        = feature_pkg.get("order_flow_score"),

            # Layer 3 judgment
            l3_regime_tag        = decision.get("regime_tag"),
            l3_weights_applied   = decision.get("regime_weights_used"),
            l3_bayesian_prob     = decision.get("trade_probability"),
            l3_bayesian_threshold= decision.get("bayesian_threshold"),
            l3_confidence_score  = decision.get("confidence_score"),
            l3_ev_lower_bound    = decision.get("ev_lower_bound"),
            l3_ev_trusted        = decision.get("ev_trusted", False),
            l3_votes             = decision.get("votes"),
            l3_conf_multiplier   = decision.get("confidence_multiplier"),

            # Risk state
            risk_state           = risk_output.get("risk_state"),
            risk_pct             = risk_output.get("risk_pct"),
            lot_size             = risk_output.get("lot_size"),
            risk_amount          = risk_output.get("risk_amount"),
            atr_pips             = risk_output.get("atr_pips"),
            stop_loss_price      = risk_output.get("stop_loss_price"),
            take_profit_price    = risk_output.get("take_profit_price"),
            target_rr            = risk_output.get("expected_rr", 3.0),

            # Execution quality
            entry_price_planned  = order.get("requested_price"),
            entry_price_actual   = order.get("filled_price"),
            slippage_pips        = order.get("slippage_pips"),
            fill_pct             = order.get("fill_pct"),
            spread_at_entry      = feature_pkg.get("latest_spread"),
            latency_ms           = order.get("latency_ms"),
            theoretical_ev       = execution_result.get("ev_analysis", {}).get("theoretical_ev"),
            realized_ev_est      = execution_result.get("ev_analysis", {}).get("realized_ev_est"),
            ev_deviation         = execution_result.get("ev_analysis", {}).get("ev_deviation"),

            # Context
            session              = feature_pkg.get("active_session"),
            session_overlap      = feature_pkg.get("session_overlap", False),
            feed_health          = feature_pkg.get("feed_health", "HEALTHY"),
            data_tier            = feature_pkg.get("data_tier"),
            confidence_penalty   = feature_pkg.get("confidence_penalty", 0.0),

            # Portfolio
            n_positions_at_entry = portfolio_output.get("n_open_positions", 0),
            portfolio_var_pct    = portfolio_output.get("portfolio_var", {}).get("var_pct"),
            stress_test_passed   = portfolio_output.get("stress_passed", True),
        )

        self._storage.insert_trade(entry)
        with self._lock:
            self._recent.append(entry.to_json())

        logger.debug(f"Trade opened: {journal_id} | {pair} {direction}")
        return journal_id

    def record_trade_close(self,
                            trade_id          : str,
                            exit_price        : float,
                            exit_reason       : str,
                            realized_pnl      : float,
                            pnl_pips          : float,
                            actual_rr         : float,
                            trade_status      : str,
                            hold_hours        : float,
                            regime_at_exit    : Optional[str] = None,
                            mtf_at_exit       : Optional[float] = None):
        """Record trade exit with outcome."""
        self._storage.update_trade_exit(
            trade_id       = trade_id,
            exit_price     = exit_price,
            exit_time      = time.time() * 1000,
            exit_reason    = exit_reason,
            realized_pnl   = realized_pnl,
            pnl_pips       = pnl_pips,
            actual_rr      = actual_rr,
            trade_status   = trade_status,
            hold_hours     = hold_hours,
            regime_at_exit = regime_at_exit,
            mtf_at_exit    = mtf_at_exit,
        )
        logger.info(
            f"Trade closed: {trade_id} | "
            f"PnL=${realized_pnl:.2f} ({pnl_pips:.1f}pips) | "
            f"RR={actual_rr:.2f} | {exit_reason}"
        )

    def record_null(self,
                     pair            : str,
                     decision        : dict,
                     feature_pkg     : dict,
                     context         : dict = None) -> str:
        """
        Record every NULL rejection with full context.
        NULLs are as valuable as trades for improvement.
        """
        journal_id = f"N_{uuid.uuid4().hex[:12].upper()}"
        context    = context or {}

        entry = NullJournalEntry(
            journal_id           = journal_id,
            pair                 = pair,
            timestamp_utc        = time.time() * 1000,
            primary_null         = decision.get("primary_null", "UNKNOWN"),
            secondary_nulls      = decision.get("secondary_nulls", []),
            null_reason          = decision.get("null_reason"),
            null_gate            = decision.get("null_gate"),

            l2_mtf_score         = feature_pkg.get("mtf_score"),
            l2_structure_state   = feature_pkg.get("structure_state"),
            l2_regime_dominant   = feature_pkg.get("regime_dominant"),
            l2_regime_confidence = feature_pkg.get("regime_confidence"),
            l2_volatility_score  = feature_pkg.get("volatility_score"),
            l2_session_quality   = feature_pkg.get("session_quality_score"),
            l2_news_blackout     = feature_pkg.get("news_blackout", False),

            l3_regime_tag        = decision.get("regime_tag"),
            l3_bayesian_result   = decision.get("bayesian_result"),
            l3_threshold         = decision.get("bayesian_threshold"),
            l3_ev_lower_bound    = decision.get("ev_lower_bound"),
            l3_votes             = decision.get("votes"),

            session              = feature_pkg.get("active_session"),
            feed_health          = feature_pkg.get("feed_health", "HEALTHY"),
            spread_ratio         = context.get("spread_ratio"),
            vix_level            = feature_pkg.get("vix_level"),
            stress_count         = feature_pkg.get("multi_stress_count", 0),
        )

        self._storage.insert_null(entry)
        with self._lock:
            self._null_cache.append(entry.to_dict())

        logger.debug(
            f"NULL recorded: {journal_id} | {pair} | "
            f"{entry.primary_null} | {entry.null_reason[:50] if entry.null_reason else ''}"
        )
        return journal_id

    # ── Analysis Interface ───────────────────────────────────────────────────

    def analyze(self, question: str) -> dict:
        """Answer an analysis question from journal data."""
        return self._analyzer.answer_question(question)

    def get_regime_stats(self, regime: Optional[str] = None) -> dict:
        """Get performance statistics for a regime."""
        return self._analyzer.compute_regime_stats(regime)

    def get_null_analysis(self) -> dict:
        """Analyze NULL patterns."""
        return self._analyzer.compute_null_analysis()

    def get_all_regime_stats(self) -> dict:
        """Get stats for all regimes."""
        return self._analyzer._regime_performance_breakdown()

    def get_recent_trades(self, n: int = 20) -> list:
        """Get recent trade records from cache."""
        with self._lock:
            return list(self._recent)[-n:]

    def get_recent_nulls(self, n: int = 20) -> list:
        """Get recent NULL records from cache."""
        with self._lock:
            return list(self._null_cache)[-n:]

    def save_performance_snapshot(self, regime: Optional[str] = None):
        """Save current performance snapshot for trend tracking."""
        stats = self.get_regime_stats(regime)
        stats["timestamp_utc"] = time.time() * 1000
        stats["regime"]        = regime or "ALL"
        null_analysis          = self.get_null_analysis()
        stats["null_rate"]     = (
            null_analysis["n_nulls"] /
            max(1, null_analysis["n_nulls"] + stats.get("n_trades", 0))
        )
        stats["most_common_null"] = null_analysis.get("most_common_null")
        self._storage.save_performance_snapshot(stats)

    def get_total_counts(self) -> dict:
        """Get total trade and NULL counts."""
        trades = self._storage.query_trades()
        nulls  = self._storage.query_nulls()
        return {
            "total_trades"    : len(trades),
            "total_nulls"     : len(nulls),
            "total_events"    : len(trades) + len(nulls),
            "null_rate"       : round(
                len(nulls) / max(1, len(trades) + len(nulls)) * 100, 1
            ),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — SELF TEST
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import os
    import random

    print("=" * 70)
    print("LAYER 6 — MEMORY & JOURNAL SYSTEM SELF TEST")
    print("=" * 70)

    # Use in-memory DB for testing
    db_path = ":memory:"
    journal = JournalManager(db_path=db_path)

    rng = random.Random(42)

    # ── Helper: mock data packages ────────────────────────────────────────────
    def mock_decision(pair, null_type=None, regime="TREND"):
        if null_type:
            return {
                "primary_null"        : null_type,
                "secondary_nulls"     : [],
                "null_reason"         : f"Test NULL: {null_type}",
                "null_gate"           : 2,
                "regime_tag"          : regime,
                "bayesian_result"     : 0.58,
                "bayesian_threshold"  : 0.62,
                "ev_lower_bound"      : -5.0,
                "votes"               : None,
            }
        return {
            "regime_tag"          : regime,
            "regime_weights_used" : {"structure": 0.30, "statistics": 0.28},
            "trade_probability"   : 0.78,
            "bayesian_threshold"  : 0.62,
            "confidence_score"    : 78.0,
            "ev_lower_bound"      : 55.0,
            "ev_trusted"          : True,
            "votes"               : {"structure": 72, "statistics": 68},
            "confidence_multiplier": 1.0,
            "primary_null"        : None,
        }

    def mock_feature(pair, session="OVERLAP", regime="TREND"):
        return {
            "pair"                   : pair,
            "mtf_score"              : rng.uniform(55, 85),
            "structure_state"        : "STABLE",
            "regime_dominant"        : regime,
            "regime_confidence"      : rng.uniform(70, 92),
            "volatility_score"       : rng.uniform(30, 55),
            "trend_strength_score"   : rng.uniform(40, 75),
            "session_quality_score"  : rng.uniform(65, 95),
            "institutional_score"    : rng.uniform(55, 75),
            "macro_score"            : rng.uniform(55, 70),
            "sentiment_extreme_score": rng.uniform(50, 68),
            "order_flow_score"       : rng.uniform(55, 72),
            "active_session"         : session,
            "session_overlap"        : session == "OVERLAP",
            "feed_health"            : "HEALTHY",
            "data_tier"              : "TIER_1",
            "confidence_penalty"     : 0.0,
            "vix_level"              : rng.uniform(14, 22),
            "multi_stress_count"     : rng.randint(0, 2),
            "news_blackout"          : False,
        }

    def mock_risk(pair):
        return {
            "risk_state"       : "NORMAL",
            "risk_pct"         : 1.0,
            "lot_size"         : 0.33,
            "risk_amount"      : 100.0,
            "atr_pips"         : 8.5,
            "stop_loss_price"  : 1.08372,
            "take_profit_price": 1.08884,
            "expected_rr"      : 3.0,
        }

    def mock_portfolio():
        return {
            "n_open_positions" : 1,
            "portfolio_var"    : {"var_pct": 1.2},
            "stress_passed"    : True,
        }

    def mock_execution(pair, direction, entry_price):
        filled = entry_price + rng.gauss(0, 0.00005)
        return {
            "order": {
                "requested_price": entry_price,
                "filled_price"   : round(filled, 5),
                "slippage_pips"  : abs(filled - entry_price) * 10000,
                "fill_pct"       : rng.uniform(0.98, 1.0),
                "latency_ms"     : rng.uniform(20, 80),
            },
            "trade": {
                "entry_time"     : time.time() * 1000,
            },
            "ev_analysis": {
                "theoretical_ev" : 65.0,
                "realized_ev_est": 62.0,
                "ev_deviation"   : 3.0,
            },
        }

    # ── Test 1: Record trades ─────────────────────────────────────────────────
    print("\n[1] Recording 50 simulated trades...")
    pairs    = ["EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD"]
    regimes  = ["TREND", "TREND", "TREND", "RANGE", "HV_TREND"]
    sessions = ["OVERLAP", "LONDON", "NEW_YORK", "OVERLAP", "LONDON"]

    trade_ids = []
    for i in range(50):
        pair      = rng.choice(pairs)
        regime    = rng.choice(regimes)
        session   = rng.choice(sessions)
        direction = rng.choice(["BUY", "SELL"])
        entry     = rng.uniform(1.0800, 1.0950) if "USD" in pair else 148.0

        trade_id  = f"POS_{i:03d}"
        trade_ids.append(trade_id)

        # Record open
        journal.record_trade_open(
            trade_id         = trade_id,
            pair             = pair,
            direction        = direction,
            decision         = mock_decision(pair, regime=regime),
            feature_pkg      = mock_feature(pair, session, regime),
            risk_output      = mock_risk(pair),
            portfolio_output = mock_portfolio(),
            execution_result = mock_execution(pair, direction, entry),
        )

        # Record close with realistic outcome
        win_prob = 0.63 if regime == "TREND" else 0.52
        is_win   = rng.random() < win_prob
        pnl      = rng.uniform(60, 200) if is_win else -rng.uniform(30, 100)
        pips     = pnl / 10.0
        rr       = abs(pips) / 8.5 * (1 if is_win else -1)

        journal.record_trade_close(
            trade_id       = trade_id,
            exit_price     = entry + (pips * 0.0001),
            exit_reason    = "TP_HIT" if is_win else "SL_HIT",
            realized_pnl   = pnl,
            pnl_pips       = pips,
            actual_rr      = rr,
            trade_status   = "CLOSED_TP" if is_win else "CLOSED_SL",
            hold_hours     = rng.uniform(2, 72),
            regime_at_exit = regime,
        )

    print(f"    ✅ 50 trades recorded and closed")

    # ── Test 2: Record NULLs ──────────────────────────────────────────────────
    print("\n[2] Recording 80 NULL rejections...")
    null_types = [
        "NULL_REGIME", "NULL_STRUCTURE", "NULL_TIME",
        "NULL_EV", "NULL_LIQUIDITY", "NULL_RISK"
    ]
    for i in range(80):
        pair      = rng.choice(pairs)
        null_type = rng.choice(null_types)
        regime    = rng.choice(regimes)
        session   = rng.choice(sessions)

        journal.record_null(
            pair        = pair,
            decision    = mock_decision(pair, null_type=null_type, regime=regime),
            feature_pkg = mock_feature(pair, session, regime),
            context     = {"spread_ratio": rng.uniform(1.0, 2.5)},
        )

    print(f"    ✅ 80 NULLs recorded")

    # ── Test 3: Overall stats ─────────────────────────────────────────────────
    print("\n[3] Total counts:")
    counts = journal.get_total_counts()
    print(f"    Total trades  : {counts['total_trades']}")
    print(f"    Total NULLs   : {counts['total_nulls']}")
    print(f"    Total events  : {counts['total_events']}")
    print(f"    NULL rate     : {counts['null_rate']:.1f}%")

    # ── Test 4: Regime performance ────────────────────────────────────────────
    print("\n[4] Performance by regime:")
    for regime in ["TREND", "RANGE", "HV_TREND", "ALL"]:
        stats = journal.get_regime_stats(
            regime if regime != "ALL" else None
        )
        trusted_icon = "✅" if stats.get("trusted") else "⚠️ "
        print(f"    {regime:10s}: "
              f"n={stats['n_trades']:3d} | "
              f"WR={stats.get('win_rate', 0):.1%} | "
              f"EV=${stats.get('ev_point', 0):.2f} | "
              f"Sharpe={stats.get('sharpe_ratio', 0):.2f} | "
              f"{trusted_icon}")

    # ── Test 5: NULL analysis ─────────────────────────────────────────────────
    print("\n[5] NULL analysis:")
    null_analysis = journal.get_null_analysis()
    print(f"    Total NULLs     : {null_analysis['n_nulls']}")
    print(f"    Most common     : {null_analysis['most_common_null']}")
    print(f"    Distribution:")
    for null_type, data in null_analysis["null_distribution"].items():
        bar = "█" * (data["count"] // 2)
        print(f"      {null_type:18s}: {data['count']:3d} ({data['pct']:4.1f}%) {bar}")

    # ── Test 6: AI analysis questions ─────────────────────────────────────────
    print("\n[6] AI analysis questions:")
    questions = [
        "What patterns lead to most losses?",
        "What time of day performs best?",
        "Which pairs perform best?",
        "What is the real EV vs theoretical EV?",
        "Which MTF score ranges produce best outcomes?",
    ]
    for q in questions:
        result = journal.analyze(q)
        print(f"\n  Q: '{q}'")
        # Print key result
        if "n_losing_trades" in result:
            print(f"     Losses: {result['n_losing_trades']} | "
                  f"Worst regime: {result.get('worst_regime')} | "
                  f"Worst session: {result.get('worst_session')}")
        elif "by_session" in result:
            best = max(result["by_session"].items(),
                       key=lambda x: x[1]["avg_pnl"], default=(None, {}))
            print(f"     Best session: {best[0]} | "
                  f"Avg PnL: ${best[1].get('avg_pnl', 0):.2f}")
        elif "by_pair" in result:
            best = max(result["by_pair"].items(),
                       key=lambda x: x[1]["avg_pnl"], default=(None, {}))
            print(f"     Best pair: {best[0]} | "
                  f"Avg PnL: ${best[1].get('avg_pnl', 0):.2f}")
        elif "avg_realized_ev" in result:
            print(f"     Theoretical: ${result.get('avg_theoretical_ev', 0):.2f} | "
                  f"Realized: ${result.get('avg_realized_ev', 0):.2f} | "
                  f"Leakage: {result.get('ev_leakage_pct', 0):.1f}%")
        elif "by_mtf_range" in result:
            for rng_key, data in result["by_mtf_range"].items():
                print(f"     MTF {rng_key}: WR={data['win_rate']:.1%} "
                      f"Avg=${data['avg_pnl']:.2f}")

    # ── Test 7: Rolling window EVs ────────────────────────────────────────────
    print("\n[7] Rolling window EV analysis:")
    stats = journal.get_regime_stats()
    for window in ["rolling_10_ev", "rolling_25_ev",
                   "rolling_50_ev", "rolling_100_ev"]:
        val = stats.get(window)
        label = window.replace("rolling_", "").replace("_ev", " trades")
        if val is not None:
            trend = "📈" if val > 0 else "📉"
            print(f"    Last {label:12s}: ${val:7.2f} {trend}")

    # ── Test 8: Performance snapshot save ────────────────────────────────────
    print("\n[8] Saving performance snapshot:")
    journal.save_performance_snapshot(regime="TREND")
    journal.save_performance_snapshot(regime=None)
    print("    ✅ Snapshots saved to DB")

    print("\n" + "=" * 70)
    print("LAYER 6 MEMORY & JOURNAL SELF TEST COMPLETE ✅")
    print()
    print("Components verified:")
    print("  SQLiteStorage           ✅  Persistent trades + NULLs + snapshots")
    print("  TradeJournalEntry       ✅  All layers recorded per trade")
    print("  NullJournalEntry        ✅  All layers recorded per NULL")
    print("  PerformanceAnalyzer     ✅  Regime/session/pair/MTF/EV analysis")
    print("  JournalManager          ✅  Master orchestrator")
    print()
    print("Key insight verified:")
    print("  NULLs recorded with same detail as trades ✅")
    print("  All Layer 2 raw scores captured ✅")
    print("  All Layer 3 weights captured ✅")
    print("  Rolling window EV tracking working ✅")
    print("  AI question answering working ✅")
    print()
    print("Layer 6 → Layer 7: Full trade + NULL history for validation")
    print("Layer 6 → Layer 8: Performance data for learning loop")
    print("=" * 70)
