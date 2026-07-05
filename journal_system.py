"""
═══════════════════════════════════════════════════════════════════════════════
FOREX TRADING SIGNAL SYSTEM — VERSION 6.0
FILE: journal_system.py
LAYER: 6 — MEMORY & JOURNAL SYSTEM (Supabase + SQLite fallback)
═══════════════════════════════════════════════════════════════════════════════
"""

import os
import json
import time
import uuid
import logging
import threading
from datetime import datetime, timezone
from typing import Optional
from dataclasses import dataclass, field
from collections import deque

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger("JournalSystem")

# ── Supabase connection string from environment ──
DATABASE_URL = os.getenv("DATABASE_URL", "")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — JOURNAL ENTRY STRUCTURES (unchanged)
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class TradeJournalEntry:
    journal_id: str
    trade_id: str
    pair: str
    direction: str
    timestamp_utc: float
    entry_time: Optional[float] = None
    exit_time: Optional[float] = None

    l2_mtf_score: Optional[float] = None
    l2_structure_state: Optional[str] = None
    l2_regime_probs: Optional[dict] = None
    l2_regime_confidence: Optional[float] = None
    l2_volatility_score: Optional[float] = None
    l2_trend_strength: Optional[float] = None
    l2_session_quality: Optional[float] = None
    l2_institutional: Optional[float] = None
    l2_macro_score: Optional[float] = None
    l2_sentiment_score: Optional[float] = None
    l2_order_flow: Optional[float] = None

    l3_regime_tag: Optional[str] = None
    l3_weights_applied: Optional[dict] = None
    l3_bayesian_prob: Optional[float] = None
    l3_bayesian_threshold: Optional[float] = None
    l3_confidence_score: Optional[float] = None
    l3_ev_lower_bound: Optional[float] = None
    l3_ev_trusted: bool = False
    l3_votes: Optional[dict] = None
    l3_conf_multiplier: Optional[float] = None

    risk_state: Optional[str] = None
    risk_pct: Optional[float] = None
    lot_size: Optional[float] = None
    risk_amount: Optional[float] = None
    atr_pips: Optional[float] = None
    stop_loss_price: Optional[float] = None
    take_profit_price: Optional[float] = None
    target_rr: float = 3.0

    entry_price_planned: Optional[float] = None
    entry_price_actual: Optional[float] = None
    slippage_pips: Optional[float] = None
    fill_pct: Optional[float] = None
    spread_at_entry: Optional[float] = None
    latency_ms: Optional[float] = None
    theoretical_ev: Optional[float] = None
    realized_ev_est: Optional[float] = None
    ev_deviation: Optional[float] = None

    session: Optional[str] = None
    session_overlap: bool = False
    feed_health: str = "HEALTHY"
    data_tier: Optional[str] = None
    confidence_penalty: float = 0.0

    n_positions_at_entry: int = 0
    portfolio_var_pct: Optional[float] = None
    stress_test_passed: bool = True

    pfvw_duration_min: Optional[float] = None
    pfvw_exit_reason: Optional[str] = None
    pfvw_validations: int = 0

    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None
    realized_pnl: Optional[float] = None
    pnl_pips: Optional[float] = None
    actual_rr: Optional[float] = None
    trade_status: Optional[str] = None
    hold_duration_hours: Optional[float] = None
    was_winner: Optional[bool] = None

    primary_cause: Optional[str] = None
    contributing_factors: Optional[list] = field(default_factory=list)
    regime_at_exit: Optional[str] = None
    mtf_at_exit: Optional[float] = None

    def to_dict(self) -> dict:
        d = {}
        for f in self.__dataclass_fields__:
            val = getattr(self, f)
            if isinstance(val, (dict, list)):
                d[f] = json.dumps(val) if val else None
            else:
                d[f] = val
        return d


@dataclass
class NullJournalEntry:
    journal_id: str
    pair: str
    timestamp_utc: float
    primary_null: str
    secondary_nulls: Optional[list] = field(default_factory=list)
    null_reason: Optional[str] = None
    null_gate: Optional[int] = None

    l2_mtf_score: Optional[float] = None
    l2_structure_state: Optional[str] = None
    l2_regime_dominant: Optional[str] = None
    l2_regime_confidence: Optional[float] = None
    l2_volatility_score: Optional[float] = None
    l2_session_quality: Optional[float] = None
    l2_news_blackout: bool = False

    l3_regime_tag: Optional[str] = None
    l3_bayesian_result: Optional[float] = None
    l3_threshold: Optional[float] = None
    l3_ev_lower_bound: Optional[float] = None
    l3_votes: Optional[dict] = None

    session: Optional[str] = None
    feed_health: str = "HEALTHY"
    spread_ratio: Optional[float] = None
    vix_level: Optional[float] = None
    stress_count: int = 0

    causal_notes: Optional[str] = None

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
# SECTION 2 — SUPABASE STORAGE ENGINE (unchanged)
# ═══════════════════════════════════════════════════════════════════════════════

class SupabaseStorage:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self._local = threading.local()
        self._lock = threading.Lock()
        self._init_tables()

    def _get_conn(self):
        if not hasattr(self._local, "conn") or self._local.conn is None:
            # Add connection timeout and SSL requirement
            self._local.conn = psycopg2.connect(
                self.database_url,
                connect_timeout=10,
                sslmode='require'
            )
            self._local.conn.autocommit = True
        return self._local.conn

    def _init_tables(self):
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("""
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
            )
        """)
        cur.execute("""
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
            )
        """)
        cur.execute("""
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
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_pair ON trades(pair)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_regime ON trades(l3_regime_tag)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_time ON trades(timestamp_utc)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nulls_pair ON nulls(pair)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nulls_type ON nulls(primary_null)")
        conn.commit()
        cur.close()
        logger.info("Supabase tables initialised ✅")

    def insert_trade(self, entry: TradeJournalEntry):
        conn = self._get_conn()
        cur = conn.cursor()
        d = entry.to_dict()
        columns = ", ".join(d.keys())
        placeholders = ", ".join(["%s"] * len(d))
        values = list(d.values())
        query = f"INSERT INTO trades ({columns}) VALUES ({placeholders}) ON CONFLICT (journal_id) DO UPDATE SET "
        set_clause = ", ".join([f"{k} = EXCLUDED.{k}" for k in d.keys()])
        query += set_clause
        try:
            cur.execute(query, values)
            conn.commit()
        except Exception as e:
            logger.error(f"Trade insert failed: {e}")
        finally:
            cur.close()

    def insert_null(self, entry: NullJournalEntry):
        conn = self._get_conn()
        cur = conn.cursor()
        d = entry.to_dict()
        columns = ", ".join(d.keys())
        placeholders = ", ".join(["%s"] * len(d))
        values = list(d.values())
        query = f"INSERT INTO nulls ({columns}) VALUES ({placeholders}) ON CONFLICT (journal_id) DO UPDATE SET "
        set_clause = ", ".join([f"{k} = EXCLUDED.{k}" for k in d.keys()])
        query += set_clause
        try:
            cur.execute(query, values)
            conn.commit()
        except Exception as e:
            logger.error(f"NULL insert failed: {e}")
        finally:
            cur.close()

    def update_trade_exit(self, trade_id: str, exit_price: float,
                           exit_time: float, exit_reason: str,
                           realized_pnl: float, pnl_pips: float,
                           actual_rr: float, trade_status: str,
                           hold_hours: float, regime_at_exit: Optional[str] = None,
                           mtf_at_exit: Optional[float] = None):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                UPDATE trades SET
                    exit_price = %s,
                    exit_time = %s,
                    exit_reason = %s,
                    realized_pnl = %s,
                    pnl_pips = %s,
                    actual_rr = %s,
                    trade_status = %s,
                    hold_duration_hours = %s,
                    was_winner = %s,
                    regime_at_exit = %s,
                    mtf_at_exit = %s
                WHERE trade_id = %s
            """, (
                exit_price, exit_time, exit_reason,
                realized_pnl, pnl_pips, actual_rr,
                trade_status, hold_hours,
                1 if realized_pnl > 0 else 0,
                regime_at_exit, mtf_at_exit,
                trade_id
            ))
            conn.commit()
        except Exception as e:
            logger.error(f"Trade exit update failed: {e}")
        finally:
            cur.close()

    def query_trades(self, pair: Optional[str] = None,
                      regime: Optional[str] = None,
                      last_n: Optional[int] = None,
                      winners_only: bool = False,
                      losers_only: bool = False) -> list:
        conn = self._get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        where = []
        params = []
        if pair:
            where.append("pair = %s")
            params.append(pair)
        if regime:
            where.append("l3_regime_tag = %s")
            params.append(regime)
        if winners_only:
            where.append("was_winner = 1")
        if losers_only:
            where.append("was_winner = 0")
        where.append("exit_time IS NOT NULL")
        sql = "SELECT * FROM trades"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY timestamp_utc DESC"
        if last_n:
            sql += f" LIMIT {last_n}"
        try:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"Trade query failed: {e}")
            return []
        finally:
            cur.close()

    def query_nulls(self, pair: Optional[str] = None,
                     null_type: Optional[str] = None,
                     last_n: Optional[int] = None) -> list:
        conn = self._get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        where = []
        params = []
        if pair:
            where.append("pair = %s")
            params.append(pair)
        if null_type:
            where.append("primary_null = %s")
            params.append(null_type)
        sql = "SELECT * FROM nulls"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY timestamp_utc DESC"
        if last_n:
            sql += f" LIMIT {last_n}"
        try:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"NULL query failed: {e}")
            return []
        finally:
            cur.close()

    def save_performance_snapshot(self, snapshot: dict):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            snap_id = f"SNAP_{int(time.time()*1000)}"
            cur.execute("""
                INSERT INTO performance_snapshots
                (snapshot_id, timestamp_utc, regime, n_trades, n_wins,
                 win_rate, avg_pnl, avg_winner, avg_loser, ev_point,
                 sharpe_ratio, max_drawdown_pct, rolling_10_ev,
                 rolling_25_ev, rolling_50_ev, null_rate, most_common_null)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                snap_id,
                snapshot.get("timestamp_utc", time.time() * 1000),
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
        except Exception as e:
            logger.error(f"Snapshot save failed: {e}")
        finally:
            cur.close()


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — SQLite STORAGE (unchanged)
# ═══════════════════════════════════════════════════════════════════════════════

class SQLiteStorage:
    def __init__(self, db_path: str = "journal.db"):
        self.db_path = db_path
        self._local = threading.local()
        self._lock = threading.Lock()
        self._init_db()

    def _get_conn(self):
        if not hasattr(self._local, "conn") or self._local.conn is None:
            import sqlite3
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
        import sqlite3
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

            CREATE INDEX IF NOT EXISTS idx_trades_pair ON trades(pair);
            CREATE INDEX IF NOT EXISTS idx_trades_regime ON trades(l3_regime_tag);
            CREATE INDEX IF NOT EXISTS idx_trades_time ON trades(timestamp_utc);
            CREATE INDEX IF NOT EXISTS idx_nulls_pair ON nulls(pair);
            CREATE INDEX IF NOT EXISTS idx_nulls_type ON nulls(primary_null);
        """)
        conn.commit()

    def insert_trade(self, entry):
        import sqlite3
        conn = self._get_conn()
        d = entry.to_dict()
        cols = ", ".join(d.keys())
        placeh = ", ".join(["?" for _ in d])
        vals = list(d.values())
        try:
            conn.execute(f"INSERT OR REPLACE INTO trades ({cols}) VALUES ({placeh})", vals)
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Trade insert failed: {e}")

    def insert_null(self, entry):
        import sqlite3
        conn = self._get_conn()
        d = entry.to_dict()
        cols = ", ".join(d.keys())
        placeh = ", ".join(["?" for _ in d])
        vals = list(d.values())
        try:
            conn.execute(f"INSERT OR REPLACE INTO nulls ({cols}) VALUES ({placeh})", vals)
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"NULL insert failed: {e}")

    def update_trade_exit(self, trade_id, exit_price, exit_time, exit_reason,
                           realized_pnl, pnl_pips, actual_rr, trade_status,
                           hold_hours, regime_at_exit=None, mtf_at_exit=None):
        import sqlite3
        conn = self._get_conn()
        try:
            conn.execute("""
                UPDATE trades SET
                    exit_price = ?,
                    exit_time = ?,
                    exit_reason = ?,
                    realized_pnl = ?,
                    pnl_pips = ?,
                    actual_rr = ?,
                    trade_status = ?,
                    hold_duration_hours = ?,
                    was_winner = ?,
                    regime_at_exit = ?,
                    mtf_at_exit = ?
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

    def query_trades(self, pair=None, regime=None, last_n=None,
                      winners_only=False, losers_only=False):
        conn = self._get_conn()
        where = []
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
        except Exception as e:
            logger.error(f"Trade query failed: {e}")
            return []

    def query_nulls(self, pair=None, null_type=None, last_n=None):
        conn = self._get_conn()
        where = []
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
        except Exception as e:
            logger.error(f"NULL query failed: {e}")
            return []

    def save_performance_snapshot(self, snapshot):
        import sqlite3
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
# SECTION 4 — PERFORMANCE ANALYZER (unchanged)
# ═══════════════════════════════════════════════════════════════════════════════

class PerformanceAnalyzer:
    def __init__(self, storage):
        self._storage = storage

    def compute_regime_stats(self, regime: Optional[str] = None) -> dict:
        trades = self._storage.query_trades(regime=regime)
        if not trades:
            return {"regime": regime or "ALL", "n_trades": 0, "sufficient": False}
        import statistics
        import math
        pnls = [t["realized_pnl"] for t in trades if t.get("realized_pnl") is not None]
        winners = [p for p in pnls if p > 0]
        losers = [p for p in pnls if p <= 0]
        n = len(pnls)
        win_rate = len(winners) / n if n > 0 else 0
        avg_win = sum(winners) / len(winners) if winners else 0
        avg_loss = sum(losers) / len(losers) if losers else 0
        avg_pnl = sum(pnls) / n if n > 0 else 0
        ev_point = (win_rate * avg_win) + ((1 - win_rate) * avg_loss)
        if len(pnls) > 1:
            pnl_std = statistics.stdev(pnls)
            sharpe = (avg_pnl / pnl_std * math.sqrt(252)) if pnl_std > 0 else 0
        else:
            sharpe = 0
        def rolling_ev(window):
            subset = pnls[-window:]
            if not subset:
                return None
            w = [p for p in subset if p > 0]
            l = [p for p in subset if p <= 0]
            wr = len(w) / len(subset)
            aw = sum(w) / len(w) if w else 0
            al = sum(l) / len(l) if l else 0
            return round((wr * aw) + ((1 - wr) * al), 2)
        cumulative = 0.0
        peak = 0.0
        max_dd = 0.0
        for pnl in pnls:
            cumulative += pnl
            if cumulative > peak:
                peak = cumulative
            dd = (peak - cumulative) / peak if peak > 0 else 0
            max_dd = max(max_dd, dd)
        return {
            "regime": regime or "ALL",
            "n_trades": n,
            "n_wins": len(winners),
            "n_losses": len(losers),
            "win_rate": round(win_rate, 3),
            "avg_pnl": round(avg_pnl, 2),
            "avg_winner": round(avg_win, 2),
            "avg_loser": round(avg_loss, 2),
            "ev_point": round(ev_point, 2),
            "sharpe_ratio": round(sharpe, 3),
            "max_drawdown_pct": round(max_dd * 100, 2),
            "rolling_10_ev": rolling_ev(10),
            "rolling_25_ev": rolling_ev(25),
            "rolling_50_ev": rolling_ev(50),
            "rolling_100_ev": rolling_ev(100),
            "sufficient": n >= 100,
            "trusted": n >= 100 and len(winners) >= 30,
        }

    def compute_null_analysis(self) -> dict:
        nulls = self._storage.query_nulls()
        if not nulls:
            return {"n_nulls": 0, "message": "No NULL records"}
        from collections import defaultdict
        null_counts = defaultdict(int)
        for n in nulls:
            nt = n.get("primary_null", "UNKNOWN")
            null_counts[nt] += 1
        total = len(nulls)
        return {
            "n_nulls": total,
            "null_distribution": {
                k: {"count": v, "pct": round(v / total * 100, 1)}
                for k, v in sorted(null_counts.items(), key=lambda x: -x[1])
            },
            "most_common_null": max(null_counts, key=null_counts.get) if null_counts else None,
        }

    def answer_question(self, question: str) -> dict:
        question_lower = question.lower()
        if "loss" in question_lower:
            losers = self._storage.query_trades(losers_only=True)
            return {"question": "What patterns lead to most losses?", "n_losing_trades": len(losers)}
        elif "win" in question_lower:
            winners = self._storage.query_trades(winners_only=True)
            return {"question": "What setups have best outcomes?", "n_winning_trades": len(winners)}
        elif "null" in question_lower:
            return self.compute_null_analysis()
        elif "regime" in question_lower:
            return self._regime_performance_breakdown()
        else:
            return {"message": "General stats", "stats": self.compute_regime_stats()}

    def _regime_performance_breakdown(self) -> dict:
        regimes = ["TREND", "HV_TREND", "RANGE", "NEWS"]
        breakdown = {}
        for regime in regimes:
            breakdown[regime] = self.compute_regime_stats(regime)
        breakdown["ALL"] = self.compute_regime_stats()
        return breakdown


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — JOURNAL MANAGER (with Supabase fallback)
# ═══════════════════════════════════════════════════════════════════════════════

class JournalManager:
    def __init__(self, db_path: str = "journal.db", database_url: Optional[str] = None):
        self.db_path = db_path
        self._database_url = database_url or DATABASE_URL

        # Try Supabase first; if it fails, fall back to SQLite
        if self._database_url:
            try:
                self._storage = SupabaseStorage(self._database_url)
                logger.info("Using Supabase for journal storage")
            except Exception as e:
                logger.warning(f"Supabase connection failed: {e}. Falling back to SQLite.")
                self._storage = SQLiteStorage(db_path)
        else:
            self._storage = SQLiteStorage(db_path)

        self._analyzer = PerformanceAnalyzer(self._storage)
        self._recent = deque(maxlen=500)
        self._null_cache = deque(maxlen=500)
        self._lock = threading.Lock()
        logger.info("JournalManager initialized")

    # All other methods (record_trade_open, record_trade_close, record_null, etc.) remain unchanged
    def record_trade_open(self, trade_id: str, pair: str, direction: str,
                           decision: dict, feature_pkg: dict,
                           risk_output: dict, portfolio_output: dict,
                           execution_result: dict) -> str:
        journal_id = f"J_{uuid.uuid4().hex[:12].upper()}"
        now_ms = time.time() * 1000

        order = execution_result.get("order", {})
        trade = execution_result.get("trade", {})

        entry = TradeJournalEntry(
            journal_id=journal_id,
            trade_id=trade_id,
            pair=pair,
            direction=direction,
            timestamp_utc=now_ms,
            entry_time=trade.get("entry_time"),
            l2_mtf_score=feature_pkg.get("mtf_score"),
            l2_structure_state=feature_pkg.get("structure_state"),
            l2_regime_probs=feature_pkg.get("regime_probs"),
            l2_regime_confidence=feature_pkg.get("regime_confidence"),
            l2_volatility_score=feature_pkg.get("volatility_score"),
            l2_trend_strength=feature_pkg.get("trend_strength_score"),
            l2_session_quality=feature_pkg.get("session_quality_score"),
            l2_institutional=feature_pkg.get("institutional_score"),
            l2_macro_score=feature_pkg.get("macro_score"),
            l2_sentiment_score=feature_pkg.get("sentiment_extreme_score"),
            l2_order_flow=feature_pkg.get("order_flow_score"),
            l3_regime_tag=decision.get("regime_tag"),
            l3_weights_applied=decision.get("regime_weights_used"),
            l3_bayesian_prob=decision.get("trade_probability"),
            l3_bayesian_threshold=decision.get("bayesian_threshold"),
            l3_confidence_score=decision.get("confidence_score"),
            l3_ev_lower_bound=decision.get("ev_lower_bound"),
            l3_ev_trusted=decision.get("ev_trusted", False),
            l3_votes=decision.get("votes"),
            l3_conf_multiplier=decision.get("confidence_multiplier"),
            risk_state=risk_output.get("risk_state"),
            risk_pct=risk_output.get("risk_pct"),
            lot_size=risk_output.get("lot_size"),
            risk_amount=risk_output.get("risk_amount"),
            atr_pips=risk_output.get("atr_pips"),
            stop_loss_price=risk_output.get("stop_loss_price"),
            take_profit_price=risk_output.get("take_profit_price"),
            target_rr=risk_output.get("expected_rr", 3.0),
            entry_price_planned=order.get("requested_price"),
            entry_price_actual=order.get("filled_price"),
            slippage_pips=order.get("slippage_pips"),
            fill_pct=order.get("fill_pct"),
            spread_at_entry=feature_pkg.get("latest_spread"),
            latency_ms=order.get("latency_ms"),
            theoretical_ev=execution_result.get("ev_analysis", {}).get("theoretical_ev"),
            realized_ev_est=execution_result.get("ev_analysis", {}).get("realized_ev_est"),
            ev_deviation=execution_result.get("ev_analysis", {}).get("ev_deviation"),
            session=feature_pkg.get("active_session"),
            session_overlap=feature_pkg.get("session_overlap", False),
            feed_health=feature_pkg.get("feed_health", "HEALTHY"),
            data_tier=feature_pkg.get("data_tier"),
            confidence_penalty=feature_pkg.get("confidence_penalty", 0.0),
            n_positions_at_entry=portfolio_output.get("n_open_positions", 0),
            portfolio_var_pct=portfolio_output.get("portfolio_var", {}).get("var_pct"),
            stress_test_passed=portfolio_output.get("stress_passed", True),
        )

        self._storage.insert_trade(entry)
        with self._lock:
            self._recent.append(entry.to_dict())
        return journal_id

    def record_trade_close(self, trade_id: str, exit_price: float,
                            exit_reason: str, realized_pnl: float,
                            pnl_pips: float, actual_rr: float,
                            trade_status: str, hold_hours: float,
                            regime_at_exit: Optional[str] = None,
                            mtf_at_exit: Optional[float] = None):
        self._storage.update_trade_exit(
            trade_id=trade_id,
            exit_price=exit_price,
            exit_time=time.time() * 1000,
            exit_reason=exit_reason,
            realized_pnl=realized_pnl,
            pnl_pips=pnl_pips,
            actual_rr=actual_rr,
            trade_status=trade_status,
            hold_hours=hold_hours,
            regime_at_exit=regime_at_exit,
            mtf_at_exit=mtf_at_exit,
        )

    def record_null(self, pair: str, decision: dict,
                     feature_pkg: dict, context: dict = None) -> str:
        journal_id = f"N_{uuid.uuid4().hex[:12].upper()}"
        context = context or {}

        entry = NullJournalEntry(
            journal_id=journal_id,
            pair=pair,
            timestamp_utc=time.time() * 1000,
            primary_null=decision.get("primary_null", "UNKNOWN"),
            secondary_nulls=decision.get("secondary_nulls", []),
            null_reason=decision.get("null_reason"),
            null_gate=decision.get("null_gate"),
            l2_mtf_score=feature_pkg.get("mtf_score"),
            l2_structure_state=feature_pkg.get("structure_state"),
            l2_regime_dominant=feature_pkg.get("regime_dominant"),
            l2_regime_confidence=feature_pkg.get("regime_confidence"),
            l2_volatility_score=feature_pkg.get("volatility_score"),
            l2_session_quality=feature_pkg.get("session_quality_score"),
            l2_news_blackout=feature_pkg.get("news_blackout", False),
            l3_regime_tag=decision.get("regime_tag"),
            l3_bayesian_result=decision.get("bayesian_result"),
            l3_threshold=decision.get("bayesian_threshold"),
            l3_ev_lower_bound=decision.get("ev_lower_bound"),
            l3_votes=decision.get("votes"),
            session=feature_pkg.get("active_session"),
            feed_health=feature_pkg.get("feed_health", "HEALTHY"),
            spread_ratio=context.get("spread_ratio"),
            vix_level=feature_pkg.get("vix_level"),
            stress_count=feature_pkg.get("multi_stress_count", 0),
        )

        self._storage.insert_null(entry)
        with self._lock:
            self._null_cache.append(entry.to_dict())
        return journal_id

    def analyze(self, question: str) -> dict:
        return self._analyzer.answer_question(question)

    def get_regime_stats(self, regime: Optional[str] = None) -> dict:
        return self._analyzer.compute_regime_stats(regime)

    def get_null_analysis(self) -> dict:
        return self._analyzer.compute_null_analysis()

    def get_all_regime_stats(self) -> dict:
        return self._analyzer._regime_performance_breakdown()

    def get_recent_trades(self, n: int = 20) -> list:
        with self._lock:
            return list(self._recent)[-n:]

    def get_recent_nulls(self, n: int = 20) -> list:
        with self._lock:
            return list(self._null_cache)[-n:]

    def get_total_counts(self) -> dict:
        trades = self._storage.query_trades()
        nulls = self._storage.query_nulls()
        return {
            "total_trades": len(trades),
            "total_nulls": len(nulls),
            "total_events": len(trades) + len(nulls),
            "null_rate": round(len(nulls) / max(1, len(trades) + len(nulls)) * 100, 1),
        }

    def save_performance_snapshot(self, regime: Optional[str] = None):
        stats = self.get_regime_stats(regime)
        stats["timestamp_utc"] = time.time() * 1000
        stats["regime"] = regime or "ALL"
        null_analysis = self.get_null_analysis()
        stats["null_rate"] = (
            null_analysis["n_nulls"] /
            max(1, null_analysis["n_nulls"] + stats.get("n_trades", 0))
        )
        stats["most_common_null"] = null_analysis.get("most_common_null")
        self._storage.save_performance_snapshot(stats)
