"""
═══════════════════════════════════════════════════════════════════════════════
FOREX TRADING SIGNAL SYSTEM — VERSION 6.0
FILE: journal_dashboard.py
JOURNAL — FULL INTELLIGENCE DASHBOARD
═══════════════════════════════════════════════════════════════════════════════

FEATURES:
    - All 500+ trades/rejections/NULLs from journal.db
    - Pattern recognition — most frequent setups at top
    - Deep ML engine (scikit-learn) — hidden pattern discovery
    - Multi-AI analysis (Claude → DeepSeek → GPT-4 via Puter)
    - News + COT + market structure per trade
    - Weekly intelligence report
    - Everything recorded and analysed

RUN:
    python journal_dashboard.py
    Open http://localhost:8082

═══════════════════════════════════════════════════════════════════════════════
"""

import os
import json
import time
import math
import sqlite3
import logging
import requests
import threading
import statistics
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from collections import defaultdict
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s"
)
logger = logging.getLogger("JournalDashboard")

PORT     = 8082
DB_PATH  = os.getenv("JOURNAL_DB", "journal.db")

# New Puter token
PUTER_TOKEN = os.getenv("PUTER_TOKEN",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InYyIn0.eyJ0IjoidCIsInYiOiIyIiwidG9rZW5fdWlkIjoiMGJiZWFmYTMtZTIwMS00NzI2LWI5NDQtZGNjMjk2Mjg1MmQ0IiwidXUiOiJ1QXhxUnhFQlRNYUNhdnFuQUIwQ2NBPT0iLCJzdSI6ImpPcHVNMlJoUXJLNmhvb25NVFcyVFE9PSIsImFpIjoidUF4cVJ4RUJUTWFDYXZxbkFCMENjQT09IiwiZnVsbF9hY2Nlc3MiOnRydWUsImlhdCI6MTc4MzE1OTQ2MH0.WcaXkVrX9Y5QNQGZB8UdM4_L7uiJzVOzruR3-rgVmpU")

GEMINI_KEY = os.getenv("GEMINI_API_KEY",
    "AQ.Ab8RN6Izm5_rW8-M6IHzehlpVvfoNFEecCPIRo1UEEMd27k6wQ")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — MULTI-AI ENGINE
# Claude + DeepSeek + GPT-4 via Puter — never runs out
# ═══════════════════════════════════════════════════════════════════════════════

class MultiAI:
    """
    Combines Claude + DeepSeek + GPT-4 via Puter.
    Falls back to Gemini if Puter unavailable.
    Knows your full 10-layer system architecture.
    """

    PUTER_URL = "https://api.puter.com/drivers/call"
    MODELS    = [
        ("claude-sonnet-4-5", "Claude"),
        ("deepseek-chat",     "DeepSeek"),
        ("gpt-4o-mini",       "GPT-4o"),
    ]

    SYSTEM_CONTEXT = """You are the AI analyst for a professional 10-layer forex trading system called The Dogma FX System.

SYSTEM ARCHITECTURE:
Layer 1:  Data feeds (OANDA, Dukascopy, news, COT, multi-asset, options)
Layer 2:  Feature engine (MTF score, regime, volatility, trend, session, institutional, macro, sentiment, order flow)
Layer 3:  Decision engine (Bayesian probability, 4-gate structure, NULL classification)
Layer 4A: Risk control (lot sizing, ATR stops, kill switch)
Layer 4B: Portfolio exposure (VaR, correlation, stress tests)
Layer 5:  Execution engine (PFVW validation, fill quality)
Layer 6:  Journal system (records everything to SQLite)
Layer 7:  Validation engine (statistical significance testing)
Layer 8:  Learning loop (evidence-driven parameter updates)
Layer 9:  Chaos mode (black swan protection, recovery phases)
Layer 10: Optimization (ATR trailing, partial exits, position management)

NULL TYPES: NULL_REGIME, NULL_STRUCTURE, NULL_EV, NULL_TIME, NULL_LIQUIDITY, NULL_RISK
SESSIONS: OVERLAP (best), LONDON, NEW_YORK, ASIA (weak), DEAD (blocked)
REGIMES: TREND, HV_TREND, RANGE, NEWS, CRISIS

The system targets 1-3 trades per week per pair with 55-65% win rate.
MTF minimum: 55 (STABLE: 75+, WEAK: 55-75, BROKEN: <55)
ADX minimum: 25 for TREND regime
Bayesian threshold: 0.62
Confidence minimum: 70

Your role: Analyse trades, find patterns, identify improvements, explain in plain English."""

    def __init__(self):
        self._headers       = {
            "Authorization": f"Bearer {PUTER_TOKEN}",
            "Content-Type" : "application/json",
        }
        self._working_model = None
        self._cache         = {}

    def analyze(self, prompt: str, cache_key: str = None) -> str:
        """Analyze with full system context. Cache results."""
        if cache_key and cache_key in self._cache:
            return self._cache[cache_key]

        full_prompt = f"{self.SYSTEM_CONTEXT}\n\n{prompt}"

        # Try cached model first
        if self._working_model:
            r = self._call_puter(full_prompt, self._working_model)
            if r:
                if cache_key: self._cache[cache_key] = r
                return r

        # Try all models
        for model_id, name in self.MODELS:
            r = self._call_puter(full_prompt, model_id)
            if r:
                self._working_model = model_id
                logger.info(f"MultiAI: using {name}")
                if cache_key: self._cache[cache_key] = r
                return r

        # Gemini fallback
        r = self._call_gemini(full_prompt)
        if cache_key: self._cache[cache_key] = r
        return r

    def _call_puter(self, prompt: str, model: str) -> Optional[str]:
        try:
            body = {
                "interface": "puter-chat-completion",
                "driver"   : model,
                "test_mode": False,
                "method"   : "complete",
                "args"     : {
                    "messages"   : [{"role": "user", "content": prompt}],
                    "max_tokens" : 800,
                    "temperature": 0.2,
                }
            }
            r = requests.post(
                self.PUTER_URL, headers=self._headers,
                json=body, timeout=30
            )
            if r.status_code == 200:
                d = r.json()
                if "result" in d:
                    res = d["result"]
                    if isinstance(res, dict):
                        c = res.get("choices",[])
                        if c: return c[0].get("message",{}).get("content","")
                        return res.get("content","")
                    if isinstance(res, str): return res
                if "message" in d:
                    return d["message"].get("content","")
        except Exception as e:
            logger.debug(f"Puter {model}: {e}")
        return None

    def _call_gemini(self, prompt: str) -> str:
        if not GEMINI_KEY:
            return "AI unavailable"
        try:
            url  = (
                "https://generativelanguage.googleapis.com/v1beta"
                f"/models/gemini-2.0-flash:generateContent?key={GEMINI_KEY}"
            )
            body = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature":0.2,"maxOutputTokens":800},
            }
            r = requests.post(url, json=body, timeout=30)
            if r.status_code == 200:
                return (r.json()["candidates"][0]["content"]
                        ["parts"][0]["text"].strip())
            return f"AI error {r.status_code}"
        except Exception as e:
            return f"AI error: {e}"

    def analyze_trade(self, t: dict) -> str:
        result   = t.get("result","WIN" if t.get("was_winner") else "LOSS")
        pair     = t.get("pair","")
        null_r   = t.get("primary_null") or t.get("primary_cause","")
        prompt   = f"""Analyse this trade from The Dogma FX System:

TRADE DATA:
Pair: {pair} | Direction: {t.get('direction','')} | Result: {result}
PnL: ${t.get('realized_pnl',0):.2f} | Pips: {t.get('pnl_pips',0):.1f}
RR: {t.get('actual_rr',0):.2f} | Hold: {t.get('hold_duration_hours',0):.1f}h
Exit: {t.get('exit_reason','')} | Session: {t.get('session','')}

LAYER 2 SCORES:
MTF: {t.get('l2_mtf_score',0):.1f}/100
Regime: {t.get('l3_regime_tag','')} (conf: {t.get('l2_regime_confidence',0):.1f}%)
Volatility: {t.get('l2_volatility_score',0):.1f}
Trend strength: {t.get('l2_trend_strength',0):.1f}
Session quality: {t.get('l2_session_quality',0):.1f}
Institutional: {t.get('l2_institutional',0):.1f}
Macro: {t.get('l2_macro_score',0):.1f}
Sentiment: {t.get('l2_sentiment_score',0):.1f}
Order flow: {t.get('l2_order_flow',0):.1f}

LAYER 3 DECISION:
Bayesian: {t.get('l3_bayesian_prob',0):.3f} (threshold: {t.get('l3_bayesian_threshold',0.62):.2f})
Confidence: {t.get('l3_confidence_score',0):.1f}/100
EV lower bound: ${t.get('l3_ev_lower_bound',0):.2f}

Explain:
1. WHY this trade {'won' if result in ('WIN','1') else 'lost'} — specific factors
2. Which Layer 2 score was most critical?
3. Was the Layer 3 decision correct given the scores?
4. One specific improvement for this exact setup
Keep it practical and short."""
        return self.analyze(prompt, cache_key=f"trade_{t.get('journal_id','')}")

    def analyze_null(self, n: dict) -> str:
        prompt = f"""Analyse this rejected trade (NULL) from The Dogma FX System:

NULL DATA:
Pair: {n.get('pair','')} | Primary NULL: {n.get('primary_null','')}
Null gate: {n.get('null_gate','')} | Reason: {n.get('null_reason','')}
Session: {n.get('session','')} | Regime: {n.get('l3_regime_tag','')}

SCORES AT REJECTION:
MTF: {n.get('l2_mtf_score',0):.1f}/100
Regime confidence: {n.get('l2_regime_confidence',0):.1f}%
Volatility: {n.get('l2_volatility_score',0):.1f}
Session quality: {n.get('l2_session_quality',0):.1f}
Bayesian: {n.get('l3_bayesian_result',0):.3f} (threshold: {n.get('l3_threshold',0.62):.2f})
EV lower: ${n.get('l3_ev_lower_bound',0):.2f}
News blackout: {n.get('l2_news_blackout',False)}

Explain:
1. Was this rejection CORRECT or was the filter TOO STRICT?
2. What specific threshold caused the rejection?
3. If this setup repeats often — should we adjust anything?
4. What would need to change for this to become a valid signal?
Short and direct."""
        return self.analyze(prompt, cache_key=f"null_{n.get('journal_id','')}")

    def analyze_pattern(self, pattern: dict) -> str:
        prompt = f"""Analyse this repeating pattern found in The Dogma FX System journal:

PATTERN:
Setup: {pattern.get('setup_key','')}
Occurrences: {pattern.get('count',0)} times
Win rate: {pattern.get('win_rate',0)*100:.1f}%
Avg PnL: ${pattern.get('avg_pnl',0):.2f}
Total PnL: ${pattern.get('total_pnl',0):.2f}
Most common NULL: {pattern.get('top_null','')}
Best session: {pattern.get('best_session','')}
Avg MTF: {pattern.get('avg_mtf',0):.1f}
Avg confidence: {pattern.get('avg_conf',0):.1f}

ML INSIGHTS:
Feature importance: {pattern.get('ml_insights','')}
Predicted improvement: {pattern.get('predicted_improvement','')}

Provide:
1. Is this pattern worth trading? Why?
2. What is causing the {pattern.get('win_rate',0)*100:.1f}% win rate?
3. Specific threshold adjustment to improve this pattern
4. Risk warning if any
Be direct — this drives real trading decisions."""
        return self.analyze(prompt, cache_key=f"pattern_{pattern.get('setup_key','')}")

    def weekly_intelligence_report(self, data: dict) -> str:
        prompt = f"""Generate the weekly intelligence report for The Dogma FX System.

WEEK SUMMARY:
Period: {data.get('period','')}
Total events: {data.get('total_events',0)}
Trades taken: {data.get('trades',0)} | Win rate: {data.get('win_rate',0)*100:.1f}%
NULLs issued: {data.get('nulls',0)} | NULL rate: {data.get('null_rate',0)*100:.1f}%
Total PnL: ${data.get('total_pnl',0):.2f}

TOP PATTERNS THIS WEEK:
{data.get('top_patterns','')}

ML DISCOVERIES:
{data.get('ml_discoveries','')}

FILTER ANALYSIS:
Too strict (blocking profitable trades): {data.get('too_strict','')}
Too loose (allowing losing trades): {data.get('too_loose','')}

NEWS IMPACT:
{data.get('news_impact','')}

COT ALIGNMENT:
{data.get('cot_alignment','')}

Generate:
1. EXECUTIVE SUMMARY (2 sentences)
2. BIGGEST WIN THIS WEEK — why it worked
3. BIGGEST PROBLEM — what to fix
4. TOP 3 FILTER ADJUSTMENTS with specific numbers
5. HIDDEN PATTERN discovered by ML
6. NEXT WEEK FORECAST — what to watch
7. SYSTEM HEALTH SCORE (1-10) with explanation

Be specific with numbers. This is the weekly bible for improving the system."""
        return self.analyze(prompt)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — ML ENGINE
# scikit-learn pattern discovery + deep learning
# ═══════════════════════════════════════════════════════════════════════════════

class MLEngine:
    """
    Machine learning engine for hidden pattern discovery.

    Uses scikit-learn:
    - RandomForest for feature importance
    - IsolationForest for anomaly/hidden pattern detection
    - KMeans clustering for trade grouping
    - GradientBoosting for win prediction

    Learns from all raw journal data — finds patterns
    the coded system hasn't discovered yet.
    """

    def __init__(self):
        self._models   = {}
        self._trained  = False
        self._insights = {}

    def train(self, trades: list, nulls: list) -> dict:
        """Train ML models on all journal data."""
        try:
            from sklearn.ensemble import (
                RandomForestClassifier, GradientBoostingClassifier,
                IsolationForest
            )
            from sklearn.cluster      import KMeans
            from sklearn.preprocessing import StandardScaler
            from sklearn.model_selection import cross_val_score
            import numpy as np

            if len(trades) < 10:
                return {"status": "insufficient_data",
                        "message": f"Need 10+ trades, have {len(trades)}"}

            # Build feature matrix from trades
            X_trades, y_trades = self._build_trade_features(trades)
            X_nulls            = self._build_null_features(nulls)

            if len(X_trades) < 5:
                return {"status": "insufficient_features"}

            X_arr = np.array(X_trades)
            y_arr = np.array(y_trades)

            scaler  = StandardScaler()
            X_scaled= scaler.fit_transform(X_arr)

            insights = {}

            # 1. Random Forest — feature importance
            rf = RandomForestClassifier(
                n_estimators=100, random_state=42, n_jobs=-1
            )
            rf.fit(X_scaled, y_arr)
            feature_names = self._feature_names()
            importances   = rf.feature_importances_
            top_features  = sorted(
                zip(feature_names, importances),
                key=lambda x: -x[1]
            )[:5]
            insights["top_predictive_features"] = [
                {"feature": f, "importance": round(imp*100, 1)}
                for f, imp in top_features
            ]

            # 2. Cross-validated accuracy
            if len(X_scaled) >= 10:
                cv_scores = cross_val_score(rf, X_scaled, y_arr, cv=min(5,len(X_scaled)//2))
                insights["ml_win_prediction_accuracy"] = round(cv_scores.mean()*100, 1)
            else:
                insights["ml_win_prediction_accuracy"] = 0

            # 3. Gradient Boosting — find optimal thresholds
            gb = GradientBoostingClassifier(
                n_estimators=50, random_state=42
            )
            gb.fit(X_scaled, y_arr)
            self._models["gb"] = (gb, scaler)

            # 4. KMeans clustering — discover trade groups
            n_clusters = min(5, len(X_scaled)//3)
            if n_clusters >= 2:
                km = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
                clusters = km.fit_predict(X_scaled)
                cluster_analysis = self._analyze_clusters(
                    trades, clusters, n_clusters
                )
                insights["clusters"] = cluster_analysis

            # 5. Isolation Forest — find anomalous profitable trades (hidden gems)
            iso = IsolationForest(contamination=0.1, random_state=42)
            anomaly_scores = iso.fit_predict(X_scaled)
            hidden_gems = []
            for i, (score, trade) in enumerate(zip(anomaly_scores, trades)):
                if score == -1 and trade.get("was_winner", 0) == 1:
                    hidden_gems.append({
                        "pair"   : trade.get("pair",""),
                        "session": trade.get("session",""),
                        "regime" : trade.get("l3_regime_tag",""),
                        "pnl"    : trade.get("realized_pnl",0),
                        "note"   : "Profitable but doesn't match normal pattern — hidden edge"
                    })
            insights["hidden_gems"] = hidden_gems[:5]

            # 6. Filter analysis — find over-strict filters
            filter_analysis = self._analyze_null_patterns(nulls, trades)
            insights["filter_analysis"] = filter_analysis

            # 7. Session × Regime matrix
            session_regime = self._session_regime_matrix(trades)
            insights["session_regime_matrix"] = session_regime

            # 8. Time-of-day patterns
            tod_patterns = self._time_patterns(trades)
            insights["time_patterns"] = tod_patterns

            # 9. Predict win probability for each NULL
            # (would these rejected trades have won?)
            if nulls and len(X_nulls) > 0 and len(X_scaled) >= 10:
                try:
                    X_null_arr    = np.array(X_nulls)
                    X_null_scaled = scaler.transform(
                        X_null_arr[:, :X_arr.shape[1]]
                    )
                    null_win_probs = gb.predict_proba(X_null_scaled)[:,1]
                    high_prob_nulls= []
                    for prob, null in zip(null_win_probs, nulls):
                        if prob > 0.65:
                            high_prob_nulls.append({
                                "pair"        : null.get("pair",""),
                                "null_type"   : null.get("primary_null",""),
                                "ml_win_prob" : round(float(prob)*100, 1),
                                "session"     : null.get("session",""),
                                "note"        : "ML predicts this would have won — filter may be too strict"
                            })
                    insights["should_have_traded"] = sorted(
                        high_prob_nulls, key=lambda x: -x["ml_win_prob"]
                    )[:10]
                except Exception:
                    insights["should_have_traded"] = []

            self._trained  = True
            self._insights = insights
            insights["status"]       = "trained"
            insights["trades_used"]  = len(trades)
            insights["nulls_used"]   = len(nulls)
            logger.info(f"ML trained: {len(trades)} trades, {len(nulls)} NULLs")
            return insights

        except ImportError:
            return {"status": "sklearn_not_installed",
                    "message": "Run: pip install scikit-learn"}
        except Exception as e:
            logger.error(f"ML training error: {e}")
            return {"status": "error", "message": str(e)}

    def predict_win(self, features: dict) -> float:
        """Predict win probability for a trade setup."""
        if not self._trained or "gb" not in self._models:
            return 0.5
        try:
            import numpy as np
            gb, scaler = self._models["gb"]
            X = np.array([self._extract_features(features)])
            X_scaled = scaler.transform(X)
            return float(gb.predict_proba(X_scaled)[0][1])
        except Exception:
            return 0.5

    def _build_trade_features(self, trades: list):
        X, y = [], []
        for t in trades:
            f = self._extract_features(t)
            if f:
                X.append(f)
                y.append(1 if t.get("was_winner",0) else 0)
        return X, y

    def _build_null_features(self, nulls: list):
        X = []
        for n in nulls:
            f = self._extract_features(n)
            if f: X.append(f)
        return X

    def _extract_features(self, t: dict) -> list:
        try:
            sess_map = {"OVERLAP":4,"LONDON":3,"NEW_YORK":2,"ASIA":1,"DEAD":0}
            reg_map  = {"TREND":4,"HV_TREND":3,"RANGE":2,"NEWS":1,"CRISIS":0}
            return [
                float(t.get("l2_mtf_score")         or 50),
                float(t.get("l2_regime_confidence")  or 50),
                float(t.get("l2_volatility_score")   or 50),
                float(t.get("l2_trend_strength")     or 50),
                float(t.get("l2_session_quality")    or 50),
                float(t.get("l2_institutional")      or 50),
                float(t.get("l2_macro_score")        or 50),
                float(t.get("l2_sentiment_score")    or 50),
                float(t.get("l2_order_flow")         or 50),
                float(t.get("l3_bayesian_prob")      or 0.62),
                float(t.get("l3_confidence_score")   or 70),
                float(t.get("l3_ev_lower_bound")     or 0),
                float(sess_map.get(t.get("session",""),2)),
                float(reg_map.get(t.get("l3_regime_tag",""),2)),
                float(t.get("spread_at_entry")       or 1),
            ]
        except Exception:
            return []

    def _feature_names(self) -> list:
        return [
            "mtf_score","regime_conf","volatility","trend_strength",
            "session_quality","institutional","macro","sentiment",
            "order_flow","bayesian","confidence","ev_lower",
            "session","regime","spread",
        ]

    def _analyze_clusters(self, trades, clusters, n_clusters) -> list:
        result = []
        for c in range(n_clusters):
            group = [t for t, cl in zip(trades, clusters) if cl == c]
            if not group: continue
            pnls = [t.get("realized_pnl",0) for t in group if t.get("realized_pnl")]
            wins = [p for p in pnls if p > 0]
            sessions = defaultdict(int)
            regimes  = defaultdict(int)
            for t in group:
                sessions[t.get("session","?")] += 1
                regimes[t.get("l3_regime_tag","?")] += 1
            result.append({
                "cluster"     : c,
                "size"        : len(group),
                "win_rate"    : round(len(wins)/len(pnls), 3) if pnls else 0,
                "avg_pnl"     : round(sum(pnls)/len(pnls), 2) if pnls else 0,
                "top_session" : max(sessions, key=sessions.get) if sessions else "",
                "top_regime"  : max(regimes,  key=regimes.get)  if regimes  else "",
            })
        return sorted(result, key=lambda x: -x.get("win_rate",0))

    def _analyze_null_patterns(self, nulls, trades) -> dict:
        if not nulls: return {}
        null_types = defaultdict(int)
        for n in nulls:
            null_types[n.get("primary_null","?")] += 1
        total_nulls = len(nulls)

        # Check if any NULL type is dominant (>40%) — possible over-filtering
        over_strict = []
        for nt, cnt in null_types.items():
            pct = cnt / total_nulls
            if pct > 0.35:
                over_strict.append({
                    "null_type" : nt,
                    "count"     : cnt,
                    "pct"       : round(pct*100,1),
                    "verdict"   : "POSSIBLY TOO STRICT — dominates rejections",
                })

        # Win rate comparison: when NOT null vs overall
        total_trades = len(trades)
        wins = sum(1 for t in trades if t.get("was_winner",0))
        wr   = wins/total_trades if total_trades > 0 else 0

        return {
            "null_distribution": dict(null_types),
            "possibly_too_strict": over_strict,
            "trade_win_rate"    : round(wr*100, 1),
            "null_rate"         : round(total_nulls/(total_nulls+total_trades)*100, 1),
        }

    def _session_regime_matrix(self, trades) -> dict:
        matrix = defaultdict(lambda: defaultdict(list))
        for t in trades:
            sess   = t.get("session","?")
            regime = t.get("l3_regime_tag","?")
            pnl    = t.get("realized_pnl",0)
            if pnl: matrix[sess][regime].append(pnl)

        result = {}
        for sess, regimes in matrix.items():
            result[sess] = {}
            for regime, pnls in regimes.items():
                wins = [p for p in pnls if p > 0]
                result[sess][regime] = {
                    "n"       : len(pnls),
                    "win_rate": round(len(wins)/len(pnls)*100,1) if pnls else 0,
                    "avg_pnl" : round(sum(pnls)/len(pnls),2)    if pnls else 0,
                }
        return result

    def _time_patterns(self, trades) -> list:
        by_hour = defaultdict(list)
        for t in trades:
            ts = t.get("entry_time") or t.get("timestamp_utc",0)
            if ts:
                hour = datetime.fromtimestamp(ts/1000, tz=timezone.utc).hour
                pnl  = t.get("realized_pnl",0)
                if pnl: by_hour[hour].append(pnl)

        patterns = []
        for hour, pnls in by_hour.items():
            wins = [p for p in pnls if p > 0]
            if len(pnls) >= 3:
                patterns.append({
                    "hour"    : hour,
                    "n"       : len(pnls),
                    "win_rate": round(len(wins)/len(pnls)*100,1),
                    "avg_pnl" : round(sum(pnls)/len(pnls),2),
                })
        return sorted(patterns, key=lambda x: -x["avg_pnl"])


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — JOURNAL DATA LOADER
# ═══════════════════════════════════════════════════════════════════════════════

class JournalLoader:
    """Loads all data from journal.db."""

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path

    def _conn(self):
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    def get_all_trades(self, limit: int = 1000) -> list:
        try:
            conn = self._conn()
            rows = conn.execute(
                "SELECT * FROM trades ORDER BY timestamp_utc DESC LIMIT ?",
                (limit,)
            ).fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.debug(f"Load trades: {e}")
            return []

    def get_all_nulls(self, limit: int = 2000) -> list:
        try:
            conn = self._conn()
            rows = conn.execute(
                "SELECT * FROM nulls ORDER BY timestamp_utc DESC LIMIT ?",
                (limit,)
            ).fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.debug(f"Load nulls: {e}")
            return []

    def get_patterns(self, trades: list, nulls: list) -> list:
        """Group all events by setup pattern, most frequent first."""
        groups = defaultdict(lambda: {
            "trades":[], "nulls":[], "wins":[], "losses":[]
        })

        for t in trades:
            key = (
                f"{t.get('pair','?')}_"
                f"{t.get('l3_regime_tag','?')}_"
                f"{t.get('session','?')}"
            )
            groups[key]["trades"].append(t)
            if t.get("was_winner",0):
                groups[key]["wins"].append(t)
            else:
                groups[key]["losses"].append(t)

        for n in nulls:
            key = (
                f"{n.get('pair','?')}_"
                f"{n.get('l3_regime_tag','?')}_"
                f"{n.get('session','?')}"
            )
            groups[key]["nulls"].append(n)

        patterns = []
        for setup_key, data in groups.items():
            all_t  = data["trades"]
            all_n  = data["nulls"]
            wins   = data["wins"]
            losses = data["losses"]
            pnls   = [t.get("realized_pnl",0) for t in all_t if t.get("realized_pnl")]
            count  = len(all_t) + len(all_n)
            if count == 0: continue

            null_counts = defaultdict(int)
            for n in all_n:
                null_counts[n.get("primary_null","?")] += 1
            top_null = max(null_counts, key=null_counts.get) if null_counts else ""

            mtf_scores = [t.get("l2_mtf_score",0) for t in all_t if t.get("l2_mtf_score")]
            conf_scores= [t.get("l3_confidence_score",0) for t in all_t if t.get("l3_confidence_score")]

            patterns.append({
                "setup_key"   : setup_key,
                "count"       : count,
                "trade_count" : len(all_t),
                "null_count"  : len(all_n),
                "win_count"   : len(wins),
                "loss_count"  : len(losses),
                "win_rate"    : round(len(wins)/len(all_t),3) if all_t else 0,
                "total_pnl"   : round(sum(pnls),2),
                "avg_pnl"     : round(sum(pnls)/len(pnls),2) if pnls else 0,
                "top_null"    : top_null,
                "avg_mtf"     : round(sum(mtf_scores)/len(mtf_scores),1) if mtf_scores else 0,
                "avg_conf"    : round(sum(conf_scores)/len(conf_scores),1) if conf_scores else 0,
                "best_session": setup_key.split("_")[-1],
                "trades"      : all_t[:5],
                "nulls"       : all_n[:5],
            })

        return sorted(patterns, key=lambda x: -x["count"])

    def get_weekly_summary(self, trades: list, nulls: list) -> dict:
        now   = datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)
        ws_ms = week_start.timestamp() * 1000

        wt = [t for t in trades if (t.get("timestamp_utc",0) or 0) >= ws_ms]
        wn = [n for n in nulls  if (n.get("timestamp_utc",0) or 0) >= ws_ms]

        pnls = [t.get("realized_pnl",0) for t in wt if t.get("realized_pnl")]
        wins = [p for p in pnls if p > 0]
        wr   = len(wins)/len(pnls) if pnls else 0

        null_counts = defaultdict(int)
        for n in wn:
            null_counts[n.get("primary_null","?")] += 1

        return {
            "period"      : f"{week_start.strftime('%d/%m')} → {now.strftime('%d/%m/%Y')}",
            "trades"      : len(wt),
            "nulls"       : len(wn),
            "total_events": len(wt) + len(wn),
            "wins"        : len(wins),
            "win_rate"    : wr,
            "null_rate"   : len(wn)/(len(wt)+len(wn)) if (wt or wn) else 0,
            "total_pnl"   : round(sum(pnls),2),
            "null_dist"   : dict(null_counts),
        }

    def get_stats(self, trades: list, nulls: list) -> dict:
        pnls  = [t.get("realized_pnl",0) for t in trades if t.get("realized_pnl")]
        wins  = [p for p in pnls if p > 0]
        n     = len(pnls)
        wr    = len(wins)/n if n > 0 else 0
        avg_w = sum(wins)/len(wins) if wins else 0
        avg_l = sum(p for p in pnls if p<=0)/max(1,len([p for p in pnls if p<=0]))

        null_counts = defaultdict(int)
        for null in nulls:
            null_counts[null.get("primary_null","?")] += 1

        return {
            "total_trades" : n,
            "total_nulls"  : len(nulls),
            "total_events" : n + len(nulls),
            "wins"         : len(wins),
            "losses"       : n - len(wins),
            "win_rate"     : round(wr*100, 1),
            "total_pnl"    : round(sum(pnls), 2),
            "avg_win"      : round(avg_w, 2),
            "avg_loss"     : round(avg_l, 2),
            "null_rate"    : round(len(nulls)/(n+len(nulls))*100,1) if (n+len(nulls)) else 0,
            "null_dist"    : dict(null_counts),
            "top_null"     : max(null_counts, key=null_counts.get) if null_counts else "—",
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — DATA CACHE (refreshes every 60s)
# ═══════════════════════════════════════════════════════════════════════════════

class DataCache:
    def __init__(self):
        self._lock     = threading.Lock()
        self._trades   = []
        self._nulls    = []
        self._patterns = []
        self._stats    = {}
        self._weekly   = {}
        self._ml       = {}
        self._last     = 0
        self._loader   = JournalLoader()
        self._ml_eng   = MLEngine()
        self._ai       = MultiAI()
        self.refresh()

    def refresh(self):
        try:
            trades   = self._loader.get_all_trades(2000)
            nulls    = self._loader.get_all_nulls(2000)
            patterns = self._loader.get_patterns(trades, nulls)
            stats    = self._loader.get_stats(trades, nulls)
            weekly   = self._loader.get_weekly_summary(trades, nulls)
            ml       = self._ml_eng.train(trades, nulls)

            with self._lock:
                self._trades   = trades
                self._nulls    = nulls
                self._patterns = patterns
                self._stats    = stats
                self._weekly   = weekly
                self._ml       = ml
                self._last     = time.time()

            logger.info(
                f"Cache refreshed: {len(trades)} trades, "
                f"{len(nulls)} NULLs, {len(patterns)} patterns"
            )
        except Exception as e:
            logger.error(f"Cache refresh error: {e}")

    def get(self):
        if time.time() - self._last > 60:
            threading.Thread(target=self.refresh, daemon=True).start()
        with self._lock:
            return {
                "trades"  : self._trades,
                "nulls"   : self._nulls,
                "patterns": self._patterns,
                "stats"   : self._stats,
                "weekly"  : self._weekly,
                "ml"      : self._ml,
            }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — HTML BUILDER
# ═══════════════════════════════════════════════════════════════════════════════

def build_journal_html(data: dict) -> str:
    stats    = data.get("stats",    {})
    patterns = data.get("patterns", [])
    trades   = data.get("trades",   [])
    nulls    = data.get("nulls",    [])
    ml       = data.get("ml",       {})
    weekly   = data.get("weekly",   {})

    def fmt_ts(ts):
        if not ts: return "--"
        try:
            return datetime.fromtimestamp(
                ts/1000, tz=timezone.utc
            ).strftime("%d/%m/%Y %H:%M")
        except Exception:
            return "--"

    # Pattern cards
    pattern_cards = ""
    for p in patterns[:50]:
        wr     = p.get("win_rate",0)
        wr_cls = "pos" if wr >= 0.55 else ("warn" if wr >= 0.45 else "neg")
        pnl    = p.get("total_pnl",0)
        pnl_s  = f"+${pnl:.0f}" if pnl >= 0 else f"-${abs(pnl):.0f}"
        pnl_cls= "pos" if pnl >= 0 else "neg"
        pattern_cards += f"""
        <div class="pattern-card" onclick="loadPattern('{p['setup_key']}')">
          <div class="pc-header">
            <span class="pc-key">{p['setup_key'].replace('_',' / ')}</span>
            <span class="pc-count">{p['count']}×</span>
          </div>
          <div class="pc-stats">
            <div class="pc-stat">
              <div class="pc-label">Win rate</div>
              <div class="pc-val {wr_cls}">{wr*100:.1f}%</div>
            </div>
            <div class="pc-stat">
              <div class="pc-label">Total PnL</div>
              <div class="pc-val {pnl_cls}">{pnl_s}</div>
            </div>
            <div class="pc-stat">
              <div class="pc-label">Trades</div>
              <div class="pc-val">{p['trade_count']}</div>
            </div>
            <div class="pc-stat">
              <div class="pc-label">NULLs</div>
              <div class="pc-val warn">{p['null_count']}</div>
            </div>
          </div>
          <div class="pc-footer">
            <span class="pc-tag">MTF: {p['avg_mtf']:.0f}</span>
            <span class="pc-tag">Conf: {p['avg_conf']:.0f}</span>
            <span class="pc-tag neg">Top NULL: {p['top_null'] or '—'}</span>
          </div>
        </div>"""

    # Trade rows
    trade_rows = ""
    for t in trades[:200]:
        won    = t.get("was_winner",0)
        pnl    = t.get("realized_pnl",0) or 0
        pnl_s  = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
        pnl_c  = "pos" if pnl >= 0 else "neg"
        trade_rows += f"""
        <tr class="trade-row {'win-row' if won else 'loss-row'}"
            onclick="loadTrade('{t.get('journal_id','')}','trade')">
          <td>{fmt_ts(t.get('entry_time') or t.get('timestamp_utc',0))}</td>
          <td><strong>{t.get('pair','')}</strong></td>
          <td class="{'dir-buy' if t.get('direction','')=='BUY' else 'dir-sell'}">{t.get('direction','')}</td>
          <td class="{pnl_c}">{pnl_s}</td>
          <td>{t.get('pnl_pips',0) or 0:.1f}</td>
          <td>{t.get('actual_rr',0) or 0:.2f}R</td>
          <td>{t.get('l3_regime_tag','')}</td>
          <td>{t.get('session','')}</td>
          <td>{t.get('l2_mtf_score',0) or 0:.0f}</td>
          <td>{t.get('l3_confidence_score',0) or 0:.0f}</td>
          <td>{'✅' if won else '❌'}</td>
        </tr>"""

    # NULL rows
    null_rows = ""
    for n in nulls[:200]:
        null_rows += f"""
        <tr class="null-row" onclick="loadTrade('{n.get('journal_id','')}','null')">
          <td>{fmt_ts(n.get('timestamp_utc',0))}</td>
          <td><strong>{n.get('pair','')}</strong></td>
          <td>—</td>
          <td class="neg">{n.get('primary_null','')}</td>
          <td colspan="2">{n.get('null_reason','')[:40] if n.get('null_reason') else ''}</td>
          <td>{n.get('l3_regime_tag','')}</td>
          <td>{n.get('session','')}</td>
          <td>{n.get('l2_mtf_score',0) or 0:.0f}</td>
          <td>{n.get('l2_regime_confidence',0) or 0:.0f}</td>
          <td>⛔</td>
        </tr>"""

    # ML insights
    ml_html = ""
    if ml.get("status") == "trained":
        top_f = ml.get("top_predictive_features",[])
        for f in top_f:
            ml_html += f"""
            <div class="ml-feature">
              <span class="ml-fname">{f['feature'].replace('_',' ').title()}</span>
              <div class="ml-bar-wrap">
                <div class="ml-bar" style="width:{f['importance']}%"></div>
              </div>
              <span class="ml-fval">{f['importance']:.1f}%</span>
            </div>"""

        gems = ml.get("hidden_gems",[])
        if gems:
            ml_html += '<div class="ml-section-title">🔥 Hidden Profitable Patterns</div>'
            for g in gems:
                ml_html += f"""
                <div class="ml-gem">
                  {g.get('pair','')} {g.get('session','')} {g.get('regime','')}
                  — PnL ${g.get('pnl',0):.2f} — {g.get('note','')}
                </div>"""

        strict = ml.get("filter_analysis",{}).get("possibly_too_strict",[])
        if strict:
            ml_html += '<div class="ml-section-title">⚠️ Possibly Over-Strict Filters</div>'
            for s in strict:
                ml_html += f"""
                <div class="ml-strict">
                  {s['null_type']}: {s['pct']}% of all rejections — {s['verdict']}
                </div>"""

        should = ml.get("should_have_traded",[])
        if should:
            ml_html += '<div class="ml-section-title">💡 ML says these NULLs should have traded</div>'
            for s in should[:5]:
                ml_html += f"""
                <div class="ml-should">
                  {s['pair']} {s['session']} — {s['null_type']} — ML win prob: {s['ml_win_prob']}%
                </div>"""
    else:
        ml_html = f'<div class="ml-waiting">⏳ {ml.get("message","ML training — need more trades")}</div>'

    # Null distribution
    null_dist_html = ""
    nd = stats.get("null_dist",{})
    total_n = sum(nd.values()) or 1
    colors  = ["#3b82f6","#10b981","#8b5cf6","#f59e0b","#ef4444","#6b7280","#ec4899"]
    for i,(k,v) in enumerate(sorted(nd.items(), key=lambda x:-x[1])[:7]):
        pct = round(v/total_n*100,1)
        col = colors[i%len(colors)]
        null_dist_html += f"""
        <div class="null-bar-row">
          <span class="null-bar-label">{k}</span>
          <div class="null-bar-track">
            <div class="null-bar-fill" style="width:{pct}%;background:{col}"></div>
          </div>
          <span class="null-bar-pct">{pct}%({v})</span>
        </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Journal Intelligence — Dogma FX</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f1117;color:#e2e8f0;font-size:13px}}

  .topbar{{display:flex;align-items:center;justify-content:space-between;padding:12px 20px;background:#1a1d27;border-bottom:1px solid #2d3142;position:sticky;top:0;z-index:50}}
  .logo{{font-size:14px;font-weight:600;color:#f1f5f9}}
  .logo span{{color:#64748b;font-weight:400}}
  .back-btn{{font-size:12px;color:#60a5fa;cursor:pointer;text-decoration:none}}
  .tabs{{display:flex;gap:4px}}
  .tab{{padding:6px 14px;border-radius:6px;font-size:12px;font-weight:500;cursor:pointer;border:none;background:#2d3142;color:#94a3b8}}
  .tab.active{{background:#3b82f6;color:white}}

  .main{{padding:16px 20px;max-width:1600px;margin:0 auto}}

  .stats-row{{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:8px;margin-bottom:16px}}
  .stat{{background:#1a1d27;border:1px solid #2d3142;border-radius:8px;padding:12px 14px}}
  .stat-label{{font-size:10px;color:#64748b;text-transform:uppercase;letter-spacing:.04em;margin-bottom:4px}}
  .stat-value{{font-size:18px;font-weight:600;color:#f1f5f9}}
  .stat-sub{{font-size:10px;color:#64748b;margin-top:2px}}
  .pos{{color:#22c55e}}.neg{{color:#ef4444}}.warn{{color:#f59e0b}}

  .section{{background:#1a1d27;border:1px solid #2d3142;border-radius:10px;padding:14px 16px;margin-bottom:14px}}
  .section-title{{font-size:11px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:.05em;margin-bottom:12px;display:flex;align-items:center;justify-content:space-between}}

  .pattern-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:8px}}
  .pattern-card{{background:#0f1117;border:1px solid #2d3142;border-radius:8px;padding:12px;cursor:pointer;transition:border-color .15s}}
  .pattern-card:hover{{border-color:#3b82f6}}
  .pc-header{{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}}
  .pc-key{{font-size:12px;font-weight:600;color:#f1f5f9}}
  .pc-count{{font-size:10px;background:#374151;color:#9ca3af;padding:2px 7px;border-radius:10px}}
  .pc-stats{{display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:6px;margin-bottom:8px}}
  .pc-stat{{text-align:center}}
  .pc-label{{font-size:9px;color:#64748b;text-transform:uppercase}}
  .pc-val{{font-size:13px;font-weight:600}}
  .pc-footer{{display:flex;gap:4px;flex-wrap:wrap}}
  .pc-tag{{font-size:9px;padding:2px 6px;border-radius:4px;background:#2d3142;color:#94a3b8}}
  .pc-tag.neg{{background:#450a0a;color:#f87171}}

  table{{width:100%;border-collapse:collapse;font-size:12px}}
  th{{color:#64748b;font-weight:500;text-align:left;padding:0 8px 8px 0;border-bottom:1px solid #2d3142;font-size:10px;text-transform:uppercase;letter-spacing:.04em;white-space:nowrap}}
  td{{padding:7px 8px 7px 0;border-bottom:1px solid #111827;cursor:pointer}}
  tr:hover td{{background:#1a1d27}}
  .win-row td:first-child{{border-left:2px solid #22c55e}}
  .loss-row td:first-child{{border-left:2px solid #ef4444}}
  .null-row td:first-child{{border-left:2px solid #f59e0b}}
  .dir-buy{{color:#22c55e;font-weight:600}}.dir-sell{{color:#ef4444;font-weight:600}}

  .null-bar-row{{display:flex;align-items:center;gap:8px;margin-bottom:6px}}
  .null-bar-label{{font-size:11px;color:#94a3b8;min-width:140px}}
  .null-bar-track{{flex:1;height:5px;background:#2d3142;border-radius:3px;overflow:hidden}}
  .null-bar-fill{{height:100%;border-radius:3px}}
  .null-bar-pct{{font-size:10px;color:#64748b;min-width:60px;text-align:right}}

  .ml-feature{{display:flex;align-items:center;gap:8px;margin-bottom:6px}}
  .ml-fname{{font-size:11px;color:#94a3b8;min-width:140px}}
  .ml-bar-wrap{{flex:1;height:5px;background:#2d3142;border-radius:3px;overflow:hidden}}
  .ml-bar{{height:100%;background:#3b82f6;border-radius:3px}}
  .ml-fval{{font-size:10px;color:#60a5fa;min-width:35px;text-align:right}}
  .ml-section-title{{font-size:11px;font-weight:600;color:#64748b;margin:12px 0 6px;text-transform:uppercase;letter-spacing:.04em}}
  .ml-gem,.ml-strict,.ml-should{{font-size:12px;color:#e2e8f0;padding:5px 8px;background:#0f1117;border-radius:6px;margin-bottom:4px;border-left:3px solid #3b82f6}}
  .ml-strict{{border-left-color:#f59e0b}}
  .ml-should{{border-left-color:#22c55e}}
  .ml-waiting{{color:#64748b;font-style:italic;padding:20px;text-align:center}}

  .modal-overlay{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:100;overflow-y:auto;padding:20px}}
  .modal{{background:#1a1d27;border:1px solid #2d3142;border-radius:12px;max-width:750px;margin:0 auto;padding:22px}}
  .modal-header{{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px}}
  .modal-title{{font-size:15px;font-weight:600;color:#f1f5f9}}
  .modal-close{{background:none;border:none;color:#64748b;font-size:20px;cursor:pointer}}
  .modal-row{{display:flex;justify-content:space-between;font-size:12px;padding:5px 0;border-bottom:1px solid #111827}}
  .modal-row:last-child{{border-bottom:none}}
  .modal-key{{color:#64748b}}.modal-val{{color:#e2e8f0;font-weight:500}}
  .modal-section{{margin-bottom:14px}}
  .modal-section-title{{font-size:10px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:.05em;margin-bottom:8px;padding-bottom:5px;border-bottom:1px solid #2d3142}}

  .ai-box{{background:#0f1117;border:1px solid #1e3a5f;border-radius:8px;padding:14px;margin-top:14px}}
  .ai-title{{font-size:11px;font-weight:600;color:#60a5fa;margin-bottom:8px;display:flex;align-items:center;gap:6px}}
  .ai-text{{font-size:12px;color:#cbd5e1;line-height:1.7;white-space:pre-wrap}}
  .ai-loading{{font-size:12px;color:#64748b;font-style:italic}}
  .ai-btn{{background:#1e3a5f;color:#60a5fa;border:1px solid #2563eb;border-radius:6px;padding:6px 14px;font-size:11px;cursor:pointer;margin-top:10px}}
  .ai-btn:hover{{background:#2563eb;color:white}}

  .weekly-box{{background:#0f1117;border:1px solid #2d3142;border-radius:8px;padding:14px}}
  .weekly-text{{font-size:12px;color:#cbd5e1;line-height:1.7;white-space:pre-wrap}}

  .tab-content{{display:none}}.tab-content.active{{display:block}}

  @media(max-width:700px){{.pattern-grid{{grid-template-columns:1fr}}.stats-row{{grid-template-columns:repeat(3,1fr)}}}}
</style>
</head>
<body>

<div class="topbar">
  <div style="display:flex;align-items:center;gap:12px">
    <a href="http://localhost:8080" class="back-btn">← Dashboard</a>
    <div class="logo">Journal Intelligence <span>— Dogma FX</span></div>
  </div>
  <div class="tabs">
    <button class="tab active" onclick="showTab('patterns')">Patterns</button>
    <button class="tab" onclick="showTab('trades')">Trades</button>
    <button class="tab" onclick="showTab('nulls')">Rejections</button>
    <button class="tab" onclick="showTab('ml')">ML Engine</button>
    <button class="tab" onclick="showTab('weekly')">Weekly Report</button>
  </div>
  <div style="font-size:11px;color:#64748b">{stats.get('total_events',0)} events total</div>
</div>

<div class="main">

  <!-- Stats -->
  <div class="stats-row">
    <div class="stat"><div class="stat-label">Total Events</div><div class="stat-value">{stats.get('total_events',0)}</div><div class="stat-sub">trades + NULLs</div></div>
    <div class="stat"><div class="stat-label">Trades Taken</div><div class="stat-value">{stats.get('total_trades',0)}</div><div class="stat-sub">{stats.get('wins',0)}W / {stats.get('losses',0)}L</div></div>
    <div class="stat"><div class="stat-label">Win Rate</div><div class="stat-value {'pos' if stats.get('win_rate',0)>=55 else 'warn'}">{stats.get('win_rate',0)}%</div><div class="stat-sub">target: 55-65%</div></div>
    <div class="stat"><div class="stat-label">Total PnL</div><div class="stat-value {'pos' if stats.get('total_pnl',0)>=0 else 'neg'}">${stats.get('total_pnl',0):+.0f}</div><div class="stat-sub">all closed trades</div></div>
    <div class="stat"><div class="stat-label">Rejections</div><div class="stat-value warn">{stats.get('total_nulls',0)}</div><div class="stat-sub">NULL rate: {stats.get('null_rate',0)}%</div></div>
    <div class="stat"><div class="stat-label">Top NULL</div><div class="stat-value" style="font-size:12px">{stats.get('top_null','—')}</div><div class="stat-sub">most frequent</div></div>
    <div class="stat"><div class="stat-label">ML Accuracy</div><div class="stat-value {'pos' if (ml.get('ml_win_prediction_accuracy') or 0)>=60 else 'warn'}">{ml.get('ml_win_prediction_accuracy',0) or '—'}{'%' if ml.get('ml_win_prediction_accuracy') else ''}</div><div class="stat-sub">win prediction</div></div>
    <div class="stat"><div class="stat-label">This Week</div><div class="stat-value">{weekly.get('trades',0)}</div><div class="stat-sub">trades taken</div></div>
  </div>

  <!-- PATTERNS TAB -->
  <div id="tab-patterns" class="tab-content active">
    <div class="section">
      <div class="section-title">
        Repeating Patterns — most frequent first
        <span style="font-size:10px;font-weight:400">{len(patterns)} unique setups</span>
      </div>
      <div class="pattern-grid">{pattern_cards}</div>
    </div>
  </div>

  <!-- TRADES TAB -->
  <div id="tab-trades" class="tab-content">
    <div class="section">
      <div class="section-title">All Trades — click any row for full detail + AI analysis</div>
      <div style="overflow-x:auto">
        <table>
          <thead><tr>
            <th>Date</th><th>Pair</th><th>Dir</th><th>PnL</th>
            <th>Pips</th><th>RR</th><th>Regime</th><th>Session</th>
            <th>MTF</th><th>Conf</th><th>Result</th>
          </tr></thead>
          <tbody>{trade_rows}</tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- NULLS TAB -->
  <div id="tab-nulls" class="tab-content">
    <div class="section" style="margin-bottom:14px">
      <div class="section-title">NULL Distribution</div>
      {null_dist_html}
    </div>
    <div class="section">
      <div class="section-title">All Rejections — click any row for AI analysis</div>
      <div style="overflow-x:auto">
        <table>
          <thead><tr>
            <th>Date</th><th>Pair</th><th>Dir</th><th>NULL Type</th>
            <th colspan="2">Reason</th><th>Regime</th><th>Session</th>
            <th>MTF</th><th>RegConf</th><th>Status</th>
          </tr></thead>
          <tbody>{null_rows}</tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- ML ENGINE TAB -->
  <div id="tab-ml" class="tab-content">
    <div class="section">
      <div class="section-title">
        Machine Learning Engine
        <span style="font-size:10px;font-weight:400">
          {'Trained on '+str(ml.get('trades_used',0))+' trades + '+str(ml.get('nulls_used',0))+' NULLs' if ml.get('status')=='trained' else ml.get('message','waiting')}
        </span>
      </div>
      <div class="ml-section-title">📊 Top Predictive Features (what actually drives wins)</div>
      {ml_html}
    </div>

    <div class="section">
      <div class="section-title">Session × Regime Performance Matrix</div>
      <div id="matrixTable"></div>
    </div>

    <div class="section">
      <div class="section-title">Best Hours to Trade (UTC)</div>
      <div id="timeChart"></div>
    </div>
  </div>

  <!-- WEEKLY REPORT TAB -->
  <div id="tab-weekly" class="tab-content">
    <div class="section">
      <div class="section-title">Weekly Intelligence Report — {weekly.get('period','')}</div>
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:16px">
        <div class="stat"><div class="stat-label">Trades</div><div class="stat-value">{weekly.get('trades',0)}</div></div>
        <div class="stat"><div class="stat-label">NULLs</div><div class="stat-value warn">{weekly.get('nulls',0)}</div></div>
        <div class="stat"><div class="stat-label">Win Rate</div><div class="stat-value {'pos' if weekly.get('win_rate',0)>=0.55 else 'warn'}">{weekly.get('win_rate',0)*100:.1f}%</div></div>
        <div class="stat"><div class="stat-label">PnL</div><div class="stat-value {'pos' if weekly.get('total_pnl',0)>=0 else 'neg'}">${weekly.get('total_pnl',0):+.0f}</div></div>
      </div>
      <div class="weekly-box">
        <div class="ai-title">🤖 AI Weekly Intelligence Report (Claude + DeepSeek + GPT-4)</div>
        <div class="weekly-text ai-loading" id="weeklyAI">
          Click below to generate this week's intelligence report...
        </div>
        <button class="ai-btn" onclick="generateWeekly()">
          🤖 Generate Weekly Intelligence Report
        </button>
      </div>
    </div>
  </div>

</div>

<!-- Trade/NULL Detail Modal -->
<div class="modal-overlay" id="modal" onclick="closeModal(event)">
  <div class="modal">
    <div class="modal-header">
      <div class="modal-title" id="modalTitle">Detail</div>
      <button class="modal-close" onclick="closeModalDirect()">✕</button>
    </div>
    <div id="modalBody"></div>
  </div>
</div>

<script>
var allData = {json.dumps({
    "trades" : {t.get("journal_id",""):t for t in trades[:200]},
    "nulls"  : {n.get("journal_id",""):n for n in nulls[:200]},
    "patterns": {p["setup_key"]:p for p in patterns[:50]},
    "ml"     : ml,
    "weekly" : weekly,
}, default=str)};

function showTab(name) {{
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById('tab-'+name).classList.add('active');
  event.target.classList.add('active');
  if (name === 'ml') renderMatrix();
  if (name === 'weekly') renderTimeChart();
}}

function fmt(ts) {{
  if (!ts) return '--';
  var d = new Date(ts);
  return ('0'+d.getUTCDate()).slice(-2)+'/'+
         ('0'+(d.getUTCMonth()+1)).slice(-2)+'/'+
         d.getUTCFullYear()+' '+
         ('0'+d.getUTCHours()).slice(-2)+':'+
         ('0'+d.getUTCMinutes()).slice(-2);
}}

function closeModal(e) {{
  if (e.target === document.getElementById('modal')) closeModalDirect();
}}
function closeModalDirect() {{
  document.getElementById('modal').style.display = 'none';
}}

function loadTrade(id, type) {{
  var item = type === 'trade' ? allData.trades[id] : allData.nulls[id];
  if (!item) return;

  document.getElementById('modal').style.display = 'block';

  if (type === 'trade') {{
    var won    = item.was_winner;
    var pnl    = (item.realized_pnl||0).toFixed(2);
    var pnlCls = item.realized_pnl>=0 ? 'pos' : 'neg';
    document.getElementById('modalTitle').textContent =
      (won?'✅':'❌') + ' ' + item.pair + ' ' + (item.direction||'') + ' — ' + fmt(item.entry_time||item.timestamp_utc);

    document.getElementById('modalBody').innerHTML = `
      <div class="modal-section">
        <div class="modal-section-title">Trade Summary</div>
        <div class="modal-row"><span class="modal-key">Result</span><span class="modal-val ${{pnlCls}}">${{won?'WIN':'LOSS'}} — $${{pnl}}</span></div>
        <div class="modal-row"><span class="modal-key">Pips</span><span class="modal-val">${{(item.pnl_pips||0).toFixed(1)}}</span></div>
        <div class="modal-row"><span class="modal-key">RR</span><span class="modal-val">${{(item.actual_rr||0).toFixed(2)}}R</span></div>
        <div class="modal-row"><span class="modal-key">Lot size</span><span class="modal-val">${{item.lot_size||0}}</span></div>
        <div class="modal-row"><span class="modal-key">Hold time</span><span class="modal-val">${{(item.hold_duration_hours||0).toFixed(1)}}h</span></div>
        <div class="modal-row"><span class="modal-key">Exit reason</span><span class="modal-val">${{item.exit_reason||'—'}}</span></div>
        <div class="modal-row"><span class="modal-key">Session</span><span class="modal-val">${{item.session||'—'}}</span></div>
        <div class="modal-row"><span class="modal-key">Regime</span><span class="modal-val">${{item.l3_regime_tag||'—'}}</span></div>
      </div>
      <div class="modal-section">
        <div class="modal-section-title">Layer 2 Scores</div>
        <div class="modal-row"><span class="modal-key">MTF Score</span><span class="modal-val">${{(item.l2_mtf_score||0).toFixed(1)}}/100</span></div>
        <div class="modal-row"><span class="modal-key">Regime confidence</span><span class="modal-val">${{(item.l2_regime_confidence||0).toFixed(1)}}%</span></div>
        <div class="modal-row"><span class="modal-key">Volatility</span><span class="modal-val">${{(item.l2_volatility_score||0).toFixed(1)}}</span></div>
        <div class="modal-row"><span class="modal-key">Trend strength</span><span class="modal-val">${{(item.l2_trend_strength||0).toFixed(1)}}</span></div>
        <div class="modal-row"><span class="modal-key">Session quality</span><span class="modal-val">${{(item.l2_session_quality||0).toFixed(1)}}</span></div>
        <div class="modal-row"><span class="modal-key">Institutional</span><span class="modal-val">${{(item.l2_institutional||0).toFixed(1)}}</span></div>
        <div class="modal-row"><span class="modal-key">Macro</span><span class="modal-val">${{(item.l2_macro_score||0).toFixed(1)}}</span></div>
        <div class="modal-row"><span class="modal-key">Sentiment</span><span class="modal-val">${{(item.l2_sentiment_score||0).toFixed(1)}}</span></div>
        <div class="modal-row"><span class="modal-key">Order flow</span><span class="modal-val">${{(item.l2_order_flow||0).toFixed(1)}}</span></div>
      </div>
      <div class="modal-section">
        <div class="modal-section-title">Layer 3 Decision</div>
        <div class="modal-row"><span class="modal-key">Bayesian prob</span><span class="modal-val">${{(item.l3_bayesian_prob||0).toFixed(3)}} (threshold: ${{item.l3_bayesian_threshold||0.62}})</span></div>
        <div class="modal-row"><span class="modal-key">Confidence</span><span class="modal-val">${{(item.l3_confidence_score||0).toFixed(1)}}/100</span></div>
        <div class="modal-row"><span class="modal-key">EV lower bound</span><span class="modal-val">$${{(item.l3_ev_lower_bound||0).toFixed(2)}}</span></div>
      </div>
      <div class="ai-box">
        <div class="ai-title">🤖 AI Analysis (Claude + DeepSeek + GPT-4)</div>
        <div class="ai-text ai-loading" id="aiText">Click below for AI analysis of this trade...</div>
        <button class="ai-btn" onclick="getAI('${{id}}','trade')">🤖 Analyse this trade</button>
      </div>`;
  }} else {{
    document.getElementById('modalTitle').textContent =
      '⛔ NULL — ' + item.pair + ' — ' + item.primary_null + ' — ' + fmt(item.timestamp_utc);
    document.getElementById('modalBody').innerHTML = `
      <div class="modal-section">
        <div class="modal-section-title">Rejection Detail</div>
        <div class="modal-row"><span class="modal-key">NULL type</span><span class="modal-val neg">${{item.primary_null||'—'}}</span></div>
        <div class="modal-row"><span class="modal-key">Null gate</span><span class="modal-val">${{item.null_gate||'—'}}</span></div>
        <div class="modal-row"><span class="modal-key">Reason</span><span class="modal-val">${{item.null_reason||'—'}}</span></div>
        <div class="modal-row"><span class="modal-key">Session</span><span class="modal-val">${{item.session||'—'}}</span></div>
        <div class="modal-row"><span class="modal-key">Regime</span><span class="modal-val">${{item.l3_regime_tag||'—'}}</span></div>
        <div class="modal-row"><span class="modal-key">News blackout</span><span class="modal-val">${{item.l2_news_blackout?'YES':'NO'}}</span></div>
      </div>
      <div class="modal-section">
        <div class="modal-section-title">Scores at Rejection</div>
        <div class="modal-row"><span class="modal-key">MTF</span><span class="modal-val">${{(item.l2_mtf_score||0).toFixed(1)}}/100</span></div>
        <div class="modal-row"><span class="modal-key">Regime conf</span><span class="modal-val">${{(item.l2_regime_confidence||0).toFixed(1)}}%</span></div>
        <div class="modal-row"><span class="modal-key">Volatility</span><span class="modal-val">${{(item.l2_volatility_score||0).toFixed(1)}}</span></div>
        <div class="modal-row"><span class="modal-key">Session quality</span><span class="modal-val">${{(item.l2_session_quality||0).toFixed(1)}}</span></div>
        <div class="modal-row"><span class="modal-key">Bayesian</span><span class="modal-val">${{(item.l3_bayesian_result||0).toFixed(3)}} (threshold: ${{item.l3_threshold||0.62}})</span></div>
        <div class="modal-row"><span class="modal-key">EV lower</span><span class="modal-val">$${{(item.l3_ev_lower_bound||0).toFixed(2)}}</span></div>
      </div>
      <div class="ai-box">
        <div class="ai-title">🤖 AI Analysis — Was this rejection correct?</div>
        <div class="ai-text ai-loading" id="aiText">Click below for AI analysis...</div>
        <button class="ai-btn" onclick="getAI('${{id}}','null')">🤖 Analyse this rejection</button>
      </div>`;
  }}
}}

function loadPattern(key) {{
  var p = allData.patterns[key];
  if (!p) return;
  document.getElementById('modal').style.display = 'block';
  document.getElementById('modalTitle').textContent = '📊 Pattern: ' + key.replace(/_/g,' / ');
  document.getElementById('modalBody').innerHTML = `
    <div class="modal-section">
      <div class="modal-section-title">Pattern Statistics</div>
      <div class="modal-row"><span class="modal-key">Total occurrences</span><span class="modal-val">${{p.count}}×</span></div>
      <div class="modal-row"><span class="modal-key">Trades taken</span><span class="modal-val">${{p.trade_count}}</span></div>
      <div class="modal-row"><span class="modal-key">Rejections (NULLs)</span><span class="modal-val warn">${{p.null_count}}</span></div>
      <div class="modal-row"><span class="modal-key">Win rate</span><span class="modal-val ${{p.win_rate>=0.55?'pos':'neg'}}">${{(p.win_rate*100).toFixed(1)}}%</span></div>
      <div class="modal-row"><span class="modal-key">Total PnL</span><span class="modal-val ${{p.total_pnl>=0?'pos':'neg'}}">$${{p.total_pnl.toFixed(2)}}</span></div>
      <div class="modal-row"><span class="modal-key">Avg PnL</span><span class="modal-val ${{p.avg_pnl>=0?'pos':'neg'}}">$${{p.avg_pnl.toFixed(2)}}</span></div>
      <div class="modal-row"><span class="modal-key">Avg MTF score</span><span class="modal-val">${{p.avg_mtf.toFixed(1)}}/100</span></div>
      <div class="modal-row"><span class="modal-key">Avg confidence</span><span class="modal-val">${{p.avg_conf.toFixed(1)}}/100</span></div>
      <div class="modal-row"><span class="modal-key">Most common NULL</span><span class="modal-val neg">${{p.top_null||'—'}}</span></div>
    </div>
    <div class="ai-box">
      <div class="ai-title">🤖 AI Pattern Analysis (Claude + DeepSeek + GPT-4)</div>
      <div class="ai-text ai-loading" id="aiText">Click below for deep pattern analysis...</div>
      <button class="ai-btn" onclick="getPatternAI('${{key}}')">🤖 Analyse this pattern</button>
    </div>`;
}}

function getAI(id, type) {{
  document.getElementById('aiText').textContent = '🤖 Analysing with Claude + DeepSeek + GPT-4...';
  document.querySelector('.ai-btn').disabled = true;
  fetch('/api/ai_analyze', {{
    method:'POST',
    headers:{{'Content-Type':'application/json'}},
    body: JSON.stringify({{id:id, type:type}})
  }}).then(r=>r.json()).then(d=>{{
    document.getElementById('aiText').textContent = d.analysis || d.error || 'No response';
    document.getElementById('aiText').classList.remove('ai-loading');
  }}).catch(e=>{{
    document.getElementById('aiText').textContent = 'Error: '+e;
  }});
}}

function getPatternAI(key) {{
  document.getElementById('aiText').textContent = '🤖 Analysing pattern with all AI models...';
  document.querySelector('.ai-btn').disabled = true;
  fetch('/api/pattern_analyze', {{
    method:'POST',
    headers:{{'Content-Type':'application/json'}},
    body: JSON.stringify({{key:key}})
  }}).then(r=>r.json()).then(d=>{{
    document.getElementById('aiText').textContent = d.analysis || d.error || 'No response';
    document.getElementById('aiText').classList.remove('ai-loading');
  }}).catch(e=>{{
    document.getElementById('aiText').textContent = 'Error: '+e;
  }});
}}

function generateWeekly() {{
  document.getElementById('weeklyAI').textContent = '🤖 Generating weekly intelligence report...';
  fetch('/api/weekly_report', {{method:'POST'}})
    .then(r=>r.json()).then(d=>{{
      document.getElementById('weeklyAI').textContent = d.report || d.error || 'No response';
      document.getElementById('weeklyAI').classList.remove('ai-loading');
    }}).catch(e=>{{
      document.getElementById('weeklyAI').textContent = 'Error: '+e;
    }});
}}

function renderMatrix() {{
  var matrix = allData.ml.session_regime_matrix || {{}};
  var sessions = ['OVERLAP','LONDON','NEW_YORK','ASIA'];
  var regimes  = ['TREND','HV_TREND','RANGE','NEWS'];
  var html = '<table><thead><tr><th>Session</th>';
  regimes.forEach(r=>{{ html += '<th>'+r+'</th>'; }});
  html += '</tr></thead><tbody>';
  sessions.forEach(s=>{{
    html += '<tr><td><strong>'+s+'</strong></td>';
    regimes.forEach(r=>{{
      var cell = (matrix[s]||{{}})[r];
      if (cell) {{
        var cls = cell.win_rate>=55?'pos':cell.win_rate>=45?'warn':'neg';
        html += '<td class="'+cls+'">'+cell.win_rate+'%<br><small>'+cell.n+' trades</small></td>';
      }} else {{
        html += '<td style="color:#374151">—</td>';
      }}
    }});
    html += '</tr>';
  }});
  html += '</tbody></table>';
  document.getElementById('matrixTable').innerHTML = html;
}}

function renderTimeChart() {{
  var tp = allData.ml.time_patterns || [];
  if (!tp.length) {{
    document.getElementById('timeChart').innerHTML = '<div class="ml-waiting">No time data yet</div>';
    return;
  }}
  var html = '';
  tp.slice(0,12).forEach(p=>{{
    var cls = p.avg_pnl>=0?'pos':'neg';
    var w   = Math.min(100, Math.abs(p.avg_pnl)/10);
    html += '<div class="null-bar-row">' +
      '<span class="null-bar-label">'+('0'+p.hour).slice(-2)+':00 UTC ('+p.n+' trades)</span>' +
      '<div class="null-bar-track"><div class="null-bar-fill" style="width:'+w+'%;background:'+(p.avg_pnl>=0?'#22c55e':'#ef4444')+'"></div></div>' +
      '<span class="null-bar-pct '+cls+'">$'+p.avg_pnl.toFixed(0)+' avg | '+p.win_rate+'% WR</span>' +
      '</div>';
  }});
  document.getElementById('timeChart').innerHTML = html;
}}
</script>
</body>
</html>"""


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — HTTP SERVER
# ═══════════════════════════════════════════════════════════════════════════════

cache = DataCache()
ai    = MultiAI()


class JournalHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        path = urlparse(self.path).path
        if path in ("/", "/journal"):
            data = cache.get()
            html = build_journal_html(data).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(html)))
            self.end_headers()
            self.wfile.write(html)
        elif path == "/health":
            self._json({"status": "ok"})
        else:
            self.send_response(404); self.end_headers()

    def do_POST(self):
        path   = urlparse(self.path).path
        length = int(self.headers.get("Content-Length", 0))
        body   = json.loads(self.rfile.read(length)) if length else {}

        if path == "/api/ai_analyze":
            item_id = body.get("id","")
            typ     = body.get("type","trade")
            data    = cache.get()

            if typ == "trade":
                items = {t.get("journal_id",""): t for t in data["trades"]}
                item  = items.get(item_id)
                if item:
                    analysis = ai.analyze_trade(item)
                    self._json({"analysis": analysis})
                else:
                    self._json({"error": "Trade not found"})
            else:
                items = {n.get("journal_id",""): n for n in data["nulls"]}
                item  = items.get(item_id)
                if item:
                    analysis = ai.analyze_null(item)
                    self._json({"analysis": analysis})
                else:
                    self._json({"error": "NULL not found"})

        elif path == "/api/pattern_analyze":
            key      = body.get("key","")
            data     = cache.get()
            patterns = {p["setup_key"]: p for p in data["patterns"]}
            pattern  = patterns.get(key)
            if pattern:
                # Add ML insights to pattern
                ml = data.get("ml",{})
                pattern["ml_insights"]          = str(ml.get("top_predictive_features",""))
                pattern["predicted_improvement"] = "See ML engine tab for details"
                analysis = ai.analyze_pattern(pattern)
                self._json({"analysis": analysis})
            else:
                self._json({"error": "Pattern not found"})

        elif path == "/api/weekly_report":
            data   = cache.get()
            weekly = data.get("weekly",{})
            ml     = data.get("ml",{})
            nulls  = data.get("nulls",[])

            # Build null distribution string
            nd      = weekly.get("null_dist",{})
            nd_str  = " | ".join([f"{k}:{v}" for k,v in nd.items()])

            # Top patterns
            patterns    = data.get("patterns",[])[:3]
            pat_str     = "\n".join([
                f"  {p['setup_key']}: {p['count']}× WR={p['win_rate']*100:.0f}% PnL=${p['total_pnl']:.0f}"
                for p in patterns
            ])

            # ML discoveries
            gems   = ml.get("hidden_gems",[])
            strict = ml.get("filter_analysis",{}).get("possibly_too_strict",[])
            ml_str = (
                "Hidden gems: " + str([g["pair"]+" "+g["session"] for g in gems[:3]]) +
                "\nOver-strict: " + str([s["null_type"] for s in strict])
            )

            weekly_data = {
                "period"         : weekly.get("period",""),
                "total_events"   : weekly.get("total_events",0),
                "trades"         : weekly.get("trades",0),
                "nulls"          : weekly.get("nulls",0),
                "win_rate"       : weekly.get("win_rate",0),
                "null_rate"      : weekly.get("null_rate",0),
                "total_pnl"      : weekly.get("total_pnl",0),
                "top_patterns"   : pat_str,
                "ml_discoveries" : ml_str,
                "too_strict"     : nd_str,
                "too_loose"      : "—",
                "news_impact"    : "See journal for news data",
                "cot_alignment"  : "Check COT tab in main dashboard",
            }
            report = ai.weekly_intelligence_report(weekly_data)
            self._json({"report": report})

        else:
            self.send_response(404); self.end_headers()

    def _json(self, obj):
        data = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args):
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║   DOGMA FX — JOURNAL INTELLIGENCE DASHBOARD             ║")
    print("╠══════════════════════════════════════════════════════════╣")
    print(f"║   URL      : http://localhost:{PORT}                      ║")
    print(f"║   Journal  : {DB_PATH:<48s}║")
    print(f"║   AI       : Claude + DeepSeek + GPT-4 via Puter       ║")
    print(f"║   ML       : scikit-learn pattern discovery             ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()

    server = HTTPServer(("0.0.0.0", PORT), JournalHandler)
    logger.info(f"Journal dashboard running at http://localhost:{PORT}")
    logger.info("Press Ctrl+C to stop")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nJournal dashboard stopped.")
        server.shutdown()


if __name__ == "__main__":
    main()
