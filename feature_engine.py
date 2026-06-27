"""
═══════════════════════════════════════════════════════════════════════════════
FOREX TRADING SIGNAL SYSTEM — VERSION 6.0
FILE: feature_engine.py
LAYER: 2 — FEATURE ENGINE (MEASUREMENT ONLY)
═══════════════════════════════════════════════════════════════════════════════

PURPOSE:
    Converts all raw Layer 1 data packages into structured,
    standardized measurement scores for Layer 3 consumption.

ARCHITECTURAL RULES (PERMANENT — NEVER VIOLATED):
    ✅ Layer 2 MEASURES and OUTPUTS scores only
    ✅ Every output is regime-agnostic
    ✅ Every output is weight-agnostic
    ✅ Every output is decision-agnostic
    ❌ Layer 2 NEVER interprets what a score means
    ❌ Layer 2 NEVER decides how important a score is
    ❌ Layer 2 NEVER uses words like "bullish", "bearish",
       "strong", "weak", "favorable", "unfavorable"
    ❌ Layer 2 NEVER weights any score
    ❌ Layer 2 NEVER makes NULL decisions

    "A thermometer measures temperature.
     It does not decide if you need a coat.
     Layer 2 is the thermometer.
     Layer 3 wears the coat."

WHAT LAYER 2 OUTPUTS (raw scores only):
    MTF Alignment Score          → 0→100 (degree of TF agreement)
    Regime Probability Distribution → per-regime probability %
    Regime Confidence Score      → 0→100 (certainty of classification)
    Volatility Score             → 0→100 (normalized ATR ratio)
    Structure State              → STABLE/WEAK/TRANSITIONING/BROKEN
    Trend Strength Score         → 0→100 (ADX normalized)
    Session Quality Score        → 0→100 (liquidity measurement)
    News Impact Score            → 0→100 (news activity level)
    Institutional Score          → 0→100 (COT + options positioning)
    Macro Score                  → 0→100 (DXY/Gold/VIX combined)
    Sentiment Extreme Score      → 0→100 (retail crowd positioning)
    Liquidity Zone Map           → price levels (institutional orders)
    SMT Divergence Score         → 0→100 (correlated pair divergence)
    Correlation Strength Score   → 0→100 (asset relationship)
    Order Flow Imbalance Score   → 0→100 (buy vs sell pressure)
    Data Tier Score              → TIER_1/TIER_2/TIER_3 per input
    Feed Health Flags            → per-source health status
    Transition State Flag        → bool + transition type

FLOW:
    Layer 1 packages (9 sources)
         ↓
    Layer 2 Feature Engine
         ↓
    FeaturePackage (all scores, zero interpretation)
         ↓
    Layer 3 Decision Engine (assigns weights + makes decisions)

═══════════════════════════════════════════════════════════════════════════════
"""

import time
import logging
import threading
import numpy as np
from datetime import datetime, timezone
from typing import Optional
from dataclasses import dataclass, field
from collections import deque

logger = logging.getLogger("FeatureEngine")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — FEATURE PACKAGE (OUTPUT STRUCTURE)
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class FeaturePackage:
    """
    Complete Layer 2 output for one forex pair at one moment.

    Every field is a raw measurement or label.
    No field carries interpretation, weight, or trade implication.
    Layer 3 reads this and assigns all meaning.

    CRITICAL: Adding interpretation to any field here
    is an architectural violation that causes
    distributed decision authority and overconfidence stacking.
    """

    pair            : str
    timestamp_utc   : float

    # ── MTF Alignment ────────────────────────────────────────────────────────
    mtf_score           : Optional[float] = None   # 0→100
    mtf_w1_score        : Optional[float] = None   # Weekly TF score
    mtf_d1_score        : Optional[float] = None   # Daily TF score
    mtf_h4_score        : Optional[float] = None   # 4H TF score
    mtf_h1_score        : Optional[float] = None   # 1H TF score
    mtf_m15_score       : Optional[float] = None   # 15M TF score
    structure_state     : str = "UNKNOWN"           # STABLE/WEAK/TRANS/BROKEN
    transition_detected : bool = False
    transition_type     : Optional[str] = None      # e.g. "TREND_TO_RANGE"

    # ── Regime Probability Distribution ──────────────────────────────────────
    regime_trend_prob       : Optional[float] = None  # 0→1
    regime_range_prob       : Optional[float] = None
    regime_hv_trend_prob    : Optional[float] = None  # High vol trend
    regime_news_prob        : Optional[float] = None
    regime_crisis_prob      : Optional[float] = None
    regime_dominant         : str = "UNKNOWN"         # Highest probability
    regime_confidence       : Optional[float] = None  # 0→100
    regime_ambiguous        : bool = False            # True if no dominant

    # ── Volatility ───────────────────────────────────────────────────────────
    volatility_score        : Optional[float] = None  # 0→100 normalized ATR
    atr_ratio               : Optional[float] = None  # Current/average ATR
    atr_value               : Optional[float] = None  # Raw ATR pips
    implied_vol             : Optional[float] = None  # IV % annualized
    realized_vol            : Optional[float] = None  # RV % annualized
    iv_rv_spread            : Optional[float] = None  # IV minus RV
    vol_percentile          : Optional[float] = None  # 0→100 in history

    # ── Trend Strength ───────────────────────────────────────────────────────
    trend_strength_score    : Optional[float] = None  # 0→100 (ADX normalized)
    adx_value               : Optional[float] = None  # Raw ADX
    directional_bias_ratio  : Optional[float] = None  # 0→1 up-move ratio
    movement_consistency    : Optional[float] = None  # 0→1
    acceleration            : Optional[float] = None  # Rate of change
    noise_ratio             : Optional[float] = None  # 0→1 choppiness

    # ── Session Quality ───────────────────────────────────────────────────────
    session_quality_score   : Optional[float] = None  # 0→100
    active_session          : str = "UNKNOWN"          # LONDON/NY/OVERLAP/ASIAN
    session_overlap         : bool = False
    spread_score            : Optional[float] = None   # 0→100 (inverted spread)
    liquidity_score         : Optional[float] = None   # 0→100

    # ── News Impact ──────────────────────────────────────────────────────────
    news_impact_score       : Optional[float] = None  # 0→100
    pre_news_window         : bool = False
    post_news_window        : bool = False
    news_blackout           : bool = False
    minutes_to_next_event   : Optional[float] = None
    highest_impact_next4h   : str = "NONE"            # HIGH/MEDIUM/LOW/NONE

    # ── Institutional Score ───────────────────────────────────────────────────
    institutional_score     : Optional[float] = None  # 0→100 combined
    cot_positioning_score   : Optional[float] = None  # 0→100 per currency
    cot_extreme_flag        : bool = False
    cot_trend_direction     : str = "STABLE"           # INCREASING/DECREASING
    options_flow_score      : Optional[float] = None  # 0→100
    risk_reversal_score     : Optional[float] = None  # 0→100 (mapped from rr)
    put_call_ratio          : Optional[float] = None  # Raw ratio

    # ── Macro Score ──────────────────────────────────────────────────────────
    macro_score             : Optional[float] = None  # 0→100
    dxy_change_pct          : Optional[float] = None  # Raw DXY move %
    dxy_volatility_score    : Optional[float] = None  # 0→100
    gold_change_pct         : Optional[float] = None
    vix_level               : Optional[float] = None  # Raw VIX
    vix_score               : Optional[float] = None  # 0→100 normalized
    us10y_yield             : Optional[float] = None  # Raw yield %
    multi_stress_count      : int = 0                  # Number of stress signals

    # ── Sentiment Extreme ─────────────────────────────────────────────────────
    sentiment_extreme_score : Optional[float] = None  # 0→100
    retail_long_pct         : Optional[float] = None  # Raw % retail long
    retail_short_pct        : Optional[float] = None  # Raw % retail short
    retail_extreme_flag     : bool = False
    news_sentiment_score    : Optional[float] = None  # -1→+1 news sentiment
    fear_greed_score        : Optional[float] = None  # 0→100

    # ── Liquidity Zones ──────────────────────────────────────────────────────
    liquidity_zones         : list = field(default_factory=list)  # Price levels
    nearest_support         : Optional[float] = None
    nearest_resistance      : Optional[float] = None
    distance_to_support     : Optional[float] = None  # Pips
    distance_to_resistance  : Optional[float] = None  # Pips

    # ── SMT Divergence ────────────────────────────────────────────────────────
    smt_divergence_score    : Optional[float] = None  # 0→100
    smt_divergence_detected : bool = False
    smt_pair_a              : Optional[str] = None
    smt_pair_b              : Optional[str] = None
    smt_correlation         : Optional[float] = None

    # ── Correlation Strength ──────────────────────────────────────────────────
    correlation_score       : Optional[float] = None  # 0→100
    dxy_correlation         : Optional[float] = None  # -1→+1
    corr_breakdown_flag     : bool = False

    # ── Order Flow Imbalance ──────────────────────────────────────────────────
    order_flow_score        : Optional[float] = None  # 0→100
    position_book_bias      : Optional[float] = None  # -1→+1
    velocity_pips_per_s     : Optional[float] = None  # Raw velocity

    # ── Data Quality ─────────────────────────────────────────────────────────
    data_tier               : str = "TIER_1"
    feed_health             : str = "HEALTHY"
    null_data               : bool = False
    missing_inputs          : list = field(default_factory=list)
    confidence_penalty      : float = 0.0  # Penalty for missing/degraded data

    def to_dict(self) -> dict:
        """Serialize to dict for Layer 3 consumption."""
        return {
            "pair"                   : self.pair,
            "timestamp_utc"          : self.timestamp_utc,
            "datetime_utc"           : datetime.fromtimestamp(
                                           self.timestamp_utc / 1000,
                                           tz=timezone.utc
                                       ).isoformat(),

            # MTF
            "mtf_score"              : self.mtf_score,
            "mtf_w1"                 : self.mtf_w1_score,
            "mtf_d1"                 : self.mtf_d1_score,
            "mtf_h4"                 : self.mtf_h4_score,
            "mtf_h1"                 : self.mtf_h1_score,
            "mtf_m15"                : self.mtf_m15_score,
            "structure_state"        : self.structure_state,
            "transition_detected"    : self.transition_detected,
            "transition_type"        : self.transition_type,

            # Regime
            "regime_probs"           : {
                "trend"    : self.regime_trend_prob,
                "range"    : self.regime_range_prob,
                "hv_trend" : self.regime_hv_trend_prob,
                "news"     : self.regime_news_prob,
                "crisis"   : self.regime_crisis_prob,
            },
            "regime_dominant"        : self.regime_dominant,
            "regime_confidence"      : self.regime_confidence,
            "regime_ambiguous"       : self.regime_ambiguous,

            # Volatility
            "volatility_score"       : self.volatility_score,
            "atr_ratio"              : self.atr_ratio,
            "atr_value"              : self.atr_value,
            "implied_vol"            : self.implied_vol,
            "realized_vol"           : self.realized_vol,
            "iv_rv_spread"           : self.iv_rv_spread,
            "vol_percentile"         : self.vol_percentile,

            # Trend
            "trend_strength_score"   : self.trend_strength_score,
            "adx_value"              : self.adx_value,
            "directional_bias_ratio" : self.directional_bias_ratio,
            "movement_consistency"   : self.movement_consistency,
            "acceleration"           : self.acceleration,
            "noise_ratio"            : self.noise_ratio,

            # Session
            "session_quality_score"  : self.session_quality_score,
            "active_session"         : self.active_session,
            "session_overlap"        : self.session_overlap,
            "spread_score"           : self.spread_score,
            "liquidity_score"        : self.liquidity_score,

            # News
            "news_impact_score"      : self.news_impact_score,
            "pre_news_window"        : self.pre_news_window,
            "post_news_window"       : self.post_news_window,
            "news_blackout"          : self.news_blackout,
            "minutes_to_next_event"  : self.minutes_to_next_event,
            "highest_impact_next4h"  : self.highest_impact_next4h,

            # Institutional
            "institutional_score"    : self.institutional_score,
            "cot_positioning_score"  : self.cot_positioning_score,
            "cot_extreme_flag"       : self.cot_extreme_flag,
            "cot_trend_direction"    : self.cot_trend_direction,
            "options_flow_score"     : self.options_flow_score,
            "risk_reversal_score"    : self.risk_reversal_score,
            "put_call_ratio"         : self.put_call_ratio,

            # Macro
            "macro_score"            : self.macro_score,
            "dxy_change_pct"         : self.dxy_change_pct,
            "dxy_volatility_score"   : self.dxy_volatility_score,
            "gold_change_pct"        : self.gold_change_pct,
            "vix_level"              : self.vix_level,
            "vix_score"              : self.vix_score,
            "us10y_yield"            : self.us10y_yield,
            "multi_stress_count"     : self.multi_stress_count,

            # Sentiment
            "sentiment_extreme_score": self.sentiment_extreme_score,
            "retail_long_pct"        : self.retail_long_pct,
            "retail_short_pct"       : self.retail_short_pct,
            "retail_extreme_flag"    : self.retail_extreme_flag,
            "news_sentiment_score"   : self.news_sentiment_score,
            "fear_greed_score"       : self.fear_greed_score,

            # Liquidity zones
            "liquidity_zones"        : self.liquidity_zones,
            "nearest_support"        : self.nearest_support,
            "nearest_resistance"     : self.nearest_resistance,
            "distance_to_support"    : self.distance_to_support,
            "distance_to_resistance" : self.distance_to_resistance,

            # SMT
            "smt_divergence_score"   : self.smt_divergence_score,
            "smt_divergence_detected": self.smt_divergence_detected,
            "smt_correlation"        : self.smt_correlation,

            # Correlation
            "correlation_score"      : self.correlation_score,
            "dxy_correlation"        : self.dxy_correlation,
            "corr_breakdown_flag"    : self.corr_breakdown_flag,

            # Order flow
            "order_flow_score"       : self.order_flow_score,
            "position_book_bias"     : self.position_book_bias,
            "velocity_pips_per_s"    : self.velocity_pips_per_s,

            # Data quality
            "data_tier"              : self.data_tier,
            "feed_health"            : self.feed_health,
            "null_data"              : self.null_data,
            "missing_inputs"         : self.missing_inputs,
            "confidence_penalty"     : self.confidence_penalty,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — MTF SCORER
# ═══════════════════════════════════════════════════════════════════════════════

class MTFScorer:
    """
    Measures multi-timeframe alignment across all timeframes.

    Output: continuous score 0→100 + per-TF breakdown
    Pure measurement — no trade implication.

    Methodology:
        For each timeframe, measure:
        1. Trend direction score (0→100 based on price vs EMAs)
        2. Momentum alignment (RSI relative to 50)
        3. Structure integrity (price respecting key levels)

        Weighted combination → overall MTF score

    Weights by timeframe (higher TF = more weight):
        W1  → 30%
        D1  → 25%
        H4  → 20%
        H1  → 15%
        M15 → 10%

    Structure State classification:
        75→100 = STABLE      (strong agreement)
        50→74  = WEAK        (partial agreement)
        25→49  = TRANSITIONING (changing)
        0→24   = BROKEN      (contradiction)
    """

    TF_WEIGHTS = {
        "W1" : 0.30,
        "D1" : 0.25,
        "H4" : 0.20,
        "H1" : 0.15,
        "M15": 0.10,
    }

    @staticmethod
    def score_timeframe(bars: list, ema_fast: int = 20,
                        ema_slow: int = 50) -> Optional[float]:
        """
        Score single timeframe alignment 0→100.
        Uses EMA relationship + momentum direction.

        Args:
            bars: list of OHLC bar dicts (newest last)
            ema_fast: fast EMA period
            ema_slow: slow EMA period

        Returns:
            float 0→100 or None if insufficient data
        """
        if not bars or len(bars) < ema_slow + 5:
            return None

        closes = [b.get("mid_close") or b.get("close", 0)
                  for b in bars if b]
        closes = [c for c in closes if c > 0]

        if len(closes) < ema_slow:
            return None

        # Calculate EMAs
        ema_f = MTFScorer._ema(closes, ema_fast)
        ema_s = MTFScorer._ema(closes, ema_slow)

        if not ema_f or not ema_s:
            return None

        current_price = closes[-1]
        ema_f_val     = ema_f[-1]
        ema_s_val     = ema_s[-1]

        # Component 1: EMA alignment (0→100)
        # Price above both EMAs + fast above slow = bullish alignment
        # Price below both EMAs + fast below slow = bearish alignment
        # Mixed = neutral
        price_vs_fast  = (current_price - ema_f_val) / ema_f_val * 100
        price_vs_slow  = (current_price - ema_s_val) / ema_s_val * 100
        fast_vs_slow   = (ema_f_val - ema_s_val) / ema_s_val * 100

        # Normalize price deviation to score
        # Strong alignment = price well above/below both EMAs
        # Score 50 = neutral/mixed, 100 = strong up, 0 = strong down
        deviation = (price_vs_fast + price_vs_slow) / 2.0
        ema_score  = max(0.0, min(100.0, 50.0 + deviation * 200.0))

        # Component 2: EMA crossover alignment (0→100)
        cross_score = max(0.0, min(100.0, 50.0 + fast_vs_slow * 500.0))

        # Component 3: Momentum direction (last N bars)
        n = min(10, len(closes) - 1)
        momentum = closes[-1] - closes[-n]
        mom_score = max(0.0, min(100.0, 50.0 + (momentum / closes[-n]) * 5000.0))

        # Weighted combination
        tf_score = (ema_score * 0.40 + cross_score * 0.35 + mom_score * 0.25)
        return round(max(0.0, min(100.0, tf_score)), 1)

    @staticmethod
    def _ema(values: list, period: int) -> Optional[list]:
        """Calculate EMA series."""
        if len(values) < period:
            return None
        k      = 2.0 / (period + 1)
        ema    = [sum(values[:period]) / period]
        for v in values[period:]:
            ema.append(v * k + ema[-1] * (1 - k))
        return ema

    @classmethod
    def compute(cls, bars_by_tf: dict) -> dict:
        """
        Compute full MTF score from bars across timeframes.

        Args:
            bars_by_tf: dict of timeframe → list of OHLC bars
                        e.g. {"W1": [...], "D1": [...], "H4": [...]}

        Returns:
            dict with mtf_score + per-TF scores + structure_state
        """
        tf_scores    = {}
        weighted_sum = 0.0
        weight_total = 0.0

        for tf, weight in cls.TF_WEIGHTS.items():
            bars  = bars_by_tf.get(tf, [])
            score = cls.score_timeframe(bars)
            if score is not None:
                tf_scores[tf]  = score
                weighted_sum  += score * weight
                weight_total  += weight

        if weight_total == 0:
            mtf_score = 50.0  # Default neutral
        else:
            mtf_score = round(weighted_sum / weight_total, 1)

        # Structure state classification
        if mtf_score >= 75:
            structure_state = "STABLE"
        elif mtf_score >= 50:
            structure_state = "WEAK"
        elif mtf_score >= 25:
            structure_state = "TRANSITIONING"
        else:
            structure_state = "BROKEN"

        # Transition detection
        # If score is in 25-60 range AND moving toward extremes
        transition = (25 <= mtf_score <= 60)

        return {
            "mtf_score"        : mtf_score,
            "tf_scores"        : tf_scores,
            "structure_state"  : structure_state,
            "transition"       : transition,
            "tfs_computed"     : list(tf_scores.keys()),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — REGIME PROBABILITY ESTIMATOR
# ═══════════════════════════════════════════════════════════════════════════════

class RegimeProbabilityEstimator:
    """
    Estimates probability of each market regime.

    Output: probability distribution across 5 regimes
    Pure measurement — Layer 3 assigns weights to regimes.

    Regimes:
        TREND      → directional, ADX > 25, ATR normal
        RANGE      → mean-reverting, ADX < 20, ATR low
        HV_TREND   → directional, ATR elevated
        NEWS       → event-driven, spread widening
        CRISIS     → multiple stress indicators

    Method:
        Uses Naive Bayes-style probability estimation
        from multiple independent indicators.
        Each indicator votes for one or more regimes.
        Votes are normalized to probability distribution.
    """

    # Minimum probability for dominant regime
    DOMINANCE_THRESHOLD = 0.65

    @classmethod
    def estimate(cls,
                 adx          : Optional[float],
                 atr_ratio    : Optional[float],
                 spread_ratio : Optional[float],
                 news_active  : bool,
                 stress_count : int,
                 vix          : Optional[float],
                 mtf_score    : Optional[float]) -> dict:
        """
        Estimate regime probabilities from raw indicators.

        All inputs are raw measurements from Layer 1.
        Output is probability distribution — not a decision.
        """
        # Initialize vote scores per regime
        votes = {
            "TREND"   : 0.0,
            "RANGE"   : 0.0,
            "HV_TREND": 0.0,
            "NEWS"    : 0.0,
            "CRISIS"  : 0.0,
        }

        # ── ADX vote ─────────────────────────────────────────────────────────
        if adx is not None:
            if adx >= 30:
                votes["TREND"]    += 2.0
                votes["HV_TREND"] += 1.0
            elif adx >= 25:
                votes["TREND"]    += 1.5
                votes["HV_TREND"] += 0.5
            elif adx >= 20:
                votes["TREND"]    += 0.5
                votes["RANGE"]    += 0.5
            else:
                votes["RANGE"]    += 2.0
                votes["HV_TREND"] += 0.3

        # ── ATR ratio vote ────────────────────────────────────────────────────
        if atr_ratio is not None:
            if atr_ratio >= 2.0:
                votes["CRISIS"]   += 2.0
                votes["HV_TREND"] += 1.0
            elif atr_ratio >= 1.5:
                votes["HV_TREND"] += 2.0
                votes["CRISIS"]   += 0.5
            elif atr_ratio >= 1.2:
                votes["HV_TREND"] += 1.0
                votes["TREND"]    += 0.5
            else:
                votes["TREND"]    += 1.0
                votes["RANGE"]    += 1.0

        # ── Spread ratio vote ─────────────────────────────────────────────────
        if spread_ratio is not None:
            if spread_ratio >= 5.0:
                votes["CRISIS"]   += 3.0
                votes["NEWS"]     += 2.0
            elif spread_ratio >= 3.0:
                votes["NEWS"]     += 2.0
                votes["CRISIS"]   += 1.0
            elif spread_ratio >= 2.0:
                votes["NEWS"]     += 1.0
                votes["HV_TREND"] += 0.5

        # ── News active vote ──────────────────────────────────────────────────
        if news_active:
            votes["NEWS"]     += 3.0
            votes["HV_TREND"] += 0.5

        # ── Multi-stress vote ─────────────────────────────────────────────────
        if stress_count >= 4:
            votes["CRISIS"]   += 4.0
        elif stress_count >= 3:
            votes["CRISIS"]   += 2.0
            votes["HV_TREND"] += 1.0
        elif stress_count >= 2:
            votes["HV_TREND"] += 1.5

        # ── VIX vote (reference only for forex) ───────────────────────────────
        if vix is not None:
            if vix >= 40:
                votes["CRISIS"]   += 2.0
            elif vix >= 30:
                votes["HV_TREND"] += 1.5
                votes["CRISIS"]   += 0.5
            elif vix >= 20:
                votes["HV_TREND"] += 0.5

        # ── MTF score vote ────────────────────────────────────────────────────
        if mtf_score is not None:
            if mtf_score >= 75 or mtf_score <= 25:
                votes["TREND"]    += 1.5
            elif 45 <= mtf_score <= 55:
                votes["RANGE"]    += 1.0
            else:
                votes["RANGE"]    += 0.5
                votes["TREND"]    += 0.5

        # ── Normalize to probability distribution ─────────────────────────────
        total = sum(votes.values())
        if total == 0:
            # No data — equal probability
            probs = {k: 0.2 for k in votes}
        else:
            probs = {k: round(v / total, 4) for k, v in votes.items()}

        # ── Determine dominant regime ─────────────────────────────────────────
        dominant      = max(probs, key=probs.get)
        dominant_prob = probs[dominant]
        ambiguous     = dominant_prob < cls.DOMINANCE_THRESHOLD

        # ── Regime confidence score (0→100) ──────────────────────────────────
        # How certain are we about the dominant regime?
        second_highest = sorted(probs.values(), reverse=True)[1]
        separation     = dominant_prob - second_highest
        confidence     = round(min(100.0, separation * 200.0), 1)

        return {
            "probs"      : probs,
            "dominant"   : dominant,
            "confidence" : confidence,
            "ambiguous"  : ambiguous,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — VOLATILITY SCORER
# ═══════════════════════════════════════════════════════════════════════════════

class VolatilityScorer:
    """
    Converts raw volatility data into normalized 0→100 score.
    Pure measurement — no interpretation.

    Score meaning (Layer 3 assigns significance):
        0   = extremely low volatility
        50  = average historical volatility
        100 = extremely high volatility
    """

    @staticmethod
    def compute(atr_value      : Optional[float],
                atr_avg        : Optional[float],
                implied_vol    : Optional[float],
                vol_percentile : Optional[float]) -> dict:
        """
        Compute normalized volatility score from available inputs.
        """
        scores = []

        # ATR ratio component
        atr_ratio = None
        if atr_value and atr_avg and atr_avg > 0:
            atr_ratio = atr_value / atr_avg
            # Map: 0.5x→2.5x normal → 0→100
            atr_score = max(0.0, min(100.0,
                (atr_ratio - 0.5) / 2.0 * 100.0
            ))
            scores.append(atr_score)

        # Implied vol component
        if implied_vol is not None:
            # Map: 2%→20% annualized IV → 0→100
            iv_score = max(0.0, min(100.0,
                (implied_vol - 2.0) / 18.0 * 100.0
            ))
            scores.append(iv_score)

        # Vol percentile component
        if vol_percentile is not None:
            scores.append(vol_percentile)

        if not scores:
            vol_score = 50.0  # Unknown → neutral
        else:
            vol_score = round(sum(scores) / len(scores), 1)

        return {
            "volatility_score": vol_score,
            "atr_ratio"       : atr_ratio,
            "components_used" : len(scores),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — TREND STRENGTH SCORER
# ═══════════════════════════════════════════════════════════════════════════════

class TrendStrengthScorer:
    """
    Measures trend strength from multiple indicators.
    Output: 0→100 score.
    Pure measurement — "strong trend" is not said here.
    """

    @staticmethod
    def compute_adx(highs: list, lows: list,
                    closes: list, period: int = 14) -> Optional[float]:
        """
        Calculate ADX from OHLC data.
        Returns raw ADX value (not normalized).
        """
        if len(closes) < period + 5:
            return None

        try:
            # True Range
            tr_list = []
            for i in range(1, len(closes)):
                hl  = highs[i] - lows[i]
                hc  = abs(highs[i] - closes[i-1])
                lc  = abs(lows[i] - closes[i-1])
                tr_list.append(max(hl, hc, lc))

            # +DM and -DM
            plus_dm  = []
            minus_dm = []
            for i in range(1, len(highs)):
                up   = highs[i] - highs[i-1]
                down = lows[i-1] - lows[i]
                plus_dm.append(up if up > down and up > 0 else 0)
                minus_dm.append(down if down > up and down > 0 else 0)

            # Smooth with Wilder's method
            def wilder_smooth(data, p):
                result = [sum(data[:p])]
                for v in data[p:]:
                    result.append(result[-1] - result[-1]/p + v)
                return result

            atr_s     = wilder_smooth(tr_list, period)
            plus_s    = wilder_smooth(plus_dm, period)
            minus_s   = wilder_smooth(minus_dm, period)

            # +DI and -DI
            plus_di   = [100 * p / a for p, a in zip(plus_s, atr_s) if a > 0]
            minus_di  = [100 * m / a for m, a in zip(minus_s, atr_s) if a > 0]

            # DX and ADX
            dx_list   = []
            for pd, md in zip(plus_di, minus_di):
                diff  = abs(pd - md)
                sumv  = pd + md
                dx_list.append(100 * diff / sumv if sumv > 0 else 0)

            if len(dx_list) < period:
                return None

            adx = sum(dx_list[-period:]) / period
            return round(adx, 2)

        except Exception:
            return None

    @staticmethod
    def normalize_adx(adx: float) -> float:
        """
        Normalize ADX to 0→100 score.
        ADX 0→60 maps to score 0→100.
        """
        return round(min(100.0, (adx / 60.0) * 100.0), 1)

    @staticmethod
    def compute(bars: list, period: int = 14) -> dict:
        """
        Compute trend strength score from OHLC bars.
        """
        if not bars or len(bars) < period + 5:
            return {"trend_strength_score": None, "adx_value": None}

        highs  = [b.get("mid_high") or b.get("high", 0) for b in bars]
        lows   = [b.get("mid_low")  or b.get("low",  0) for b in bars]
        closes = [b.get("mid_close") or b.get("close", 0) for b in bars]

        highs  = [h for h in highs  if h > 0]
        lows   = [l for l in lows   if l > 0]
        closes = [c for c in closes if c > 0]

        min_len = min(len(highs), len(lows), len(closes))
        if min_len < period + 5:
            return {"trend_strength_score": None, "adx_value": None}

        adx = TrendStrengthScorer.compute_adx(
            highs[-min_len:], lows[-min_len:], closes[-min_len:], period
        )
        if adx is None:
            return {"trend_strength_score": None, "adx_value": None}

        return {
            "trend_strength_score": TrendStrengthScorer.normalize_adx(adx),
            "adx_value"           : adx,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — SESSION QUALITY SCORER
# ═══════════════════════════════════════════════════════════════════════════════

class SessionQualityScorer:
    """
    Measures session quality from time + spread data.
    Output: 0→100 score + session label.
    Pure measurement — no trade recommendation.

    Session scoring (UTC hours):
        London/NY overlap (12-16 UTC): 95→100
        London (07-16 UTC):             70→90
        New York (12-21 UTC):           65→85
        Asian (00-09 UTC):              20→40
        Dead hours (21-00 UTC):         5→15
    """

    SESSION_SCORES = {
        "OVERLAP" : 95.0,
        "LONDON"  : 80.0,
        "NEW_YORK": 75.0,
        "ASIAN"   : 25.0,
        "DEAD"    : 10.0,
    }

    @staticmethod
    def get_session(utc_hour: int) -> tuple:
        """
        Determine active session and overlap status.
        Returns (session_name, is_overlap, base_score).
        """
        # London/NY overlap = best liquidity
        if 12 <= utc_hour < 16:
            return "OVERLAP", True, SessionQualityScorer.SESSION_SCORES["OVERLAP"]
        # London session
        elif 7 <= utc_hour < 16:
            return "LONDON", False, SessionQualityScorer.SESSION_SCORES["LONDON"]
        # New York session
        elif 12 <= utc_hour < 21:
            return "NEW_YORK", False, SessionQualityScorer.SESSION_SCORES["NEW_YORK"]
        # Asian session
        elif 0 <= utc_hour < 9:
            return "ASIAN", False, SessionQualityScorer.SESSION_SCORES["ASIAN"]
        # Dead hours
        else:
            return "DEAD", False, SessionQualityScorer.SESSION_SCORES["DEAD"]

    @staticmethod
    def compute(spread          : Optional[float],
                avg_spread      : Optional[float],
                timestamp_utc   : Optional[float] = None) -> dict:
        """
        Compute session quality score.
        Combines time-of-day base score with spread quality.
        """
        # Get current UTC hour
        if timestamp_utc:
            dt       = datetime.fromtimestamp(timestamp_utc / 1000, tz=timezone.utc)
            utc_hour = dt.hour
        else:
            utc_hour = datetime.now(timezone.utc).hour

        session, overlap, base_score = SessionQualityScorer.get_session(utc_hour)

        # Spread quality modifier
        spread_score = 50.0  # Default
        if spread is not None and avg_spread is not None and avg_spread > 0:
            spread_ratio = spread / avg_spread
            # Low spread = high quality, high spread = low quality
            # Ratio 0.5→3.0 maps to score 100→0
            spread_score = max(0.0, min(100.0,
                100.0 - ((spread_ratio - 0.5) / 2.5) * 100.0
            ))

        # Combined session quality
        session_quality = round(
            base_score * 0.70 + spread_score * 0.30, 1
        )

        return {
            "session_quality_score": session_quality,
            "active_session"       : session,
            "session_overlap"      : overlap,
            "spread_score"         : round(spread_score, 1),
            "utc_hour"             : utc_hour,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — INSTITUTIONAL SCORER
# ═══════════════════════════════════════════════════════════════════════════════

class InstitutionalScorer:
    """
    Combines COT + options flow into institutional positioning score.
    Output: 0→100 single score.
    Pure measurement — no directional interpretation.

    Score meaning (Layer 3 assigns significance):
        0   = extreme bearish institutional positioning
        50  = neutral positioning
        100 = extreme bullish institutional positioning
    """

    @staticmethod
    def compute(cot_score     : Optional[float],
                options_score : Optional[float],
                rr_score      : Optional[float],
                pc_ratio      : Optional[float]) -> dict:
        """
        Combine institutional signals into single score.
        Weights are equal — Layer 3 adjusts per regime.
        """
        components = {}

        if cot_score is not None:
            components["cot"] = cot_score

        if options_score is not None:
            components["options_flow"] = options_score

        if rr_score is not None:
            components["risk_reversal"] = rr_score

        if pc_ratio is not None:
            # Map put/call ratio to 0→100
            # High ratio = bearish (0), low ratio = bullish (100)
            pc_mapped = max(0.0, min(100.0, (1.5 - pc_ratio) / 1.0 * 100.0))
            components["put_call"] = pc_mapped

        if not components:
            inst_score = 50.0  # Neutral — no data
        else:
            inst_score = round(
                sum(components.values()) / len(components), 1
            )

        return {
            "institutional_score": inst_score,
            "components"         : components,
            "inputs_used"        : len(components),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — MACRO SCORER
# ═══════════════════════════════════════════════════════════════════════════════

class MacroScorer:
    """
    Combines multi-asset data into macro environment score.
    Output: 0→100 score per pair.
    Pure measurement — no regime conclusion.

    Score meaning (Layer 3 assigns significance):
        0   = maximum macro stress / risk-off
        50  = neutral macro environment
        100 = maximum macro calm / risk-on
    """

    @staticmethod
    def compute(pair          : str,
                dxy_change    : Optional[float],
                dxy_vol_score : Optional[float],
                gold_change   : Optional[float],
                vix_level     : Optional[float],
                stress_count  : int,
                correlations  : dict) -> dict:
        """
        Compute macro score for a specific pair.
        Uses DXY, Gold, VIX, stress count as inputs.
        """
        components = {}

        # DXY impact depends on pair
        # EUR/USD, GBP/USD, AUD/USD — inverse DXY
        # USD/JPY, USD/CHF — positive DXY
        usd_direct_pairs = ["USD/JPY", "USD/CHF", "USD/CAD"]
        dxy_direction    = 1.0 if pair in usd_direct_pairs else -1.0

        if dxy_change is not None:
            # Map ±3% daily move to 0→100 score
            # dxy_direction adjusts for pair direction
            dxy_impact = dxy_change * dxy_direction
            dxy_score  = max(0.0, min(100.0,
                50.0 + dxy_impact * 15.0
            ))
            components["dxy"] = round(dxy_score, 1)

        if dxy_vol_score is not None:
            # High DXY volatility = macro stress = low score
            dxy_vol_component = 100.0 - dxy_vol_score
            components["dxy_vol"] = round(dxy_vol_component, 1)

        # Gold movement (risk-off = gold up = stress)
        if gold_change is not None:
            # Rising gold = risk-off = lower macro score for risk pairs
            gold_impact = -gold_change  # Inverse
            gold_score  = max(0.0, min(100.0,
                50.0 + gold_impact * 10.0
            ))
            components["gold"] = round(gold_score, 1)

        # VIX (reference only — not primary)
        if vix_level is not None:
            # VIX 10→40 maps to 100→0
            vix_score = max(0.0, min(100.0,
                100.0 - ((vix_level - 10.0) / 30.0) * 100.0
            ))
            components["vix"] = round(vix_score, 1)

        # Stress count component
        stress_score = max(0.0, min(100.0,
            100.0 - stress_count * 20.0
        ))
        components["stress_count"] = round(stress_score, 1)

        if not components:
            macro_score = 50.0
        else:
            macro_score = round(
                sum(components.values()) / len(components), 1
            )

        return {
            "macro_score" : macro_score,
            "components"  : components,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 9 — SENTIMENT EXTREME SCORER
# ═══════════════════════════════════════════════════════════════════════════════

class SentimentExtremeScorer:
    """
    Measures how extreme retail positioning is.
    Output: 0→100 score.
    Pure measurement — no contrarian trade conclusion.

    Score meaning (Layer 3 assigns significance):
        0   = extreme retail bearish positioning
        50  = balanced positioning
        100 = extreme retail bullish positioning
    """

    @staticmethod
    def compute(retail_long_pct   : Optional[float],
                news_sentiment    : Optional[float],
                fear_greed        : Optional[float]) -> dict:
        """
        Compute sentiment extreme score.
        """
        components = {}

        # Retail positioning component
        if retail_long_pct is not None:
            # Direct mapping: 0% long = 0, 100% long = 100
            components["retail_positioning"] = retail_long_pct

        # News sentiment component (-1→+1 mapped to 0→100)
        if news_sentiment is not None:
            news_mapped = (news_sentiment + 1.0) / 2.0 * 100.0
            components["news_sentiment"] = round(news_mapped, 1)

        # Fear & greed component (already 0→100)
        if fear_greed is not None:
            components["fear_greed"] = fear_greed

        if not components:
            sentiment_score = 50.0
        else:
            sentiment_score = round(
                sum(components.values()) / len(components), 1
            )

        # Extreme flag
        extreme = sentiment_score >= 75 or sentiment_score <= 25

        return {
            "sentiment_extreme_score": sentiment_score,
            "extreme_flag"           : extreme,
            "components"             : components,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 10 — ORDER FLOW IMBALANCE SCORER
# ═══════════════════════════════════════════════════════════════════════════════

class OrderFlowImbalanceScorer:
    """
    Measures buy vs sell pressure imbalance.
    Output: 0→100 score.
    Pure measurement — no directional label.

    Score meaning (Layer 3 assigns significance):
        0   = strong sell pressure
        50  = balanced flow
        100 = strong buy pressure
    """

    @staticmethod
    def compute(position_book_bias  : Optional[float],
                velocity_pips_per_s : Optional[float],
                directional_bias    : Optional[float]) -> dict:
        """
        Combine order flow signals into imbalance score.
        """
        components = {}

        # Position book bias (-1→+1 mapped to 0→100)
        if position_book_bias is not None:
            pb_score = (position_book_bias + 1.0) / 2.0 * 100.0
            components["position_book"] = round(pb_score, 1)

        # Price velocity
        if velocity_pips_per_s is not None:
            # Normalize: ±5 pips/sec → 0→100
            vel_score = max(0.0, min(100.0,
                50.0 + velocity_pips_per_s * 10.0
            ))
            components["velocity"] = round(vel_score, 1)

        # Directional bias (0→1 mapped to 0→100)
        if directional_bias is not None:
            components["directional_bias"] = round(directional_bias * 100.0, 1)

        if not components:
            flow_score = 50.0
        else:
            flow_score = round(
                sum(components.values()) / len(components), 1
            )

        return {
            "order_flow_score": flow_score,
            "components"      : components,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 11 — DATA QUALITY ASSESSOR
# ═══════════════════════════════════════════════════════════════════════════════

class DataQualityAssessor:
    """
    Assesses overall data quality across all Layer 1 inputs.
    Produces confidence penalty for missing/degraded data.

    RULE: Layer 3 applies the penalty — this only measures it.
    """

    REQUIRED_INPUTS = [
        "price_feed",
        "mtf_bars",
        "session_data",
    ]

    OPTIONAL_INPUTS = {
        "economic_calendar" : 0.05,  # 5% penalty if missing
        "multi_asset"       : 0.08,
        "cot_data"          : 0.06,
        "news_sentiment"    : 0.07,
        "options_flow"      : 0.05,
    }

    @staticmethod
    def assess(available_inputs : list,
               feed_health      : str,
               null_data        : bool) -> dict:
        """
        Assess data quality and compute confidence penalty.
        """
        missing      = []
        penalty      = 0.0

        # Check required inputs
        for req in DataQualityAssessor.REQUIRED_INPUTS:
            if req not in available_inputs:
                missing.append(req)
                penalty += 0.20  # Heavy penalty for required input

        # Check optional inputs
        for opt, opt_penalty in DataQualityAssessor.OPTIONAL_INPUTS.items():
            if opt not in available_inputs:
                missing.append(opt)
                penalty += opt_penalty

        # Feed health penalty
        health_penalties = {
            "HEALTHY"        : 0.00,
            "DEGRADED"       : 0.05,
            "PARTIAL_OUTAGE" : 0.10,
            "CRITICAL_OUTAGE": 0.30,
            "TOTAL_OUTAGE"   : 1.00,
        }
        penalty += health_penalties.get(feed_health, 0.10)

        # NULL data
        if null_data:
            penalty = 1.0  # Maximum penalty

        penalty = min(1.0, penalty)

        return {
            "confidence_penalty": round(penalty, 3),
            "missing_inputs"    : missing,
            "data_complete"     : len(missing) == 0,
            "feed_health"       : feed_health,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 12 — FEATURE ENGINE (MAIN CLASS)
# ═══════════════════════════════════════════════════════════════════════════════

class FeatureEngine:
    """
    Master Layer 2 orchestrator.

    Takes all Layer 1 packages and produces
    a FeaturePackage for Layer 3 consumption.

    CRITICAL RULE:
        This engine produces measurements ONLY.
        No score produced here carries interpretation.
        No weight is assigned to any score.
        No trade decision is made here.
        All of that belongs exclusively to Layer 3.
    """

    def __init__(self):
        self._mtf_scorer      = MTFScorer()
        self._regime_est      = RegimeProbabilityEstimator()
        self._vol_scorer      = VolatilityScorer()
        self._trend_scorer    = TrendStrengthScorer()
        self._session_scorer  = SessionQualityScorer()
        self._inst_scorer     = InstitutionalScorer()
        self._macro_scorer    = MacroScorer()
        self._sentiment_scorer= SentimentExtremeScorer()
        self._flow_scorer     = OrderFlowImbalanceScorer()
        self._quality_assessor= DataQualityAssessor()

        # ATR history per pair for ratio computation
        self._atr_history : dict[str, deque] = {}
        self._spread_history: dict[str, deque] = {}
        self._lock = threading.Lock()

        logger.info("FeatureEngine initialized")

    def compute(self,
                pair              : str,
                layer1_price      : dict,
                layer1_calendar   : dict,
                layer1_multi_asset: dict,
                layer1_cot        : dict,
                layer1_news       : dict,
                layer1_options    : dict) -> FeaturePackage:
        """
        Compute complete FeaturePackage for one pair.

        Args:
            pair              : e.g. "EUR/USD"
            layer1_price      : from market_data_feed.Layer1OutputFeed
            layer1_calendar   : from economic_calendar.EconomicCalendarManager
            layer1_multi_asset: from multi_asset_feed.MultiAssetManager
            layer1_cot        : from cot_report.COTManager
            layer1_news       : from news_sentiment.NewsSentimentManager
            layer1_options    : from options_flow.OptionsFlowManager

        Returns:
            FeaturePackage with all raw scores
        """
        now_ms = time.time() * 1000
        pkg    = FeaturePackage(pair=pair, timestamp_utc=now_ms)

        # Track available inputs
        available = []

        # ── Extract raw data from Layer 1 packages ───────────────────────────
        price_data   = layer1_price   or {}
        cal_data     = layer1_calendar or {}
        asset_data   = layer1_multi_asset or {}
        cot_data     = layer1_cot     or {}
        news_data    = layer1_news    or {}
        options_data = layer1_options or {}

        # ── Feed health assessment ────────────────────────────────────────────
        feed_health = price_data.get("feed_health", "UNKNOWN")
        null_data   = price_data.get("null_data", False)
        pkg.feed_health = feed_health
        pkg.null_data   = null_data

        if null_data:
            pkg.missing_inputs   = ["ALL_FEEDS_DOWN"]
            pkg.confidence_penalty = 1.0
            return pkg

        # ── PRICE DATA ────────────────────────────────────────────────────────
        if price_data.get("latest_bid"):
            available.append("price_feed")

            # Raw price measurements
            spread = price_data.get("latest_spread")
            velocity = price_data.get("latest_velocity")
            quality  = price_data.get("latest_quality", 1.0)
            anomalies= price_data.get("latest_anomalies", [])

            # Update spread history
            if spread:
                with self._lock:
                    if pair not in self._spread_history:
                        self._spread_history[pair] = deque(maxlen=100)
                    self._spread_history[pair].append(spread)
                    avg_spread = sum(self._spread_history[pair]) / len(
                        self._spread_history[pair]
                    )
            else:
                avg_spread = None

            pkg.velocity_pips_per_s = velocity

            # Order flow from price measurements
            directional_bias = price_data.get("directional_bias_ratio")
            pkg.directional_bias_ratio = directional_bias
            pkg.movement_consistency   = price_data.get("movement_consistency")
            pkg.noise_ratio            = price_data.get("noise_ratio")
            pkg.acceleration           = price_data.get("acceleration")

        else:
            spread     = None
            avg_spread = None
            velocity   = None

        # ── MTF BARS ──────────────────────────────────────────────────────────
        bars_by_tf = {}
        price_bars = price_data.get("bars", {})

        tf_map = {
            "W1" : ["1d"],        # Use daily as weekly proxy
            "D1" : ["1d"],
            "H4" : ["4h"],
            "H1" : ["1h"],
            "M15": ["15m"],
        }

        for tf_label, tf_keys in tf_map.items():
            for tk in tf_keys:
                if tk in price_bars:
                    bar_data = price_bars[tk]
                    if bar_data:
                        bars_by_tf[tf_label] = [bar_data] if isinstance(bar_data, dict) else bar_data
                    break

        if bars_by_tf:
            available.append("mtf_bars")

            # MTF score computation
            mtf_result = self._mtf_scorer.compute(bars_by_tf)
            pkg.mtf_score        = mtf_result["mtf_score"]
            pkg.structure_state  = mtf_result["structure_state"]
            pkg.transition_detected = mtf_result["transition"]

            tf_scores = mtf_result.get("tf_scores", {})
            pkg.mtf_w1_score  = tf_scores.get("W1")
            pkg.mtf_d1_score  = tf_scores.get("D1")
            pkg.mtf_h4_score  = tf_scores.get("H4")
            pkg.mtf_h1_score  = tf_scores.get("H1")
            pkg.mtf_m15_score = tf_scores.get("M15")

            # Trend strength from H4 bars
            h4_bars = bars_by_tf.get("H4", [])
            if isinstance(h4_bars, list) and len(h4_bars) > 20:
                ts_result = self._trend_scorer.compute(h4_bars)
                pkg.trend_strength_score = ts_result.get("trend_strength_score")
                pkg.adx_value            = ts_result.get("adx_value")

        # ── SESSION QUALITY ───────────────────────────────────────────────────
        available.append("session_data")
        sess_result = self._session_scorer.compute(
            spread        = spread,
            avg_spread    = avg_spread,
            timestamp_utc = now_ms,
        )
        pkg.session_quality_score = sess_result["session_quality_score"]
        pkg.active_session        = sess_result["active_session"]
        pkg.session_overlap       = sess_result["session_overlap"]
        pkg.spread_score          = sess_result["spread_score"]

        # ── ECONOMIC CALENDAR ─────────────────────────────────────────────────
        if cal_data:
            available.append("economic_calendar")
            pair_window = cal_data.get("pair_window", {})

            pkg.pre_news_window      = (
                pair_window.get("window_type") == "PRE_NEWS"
            )
            pkg.post_news_window     = (
                pair_window.get("window_type") == "POST_NEWS"
            )
            pkg.news_blackout        = pair_window.get("blackout", False)
            pkg.minutes_to_next_event= cal_data.get("minutes_to_next_high")
            pkg.highest_impact_next4h= cal_data.get(
                "highest_impact_next4h", "NONE"
            )

        # ── MULTI-ASSET (DXY, GOLD, VIX) ─────────────────────────────────────
        if asset_data and asset_data.get("prices"):
            available.append("multi_asset")

            prices       = asset_data.get("prices", {})
            stress_flags = asset_data.get("stress_flags", {})
            correlations = asset_data.get("correlations", {})
            smt_signals  = asset_data.get("smt_signals", {})

            # Raw values
            dxy_data     = prices.get("DXY", {})
            gold_data    = prices.get("GOLD", {})
            vix_level    = stress_flags.get("vix_level")

            pkg.dxy_change_pct     = dxy_data.get("change_pct")
            pkg.dxy_volatility_score= stress_flags.get("dxy_volatility_score")
            pkg.gold_change_pct    = gold_data.get("change_pct")
            pkg.vix_level          = vix_level
            pkg.multi_stress_count = stress_flags.get("stress_trigger_count", 0)
            pkg.corr_breakdown_flag= stress_flags.get("corr_breakdown_eur_gbp", False)

            # VIX normalized score (0→100)
            if vix_level:
                pkg.vix_score = max(0.0, min(100.0,
                    100.0 - ((vix_level - 10.0) / 30.0) * 100.0
                ))

            # DXY correlation for this pair
            pair_key = pair.replace("/", "")
            pkg.dxy_correlation = correlations.get(
                f"DXY_{pair_key}", correlations.get(f"{pair_key}_DXY")
            )

            # SMT divergence
            for group, signal in smt_signals.items():
                if signal.get("divergence_detected"):
                    pkg.smt_divergence_detected = True
                    pkg.smt_divergence_score    = 80.0
                    pkg.smt_correlation         = signal.get("correlation")
                    break

            # Macro score
            macro_result = self._macro_scorer.compute(
                pair          = pair,
                dxy_change    = pkg.dxy_change_pct,
                dxy_vol_score = pkg.dxy_volatility_score,
                gold_change   = pkg.gold_change_pct,
                vix_level     = vix_level,
                stress_count  = pkg.multi_stress_count,
                correlations  = correlations,
            )
            pkg.macro_score = macro_result["macro_score"]

        # ── COT DATA ──────────────────────────────────────────────────────────
        if cot_data and cot_data.get("positioning"):
            available.append("cot_data")

            # Find relevant currency for this pair
            pair_currencies = {
                "EUR/USD": "EUR", "GBP/USD": "GBP",
                "USD/JPY": "JPY", "USD/CHF": "CHF",
                "AUD/USD": "AUD", "USD/CAD": "CAD",
                "NZD/USD": "NZD", "EUR/GBP": "EUR",
                "EUR/JPY": "EUR",
            }
            currency = pair_currencies.get(pair)
            if currency:
                cot_pos = cot_data["positioning"].get(currency, {})
                pkg.cot_positioning_score = cot_pos.get("positioning_score")
                pkg.cot_extreme_flag      = cot_pos.get("extreme_flag", False)
                pkg.cot_trend_direction   = cot_pos.get("trend_direction", "STABLE")

        # ── NEWS & SENTIMENT ──────────────────────────────────────────────────
        if news_data:
            available.append("news_sentiment")

            pair_scores   = news_data.get("pair_scores", {})
            retail_data   = news_data.get("retail_sentiment", {})
            fear_greed    = news_data.get("fear_greed_index", {})

            pkg.news_sentiment_score  = pair_scores.get(pair)
            pkg.news_impact_score     = news_data.get("news_impact_score")
            pkg.fear_greed_score      = fear_greed.get("score")

            retail_pair = retail_data.get(pair, {})
            pkg.retail_long_pct      = retail_pair.get("long_pct")
            pkg.retail_short_pct     = retail_pair.get("short_pct")
            pkg.retail_extreme_flag  = retail_pair.get("extreme_flag", False)

            # Sentiment extreme score
            sent_result = self._sentiment_scorer.compute(
                retail_long_pct = pkg.retail_long_pct,
                news_sentiment  = pkg.news_sentiment_score,
                fear_greed      = pkg.fear_greed_score,
            )
            pkg.sentiment_extreme_score = sent_result["sentiment_extreme_score"]

        # ── OPTIONS FLOW ──────────────────────────────────────────────────────
        if options_data and options_data.get("flow_scores"):
            available.append("options_flow")

            flow_scores = options_data.get("flow_scores", {})
            iv_data     = options_data.get("implied_volatility", {})
            rr_data     = options_data.get("risk_reversals", {})
            pc_data     = options_data.get("put_call_ratios", {})
            vol_regimes = options_data.get("vol_regimes", {})

            pkg.options_flow_score = flow_scores.get(pair)

            pair_iv = iv_data.get(pair, {})
            pkg.implied_vol    = pair_iv.get("iv_1m")
            pkg.realized_vol   = pair_iv.get("rv_20d")
            pkg.iv_rv_spread   = pair_iv.get("iv_rv_spread")
            pkg.vol_percentile = options_data.get(
                "flow_score_detail", {}
            ).get(pair, {}).get("vol_percentile")

            pair_rr = rr_data.get(pair, {})
            if pair_rr.get("rr_25d_1m") is not None:
                rr_raw = pair_rr["rr_25d_1m"]
                # Map ±3 vol points to 0→100
                pkg.risk_reversal_score = max(0.0, min(100.0,
                    50.0 + (rr_raw / 3.0) * 50.0
                ))

            pair_pc = pc_data.get(pair, {})
            pkg.put_call_ratio = pair_pc.get("put_call_ratio")

        # ── VOLATILITY SCORE ──────────────────────────────────────────────────
        # Compute ATR from price bars if available
        atr_value = None
        if "1h" in price_bars:
            h1_bar = price_bars["1h"]
            if isinstance(h1_bar, dict):
                atr_value = (
                    (h1_bar.get("high", 0) - h1_bar.get("low", 0)) * 10000
                )  # Rough ATR in pips

        # Update ATR history
        with self._lock:
            if pair not in self._atr_history:
                self._atr_history[pair] = deque(maxlen=50)
            if atr_value:
                self._atr_history[pair].append(atr_value)
            atr_avg = (
                sum(self._atr_history[pair]) / len(self._atr_history[pair])
                if self._atr_history[pair] else None
            )

        pkg.atr_value = atr_value
        vol_result = self._vol_scorer.compute(
            atr_value      = atr_value,
            atr_avg        = atr_avg,
            implied_vol    = pkg.implied_vol,
            vol_percentile = pkg.vol_percentile,
        )
        pkg.volatility_score = vol_result["volatility_score"]
        pkg.atr_ratio        = vol_result["atr_ratio"]

        # ── INSTITUTIONAL SCORE ───────────────────────────────────────────────
        inst_result = self._inst_scorer.compute(
            cot_score     = pkg.cot_positioning_score,
            options_score = pkg.options_flow_score,
            rr_score      = pkg.risk_reversal_score,
            pc_ratio      = pkg.put_call_ratio,
        )
        pkg.institutional_score = inst_result["institutional_score"]

        # ── ORDER FLOW SCORE ──────────────────────────────────────────────────
        # Position book bias from options flow
        pos_books = options_data.get("position_books", {}) if options_data else {}
        pos_book  = pos_books.get(pair, {})
        pkg.position_book_bias = pos_book.get("net_bias")

        flow_result = self._flow_scorer.compute(
            position_book_bias  = pkg.position_book_bias,
            velocity_pips_per_s = pkg.velocity_pips_per_s,
            directional_bias    = pkg.directional_bias_ratio,
        )
        pkg.order_flow_score = flow_result["order_flow_score"]

        # ── REGIME PROBABILITY ────────────────────────────────────────────────
        regime_result = self._regime_est.estimate(
            adx          = pkg.adx_value,
            atr_ratio    = pkg.atr_ratio,
            spread_ratio = (
                spread / avg_spread
                if spread and avg_spread else None
            ),
            news_active  = pkg.pre_news_window or pkg.news_blackout,
            stress_count = pkg.multi_stress_count,
            vix          = pkg.vix_level,
            mtf_score    = pkg.mtf_score,
        )
        probs                     = regime_result["probs"]
        pkg.regime_trend_prob     = probs.get("TREND")
        pkg.regime_range_prob     = probs.get("RANGE")
        pkg.regime_hv_trend_prob  = probs.get("HV_TREND")
        pkg.regime_news_prob      = probs.get("NEWS")
        pkg.regime_crisis_prob    = probs.get("CRISIS")
        pkg.regime_dominant       = regime_result["dominant"]
        pkg.regime_confidence     = regime_result["confidence"]
        pkg.regime_ambiguous      = regime_result["ambiguous"]

        # ── DATA QUALITY ──────────────────────────────────────────────────────
        quality_result = self._quality_assessor.assess(
            available_inputs = available,
            feed_health      = feed_health,
            null_data        = null_data,
        )
        pkg.confidence_penalty = quality_result["confidence_penalty"]
        pkg.missing_inputs     = quality_result["missing_inputs"]

        # Data tier label
        if "options_flow" in available and "cot_data" in available:
            pkg.data_tier = "TIER_1_TIER_2"
        elif "cot_data" in available:
            pkg.data_tier = "TIER_1_TIER_2"
        else:
            pkg.data_tier = "TIER_1"

        logger.debug(
            f"FeatureEngine: {pair} computed "
            f"mtf={pkg.mtf_score} "
            f"regime={pkg.regime_dominant}({pkg.regime_confidence:.0f}%) "
            f"vol={pkg.volatility_score} "
            f"structure={pkg.structure_state}"
        )

        return pkg

    def compute_all_pairs(self,
                          pairs             : list,
                          layer1_price_mgr  ,
                          layer1_calendar   : dict,
                          layer1_multi_asset: dict,
                          layer1_cot        : dict,
                          layer1_news       : dict,
                          layer1_options    : dict) -> dict:
        """
        Compute FeaturePackage for all pairs simultaneously.
        Returns dict of pair → FeaturePackage.
        """
        packages = {}
        for pair in pairs:
            # Get pair-specific price package
            price_pkg = (
                layer1_price_mgr.get_layer1_package(pair)
                if hasattr(layer1_price_mgr, "get_layer1_package")
                else layer1_price_mgr.get(pair, {})
            )
            # Get pair-specific calendar window
            cal_pkg = dict(layer1_calendar) if layer1_calendar else {}
            if hasattr(layer1_calendar, "get_layer1_package"):
                cal_pkg = layer1_calendar.get_layer1_package(pair)

            packages[pair] = self.compute(
                pair               = pair,
                layer1_price       = price_pkg,
                layer1_calendar    = cal_pkg,
                layer1_multi_asset = layer1_multi_asset,
                layer1_cot         = layer1_cot,
                layer1_news        = layer1_news,
                layer1_options     = layer1_options,
            )

        return packages


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 13 — SELF TEST
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import random
    print("=" * 70)
    print("LAYER 2 — FEATURE ENGINE SELF TEST")
    print("=" * 70)
    print()
    print("ARCHITECTURAL RULE REMINDER:")
    print("  Layer 2 MEASURES only — Layer 3 INTERPRETS")
    print("  No score here carries trade implication")
    print()

    engine = FeatureEngine()

    # ── Build mock Layer 1 packages ──────────────────────────────────────────
    print("[1] Building mock Layer 1 packages...")

    # Mock price data
    now_ms = time.time() * 1000
    mock_bars = []
    price = 1.0850
    for i in range(100):
        price += random.gauss(0, 0.0005)
        mock_bars.append({
            "mid_open" : price - 0.0002,
            "mid_high" : price + 0.0008,
            "mid_low"  : price - 0.0006,
            "mid_close": price,
            "high"     : price + 0.0008,
            "low"      : price - 0.0006,
            "close"    : price,
            "volume"   : random.randint(100, 500),
        })

    mock_price = {
        "instrument"          : "EUR/USD",
        "feed_health"         : "HEALTHY",
        "null_data"           : False,
        "latest_bid"          : 1.0849,
        "latest_ask"          : 1.0851,
        "latest_mid"          : 1.0850,
        "latest_spread"       : 0.0002,
        "latest_velocity"     : 0.8,
        "latest_quality"      : 0.95,
        "latest_anomalies"    : ["NONE"],
        "latest_timestamp"    : now_ms,
        "data_source"         : "OANDA",
        "data_tier"           : "TIER_1",
        "directional_bias_ratio": 0.62,
        "movement_consistency": 0.71,
        "noise_ratio"         : 0.28,
        "acceleration"        : 1.2,
        "bars"                : {
            "1d" : mock_bars[-30:],
            "4h" : mock_bars[-60:],
            "1h" : mock_bars[-80:],
            "15m": mock_bars[-100:],
        },
    }

    mock_calendar = {
        "pair_window"          : {"window_type": "CLEAR", "blackout": False},
        "active_blackout"      : False,
        "blackout_pairs"       : [],
        "minutes_to_next_high" : 187.0,
        "highest_impact_next4h": "NONE",
    }

    mock_multi_asset = {
        "prices": {
            "DXY" : {"change_pct": -0.35, "price": 103.2},
            "GOLD": {"change_pct":  0.82, "price": 2055.0},
            "VIX" : {"change_pct":  2.1,  "price": 18.5},
        },
        "stress_flags": {
            "vix_level"           : 18.5,
            "vix_elevated"        : False,
            "dxy_elevated"        : False,
            "gold_stress"         : False,
            "dxy_volatility_score": 32.0,
            "corr_breakdown_eur_gbp": False,
            "stress_trigger_count": 0,
            "aggregate_stress_score": 12.0,
        },
        "correlations": {
            "EURUSD_DXY": -0.88,
            "EURUSD_GBPUSD": 0.91,
        },
        "smt_signals": {
            "USD_PAIRS_GROUP1": {
                "divergence_detected": False,
                "correlation": 0.89,
            }
        },
    }

    mock_cot = {
        "positioning": {
            "EUR": {
                "positioning_score": 68.5,
                "extreme_flag"     : False,
                "trend_direction"  : "INCREASING",
                "net_position"     : 45000,
                "long_pct"         : 58.2,
                "short_pct"        : 41.8,
            }
        }
    }

    mock_news = {
        "pair_scores"      : {"EUR/USD": 0.32, "GBP/USD": -0.18},
        "news_impact_score": 42.0,
        "fear_greed_index" : {"score": 55.0, "label": "GREED"},
        "retail_sentiment" : {
            "EUR/USD": {
                "long_pct"    : 68.5,
                "short_pct"   : 31.5,
                "extreme_flag": False,
            }
        },
    }

    mock_options = {
        "flow_scores": {"EUR/USD": 61.5, "GBP/USD": 38.2},
        "implied_volatility": {
            "EUR/USD": {
                "iv_1m"      : 7.2,
                "rv_20d"     : 6.5,
                "iv_rv_spread": 0.7,
                "vol_regime" : "NORMAL",
            }
        },
        "risk_reversals": {
            "EUR/USD": {"rr_25d_1m": 0.45}
        },
        "put_call_ratios": {
            "EUR/USD": {"put_call_ratio": 0.82}
        },
        "vol_regimes"  : {"EUR/USD": "NORMAL"},
        "position_books": {
            "EUR/USD": {
                "long_pct" : 62.5,
                "short_pct": 37.5,
                "net_bias" : 0.25,
            }
        },
        "flow_score_detail": {
            "EUR/USD": {
                "vol_percentile": 45.0,
                "iv_spike"      : False,
            }
        },
    }

    print("    Mock Layer 1 packages ready")

    # ── Compute Feature Package ───────────────────────────────────────────────
    print("\n[2] Computing EUR/USD Feature Package...")
    pkg = engine.compute(
        pair               = "EUR/USD",
        layer1_price       = mock_price,
        layer1_calendar    = mock_calendar,
        layer1_multi_asset = mock_multi_asset,
        layer1_cot         = mock_cot,
        layer1_news        = mock_news,
        layer1_options     = mock_options,
    )

    # ── Display results ───────────────────────────────────────────────────────
    print("\n" + "─" * 60)
    print(f"  FEATURE PACKAGE: {pkg.pair}")
    print(f"  Timestamp: {datetime.fromtimestamp(pkg.timestamp_utc/1000, tz=timezone.utc).isoformat()}")
    print("─" * 60)

    print("\n  📐 MTF ALIGNMENT (raw scores — no interpretation):")
    print(f"    Overall MTF Score : {pkg.mtf_score:.1f}/100")
    print(f"    W1 Score          : {pkg.mtf_w1_score}")
    print(f"    D1 Score          : {pkg.mtf_d1_score}")
    print(f"    H4 Score          : {pkg.mtf_h4_score}")
    print(f"    H1 Score          : {pkg.mtf_h1_score}")
    print(f"    Structure State   : {pkg.structure_state}")
    print(f"    Transition        : {pkg.transition_detected}")

    print("\n  🎯 REGIME PROBABILITY (distribution — not classification):")
    print(f"    TREND probability     : {pkg.regime_trend_prob:.1%}")
    print(f"    RANGE probability     : {pkg.regime_range_prob:.1%}")
    print(f"    HV_TREND probability  : {pkg.regime_hv_trend_prob:.1%}")
    print(f"    NEWS probability      : {pkg.regime_news_prob:.1%}")
    print(f"    CRISIS probability    : {pkg.regime_crisis_prob:.1%}")
    print(f"    Dominant              : {pkg.regime_dominant}")
    print(f"    Confidence            : {pkg.regime_confidence:.1f}/100")
    print(f"    Ambiguous             : {pkg.regime_ambiguous}")

    print("\n  📊 VOLATILITY (raw measurements):")
    print(f"    Volatility Score  : {pkg.volatility_score:.1f}/100")
    print(f"    ATR Ratio         : {pkg.atr_ratio}")
    print(f"    Implied Vol 1M    : {pkg.implied_vol:.1f}%" if pkg.implied_vol else "    Implied Vol 1M    : N/A")
    print(f"    Realized Vol 20D  : {pkg.realized_vol:.1f}%" if pkg.realized_vol else "    Realized Vol 20D  : N/A")
    print(f"    IV-RV Spread      : {pkg.iv_rv_spread:+.1f}%" if pkg.iv_rv_spread else "    IV-RV Spread      : N/A")
    print(f"    Vol Percentile    : {pkg.vol_percentile:.0f}th" if pkg.vol_percentile else "    Vol Percentile    : N/A")

    print("\n  📈 TREND STRENGTH (raw scores):")
    print(f"    Trend Score       : {pkg.trend_strength_score}")
    print(f"    ADX Value         : {pkg.adx_value}")
    print(f"    Directional Bias  : {pkg.directional_bias_ratio:.3f}" if pkg.directional_bias_ratio else "    Directional Bias  : N/A")
    print(f"    Movement Consist. : {pkg.movement_consistency:.3f}" if pkg.movement_consistency else "    Movement Consist. : N/A")
    print(f"    Noise Ratio       : {pkg.noise_ratio:.3f}" if pkg.noise_ratio else "    Noise Ratio       : N/A")

    print("\n  🕐 SESSION QUALITY (raw scores):")
    print(f"    Session Score     : {pkg.session_quality_score:.1f}/100")
    print(f"    Active Session    : {pkg.active_session}")
    print(f"    Overlap           : {pkg.session_overlap}")
    print(f"    Spread Score      : {pkg.spread_score:.1f}/100" if pkg.spread_score else "    Spread Score      : N/A")

    print("\n  📅 ECONOMIC CALENDAR (labels — no decisions):")
    print(f"    Pre-news window   : {pkg.pre_news_window}")
    print(f"    Post-news window  : {pkg.post_news_window}")
    print(f"    News blackout     : {pkg.news_blackout}")
    print(f"    Min to next HIGH  : {pkg.minutes_to_next_event}")
    print(f"    Highest next 4h   : {pkg.highest_impact_next4h}")

    print("\n  🏦 INSTITUTIONAL (raw scores):")
    print(f"    Institutional Score: {pkg.institutional_score:.1f}/100")
    print(f"    COT Score         : {pkg.cot_positioning_score:.1f}/100" if pkg.cot_positioning_score else "    COT Score         : N/A")
    print(f"    COT Extreme       : {pkg.cot_extreme_flag}")
    print(f"    COT Trend Dir     : {pkg.cot_trend_direction}")
    print(f"    Options Flow      : {pkg.options_flow_score:.1f}/100" if pkg.options_flow_score else "    Options Flow      : N/A")
    print(f"    Risk Reversal     : {pkg.risk_reversal_score:.1f}/100" if pkg.risk_reversal_score else "    Risk Reversal     : N/A")
    print(f"    Put/Call Ratio    : {pkg.put_call_ratio:.3f}" if pkg.put_call_ratio else "    Put/Call Ratio    : N/A")

    print("\n  🌍 MACRO (raw scores):")
    print(f"    Macro Score       : {pkg.macro_score:.1f}/100" if pkg.macro_score else "    Macro Score       : N/A")
    print(f"    DXY Change        : {pkg.dxy_change_pct:+.2f}%" if pkg.dxy_change_pct else "    DXY Change        : N/A")
    print(f"    Gold Change       : {pkg.gold_change_pct:+.2f}%" if pkg.gold_change_pct else "    Gold Change       : N/A")
    print(f"    VIX Level         : {pkg.vix_level:.1f}" if pkg.vix_level else "    VIX Level         : N/A")
    print(f"    VIX Score         : {pkg.vix_score:.1f}/100" if pkg.vix_score else "    VIX Score         : N/A")
    print(f"    Stress Count      : {pkg.multi_stress_count}")
    print(f"    Corr Breakdown    : {pkg.corr_breakdown_flag}")

    print("\n  🧠 SENTIMENT (raw scores):")
    print(f"    Sentiment Score   : {pkg.sentiment_extreme_score:.1f}/100" if pkg.sentiment_extreme_score else "    Sentiment Score   : N/A")
    print(f"    Retail Long %     : {pkg.retail_long_pct:.1f}%" if pkg.retail_long_pct else "    Retail Long %     : N/A")
    print(f"    Retail Extreme    : {pkg.retail_extreme_flag}")
    print(f"    News Sentiment    : {pkg.news_sentiment_score:+.3f}" if pkg.news_sentiment_score else "    News Sentiment    : N/A")
    print(f"    Fear & Greed      : {pkg.fear_greed_score:.1f}/100" if pkg.fear_greed_score else "    Fear & Greed      : N/A")

    print("\n  ⚡ ORDER FLOW (raw scores):")
    print(f"    Order Flow Score  : {pkg.order_flow_score:.1f}/100" if pkg.order_flow_score else "    Order Flow Score  : N/A")
    print(f"    Position Bias     : {pkg.position_book_bias:+.3f}" if pkg.position_book_bias else "    Position Bias     : N/A")
    print(f"    Velocity pips/s   : {pkg.velocity_pips_per_s:+.2f}" if pkg.velocity_pips_per_s else "    Velocity pips/s   : N/A")

    print("\n  🔌 DATA QUALITY:")
    print(f"    Feed Health       : {pkg.feed_health}")
    print(f"    Data Tier         : {pkg.data_tier}")
    print(f"    Confidence Penalty: {pkg.confidence_penalty:.1%}")
    print(f"    Missing Inputs    : {pkg.missing_inputs if pkg.missing_inputs else 'None'}")
    print(f"    NULL Data         : {pkg.null_data}")

    # ── Verify architectural rule ─────────────────────────────────────────────
    print("\n[3] Architectural rule verification:")
    print("    Checking Layer 2 output contains NO interpretation...")
    pkg_dict = pkg.to_dict()

    # These are interpretation words that should NOT appear in Layer 2 output
    # NOTE: "WEAK" and "STABLE" are valid STRUCTURE STATE labels — excluded
    forbidden_words = [
        "bullish", "bearish",
        "buy", "sell", "favorable", "unfavorable",
        "good", "bad", "recommend", "should trade",
        "opportunity", "signal",
    ]
    violations = []
    for key, value in pkg_dict.items():
        if isinstance(value, str):
            for word in forbidden_words:
                if word in value.lower():
                    violations.append(f"{key}='{value}' contains '{word}'")

    if violations:
        print(f"    ❌ VIOLATIONS FOUND: {violations}")
    else:
        print("    ✅ No interpretation found in output")
        print("    ✅ All scores are raw measurements")
        print("    ✅ Layer 3 will assign all meaning")

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("LAYER 2 FEATURE ENGINE SELF TEST COMPLETE ✅")
    print()
    print("Scores computed:")
    scores = {
        "MTF Score"           : pkg.mtf_score,
        "Volatility Score"    : pkg.volatility_score,
        "Trend Strength"      : pkg.trend_strength_score,
        "Session Quality"     : pkg.session_quality_score,
        "Institutional Score" : pkg.institutional_score,
        "Macro Score"         : pkg.macro_score,
        "Sentiment Score"     : pkg.sentiment_extreme_score,
        "Order Flow Score"    : pkg.order_flow_score,
        "Regime Confidence"   : pkg.regime_confidence,
    }
    for name, val in scores.items():
        status = f"{val:.1f}/100" if val is not None else "N/A (insufficient data)"
        print(f"  {name:22s}: {status}")

    print()
    print("Structure State    :", pkg.structure_state)
    print("Regime Dominant    :", pkg.regime_dominant)
    print("Regime Ambiguous   :", pkg.regime_ambiguous)
    print()
    print("Layer 2 → Layer 3 interface: FeaturePackage.to_dict()")
    print("Layer 3 assigns ALL weights and ALL interpretation")
    print("=" * 70)
