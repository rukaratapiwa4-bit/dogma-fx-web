"""
═══════════════════════════════════════════════════════════════════════════════
FOREX TRADING SIGNAL SYSTEM — VERSION 6.0
FILE: data_feed_config.py
LAYER: 1 — DATA PERCEPTION LAYER (Configuration & Entry Point)
═══════════════════════════════════════════════════════════════════════════════

PURPOSE:
    System configuration, instrument definitions, and entry point
    for the complete Market Data Feed module.

    This file ties together:
        market_data_feed.py  → core pipeline
        oanda_feed.py        → live OANDA WebSocket feed
        dukascopy_feed.py    → historical Dukascopy replay

HOW TO RUN:
    Live mode (OANDA):
        python data_feed_config.py --mode live --api-key YOUR_KEY --account YOUR_ID

    Historical replay (Dukascopy CSV):
        python data_feed_config.py --mode historical --csv-path path/to/ticks.csv

    Test mode (no API key needed):
        python data_feed_config.py --mode test

═══════════════════════════════════════════════════════════════════════════════
"""

import time
import logging
import argparse
from pathlib import Path
from market_data_feed import (
    create_feed_system,
    DataSource,
    Timeframe,
    FeedHealthState,
)

logger = logging.getLogger("DataFeedConfig")

# ─────────────────────────────────────────────────────────────────────────────
# INSTRUMENT CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

# Primary forex pairs — Phase 1 focus
FOREX_PAIRS_PRIMARY = [
    "EUR/USD",   # Most liquid — start here
    "GBP/USD",
    "USD/JPY",
]

# Extended pairs — add in Phase 2
FOREX_PAIRS_EXTENDED = [
    "USD/CHF",
    "AUD/USD",
    "USD/CAD",
    "NZD/USD",
    "EUR/GBP",
    "EUR/JPY",
]

# Point values for pip calculation per pair
POINT_VALUES = {
    "EUR/USD" : 0.00001,   # 5-decimal pair
    "GBP/USD" : 0.00001,
    "USD/JPY" : 0.001,     # 3-decimal pair
    "USD/CHF" : 0.00001,
    "AUD/USD" : 0.00001,
    "USD/CAD" : 0.00001,
    "NZD/USD" : 0.00001,
    "EUR/GBP" : 0.00001,
    "EUR/JPY" : 0.001,
}

# Session definitions (UTC hours)
SESSIONS = {
    "SYDNEY"  : {"open": 21, "close": 6},
    "TOKYO"   : {"open": 0,  "close": 9},
    "LONDON"  : {"open": 7,  "close": 16},
    "NEW_YORK": {"open": 12, "close": 21},
    "OVERLAP" : {"open": 12, "close": 16},  # London/NY — highest probability
}


# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM HEALTH REPORTER
# ─────────────────────────────────────────────────────────────────────────────

class SystemHealthReporter:
    """
    Periodically reports system health to console/log.
    Shows feed health, bar counts, anomaly summary per instrument.
    """

    def __init__(self, feed_mgr, layer1, interval_seconds: float = 30.0):
        self.feed_mgr  = feed_mgr
        self.layer1    = layer1
        self.interval  = interval_seconds

    def report(self):
        """Print current system health snapshot."""
        print("\n" + "─" * 60)
        print(f"SYSTEM HEALTH REPORT — {time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print("─" * 60)

        packages = self.layer1.get_all_packages()
        for instr, pkg in packages.items():
            health = pkg.get("feed_health", "UNKNOWN")
            health_icon = {
                "HEALTHY"        : "🟢",
                "DEGRADED"       : "🟡",
                "PARTIAL_OUTAGE" : "🟠",
                "CRITICAL_OUTAGE": "🔴",
                "TOTAL_OUTAGE"   : "⛔",
            }.get(health, "❓")

            bid      = pkg.get("latest_bid")
            ask      = pkg.get("latest_ask")
            spread   = pkg.get("latest_spread")
            quality  = pkg.get("latest_quality")
            anomalies= pkg.get("latest_anomalies", [])
            source   = pkg.get("data_source", "UNKNOWN")
            tier     = pkg.get("data_tier", "UNKNOWN")

            print(f"\n  {health_icon} {instr} [{source}] [{tier}]")
            if bid and ask:
                print(f"     Bid/Ask : {bid:.5f} / {ask:.5f}")
                print(f"     Spread  : {spread:.5f}" if spread else "     Spread: None")
            print(f"     Quality : {quality:.3f}" if quality else "     Quality: None")
            print(f"     Health  : {health}")
            if anomalies and anomalies != ["NONE"]:
                print(f"     ⚠️  Anomalies: {anomalies}")

            # Bar availability
            bars = pkg.get("bars", {})
            if bars:
                available = [tf for tf, b in bars.items() if b.get("complete")]
                print(f"     Bars    : {', '.join(available) if available else 'building...'}")

        print("─" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# LIVE MODE
# ─────────────────────────────────────────────────────────────────────────────

def run_live(api_key: str, account_id: str,
             instruments: list, practice: bool = True):
    """
    Start live OANDA data feed.
    Connects WebSocket and feeds ticks into pipeline continuously.
    """
    from oanda_feed import OANDAStreamingFeed

    print(f"\n🚀 Starting LIVE feed ({'practice' if practice else 'LIVE'} account)")
    print(f"   Instruments: {instruments}")
    print(f"   Press Ctrl+C to stop\n")

    feed_mgr, layer1 = create_feed_system(instruments, DataSource.LIVE_SOURCE)
    reporter = SystemHealthReporter(feed_mgr, layer1, interval_seconds=30)

    stream = OANDAStreamingFeed(
        api_key      = api_key,
        account_id   = account_id,
        feed_manager = feed_mgr,
        instruments  = instruments,
        practice     = practice,
    )

    stream.start()

    try:
        last_report = time.time()
        while True:
            time.sleep(1)
            if time.time() - last_report >= 30:
                reporter.report()
                last_report = time.time()
    except KeyboardInterrupt:
        print("\n⏹ Stopping feed...")
        stream.stop()


# ─────────────────────────────────────────────────────────────────────────────
# HISTORICAL MODE
# ─────────────────────────────────────────────────────────────────────────────

def run_historical(csv_path: str, instrument: str = "EUR/USD",
                   speed: float = 0.0):
    """
    Replay historical ticks from Dukascopy CSV file.
    """
    from dukascopy_feed import DukascopyReplayController

    filepath = Path(csv_path)
    if not filepath.exists():
        print(f"❌ File not found: {csv_path}")
        return

    print(f"\n📼 Starting HISTORICAL replay")
    print(f"   File      : {csv_path}")
    print(f"   Instrument: {instrument}")
    print(f"   Speed     : {'max' if speed == 0 else f'{speed}x'}\n")

    feed_mgr, layer1 = create_feed_system([instrument], DataSource.HISTORICAL_SOURCE)
    reporter         = SystemHealthReporter(feed_mgr, layer1)

    replay = DukascopyReplayController(feed_mgr, speed_multiplier=speed)
    replay.replay_from_csv(instrument, filepath, POINT_VALUES.get(instrument, 0.00001))

    print(f"\n✅ Replay complete: {replay.get_ticks_replayed():,} ticks")
    reporter.report()


# ─────────────────────────────────────────────────────────────────────────────
# TEST MODE
# ─────────────────────────────────────────────────────────────────────────────

def run_test():
    """
    Full system test — no API key needed.
    Tests all components with simulated data.
    """
    import random

    print("\n🧪 MARKET DATA FEED — FULL SYSTEM TEST")
    print("=" * 60)

    instruments = FOREX_PAIRS_PRIMARY
    feed_mgr, layer1 = create_feed_system(instruments, DataSource.LIVE_SOURCE)
    reporter = SystemHealthReporter(feed_mgr, layer1)

    print(f"\n[1] Testing {len(instruments)} instruments simultaneously...")

    base_prices = {
        "EUR/USD": (1.08500, 1.08503),
        "GBP/USD": (1.27100, 1.27104),
        "USD/JPY": (148.500, 148.503),
    }

    base_ts = time.time() * 1000

    # Feed 1000 ticks per instrument
    for i in range(1000):
        ts = base_ts + (i * 100)  # 100ms between ticks
        for instr, (base_bid, base_ask) in base_prices.items():
            point = POINT_VALUES.get(instr, 0.00001)
            bid   = base_bid + random.gauss(0, point * 5)
            ask   = bid + (base_ask - base_bid) + random.gauss(0, point * 0.5)
            feed_mgr.on_tick(
                instr, ts, bid, ask,
                random.uniform(0.5, 5.0),
                random.uniform(0.5, 5.0),
                random.uniform(10, 80)
            )

    print(f"    ✅ 1000 ticks × {len(instruments)} instruments fed")

    print("\n[2] Testing feed outage simulation...")
    feed_mgr.on_feed_outage("EUR/USD", "TEST_OUTAGE")
    health = feed_mgr.get_feed_health()
    print(f"    EUR/USD health after outage: {health.get('EUR/USD')}")

    print("\n[3] Timeframe bar verification...")
    for instr in instruments:
        bars_1m = feed_mgr.get_bars(instr, Timeframe.ONE_MIN, count=10)
        bars_5m = feed_mgr.get_bars(instr, Timeframe.FIVE_MIN, count=10)
        print(f"    {instr}: 1m bars={len(bars_1m)} 5m bars={len(bars_5m)}")

    print("\n[4] Layer 1 output packages:")
    packages = layer1.get_all_packages()
    for instr, pkg in packages.items():
        null = pkg.get("null_data", False)
        tier = pkg.get("data_tier", "UNKNOWN")
        bid  = pkg.get("latest_bid")
        print(f"    {instr}: null={null} tier={tier} "
              f"bid={'%.5f' % bid if bid else 'None'}")

    print("\n[5] Health report:")
    reporter.report()

    print("\n" + "=" * 60)
    print("✅ FULL SYSTEM TEST COMPLETE")
    print("=" * 60)
    print("\nFiles ready for Layer 2 Feature Engine:")
    print("  market_data_feed.py  → core pipeline (all components)")
    print("  oanda_feed.py        → live OANDA WebSocket connector")
    print("  dukascopy_feed.py    → historical Dukascopy replay")
    print("  data_feed_config.py  → configuration and entry point")
    print("\nNext file: feature_engine.py (Layer 2 — measurement scores)")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Forex Trading System — Market Data Feed"
    )
    parser.add_argument(
        "--mode",
        choices=["live", "historical", "test"],
        default="test",
        help="Run mode"
    )
    parser.add_argument("--api-key",    default="",  help="OANDA API key")
    parser.add_argument("--account",    default="",  help="OANDA account ID")
    parser.add_argument("--csv-path",   default="",  help="Dukascopy CSV path")
    parser.add_argument("--instrument", default="EUR/USD", help="Instrument")
    parser.add_argument("--practice",   action="store_true", default=True,
                        help="Use practice account (default: True)")
    parser.add_argument("--speed",      type=float, default=0.0,
                        help="Replay speed multiplier (0=max)")

    args = parser.parse_args()

    if args.mode == "live":
        if not args.api_key or not args.account:
            print("❌ Live mode requires --api-key and --account")
        else:
            run_live(
                api_key     = args.api_key,
                account_id  = args.account,
                instruments = FOREX_PAIRS_PRIMARY,
                practice    = args.practice,
            )

    elif args.mode == "historical":
        if not args.csv_path:
            print("❌ Historical mode requires --csv-path")
        else:
            run_historical(
                csv_path   = args.csv_path,
                instrument = args.instrument,
                speed      = args.speed,
            )

    else:
        run_test()
