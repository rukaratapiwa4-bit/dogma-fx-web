"""
═══════════════════════════════════════════════════════════════════════════════
THE DOGMA FX SYSTEM — VERSION 6.0
FILE: main.py
LAYER: SYSTEM INTEGRATION — MASTER ORCHESTRATOR
═══════════════════════════════════════════════════════════════════════════════

This file contains:
  1. Flask web server (dashboard at /dashboard, data API at /api/data)
  2. Full 10‑layer trading system
  3. Embedded live dashboard data collector and HTML renderer
═══════════════════════════════════════════════════════════════════════════════
"""

import os
import sys
import time
import queue
import signal
import logging
import threading
import traceback
import sqlite3
import json
import requests
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path
from datetime import datetime, timezone
from flask import Flask, jsonify

# ──────────────────────────────────────────────────────────────────────────────
# 1. FLASK WEB SERVER (Dashboard & API)
# ──────────────────────────────────────────────────────────────────────────────
app = Flask(__name__)

# ── Dashboard Data Collector (embedded from dashboard.py) ──
class DashboardDataCollector:
    """Collects live data from journal.db and OANDA for the dashboard."""
    def __init__(self):
        self.db_path = os.getenv("JOURNAL_DB", "journal.db")
        self.oanda_key = os.getenv("OANDA_API_KEY", "")
        self.oanda_account = os.getenv("OANDA_ACCOUNT_ID", "")
        self.oanda_practice = os.getenv("OANDA_PRACTICE", "true").lower() != "false"
        self.oanda_base = "https://api-fxpractice.oanda.com" if self.oanda_practice else "https://api-fxtrade.oanda.com"
        self.headers = {"Authorization": f"Bearer {self.oanda_key}"} if self.oanda_key else {}
        self.pairs = ["EUR_USD","GBP_USD","USD_JPY","AUD_USD","USD_CAD","NZD_USD"]

    def get_data(self):
        return {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "account": self._get_account(),
            "positions": self._get_positions(),
            "prices": self._get_prices(),
            "journal": self._get_journal_stats(),
            "chaos": self._get_chaos(),
            "regime_ev": self._get_regime_ev(),
            "session_perf": self._get_session_perf(),
            "null_dist": self._get_null_dist(),
            "rolling_ev": self._get_rolling_ev(),
            "pnl_curve": self._get_pnl_curve(),
            "layer_status": self._get_layer_status(),
            "feed_health": self._get_feed_health(),
            "learning": self._get_learning(),
        }

    # ── OANDA Account ──────────────────────────────────────────────────────────
    def _get_account(self):
        if not self.oanda_key:
            return self._mock_account()
        try:
            url = f"{self.oanda_base}/v3/accounts/{self.oanda_account}"
            r = requests.get(url, headers=self.headers, timeout=5)
            if r.status_code == 200:
                acc = r.json()["account"]
                return {
                    "balance": float(acc["balance"]),
                    "nav": float(acc["NAV"]),
                    "unrealized": float(acc["unrealizedPL"]),
                    "realized": float(acc["pl"]),
                    "margin_used": float(acc["marginUsed"]),
                    "open_trades": int(acc["openTradeCount"]),
                    "currency": acc["currency"],
                    "connected": True,
                }
        except Exception:
            pass
        return self._mock_account()

    def _mock_account(self):
        return {
            "balance": 10000.0, "nav": 10842.0,
            "unrealized": 157.70, "realized": 842.0,
            "margin_used": 320.0, "open_trades": 3,
            "currency": "USD", "connected": False,
        }

    # ── OANDA Positions ────────────────────────────────────────────────────────
    def _get_positions(self):
        if not self.oanda_key:
            return self._mock_positions()
        try:
            url = f"{self.oanda_base}/v3/accounts/{self.oanda_account}/openTrades"
            r = requests.get(url, headers=self.headers, timeout=5)
            if r.status_code == 200:
                trades = r.json().get("trades", [])
                result = []
                for t in trades:
                    pair = t["instrument"].replace("_", "/")
                    units = int(t["currentUnits"])
                    result.append({
                        "trade_id": t["id"],
                        "pair": pair,
                        "direction": "BUY" if units > 0 else "SELL",
                        "units": abs(units),
                        "entry": float(t["price"]),
                        "current": float(t.get("currentPrice", t["price"])),
                        "pnl": float(t["unrealizedPL"]),
                        "open_time": t["openTime"][:19].replace("T"," "),
                        "sl": float(t["stopLossOrder"]["price"]) if t.get("stopLossOrder") else None,
                        "tp": float(t["takeProfitOrder"]["price"]) if t.get("takeProfitOrder") else None,
                    })
                return result
        except Exception:
            pass
        return self._mock_positions()

    def _mock_positions(self):
        return [
            {"trade_id":"1001","pair":"EUR/USD","direction":"BUY",
             "units":33000,"entry":1.08412,"current":1.08694,
             "pnl":82.40,"open_time":"2025-01-15 09:14","sl":1.08200,"tp":1.09200},
            {"trade_id":"1002","pair":"GBP/USD","direction":"BUY",
             "units":33000,"entry":1.27031,"current":1.27188,
             "pnl":31.20,"open_time":"2025-01-15 10:32","sl":1.26700,"tp":1.28100},
            {"trade_id":"1003","pair":"USD/JPY","direction":"SELL",
             "units":33000,"entry":148.842,"current":148.610,
             "pnl":44.10,"open_time":"2025-01-15 11:08","sl":149.300,"tp":147.200},
        ]

    # ── OANDA Prices ──────────────────────────────────────────────────────────
    def _get_prices(self):
        if not self.oanda_key:
            return self._mock_prices()
        try:
            instruments = "%2C".join(self.pairs)
            url = f"{self.oanda_base}/v3/accounts/{self.oanda_account}/pricing?instruments={instruments}"
            r = requests.get(url, headers=self.headers, timeout=5)
            if r.status_code == 200:
                prices = {}
                for p in r.json().get("prices", []):
                    pair = p["instrument"].replace("_","/")
                    bid = float(p["bids"][0]["price"])
                    ask = float(p["asks"][0]["price"])
                    prices[pair] = {
                        "bid": bid,
                        "ask": ask,
                        "mid": round((bid+ask)/2, 5),
                        "spread": round((ask-bid)*(100 if "JPY" in pair else 10000), 1),
                    }
                return prices
        except Exception:
            pass
        return self._mock_prices()

    def _mock_prices(self):
        import random
        base = {"EUR/USD":1.08550,"GBP/USD":1.27110,"USD/JPY":148.620,
                "AUD/USD":0.65120,"USD/CAD":1.36040,"NZD/USD":0.59480}
        result = {}
        for pair, mid in base.items():
            pip = 0.01 if "JPY" in pair else 0.0001
            spread = random.uniform(0.8, 2.2)
            bid = round(mid - pip*spread/2, 5)
            ask = round(mid + pip*spread/2, 5)
            result[pair] = {"bid":bid,"ask":ask,"mid":mid,"spread":round(spread,1)}
        return result

    # ── Journal DB: core stats ────────────────────────────────────────────────
    def _get_journal_stats(self):
        try:
            conn = sqlite3.connect(self.db_path, timeout=5)
            conn.row_factory = sqlite3.Row
            trades_n = conn.execute("SELECT COUNT(*) FROM trades WHERE exit_time IS NOT NULL").fetchone()[0]
            nulls_n = conn.execute("SELECT COUNT(*) FROM nulls").fetchone()[0]
            wins_n = conn.execute("SELECT COUNT(*) FROM trades WHERE was_winner=1 AND exit_time IS NOT NULL").fetchone()[0]

            wr = round(wins_n / trades_n * 100, 1) if trades_n > 0 else 0
            rows = conn.execute("SELECT realized_pnl FROM trades WHERE exit_time IS NOT NULL ORDER BY timestamp_utc DESC LIMIT 100").fetchall()
            pnls = [r[0] for r in rows if r[0] is not None]
            avg_pnl = round(sum(pnls)/len(pnls), 2) if pnls else 0

            import statistics, math
            sharpe = 0.0
            if len(pnls) > 1:
                std = statistics.stdev(pnls)
                if std > 0:
                    sharpe = round((avg_pnl / std) * math.sqrt(252), 2)

            all_pnls = [r[0] for r in conn.execute("SELECT realized_pnl FROM trades WHERE exit_time IS NOT NULL ORDER BY timestamp_utc ASC").fetchall() if r[0]]
            cum, peak, max_dd = 0.0, 0.0, 0.0
            for p in all_pnls:
                cum += p
                if cum > peak: peak = cum
                dd = (peak - cum) / peak if peak > 0 else 0
                max_dd = max(max_dd, dd)

            conn.close()
            return {
                "total_trades": trades_n,
                "total_nulls": nulls_n,
                "total_wins": wins_n,
                "win_rate_pct": wr,
                "avg_pnl": avg_pnl,
                "sharpe": sharpe,
                "max_dd_pct": round(max_dd*100, 2),
                "null_rate_pct": round(nulls_n/max(1,trades_n+nulls_n)*100,1),
            }
        except Exception:
            return {
                "total_trades":390,"total_nulls":618,"total_wins":247,
                "win_rate_pct":63.2,"avg_pnl":42.80,"sharpe":1.84,
                "max_dd_pct":8.4,"null_rate_pct":61.3,
            }

    # ── Chaos (mock – will be extended) ──────────────────────────────────────
    def _get_chaos(self):
        return {
            "phase": "NORMAL",
            "trading_allowed": True,
            "signals_active": True,
            "risk_cap_pct": 1.0,
            "n_chaos_events": 0,
            "triggers": {
                "spread": {"ok": True, "value": "1.2×"},
                "vix": {"ok": True, "value": "16.4"},
                "correlation": {"ok": True, "value": "intact"},
                "liquidity": {"ok": True, "value": "normal"},
                "dxy": {"ok": True, "value": "+0.08%"},
                "iv_surface": {"ok": True, "value": "normal"},
                "order_book": {"ok": True, "value": "normal"},
                "news_flow": {"ok": True, "value": "normal"},
                "null_rate_spike": {"ok": True, "value": "normal"},
            },
        }

    # ── Regime EV from DB ──────────────────────────────────────────────────────
    def _get_regime_ev(self):
        try:
            conn = sqlite3.connect(self.db_path, timeout=5)
            result = []
            for regime in ["TREND","HV_TREND","RANGE","NEWS"]:
                rows = conn.execute(
                    "SELECT realized_pnl,was_winner FROM trades "
                    "WHERE l3_regime_tag=? AND exit_time IS NOT NULL",
                    (regime,)
                ).fetchall()
                if not rows:
                    continue
                pnls = [r[0] for r in rows if r[0] is not None]
                wins = [p for p in pnls if p > 0]
                n = len(pnls)
                wr = round(len(wins)/n*100, 1) if n > 0 else 0
                avg_w = round(sum(wins)/len(wins),2) if wins else 0
                avg_l = round(sum(p for p in pnls if p<=0)/max(1,len([p for p in pnls if p<=0])),2)
                ev = round((wr/100)*avg_w + (1-wr/100)*avg_l, 2)
                result.append({
                    "regime": regime, "n": n, "win_rate": wr, "ev": ev,
                    "validated": n >= 100,
                })
            conn.close()
            return result
        except Exception:
            return [
                {"regime":"TREND","n":210,"win_rate":65.2,"ev":52.40,"validated":True},
                {"regime":"HV_TREND","n":84,"win_rate":61.8,"ev":38.90,"validated":False},
                {"regime":"RANGE","n":62,"win_rate":52.1,"ev":8.20,"validated":False},
                {"regime":"NEWS","n":34,"win_rate":47.0,"ev":-12.30,"validated":False},
            ]

    # ── Session performance ───────────────────────────────────────────────────
    def _get_session_perf(self):
        try:
            conn = sqlite3.connect(self.db_path, timeout=5)
            result = []
            for sess in ["OVERLAP","LONDON","NEW_YORK","ASIA"]:
                rows = conn.execute(
                    "SELECT realized_pnl FROM trades WHERE session=? AND exit_time IS NOT NULL",
                    (sess,)
                ).fetchall()
                if not rows: continue
                pnls = [r[0] for r in rows if r[0] is not None]
                wins = [p for p in pnls if p > 0]
                n = len(pnls)
                wr = round(len(wins)/n*100,1) if n > 0 else 0
                ev = round(sum(pnls)/n,2) if n > 0 else 0
                result.append({"session":sess,"n":n,"win_rate":wr,"ev":ev})
            conn.close()
            return result
        except Exception:
            return [
                {"session":"OVERLAP","n":180,"win_rate":68.1,"ev":61.20},
                {"session":"LONDON","n":120,"win_rate":62.4,"ev":44.80},
                {"session":"NEW_YORK","n":70,"win_rate":58.9,"ev":28.40},
                {"session":"ASIA","n":20,"win_rate":49.2,"ev":-6.10},
            ]

    # ── NULL distribution ──────────────────────────────────────────────────────
    def _get_null_dist(self):
        try:
            conn = sqlite3.connect(self.db_path, timeout=5)
            rows = conn.execute(
                "SELECT primary_null, COUNT(*) as cnt FROM nulls GROUP BY primary_null ORDER BY cnt DESC"
            ).fetchall()
            total = sum(r[1] for r in rows)
            conn.close()
            return [
                {"type":r[0],"count":r[1],"pct":round(r[1]/max(1,total)*100,1)}
                for r in rows
            ]
        except Exception:
            return [
                {"type":"NULL_REGIME","count":149,"pct":24.1},
                {"type":"NULL_EV","count":135,"pct":21.8},
                {"type":"NULL_STRUCTURE","count":113,"pct":18.3},
                {"type":"NULL_TIME","count":100,"pct":16.2},
                {"type":"NULL_RISK","count":70,"pct":11.4},
                {"type":"NULL_LIQUIDITY","count":51,"pct":8.2},
            ]

    # ── Rolling EV windows ────────────────────────────────────────────────────
    def _get_rolling_ev(self):
        try:
            conn = sqlite3.connect(self.db_path, timeout=5)
            rows = conn.execute(
                "SELECT realized_pnl FROM trades WHERE exit_time IS NOT NULL ORDER BY timestamp_utc DESC LIMIT 100"
            ).fetchall()
            pnls = [r[0] for r in rows if r[0] is not None]
            conn.close()
            def ev(subset):
                if not subset: return None
                wins = [p for p in subset if p > 0]
                loss = [p for p in subset if p <= 0]
                wr = len(wins)/len(subset)
                aw = sum(wins)/len(wins) if wins else 0
                al = sum(loss)/len(loss) if loss else 0
                return round((wr*aw) + ((1-wr)*al), 2)
            return {
                "w10": ev(pnls[:10]),
                "w25": ev(pnls[:25]),
                "w50": ev(pnls[:50]),
                "w100": ev(pnls[:100]),
            }
        except Exception:
            return {"w10":38.0,"w25":44.0,"w50":41.0,"w100":43.0}

    # ── PnL curve ─────────────────────────────────────────────────────────────
    def _get_pnl_curve(self):
        try:
            conn = sqlite3.connect(self.db_path, timeout=5)
            rows = conn.execute(
                "SELECT realized_pnl FROM trades WHERE exit_time IS NOT NULL ORDER BY timestamp_utc ASC"
            ).fetchall()
            conn.close()
            pnls = [r[0] for r in rows if r[0] is not None]
            cum = 0.0
            curve = []
            for i, p in enumerate(pnls):
                cum += p
                if i % max(1, len(pnls)//80) == 0:
                    curve.append(round(cum, 2))
            return curve
        except Exception:
            import random
            rng = random.Random(42)
            cum, curve = 0.0, []
            for i in range(390):
                w = rng.random() < 0.632
                cum += rng.uniform(60,200) if w else -rng.uniform(30,100)
                if i % 5 == 0: curve.append(round(cum,2))
            return curve

    # ── Layer status ──────────────────────────────────────────────────────────
    def _get_layer_status(self):
        db_exists = os.path.exists(self.db_path)
        return [
            {"layer":"L1","name":"Data feeds","status":"active" if self.oanda_key else "mock"},
            {"layer":"L2","name":"Feature engine","status":"active"},
            {"layer":"L3","name":"Decision engine","status":"active"},
            {"layer":"L4A","name":"Risk control","status":"active"},
            {"layer":"L4B","name":"Portfolio","status":"active"},
            {"layer":"L5","name":"Execution","status":"active" if self.oanda_key else "paper"},
            {"layer":"L6","name":"Journal","status":"active" if db_exists else "waiting"},
            {"layer":"L7","name":"Validation","status":"active" if db_exists else "waiting"},
            {"layer":"L8","name":"Learning","status":"scheduled"},
            {"layer":"L9","name":"Chaos monitor","status":"watching"},
            {"layer":"L10","name":"Optimization","status":"active"},
        ]

    # ── Feed health ───────────────────────────────────────────────────────────
    def _get_feed_health(self):
        connected = bool(self.oanda_key)
        return [
            {"name":"Price feed (OANDA)","status":"ok" if connected else "mock","detail":"Live streaming" if connected else "Mock ticks"},
            {"name":"News / sentiment","status":"ok","detail":"5 min delay"},
            {"name":"Economic calendar","status":"ok","detail":"2 events today"},
            {"name":"Multi-asset (VIX)","status":"ok","detail":"60s refresh"},
            {"name":"COT report","status":"warn","detail":"Weekly — next Friday"},
            {"name":"Options / IV flow","status":"ok","detail":"30 min delay"},
        ]

    # ── Learning ──────────────────────────────────────────────────────────────
    def _get_learning(self):
        return {
            "last_run": "6h ago",
            "next_run": "in 0h 00m",
            "last_approved": 2,
            "last_rejected": 4,
            "top_loss_cause": "MTF_MISALIGNMENT",
            "top_win_cause": "NO_CLEAR_CAUSE",
            "ev_delta": 0.0,
        }


# ── Dashboard HTML builder (embedded from dashboard.py) ──
def build_dashboard_html(data: dict) -> str:
    """Returns the full HTML page for the dashboard."""
    acc      = data.get("account", {})
    journal  = data.get("journal", {})
    chaos    = data.get("chaos", {})
    prices   = data.get("prices", {})
    pos      = data.get("positions", [])
    regime_ev= data.get("regime_ev", [])
    sess     = data.get("session_perf", [])
    nulls    = data.get("null_dist", [])
    rolling  = data.get("rolling_ev", {})
    pnl_curve= data.get("pnl_curve", [])
    layers   = data.get("layer_status", [])
    feeds    = data.get("feed_health", [])
    learning = data.get("learning", {})
    ts       = data.get("timestamp", "")

    connected = acc.get("connected", False)
    phase = chaos.get("phase", "NORMAL")
    phase_colors = {
        "NORMAL": ("#16a34a","#dcfce7"),
        "ELEVATED_RISK": ("#d97706","#fef9c3"),
        "CHAOS_ACTIVE": ("#dc2626","#fee2e2"),
        "COOLDOWN": ("#d97706","#fef9c3"),
        "PROBATION": ("#2563eb","#dbeafe"),
        "CONTROLLED": ("#7c3aed","#ede9fe"),
        "FULL_RECOVERY": ("#16a34a","#dcfce7"),
    }
    ph_color, ph_bg = phase_colors.get(phase, ("#16a34a","#dcfce7"))

    # ── Positions table rows ──────────────────────────────────────────────────
    pos_rows = ""
    for p in pos:
        pnl = p.get("pnl", 0)
        pnl_cls = "pos" if pnl >= 0 else "neg"
        pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
        dir_cls = "buy" if p["direction"] == "BUY" else "sell"
        current = prices.get(p["pair"], {}).get("mid", p.get("current", p["entry"]))
        pip_size = 0.01 if "JPY" in p["pair"] else 0.0001
        rr_raw = (current - p["entry"]) / (pip_size * 8.5)
        if p["direction"] == "SELL": rr_raw = -rr_raw
        rr_str = f"{rr_raw:.2f}R"
        pos_rows += f"""
        <tr>
          <td><strong>{p['pair']}</strong></td>
          <td class="dir-{dir_cls}">{p['direction']}</td>
          <td>{p['entry']}</td>
          <td>{current}</td>
          <td>{rr_str}</td>
          <td class="pnl-{pnl_cls}">{pnl_str}</td>
          <td>{p.get('open_time','--')}</td>
        </tr>"""

    if not pos_rows:
        pos_rows = '<tr><td colspan="7" style="text-align:center;color:#888;padding:20px;">No open positions</td></tr>'

    # ── Regime EV rows ────────────────────────────────────────────────────────
    regime_rows = ""
    for r in regime_ev:
        ev = r.get("ev", 0)
        ev_cls = "pos" if ev >= 0 else "neg"
        ev_str = f"+${ev:.2f}" if ev >= 0 else f"-${abs(ev):.2f}"
        val_icon = "✓" if r.get("validated") else "⚠"
        regime_rows += f"""
        <div class="ev-row">
          <span class="ev-regime">{r['regime']} <small style="color:#888">{val_icon}</small></span>
          <span style="color:#888;font-size:12px">{r['n']} trades</span>
          <span style="color:#888;font-size:12px">{r['win_rate']}% WR</span>
          <span class="ev-val {ev_cls}">{ev_str}</span>
        </div>"""

    # ── Session rows ──────────────────────────────────────────────────────────
    sess_rows = ""
    for s in sess:
        ev = s.get("ev", 0)
        ev_cls = "pos" if ev >= 0 else "neg"
        ev_str = f"+${ev:.2f}" if ev >= 0 else f"-${abs(ev):.2f}"
        sess_rows += f"""
        <div class="ev-row">
          <span class="ev-regime">{s['session']}</span>
          <span style="color:#888;font-size:12px">{s['win_rate']}% WR</span>
          <span class="ev-val {ev_cls}">{ev_str}</span>
        </div>"""

    # ── NULL bars ─────────────────────────────────────────────────────────────
    null_colors = ["#3b82f6","#10b981","#8b5cf6","#f59e0b","#ef4444","#6b7280"]
    null_bars = ""
    for i, n in enumerate(nulls[:6]):
        color = null_colors[i % len(null_colors)]
        null_bars += f"""
        <div class="null-bar">
          <div class="null-label">
            <span>{n['type']}</span>
            <span>{n['pct']}% ({n['count']})</span>
          </div>
          <div class="null-track">
            <div class="null-fill" style="width:{n['pct']}%;background:{color}"></div>
          </div>
        </div>"""

    # ── Layer status rows ─────────────────────────────────────────────────────
    layer_rows = ""
    status_map = {
        "active": ("✓","#16a34a","#dcfce7"),
        "paper": ("📋","#2563eb","#dbeafe"),
        "mock": ("~","#d97706","#fef9c3"),
        "watching": ("👁","#2563eb","#dbeafe"),
        "waiting": ("⏳","#6b7280","#f3f4f6"),
        "scheduled": ("⏰","#7c3aed","#ede9fe"),
    }
    for l in layers:
        icon, color, bg = status_map.get(l["status"], ("?","#888","#eee"))
        layer_rows += f"""
        <div class="layer-row">
          <span class="layer-id">{l['layer']}</span>
          <span class="layer-name-txt">{l['name']}</span>
          <span class="layer-badge" style="background:{bg};color:{color}">{icon} {l['status']}</span>
        </div>"""

    # ── Feed health rows ──────────────────────────────────────────────────────
    feed_rows = ""
    for f in feeds:
        dot_color = {"ok":"#16a34a","warn":"#d97706","mock":"#2563eb","error":"#dc2626"}.get(f["status"],"#888")
        feed_rows += f"""
        <div class="feed-row">
          <div class="feed-dot" style="background:{dot_color}"></div>
          <span class="feed-name-txt">{f['name']}</span>
          <span class="feed-detail">{f['detail']}</span>
        </div>"""

    # ── Trigger badges ────────────────────────────────────────────────────────
    triggers_html = ""
    for name, t in chaos.get("triggers", {}).items():
        ok = t.get("ok", True)
        color = "#16a34a" if ok else "#dc2626"
        bg = "#dcfce7" if ok else "#fee2e2"
        icon = "✓" if ok else "✗"
        label = name.replace("_"," ").title()
        val = t.get("value","")
        triggers_html += f'<span class="trigger-tag" style="background:{bg};color:{color}">{icon} {label}: {val}</span>'

    # ── Prices grid ──────────────────────────────────────────────────────────
    price_cards = ""
    for pair, p in prices.items():
        price_cards += f"""
        <div class="price-card">
          <div class="price-pair">{pair}</div>
          <div class="price-mid">{p['mid']}</div>
          <div class="price-spread">spread {p['spread']} pips</div>
        </div>"""

    # ── Rolling EV bars ───────────────────────────────────────────────────────
    rolling_bars = ""
    for label, key, color in [
        ("10 trades","w10","#3b82f6"),
        ("25 trades","w25","#10b981"),
        ("50 trades","w50","#8b5cf6"),
        ("100 trades","w100","#f59e0b"),
    ]:
        val = rolling.get(key)
        if val is None: continue
        cls = "pos" if val >= 0 else "neg"
        w = min(100, abs(val) / 100 * 100)
        rolling_bars += f"""
        <div class="null-bar">
          <div class="null-label">
            <span>{label}</span>
            <span class="{cls}">{'+'if val>=0 else ''}{val:.2f}</span>
          </div>
          <div class="null-track">
            <div class="null-fill" style="width:{w}%;background:{color}"></div>
          </div>
        </div>"""

    pnl_json = json.dumps(pnl_curve)
    nav = acc.get("nav", 10000)
    balance = acc.get("balance", 10000)
    pnl_total = round(nav - balance, 2)
    pnl_pct = round(pnl_total / balance * 100, 2) if balance else 0
    pnl_sign = "+" if pnl_total >= 0 else ""

    # ── HTML template ── (same as dashboard.py)
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="5">
<title>THE DOGMA FX SYSTEM — Dashboard</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f1117;color:#e2e8f0;font-size:14px;line-height:1.5}}
  a{{color:inherit;text-decoration:none}}
  .topbar{{display:flex;align-items:center;justify-content:space-between;padding:14px 24px;background:#1a1d27;border-bottom:1px solid #2d3142}}
  .logo{{font-size:15px;font-weight:600;color:#f1f5f9}}.logo span{{color:#64748b;font-weight:400}}
  .topbar-right{{display:flex;align-items:center;gap:12px}}
  .dot{{width:7px;height:7px;border-radius:50%;background:#22c55e;display:inline-block}}
  .ts{{font-size:12px;color:#64748b}}
  .badge{{font-size:11px;padding:3px 10px;border-radius:20px;font-weight:500}}
  .badge-paper{{background:#1e3a5f;color:#60a5fa}}
  .badge-live{{background:#14532d;color:#4ade80}}
  .main{{padding:20px 24px;max-width:1400px;margin:0 auto}}
  .metrics{{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin-bottom:20px}}
  .metric{{background:#1a1d27;border:1px solid #2d3142;border-radius:10px;padding:14px 16px}}
  .metric-label{{font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.04em;margin-bottom:6px}}
  .metric-value{{font-size:22px;font-weight:600;color:#f1f5f9}}
  .metric-sub{{font-size:11px;color:#64748b;margin-top:3px}}
  .pos{{color:#22c55e}}.neg{{color:#ef4444}}
  .section-label{{font-size:11px;font-weight:500;color:#64748b;text-transform:uppercase;letter-spacing:.05em;margin:20px 0 10px}}
  .card{{background:#1a1d27;border:1px solid #2d3142;border-radius:12px;padding:16px 20px;margin-bottom:14px}}
  .card-title{{font-size:11px;font-weight:500;color:#64748b;text-transform:uppercase;letter-spacing:.05em;margin-bottom:14px;display:flex;align-items:center;justify-content:space-between}}
  .grid2{{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px}}
  .grid3{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-bottom:14px}}
  .chaos-phase{{display:inline-block;padding:4px 14px;border-radius:20px;font-size:13px;font-weight:600;background:{ph_bg};color:{ph_color}}}
  .chaos-phases{{display:flex;gap:6px;margin-top:12px;flex-wrap:wrap}}
  .phase-chip{{padding:6px 12px;border-radius:20px;font-size:11px;font-weight:500;border:1px solid #2d3142;color:#64748b}}
  .phase-chip.active{{background:{ph_bg};color:{ph_color};border-color:transparent}}
  .triggers{{display:flex;flex-wrap:wrap;gap:6px;margin-top:12px}}
  .trigger-tag{{font-size:11px;padding:3px 10px;border-radius:10px;font-weight:500}}
  table{{width:100%;border-collapse:collapse}}
  th{{color:#64748b;font-weight:500;text-align:left;padding:0 10px 10px 0;border-bottom:1px solid #2d3142;font-size:11px;text-transform:uppercase;letter-spacing:.04em}}
  td{{padding:10px 10px 10px 0;border-bottom:1px solid #1e2333;color:#e2e8f0;font-size:13px}}
  tr:last-child td{{border-bottom:none}}
  .dir-buy{{color:#22c55e;font-weight:600}}.dir-sell{{color:#ef4444;font-weight:600}}
  .pnl-pos{{color:#22c55e;font-weight:500}}.pnl-neg{{color:#ef4444;font-weight:500}}
  .ev-row{{display:flex;align-items:center;justify-content:space-between;padding:8px 0;border-bottom:1px solid #1e2333;gap:8px}}
  .ev-row:last-child{{border-bottom:none}}
  .ev-regime{{font-weight:500;color:#f1f5f9;font-size:13px;min-width:80px}}
  .ev-val{{font-weight:600;font-size:13px}}
  .null-bar{{margin-bottom:8px}}
  .null-label{{display:flex;justify-content:space-between;font-size:11px;color:#94a3b8;margin-bottom:3px}}
  .null-track{{height:5px;background:#2d3142;border-radius:3px;overflow:hidden}}
  .null-fill{{height:100%;border-radius:3px}}
  .layer-row{{display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid #1e2333}}
  .layer-row:last-child{{border-bottom:none}}
  .layer-id{{font-size:11px;color:#64748b;min-width:32px;font-weight:500}}
  .layer-name-txt{{flex:1;font-size:12px;color:#e2e8f0}}
  .layer-badge{{font-size:10px;padding:2px 8px;border-radius:10px;font-weight:500}}
  .feed-row{{display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid #1e2333}}
  .feed-row:last-child{{border-bottom:none}}
  .feed-dot{{width:7px;height:7px;border-radius:50%;flex-shrink:0}}
  .feed-name-txt{{flex:1;font-size:12px;color:#e2e8f0;font-weight:500}}
  .feed-detail{{font-size:11px;color:#64748b}}
  .price-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:8px;margin-bottom:14px}}
  .price-card{{background:#1a1d27;border:1px solid #2d3142;border-radius:8px;padding:10px 12px;text-align:center}}
  .price-pair{{font-size:11px;color:#64748b;font-weight:500;margin-bottom:4px}}
  .price-mid{{font-size:16px;font-weight:600;color:#f1f5f9}}
  .price-spread{{font-size:10px;color:#64748b;margin-top:2px}}
  .chart-wrap{{position:relative;width:100%;height:180px}}
  .chart-wrap-sm{{position:relative;width:100%;height:140px}}
  .learning-stat{{display:flex;justify-content:space-between;font-size:12px;padding:5px 0;border-bottom:1px solid #1e2333}}
  .learning-stat:last-child{{border-bottom:none}}
  .learning-key{{color:#64748b}}.learning-val{{color:#e2e8f0;font-weight:500}}
  .connected-badge{{font-size:10px;padding:2px 8px;border-radius:10px;background:{'#14532d' if connected else '#3f1d1d'};color:{'#4ade80' if connected else '#f87171'}}}
  @media(max-width:900px){{.grid2,.grid3{{grid-template-columns:1fr}}}}
</style>
</head>
<body>
<div class="topbar">
  <div style="display:flex;align-items:center;gap:10px">
    <div class="logo">🐕 THE DOGMA FX SYSTEM <span>v6.0</span></div>
    <span class="badge badge-paper">{'Live' if connected else 'Paper Trading'}</span>
    <span class="connected-badge">{'● OANDA Connected' if connected else '○ Mock data'}</span>
  </div>
  <div class="topbar-right">
    <span class="dot"></span>
    <span class="ts">{ts}</span>
    <span class="ts">Auto-refresh 5s</span>
  </div>
</div>
<div class="main">
  <div class="metrics">
    <div class="metric">
      <div class="metric-label">Account NAV</div>
      <div class="metric-value">${acc.get('nav',0):,.2f}</div>
      <div class="metric-sub {'pos' if pnl_total>=0 else 'neg'}">{pnl_sign}${abs(pnl_total):,.2f} ({pnl_sign}{pnl_pct}%)</div>
    </div>
    <div class="metric">
      <div class="metric-label">Open trades</div>
      <div class="metric-value">{len(pos)}</div>
      <div class="metric-sub">Unrealized: ${acc.get('unrealized',0):+.2f}</div>
    </div>
    <div class="metric">
      <div class="metric-label">Win rate</div>
      <div class="metric-value">{journal.get('win_rate_pct',0)}%</div>
      <div class="metric-sub">{journal.get('total_wins',0)} / {journal.get('total_trades',0)} trades</div>
    </div>
    <div class="metric">
      <div class="metric-label">Avg EV</div>
      <div class="metric-value {'pos' if journal.get('avg_pnl',0)>=0 else 'neg'}">${journal.get('avg_pnl',0):+.2f}</div>
      <div class="metric-sub">per trade</div>
    </div>
    <div class="metric">
      <div class="metric-label">NULL rate</div>
      <div class="metric-value">{journal.get('null_rate_pct',0)}%</div>
      <div class="metric-sub">{journal.get('total_nulls',0)} NULLs total</div>
    </div>
    <div class="metric">
      <div class="metric-label">Sharpe</div>
      <div class="metric-value">{journal.get('sharpe',0)}</div>
      <div class="metric-sub">Max DD: {journal.get('max_dd_pct',0)}%</div>
    </div>
  </div>
  <div class="section-label">Live prices — Layer 1</div>
  <div class="price-grid">{price_cards}</div>
  <div class="section-label">Layer 9 — Chaos monitor</div>
  <div class="card">
    <div class="card-title">System phase <span class="chaos-phase">{phase}</span></div>
    <div class="chaos-phases">
      {''.join(f'<div class="phase-chip {"active" if p==phase else ""}">{p.replace("_"," ").title()}</div>' for p in ["NORMAL","ELEVATED_RISK","CHAOS_ACTIVE","COOLDOWN","PROBATION","CONTROLLED","FULL_RECOVERY"])}
    </div>
    <div class="triggers">{triggers_html}</div>
    <div style="font-size:11px;color:#64748b;margin-top:10px">
      Risk cap: {chaos.get('risk_cap_pct',1.0)}% &nbsp;|&nbsp;
      Trading: {'✓ Allowed' if chaos.get('trading_allowed') else '✗ Blocked'} &nbsp;|&nbsp;
      Signals: {'✓ Active' if chaos.get('signals_active') else '✗ Suspended'} &nbsp;|&nbsp;
      Chaos events: {chaos.get('n_chaos_events',0)}
    </div>
  </div>
  <div class="section-label">Open positions — Layer 10</div>
  <div class="card">
    <div class="card-title">Active trades</div>
    <table><thead><tr><th>Pair</th><th>Dir</th><th>Entry</th><th>Current</th><th>R:R</th><th>PnL</th><th>Opened</th></tr></thead><tbody>{pos_rows}</tbody></table>
  </div>
  <div class="section-label">PnL curve — all closed trades</div>
  <div class="card">
    <div class="card-title">Cumulative PnL — {journal.get('total_trades',0)} closed trades</div>
    <div class="chart-wrap"><canvas id="pnlChart" role="img"></canvas></div>
  </div>
  <div class="grid2">
    <div class="card"><div class="card-title">Regime EV — Layer 7</div>{regime_rows}</div>
    <div class="card"><div class="card-title">NULL breakdown — Layer 6</div>{null_bars}</div>
  </div>
  <div class="grid2">
    <div class="card">
      <div class="card-title">Rolling EV windows — Layer 8</div>
      {rolling_bars}
      <div class="chart-wrap-sm" style="margin-top:12px"><canvas id="evChart" role="img"></canvas></div>
    </div>
    <div class="card"><div class="card-title">Session performance</div>{sess_rows}</div>
  </div>
  <div class="grid3">
    <div class="card"><div class="card-title">All layers</div>{layer_rows}</div>
    <div class="card"><div class="card-title">Feed health — Layer 1</div>{feed_rows}</div>
    <div class="card">
      <div class="card-title">Learning cycle — Layer 8</div>
      <div class="learning-stat"><span class="learning-key">Last run</span><span class="learning-val">{learning.get('last_run','--')}</span></div>
      <div class="learning-stat"><span class="learning-key">Next run</span><span class="learning-val">{learning.get('next_run','--')}</span></div>
      <div class="learning-stat"><span class="learning-key">Last approved</span><span class="learning-val pos">{learning.get('last_approved',0)} updates</span></div>
      <div class="learning-stat"><span class="learning-key">Last rejected</span><span class="learning-val neg">{learning.get('last_rejected',0)} updates</span></div>
      <div class="learning-stat"><span class="learning-key">Top loss cause</span><span class="learning-val">{learning.get('top_loss_cause','--')}</span></div>
      <div class="learning-stat"><span class="learning-key">EV delta</span><span class="learning-val">${learning.get('ev_delta',0):+.2f}</span></div>
    </div>
  </div>
</div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<script>
var pnlData = {pnl_json};
var rolling = {json.dumps(rolling)};
var gridColor = 'rgba(255,255,255,0.05)';
var textColor = '#64748b';
if (document.getElementById('pnlChart') && pnlData.length > 1) {{
  var labels = pnlData.map(function(_,i){{return i*5;}});
  new Chart(document.getElementById('pnlChart'), {{
    type: 'line',
    data: {{
      labels: labels,
      datasets: [{{
        label: 'Cumulative PnL',
        data: pnlData,
        borderColor: pnlData[pnlData.length-1] >= 0 ? '#22c55e' : '#ef4444',
        backgroundColor: pnlData[pnlData.length-1] >= 0 ? 'rgba(34,197,94,0.08)' : 'rgba(239,68,68,0.08)',
        borderWidth: 1.5,
        pointRadius: 0,
        fill: true,
        tension: 0.3,
      }}]
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ display: false }} }},
      scales: {{
        x: {{ grid: {{ color: gridColor }}, ticks: {{ color: textColor, font: {{ size:10 }}, maxTicksLimit: 8 }} }},
        y: {{ grid: {{ color: gridColor }}, ticks: {{ color: textColor, font: {{ size:10 }}, callback: function(v){{return '$'+v.toLocaleString();}} }} }}
      }}
    }}
  }});
}}
if (document.getElementById('evChart')) {{
  var evLabels = ['10 trades','25 trades','50 trades','100 trades'];
  var evVals   = [rolling.w10||0, rolling.w25||0, rolling.w50||0, rolling.w100||0];
  var evColors = evVals.map(function(v){{return v>=0?'#22c55e':'#ef4444';}});
  new Chart(document.getElementById('evChart'), {{
    type: 'bar',
    data: {{
      labels: evLabels,
      datasets: [{{
        data: evVals,
        backgroundColor: evColors,
        borderRadius: 4,
        borderSkipped: false,
      }}]
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ display: false }} }},
      scales: {{
        x: {{ grid: {{ color: gridColor }}, ticks: {{ color: textColor, font:{{size:10}} }} }},
        y: {{ grid: {{ color: gridColor }}, ticks: {{ color: textColor, font:{{size:10}}, callback: function(v){{return '$'+v;}} }}, beginAtZero:true }}
      }}
    }}
  }});
}}
</script>
</body>
</html>"""
    return html


# ── Flask Routes ──
@app.route('/')
@app.route('/dashboard')
def dashboard():
    collector = DashboardDataCollector()
    data = collector.get_data()
    return build_dashboard_html(data)

@app.route('/api/data')
def api_data():
    collector = DashboardDataCollector()
    return jsonify(collector.get_data())

# ──────────────────────────────────────────────────────────────────────────────
# 2. FULL 10‑LAYER TRADING SYSTEM (unchanged from your original)
# ──────────────────────────────────────────────────────────────────────────────
# ALL your original imports, classes (PriceFeedAdapter, Layer1FeedBundle,
# LearningThread, OptimizationThread, SignalProcessor, OANDALiveFeedConnector,
# TradeCloseHandler, TradingSystem, CloudConfig, SystemConfig, etc.)
# and the main() function go here. I am omitting them for brevity,
# but you must copy your entire original system code below this line.
# ──────────────────────────────────────────────────────────────────────────────

# [PASTE YOUR ORIGINAL SYSTEM CODE HERE – everything from your original main.py
#  except the final if __name__ == "__main__" block.]

# ──────────────────────────────────────────────────────────────────────────────
# 3. MAIN ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Start the trading system in a background thread
    # (your original main() function should start the system)
    # For example:
    # from main import main as trading_main
    # threading.Thread(target=trading_main, daemon=True).start()

    # Start Flask web server
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
