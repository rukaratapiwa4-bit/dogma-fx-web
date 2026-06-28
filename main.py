"""
═══════════════════════════════════════════════════════════════════════════════
THE DOGMA FX SYSTEM — VERSION 6.0
FILE: main.py (FULL SYSTEM + EMBEDDED REAL‑TIME DASHBOARD)
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
import random
import statistics
import math
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path
from datetime import datetime, timezone
from flask import Flask, jsonify

# ─── FLASK APP ────────────────────────────────────────────────────────────────
app = Flask(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# 1. DASHBOARD CODE (DataCollector + build_html) – full from dashboard.py
# ──────────────────────────────────────────────────────────────────────────────

class DataCollector:
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

    # ── Account ──────────────────────────────────────────────────────────────
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

    # ── Positions ─────────────────────────────────────────────────────────────
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

    # ── Prices ───────────────────────────────────────────────────────────────
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

    # ── Journal stats ────────────────────────────────────────────────────────
    def _get_journal_stats(self):
        if not os.path.exists(self.db_path):
            return {
                "total_trades":0,"total_nulls":0,"total_wins":0,
                "win_rate_pct":0,"avg_pnl":0,"sharpe":0,
                "max_dd_pct":0,"null_rate_pct":0
            }
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
                "total_trades":0,"total_nulls":0,"total_wins":0,
                "win_rate_pct":0,"avg_pnl":0,"sharpe":0,
                "max_dd_pct":0,"null_rate_pct":0
            }

    # ── Chaos ────────────────────────────────────────────────────────────────
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

    # ── Regime EV ────────────────────────────────────────────────────────────
    def _get_regime_ev(self):
        if not os.path.exists(self.db_path):
            return []
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
            return []

    # ── Session performance ─────────────────────────────────────────────────
    def _get_session_perf(self):
        if not os.path.exists(self.db_path):
            return []
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
            return []

    # ── NULL distribution ────────────────────────────────────────────────────
    def _get_null_dist(self):
        if not os.path.exists(self.db_path):
            return []
        try:
            conn = sqlite3.connect(self.db_path, timeout=5)
            rows = conn.execute(
                "SELECT primary_null, COUNT(*) as cnt FROM nulls GROUP BY primary_null ORDER BY cnt DESC"
            ).fetchall()
            total = sum(r[1] for r in rows)
            conn.close()
            return [
                {"type": r[0], "count": r[1], "pct": round(r[1]/max(1,total)*100,1)}
                for r in rows
            ]
        except Exception:
            return []

    # ── Rolling EV ───────────────────────────────────────────────────────────
    def _get_rolling_ev(self):
        if not os.path.exists(self.db_path):
            return {}
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
            return {}

    # ── PnL curve ────────────────────────────────────────────────────────────
    def _get_pnl_curve(self):
        if not os.path.exists(self.db_path):
            return []
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
            return []

    # ── Layer status ─────────────────────────────────────────────────────────
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

    # ── Feed health ──────────────────────────────────────────────────────────
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

    # ── Learning ─────────────────────────────────────────────────────────────
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


# ── HTML BUILDER (full HTML from dashboard.py) ──────────────────────────────
def build_html(data: dict) -> str:
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

    # Build positions table
    pos_rows = ""
    for p in pos:
        pnl     = p.get("pnl", 0)
        pnl_cls = "pos" if pnl >= 0 else "neg"
        pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
        dir_cls = "buy" if p["direction"] == "BUY" else "sell"
        current = prices.get(p["pair"], {}).get("mid", p.get("current", p["entry"]))
        pip_size= 0.01 if "JPY" in p["pair"] else 0.0001
        rr_raw  = (current - p["entry"]) / (pip_size * 8.5)
        if p["direction"] == "SELL": rr_raw = -rr_raw
        rr_str  = f"{rr_raw:.2f}R"
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

    # Regime EV
    regime_rows = ""
    for r in regime_ev:
        ev      = r.get("ev", 0)
        ev_cls  = "pos" if ev >= 0 else "neg"
        ev_str  = f"+${ev:.2f}" if ev >= 0 else f"-${abs(ev):.2f}"
        val_icon= "✓" if r.get("validated") else "⚠"
        regime_rows += f"""
        <div class="ev-row">
          <span class="ev-regime">{r['regime']} <small style="color:#888">{val_icon}</small></span>
          <span style="color:#888;font-size:12px">{r['n']} trades</span>
          <span style="color:#888;font-size:12px">{r['win_rate']}% WR</span>
          <span class="ev-val {ev_cls}">{ev_str}</span>
        </div>"""

    # Session
    sess_rows = ""
    for s in sess:
        ev      = s.get("ev", 0)
        ev_cls  = "pos" if ev >= 0 else "neg"
        ev_str  = f"+${ev:.2f}" if ev >= 0 else f"-${abs(ev):.2f}"
        sess_rows += f"""
        <div class="ev-row">
          <span class="ev-regime">{s['session']}</span>
          <span style="color:#888;font-size:12px">{s['win_rate']}% WR</span>
          <span class="ev-val {ev_cls}">{ev_str}</span>
        </div>"""

    # NULL bars
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

    # Layer status
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

    # Feed health
    feed_rows = ""
    for f in feeds:
        dot_color = {"ok":"#16a34a","warn":"#d97706","mock":"#2563eb","error":"#dc2626"}.get(f["status"],"#888")
        feed_rows += f"""
        <div class="feed-row">
          <div class="feed-dot" style="background:{dot_color}"></div>
          <span class="feed-name-txt">{f['name']}</span>
          <span class="feed-detail">{f['detail']}</span>
        </div>"""

    # Triggers
    triggers_html = ""
    for name, t in chaos.get("triggers", {}).items():
        ok    = t.get("ok", True)
        color = "#16a34a" if ok else "#dc2626"
        bg    = "#dcfce7" if ok else "#fee2e2"
        icon  = "✓" if ok else "✗"
        label = name.replace("_"," ").title()
        val   = t.get("value","")
        triggers_html += f'<span class="trigger-tag" style="background:{bg};color:{color}">{icon} {label}: {val}</span>'

    # Price cards
    price_cards = ""
    for pair, p in prices.items():
        price_cards += f"""
        <div class="price-card">
          <div class="price-pair">{pair}</div>
          <div class="price-mid">{p['mid']}</div>
          <div class="price-spread">spread {p['spread']} pips</div>
        </div>"""

    # Rolling EV bars
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
        w   = min(100, abs(val) / 100 * 100)
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

    pnl_json   = json.dumps(pnl_curve)
    nav        = acc.get("nav", 10000)
    balance    = acc.get("balance", 10000)
    pnl_total  = round(nav - balance, 2)
    pnl_pct    = round(pnl_total / balance * 100, 2) if balance else 0
    pnl_sign   = "+" if pnl_total >= 0 else ""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="5">
<title>🐕 THE DOGMA FX SYSTEM – Dashboard</title>
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


# ─── FLASK ROUTES ────────────────────────────────────────────────────────────
@app.route('/')
@app.route('/dashboard')
def dashboard():
    try:
        collector = DataCollector()
        data = collector.get_data()
        return build_html(data)
    except Exception as e:
        return f"<pre>{traceback.format_exc()}</pre>", 500

@app.route('/api/data')
def api_data():
    try:
        collector = DataCollector()
        return jsonify(collector.get_data())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ──────────────────────────────────────────────────────────────────────────────
# 2. YOUR ORIGINAL TRADING SYSTEM CODE (EXACTLY AS YOU PROVIDED)
# ──────────────────────────────────────────────────────────────────────────────

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
# ═══════════════════════════════════════════════════════════════════════════════

class PriceFeedAdapter:
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
            "bid"               : state.get("latest_bid"),
            "ask"               : state.get("latest_ask"),
            "timestamp_utc"     : state.get("latest_timestamp"),
            "active_session"    : state.get("active_session", "OVERLAP"),
        }

    def get_state(self, instrument: str) -> Optional[dict]:
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
    account_balance         : float = 10_000.0
    paper_trading           : bool  = True
    pairs: list = field(default_factory=lambda: [
        "EUR/USD", "GBP/USD", "USD/JPY",
        "AUD/USD", "USD/CAD", "NZD/USD",
    ])
    oanda_api_key           : Optional[str] = None
    oanda_account_id        : Optional[str] = None
    oanda_practice          : bool  = True
    anthropic_api_key       : Optional[str] = None
    tick_interval_ms        : int   = 500
    multi_asset_refresh_s   : int   = 60
    news_refresh_min        : int   = 5
    cot_refresh_h           : float = 6.0
    options_refresh_min     : int   = 30
    learning_cycle_hours    : float = 6.0
    optimization_interval_s : int   = 30
    journal_db_path         : str   = "journal.db"
    log_level               : str   = "INFO"

    def load_from_env(self):
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
    def __init__(self, cfg: SystemConfig):
        self._cfg = cfg
        logger.info("Initialising Layer 1 feeds...")

        _raw_feed = FeedManager()
        for pair in cfg.pairs:
            _raw_feed.add_instrument(pair, DataSource.LIVE_SOURCE)

        self.price_feed    = PriceFeedAdapter(_raw_feed)
        self.price_feed_l1 = self.price_feed

        self.calendar = EconomicCalendarManager(
            api_key        = cfg.oanda_api_key,
            oanda_practice = cfg.oanda_practice,
        )
        self.news = NewsSentimentManager(
            claude_api_key = cfg.anthropic_api_key,
            oanda_api_key  = cfg.oanda_api_key,
            oanda_account  = cfg.oanda_account_id,
            oanda_practice = cfg.oanda_practice,
        )
        self.cot = COTManager(refresh_hours=cfg.cot_refresh_h)
        self.multi_asset = MultiAssetManager(
            refresh_seconds = cfg.multi_asset_refresh_s,
        )
        self.options = OptionsFlowManager(
            oanda_api_key  = cfg.oanda_api_key,
            oanda_practice = cfg.oanda_practice,
        )
        logger.info("Layer 1 feeds initialised ✅")

    def start(self):
        logger.info("Starting Layer 1 feed threads...")
        self.calendar.start()
        self.news.start()
        self.cot.start()
        self.multi_asset.start()
        self.options.start()
        logger.info("All Layer 1 feed threads running ✅")

    def stop(self):
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
        self.price_feed.on_tick(pair, ts, bid, ask, bid_vol, ask_vol, latency_ms)

    def get_snapshot(self, pair: str) -> dict:
        return {
            "price"      : self.price_feed_l1.get_layer1_package(pair) or {},
            "calendar"   : self.calendar.get_layer1_package(pair) or {},
            "news"       : self.news.get_layer1_package(pair) or {},
            "cot"        : self.cot.get_layer1_package() or {},
            "multi_asset": self.multi_asset.get_layer1_package() or {},
            "options"    : self.options.get_layer1_package() or {},
        }

    def get_feed_health(self) -> dict:
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
        trades = self._journal._storage.query_trades()
        nulls  = self._journal._storage.query_nulls()
        if not trades:
            logger.info("LearningThread: no trades yet — skipping cycle")
            return
        validation_results = self._validation.validate_all_regimes(trades)
        approved_regimes   = [r for r, v in validation_results.items() if v.get("gate_passed")]
        logger.info(
            f"Layer 7 complete: {len(approved_regimes)}/{len(validation_results)} "
            f"regimes passed gate"
        )
        result = self._learning.run_cycle(trades, nulls, regime="ALL")
        logger.info(
            f"Layer 8 complete: {result.proposals_approved} approved | "
            f"{result.proposals_rejected} rejected | "
            f"EV delta=${result.ev_delta or 0:.2f}"
        )
        self._journal.save_performance_snapshot()
        logger.info("═══ Learning cycle complete ═══")


class OptimizationThread:
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
        action = decision.action
        if action == "FULL_EXIT":
            logger.info(
                f"[Layer10→Layer5] FULL_EXIT {decision.trade_id} | "
                f"{decision.exit_reason}"
            )
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
        self._cycle += 1
        chaos_state = self._chaos.get_state()

        if not self._chaos.is_signals_active():
            logger.debug(
                f"Cycle {self._cycle}: signals suspended "
                f"(chaos phase={chaos_state})"
            )
            return

        try:
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

        feature_packages = {
            pair: fp for pair, fp in feature_packages.items()
            if fp is not None
        }
        if not feature_packages:
            return

        self._update_chaos_monitor(feature_packages)

        try:
            decisions = self._l3.decide_all_pairs(feature_packages)
        except Exception as e:
            logger.error(f"Layer 3 error: {e}")
            return

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

        logger.info(
            f"{pair}: SIGNAL {decision.direction} | "
            f"prob={decision.trade_probability:.2f} | "
            f"conf={decision.confidence_score:.0f} | "
            f"regime={decision.regime_tag}"
        )

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
        try:
            max_spread_ratio = 1.0
            multi_null_count = 0
            for pair, fp in feature_packages.items():
                if fp:
                    spread_r = getattr(fp, "spread_ratio", 1.0) or 1.0
                    max_spread_ratio = max(max_spread_ratio, spread_r)

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
            avg_spread = spread_pips
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

        self.feeds      = Layer1FeedBundle(cfg)
        self.feature_eng= FeatureEngine()
        self.decision   = DecisionEngine()
        self.risk_ctrl  = RiskControlManager(initial_balance = cfg.account_balance)
        self.portfolio  = PortfolioExposureManager(account_balance = cfg.account_balance)
        self.execution  = ExecutionEngine(paper_trading=cfg.paper_trading, account_balance=cfg.account_balance)
        self.journal    = JournalManager(db_path=cfg.journal_db_path)
        self.validation = ValidationEngine()
        self.learning   = LearningLoop(validation_engine=self.validation)
        self.chaos      = ChaosModeEngine()
        self.optimizer  = OptimizationEngine()

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

        self.live_feed  = OANDALiveFeedConnector(self.feeds, cfg)

        self.close_handler = TradeCloseHandler(
            journal      = self.journal,
            decision_eng = self.decision,
            chaos        = self.chaos,
        )

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

        if hasattr(self.execution, "set_close_callback"):
            self.execution.set_close_callback(self.close_handler.on_trade_closed)

        logger.info("All layers initialised ✅")

    def start(self):
        signal.signal(signal.SIGINT,  self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        logger.info("Starting all subsystems...")
        self.feeds.start()
        self.live_feed.start()
        self.learning_thread.start()
        self.opt_thread.start()

        self._running = True
        logger.info("═" * 60)
        logger.info("SYSTEM RUNNING — press Ctrl+C to stop")
        logger.info("═" * 60)

        self._main_loop()

    def shutdown(self, reason: str = "MANUAL"):
        if self._running:
            logger.info(f"Shutdown requested: {reason}")
            self._shutdown.set()

    def _main_loop(self):
        interval_s = self._cfg.tick_interval_ms / 1000.0
        cycle      = 0

        while not self._shutdown.is_set():
            try:
                cycle += 1
                t_start = time.time()
                self.signal_proc.run_cycle(self._cfg.pairs)
                elapsed = time.time() - t_start
                sleep_t = max(0.0, interval_s - elapsed)
                if sleep_t > 0:
                    self._shutdown.wait(timeout=sleep_t)
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Main loop error (cycle {cycle}): {e}")
                logger.debug(traceback.format_exc())
                time.sleep(1.0)

        self._teardown()

    def _teardown(self):
        logger.info("Shutting down...")
        self._running = False
        self.learning_thread.stop()
        self.opt_thread.stop()
        self.live_feed.stop()
        self.feeds.stop()
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
        self.shutdown(reason=f"OS_SIGNAL_{signum}")

    def get_status(self) -> dict:
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
# SECTION 8 — ENTRY POINT (main function)
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    cfg = SystemConfig().load_from_env()
    logging.getLogger().setLevel(getattr(logging, cfg.log_level, logging.INFO))

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

    system = TradingSystem(cfg)
    system.start()


# ──────────────────────────────────────────────────────────────────────────────
# 3. ENTRY POINT – STARTS TRADING SYSTEM + FLASK
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # ── Start the trading system in a background thread ──
    def start_trading():
        try:
            main()
        except Exception as e:
            logging.error(f"Trading system crashed: {e}")
            logging.debug(traceback.format_exc())

    trading_thread = threading.Thread(target=start_trading, daemon=True)
    trading_thread.start()
    logging.info("Trading system running in background.")

    # ── Start Flask web server ──
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
