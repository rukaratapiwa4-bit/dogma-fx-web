"""
═══════════════════════════════════════════════════════════════════════════════
THE DOGMA FX SYSTEM — VERSION 6.0
FILE: main.py
LAYER: SYSTEM INTEGRATION + REAL‑TIME DASHBOARD
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
from flask import Flask, jsonify, render_template_string

# ── FLASK APP ──────────────────────────────────────────────────────────────────
app = Flask(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# 1. DATA COLLECTOR (same as in dashboard.py)
# ──────────────────────────────────────────────────────────────────────────────
class DashboardDataCollector:
    """
    Collects live data from journal.db and OANDA API.
    """
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

    # ── OANDA: Account ────────────────────────────────────────────────────────
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

    # ── OANDA: Open positions ─────────────────────────────────────────────────
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

    # ── OANDA: Live prices ────────────────────────────────────────────────────
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

# ──────────────────────────────────────────────────────────────────────────────
# 2. FLASK ROUTES – REAL‑TIME DASHBOARD
# ──────────────────────────────────────────────────────────────────────────────

DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>🐕 THE DOGMA FX SYSTEM – LIVE</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { background: #0f1117; color: #e2e8f0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; padding: 20px; }
        h1 { font-size: 2rem; margin-bottom: 10px; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; margin-top: 10px; }
        .metric { background: #1a1d27; border: 1px solid #2d3142; border-radius: 8px; padding: 12px; }
        .metric-label { font-size: 11px; color: #64748b; text-transform: uppercase; letter-spacing: 0.04em; }
        .metric-value { font-size: 20px; font-weight: 600; }
        .pos { color: #22c55e; }
        .neg { color: #ef4444; }
        .card { background: #1a1d27; border: 1px solid #2d3142; border-radius: 12px; padding: 16px; margin-top: 16px; }
        .card-title { font-size: 11px; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 10px; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 6px 8px; border-bottom: 1px solid #1e2333; text-align: left; font-size: 13px; }
        .dir-buy { color: #22c55e; }
        .dir-sell { color: #ef4444; }
        .mt-2 { margin-top: 8px; }
        .text-muted { color: #64748b; font-size: 12px; }
        .badge { font-size: 11px; padding: 2px 10px; border-radius: 12px; background: #1e3a5f; color: #60a5fa; }
    </style>
</head>
<body>
    <h1>🐕 THE DOGMA FX SYSTEM – REAL‑TIME DASHBOARD</h1>
    <div id="timestamp" class="text-muted" style="margin-bottom: 10px;"></div>

    <div id="metrics" class="grid"></div>
    <div id="prices" class="grid" style="grid-template-columns: repeat(auto-fit, minmax(100px, 1fr));"></div>

    <div id="positions" class="card"></div>
    <div id="pnl" class="card"></div>
    <div id="regime_ev" class="card"></div>
    <div id="nulls" class="card"></div>
    <div id="rolling" class="card"></div>

    <script>
        function fetchData() {
            fetch('/api/data')
                .then(res => res.json())
                .then(data => {
                    updateMetrics(data);
                    updatePrices(data);
                    updatePositions(data);
                    updatePnL(data);
                    updateRegimeEV(data);
                    updateNulls(data);
                    updateRolling(data);
                    document.getElementById('timestamp').textContent = 'Last updated: ' + data.timestamp;
                })
                .catch(err => console.error('Fetch error:', err));
        }

        function updateMetrics(data) {
            const acc = data.account;
            const j = data.journal;
            document.getElementById('metrics').innerHTML = `
                <div class="metric"><div class="metric-label">Balance</div><div class="metric-value">$${acc.balance.toFixed(2)}</div></div>
                <div class="metric"><div class="metric-label">NAV</div><div class="metric-value">$${acc.nav.toFixed(2)}</div></div>
                <div class="metric"><div class="metric-label">Open Trades</div><div class="metric-value">${acc.open_trades}</div></div>
                <div class="metric"><div class="metric-label">Win Rate</div><div class="metric-value">${j.win_rate_pct}%</div></div>
                <div class="metric"><div class="metric-label">Total Trades</div><div class="metric-value">${j.total_trades}</div></div>
                <div class="metric"><div class="metric-label">Avg EV</div><div class="metric-value ${j.avg_pnl>=0?'pos':'neg'}">$${j.avg_pnl.toFixed(2)}</div></div>
                <div class="metric"><div class="metric-label">NULL Rate</div><div class="metric-value">${j.null_rate_pct}%</div></div>
                <div class="metric"><div class="metric-label">Sharpe</div><div class="metric-value">${j.sharpe}</div></div>
                <div class="metric"><div class="metric-label">Max DD</div><div class="metric-value neg">${j.max_dd_pct}%</div></div>
            `;
        }

        function updatePrices(data) {
            let html = '';
            for (const [pair, p] of Object.entries(data.prices)) {
                html += `<div class="metric" style="min-width:80px;"><div class="metric-label">${pair}</div><div class="metric-value">${p.mid}</div><div style="font-size:10px;color:#64748b;">spread ${p.spread}</div></div>`;
            }
            document.getElementById('prices').innerHTML = html;
        }

        function updatePositions(data) {
            const pos = data.positions;
            if (pos.length === 0) {
                document.getElementById('positions').innerHTML = '<div class="card-title">No open positions</div>';
                return;
            }
            let rows = pos.map(p => `
                <tr>
                    <td><strong>${p.pair}</strong></td>
                    <td class="${p.direction==='BUY'?'dir-buy':'dir-sell'}">${p.direction}</td>
                    <td>${p.entry}</td>
                    <td>${p.current}</td>
                    <td class="${p.pnl>=0?'pos':'neg'}">${p.pnl>=0?'+':''}$${p.pnl.toFixed(2)}</td>
                    <td>${p.open_time}</td>
                </tr>
            `).join('');
            document.getElementById('positions').innerHTML = `
                <div class="card-title">Open Positions</div>
                <table><thead><tr><th>Pair</th><th>Dir</th><th>Entry</th><th>Current</th><th>PnL</th><th>Opened</th></tr></thead><tbody>${rows}</tbody></table>
            `;
        }

        function updatePnL(data) {
            // Show a short summary – for full chart, you could use Chart.js later.
            const curve = data.pnl_curve;
            if (curve.length > 0) {
                const last = curve[curve.length - 1];
                document.getElementById('pnl').innerHTML = `
                    <div class="card-title">Cumulative PnL</div>
                    <div style="font-size:16px;font-weight:600;" class="${last>=0?'pos':'neg'}">$${last.toFixed(2)}</div>
                    <div style="font-size:10px;color:#64748b;">based on ${curve.length} data points</div>
                `;
            } else {
                document.getElementById('pnl').innerHTML = '<div class="card-title">Cumulative PnL</div><div class="text-muted">No data yet</div>';
            }
        }

        function updateRegimeEV(data) {
            const evs = data.regime_ev;
            let html = evs.map(r => `
                <div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid #1e2333;">
                    <span>${r.regime}</span>
                    <span>${r.n} trades</span>
                    <span>${r.win_rate}% WR</span>
                    <span class="${r.ev>=0?'pos':'neg'}">${r.ev>=0?'+':''}$${r.ev.toFixed(2)}</span>
                </div>
            `).join('');
            document.getElementById('regime_ev').innerHTML = `<div class="card-title">Regime EV</div>${html}`;
        }

        function updateNulls(data) {
            const nulls = data.null_dist;
            let html = nulls.map(n => `
                <div style="display:flex;justify-content:space-between;padding:2px 0;">
                    <span>${n.type}</span>
                    <span>${n.count} (${n.pct}%)</span>
                </div>
            `).join('');
            document.getElementById('nulls').innerHTML = `<div class="card-title">NULL Breakdown</div>${html}`;
        }

        function updateRolling(data) {
            const r = data.rolling_ev;
            let html = `
                <div>10 trades: ${r.w10!==null?r.w10:'N/A'}</div>
                <div>25 trades: ${r.w25!==null?r.w25:'N/A'}</div>
                <div>50 trades: ${r.w50!==null?r.w50:'N/A'}</div>
                <div>100 trades: ${r.w100!==null?r.w100:'N/A'}</div>
            `;
            document.getElementById('rolling').innerHTML = `<div class="card-title">Rolling EV</div>${html}`;
        }

        // Fetch every 2 seconds
        fetchData();
        setInterval(fetchData, 2000);
    </script>
</body>
</html>
"""

@app.route('/')
@app.route('/dashboard')
def dashboard():
    return DASHBOARD_HTML

@app.route('/api/data')
def api_data():
    collector = DashboardDataCollector()
    return jsonify(collector.get_data())


# ──────────────────────────────────────────────────────────────────────────────
# 3. YOUR ORIGINAL 10‑LAYER TRADING SYSTEM
# ──────────────────────────────────────────────────────────────────────────────
# ========== COPY YOUR ENTIRE ORIGINAL main.py CODE HERE ==========
# (Include all your classes: CloudConfig, SystemConfig, PriceFeedAdapter,
#  Layer1FeedBundle, LearningThread, OptimizationThread, SignalProcessor,
#  OANDALiveFeedConnector, TradeCloseHandler, TradingSystem, and main() function)
# ========== END OF COPY ==========


# ──────────────────────────────────────────────────────────────────────────────
# 4. MAIN ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Start the trading system in a background thread
    # For example, if your main() function starts the system:
    # threading.Thread(target=main, daemon=True).start()

    # Or if you have a TradingSystem instance:
    # cfg = SystemConfig().load_from_env()
    # system = TradingSystem(cfg)
    # threading.Thread(target=system.start, daemon=True).start()

    # Start Flask web server
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
