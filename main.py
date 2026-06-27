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

# ── DATA COLLECTOR (copied from dashboard.py, no changes) ──────────────────
class DashboardDataCollector:
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

    # ---- All private methods from dashboard.py's DataCollector ----
    # (I’m omitting them here for brevity – but they are exactly the same as in your dashboard.py)
    # You must copy them from your dashboard.py file or include the full class.
    # For space, I’ll assume you have them. If not, I’ll provide the full code in the next message.

# ── REAL‑TIME DASHBOARD HTML (with JavaScript polling) ──────────────────────
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>🐕 THE DOGMA FX SYSTEM – LIVE</title>
    <style>
        /* Your CSS – copy from dashboard.py or use the same styles */
        /* (I’m including a minimal style, but you can paste the full CSS) */
        body { background: #0f1117; color: #e2e8f0; font-family: sans-serif; padding: 20px; }
        .metric { background: #1a1d27; border: 1px solid #2d3142; border-radius: 8px; padding: 12px; display: inline-block; margin: 4px; min-width: 120px; }
        .metric-label { font-size: 11px; color: #64748b; }
        .metric-value { font-size: 20px; font-weight: 600; }
        .pos { color: #22c55e; }
        .neg { color: #ef4444; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px; }
        .card { background: #1a1d27; border: 1px solid #2d3142; border-radius: 12px; padding: 16px; margin-top: 16px; }
        .card-title { font-size: 11px; color: #64748b; text-transform: uppercase; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 6px 8px; border-bottom: 1px solid #1e2333; text-align: left; }
        .dir-buy { color: #22c55e; }
        .dir-sell { color: #ef4444; }
    </style>
</head>
<body>
    <h1>🐕 THE DOGMA FX SYSTEM – LIVE DASHBOARD</h1>
    <div id="timestamp" style="color:#64748b;font-size:12px;"></div>
    <div id="metrics" class="grid"></div>
    <div id="prices" class="grid" style="grid-template-columns: repeat(auto-fit, minmax(100px,1fr));"></div>
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
            // For simplicity, just show a summary, or you can use Chart.js to draw a curve.
            // We'll show a small text list.
            const curve = data.pnl_curve;
            if (curve.length > 0) {
                document.getElementById('pnl').innerHTML = `
                    <div class="card-title">Cumulative PnL (last ${curve.length} points)</div>
                    <div style="font-size:12px;color:#94a3b8;">${curve.join(' → ')}</div>
                `;
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

# ── HERE COMES YOUR ORIGINAL TRADING SYSTEM ──
# (Copy all your classes and functions from your original main.py below)
# ──────────────────────────────────────────────────────────────────────────────

# [PASTE YOUR ENTIRE SYSTEM CODE HERE – everything except the final
#  if __name__ == "__main__" block, including imports, CloudConfig,
#  SystemConfig, PriceFeedAdapter, Layer1FeedBundle, LearningThread,
#  OptimizationThread, SignalProcessor, OANDALiveFeedConnector,
#  TradeCloseHandler, TradingSystem, main() function, etc.]

# ── MAIN ENTRY POINT ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Start the trading system in a background thread
    # (Call your main() function or TradingSystem start)
    # For example:
    # trading_thread = threading.Thread(target=main, daemon=True)
    # trading_thread.start()

    # Start Flask
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
