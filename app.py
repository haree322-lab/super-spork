import os
import sys
import time
import json
import psutil
import subprocess
import requests
from flask import Flask, jsonify
from apscheduler.schedulers.background import BackgroundScheduler

IS_WINDOWS = os.name == "nt"

app = Flask(__name__)

prev_net = {"bytes_sent": 0, "bytes_recv": 0, "time": time.time()}


def get_network_speed():
    global prev_net
    net = psutil.net_io_counters()
    now = time.time()
    elapsed = now - prev_net["time"]
    if elapsed <= 0:
        elapsed = 1

    upload_speed = (net.bytes_sent - prev_net["bytes_sent"]) / elapsed
    download_speed = (net.bytes_recv - prev_net["bytes_recv"]) / elapsed

    prev_net = {
        "bytes_sent": net.bytes_sent,
        "bytes_recv": net.bytes_recv,
        "time": now,
    }

    return upload_speed, download_speed


def format_bytes(b):
    for unit in ["B/s", "KB/s", "MB/s", "GB/s"]:
        if b < 1024:
            return f"{b:.2f} {unit}"
        b /= 1024
    return f"{b:.2f} TB/s"


def is_bot_alive():
    for proc in psutil.process_iter(["pid", "cmdline"]):
        try:
            cmdline = proc.info.get("cmdline") or []
            if any("main.py" in arg for arg in cmdline):
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return False


def keep_alive():
    try:
        repl_slug = os.environ.get("REPL_SLUG")
        repl_owner = os.environ.get("REPL_OWNER")
        if repl_slug and repl_owner:
            url = f"https://{repl_slug}.{repl_owner}.repl.co"
            requests.get(url)
    except Exception:
        pass


scheduler = BackgroundScheduler()
if not IS_WINDOWS:
    scheduler.add_job(func=keep_alive, trigger="interval", minutes=5)
scheduler.start()


@app.route("/api/status")
def api_status():
    upload_speed, download_speed = get_network_speed()
    bot_alive = is_bot_alive()

    net = psutil.net_io_counters()
    cpu_percent = psutil.cpu_percent(interval=0.1)
    memory = psutil.virtual_memory()
    uptime_seconds = time.time() - psutil.boot_time()

    days = int(uptime_seconds // 86400)
    hours = int((uptime_seconds % 86400) // 3600)
    minutes = int((uptime_seconds % 3600) // 60)

    return jsonify(
        {
            "bot_alive": bot_alive,
            "upload_speed": format_bytes(upload_speed),
            "download_speed": format_bytes(download_speed),
            "upload_speed_raw": upload_speed,
            "download_speed_raw": download_speed,
            "total_sent": format_bytes(net.bytes_sent).replace("/s", ""),
            "total_recv": format_bytes(net.bytes_recv).replace("/s", ""),
            "cpu_percent": cpu_percent,
            "memory_percent": memory.percent,
            "memory_used": f"{memory.used / (1024**3):.2f} GB",
            "memory_total": f"{memory.total / (1024**3):.2f} GB",
            "uptime": f"{days}d {hours}h {minutes}m",
        }
    )


@app.route("/")
def home():
    return """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Thanos Bot Dashboard</title>
    <link rel="icon" type="image/x-icon" href="https://tinypic.host/images/2025/02/07/DeWatermark.ai_1738952933236-1.png">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }

        body {
            background: #0a0a0f;
            color: #e0e0e0;
            font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
            min-height: 100vh;
            overflow-x: hidden;
        }

        .bg-grid {
            position: fixed;
            top: 0; left: 0; right: 0; bottom: 0;
            background-image:
                linear-gradient(rgba(255,204,0,0.03) 1px, transparent 1px),
                linear-gradient(90deg, rgba(255,204,0,0.03) 1px, transparent 1px);
            background-size: 40px 40px;
            pointer-events: none;
            z-index: 0;
        }

        .container {
            max-width: 900px;
            margin: 0 auto;
            padding: 40px 20px;
            position: relative;
            z-index: 1;
        }

        .header {
            text-align: center;
            margin-bottom: 40px;
        }

        .header h1 {
            font-size: 2.5rem;
            font-weight: 700;
            background: linear-gradient(135deg, #ffcc00, #ff8800);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 6px;
        }

        .header p {
            color: #888;
            font-size: 0.95rem;
        }

        .status-banner {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 12px;
            padding: 16px 28px;
            border-radius: 12px;
            margin-bottom: 32px;
            font-size: 1.1rem;
            font-weight: 600;
            transition: all 0.5s ease;
        }

        .status-banner.online {
            background: rgba(0, 200, 83, 0.1);
            border: 1px solid rgba(0, 200, 83, 0.3);
            color: #00c853;
        }

        .status-banner.offline {
            background: rgba(255, 23, 68, 0.1);
            border: 1px solid rgba(255, 23, 68, 0.3);
            color: #ff1744;
        }

        .pulse-dot {
            width: 12px;
            height: 12px;
            border-radius: 50%;
            position: relative;
        }

        .pulse-dot.online { background: #00c853; }
        .pulse-dot.offline { background: #ff1744; }

        .pulse-dot::before {
            content: '';
            position: absolute;
            top: -4px; left: -4px;
            width: 20px;
            height: 20px;
            border-radius: 50%;
            animation: pulse 2s infinite;
        }

        .pulse-dot.online::before { background: rgba(0, 200, 83, 0.4); }
        .pulse-dot.offline::before { background: rgba(255, 23, 68, 0.4); }

        @keyframes pulse {
            0% { transform: scale(1); opacity: 1; }
            100% { transform: scale(2); opacity: 0; }
        }

        .cards {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
            gap: 20px;
            margin-bottom: 32px;
        }

        .card {
            background: rgba(255,255,255,0.03);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 16px;
            padding: 24px;
            transition: transform 0.2s, border-color 0.3s;
        }

        .card:hover {
            transform: translateY(-2px);
            border-color: rgba(255,204,0,0.3);
        }

        .card-label {
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 1.5px;
            color: #888;
            margin-bottom: 12px;
        }

        .card-value {
            font-size: 1.8rem;
            font-weight: 700;
            font-variant-numeric: tabular-nums;
        }

        .card-sub {
            font-size: 0.85rem;
            color: #666;
            margin-top: 6px;
        }

        .speed-bar-track {
            width: 100%;
            height: 6px;
            background: rgba(255,255,255,0.05);
            border-radius: 3px;
            margin-top: 14px;
            overflow: hidden;
        }

        .speed-bar {
            height: 100%;
            border-radius: 3px;
            transition: width 0.8s ease;
            min-width: 2%;
        }

        .speed-bar.download { background: linear-gradient(90deg, #00c853, #69f0ae); }
        .speed-bar.upload { background: linear-gradient(90deg, #ffcc00, #ff8800); }

        .sys-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 16px;
        }

        .sys-card {
            background: rgba(255,255,255,0.02);
            border: 1px solid rgba(255,255,255,0.06);
            border-radius: 12px;
            padding: 18px;
            text-align: center;
        }

        .sys-label {
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 1px;
            color: #666;
            margin-bottom: 8px;
        }

        .sys-value {
            font-size: 1.2rem;
            font-weight: 600;
            color: #ccc;
        }

        .footer {
            text-align: center;
            margin-top: 48px;
            padding: 20px;
            color: #444;
            font-size: 0.8rem;
            border-top: 1px solid rgba(255,255,255,0.05);
        }

        .footer img {
            border-radius: 50%;
            vertical-align: middle;
            margin: 0 4px;
        }

        .update-indicator {
            display: inline-block;
            width: 6px;
            height: 6px;
            background: #ffcc00;
            border-radius: 50%;
            margin-left: 8px;
            animation: blink 1s infinite;
        }

        @keyframes blink {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.3; }
        }

        @media (max-width: 600px) {
            .header h1 { font-size: 1.8rem; }
            .card-value { font-size: 1.4rem; }
            .container { padding: 24px 16px; }
        }
    </style>
</head>
<body>
    <div class="bg-grid"></div>
    <div class="container">
        <div class="header">
            <h1>Thanos Bot</h1>
            <p>Real-time System Dashboard <span class="update-indicator"></span></p>
        </div>

        <div id="statusBanner" class="status-banner offline">
            <div id="pulseDot" class="pulse-dot offline"></div>
            <span id="statusText">Checking...</span>
        </div>

        <div class="cards">
            <div class="card">
                <div class="card-label">Download Speed</div>
                <div class="card-value" id="downloadSpeed">-- B/s</div>
                <div class="card-sub">Total: <span id="totalRecv">--</span></div>
                <div class="speed-bar-track">
                    <div class="speed-bar download" id="downloadBar" style="width: 2%"></div>
                </div>
            </div>
            <div class="card">
                <div class="card-label">Upload Speed</div>
                <div class="card-value" id="uploadSpeed">-- B/s</div>
                <div class="card-sub">Total: <span id="totalSent">--</span></div>
                <div class="speed-bar-track">
                    <div class="speed-bar upload" id="uploadBar" style="width: 2%"></div>
                </div>
            </div>
        </div>

        <div class="sys-grid">
            <div class="sys-card">
                <div class="sys-label">CPU Usage</div>
                <div class="sys-value" id="cpuUsage">--%</div>
            </div>
            <div class="sys-card">
                <div class="sys-label">Memory</div>
                <div class="sys-value" id="memUsage">--</div>
            </div>
            <div class="sys-card">
                <div class="sys-label">RAM Used</div>
                <div class="sys-value" id="memDetail">--</div>
            </div>
            <div class="sys-card">
                <div class="sys-label">Uptime</div>
                <div class="sys-value" id="uptime">--</div>
            </div>
        </div>

        <div class="footer">
            <img src="https://files.catbox.moe/ui41xs.jpg" width="28" height="28">
            Powered by Thanos
            <img src="https://files.catbox.moe/ui41xs.jpg" width="28" height="28">
            <p style="margin-top: 8px;">&copy; 2025 Thanos Bot</p>
        </div>
    </div>

    <script>
        const MAX_SPEED = 10 * 1024 * 1024;

        async function fetchStatus() {
            try {
                const res = await fetch('/api/status');
                const data = await res.json();

                document.getElementById('downloadSpeed').textContent = data.download_speed;
                document.getElementById('uploadSpeed').textContent = data.upload_speed;
                document.getElementById('totalRecv').textContent = data.total_recv;
                document.getElementById('totalSent').textContent = data.total_sent;

                const dlPercent = Math.min((data.download_speed_raw / MAX_SPEED) * 100, 100);
                const ulPercent = Math.min((data.upload_speed_raw / MAX_SPEED) * 100, 100);
                document.getElementById('downloadBar').style.width = Math.max(dlPercent, 2) + '%';
                document.getElementById('uploadBar').style.width = Math.max(ulPercent, 2) + '%';

                document.getElementById('cpuUsage').textContent = data.cpu_percent + '%';
                document.getElementById('memUsage').textContent = data.memory_percent + '%';
                document.getElementById('memDetail').textContent = data.memory_used + ' / ' + data.memory_total;
                document.getElementById('uptime').textContent = data.uptime;

                const banner = document.getElementById('statusBanner');
                const dot = document.getElementById('pulseDot');
                const text = document.getElementById('statusText');

                if (data.bot_alive) {
                    banner.className = 'status-banner online';
                    dot.className = 'pulse-dot online';
                    text.textContent = 'Bot is Live';
                } else {
                    banner.className = 'status-banner offline';
                    dot.className = 'pulse-dot offline';
                    text.textContent = 'Bot is Offline';
                }
            } catch (e) {
                document.getElementById('statusText').textContent = 'Connection Error';
            }
        }

        fetchStatus();
        setInterval(fetchStatus, 2000);
    </script>
</body>
</html>"""


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=5000)
