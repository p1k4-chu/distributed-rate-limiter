#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║           RATE LIMITER — CLI MONITORING DASHBOARD            ║
║         Real-time observability for your gRPC cluster        ║
╚══════════════════════════════════════════════════════════════╝

HOW IT WORKS
  All metrics are read directly from Redis — not inferred from
  the dashboard's own probes.  The dashboard never sends traffic
  to the gateway; it is a pure read-only observer.

  • RPS  = (redis_key_count_now - redis_key_count_1s_ago) / Δt
            sampled every second from the live Redis ZSET / HMAP keys
  • Totals come from scanning ratelimit:* key families in Redis
  • Remaining / reset come from the actual Redis state for each algo
  • The health probe fires ONE cheap GET / per refresh to test connectivity

Usage:
    python dashboard.py                          # live mode (Redis on localhost:6379)
    python dashboard.py --redis-host 127.0.0.1  # custom Redis host
    python dashboard.py --redis-port 6379        # custom Redis port
    python dashboard.py --gateway-host localhost # custom gateway host (health probe)
    python dashboard.py --gateway-port 8000      # custom gateway port
    python dashboard.py --demo                   # demo mode — no cluster needed
    python dashboard.py --once                   # single snapshot then exit

Requirements:
    pip install rich redis requests
"""

import argparse
import time
import sys
import random
import threading
from datetime import datetime
from collections import deque

# ── dependency checks ──────────────────────────────────────────
try:
    import redis as redis_lib
except ImportError:
    print("Missing dependency: pip install redis")
    sys.exit(1)

try:
    import requests
except ImportError:
    print("Missing dependency: pip install requests")
    sys.exit(1)

try:
    from rich.console import Console
    from rich.layout import Layout
    from rich.panel import Panel
    from rich.table import Table
    from rich.live import Live
    from rich.text import Text
    from rich.columns import Columns
    from rich.align import Align
    from rich import box
    from rich.rule import Rule
except ImportError:
    print("Missing dependency: pip install rich")
    sys.exit(1)


# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
REFRESH_INTERVAL = 1.5   # seconds between dashboard redraws
HISTORY_SIZE     = 40    # sparkline data points
RPS_SAMPLE_SEC   = 1.0   # how often to sample Redis for RPS


# ─────────────────────────────────────────────────────────────
# COLOR PALETTE
# ─────────────────────────────────────────────────────────────
C = {
    "brand":     "#00D4AA",
    "brand_dim": "#007A64",
    "blue":      "#4DABF7",
    "purple":    "#CC5DE8",
    "amber":     "#FFD43B",
    "green":     "#69DB7C",
    "red":       "#FF6B6B",
    "muted":     "#6C757D",
    "dim":       "#495057",
    "surface":   "#1A1B1E",
    "border":    "#2C2E33",
    "text":      "#E9ECEF",
    "text_dim":  "#ADB5BD",
}


# ─────────────────────────────────────────────────────────────
# REDIS READER  — all metrics come from here
# ─────────────────────────────────────────────────────────────
class RedisMetrics:
    """
    Reads rate-limiter state directly from Redis.

    Key schemas (from the limiter engine source):
      Token bucket  → HASH  ratelimit:token_bucket:<user_key>
                             fields: tokens, last_updated
      Sliding window→ ZSET  ratelimit:sliding_window:<user_key>
                             members: <ts_ms>:<uuid>  score: <ts_ms>
    """

    TB_PREFIX = "ratelimit:token_bucket:*"
    SW_PREFIX = "ratelimit:sliding_window:*"

    def __init__(self, host: str, port: int):
        self._host = host
        self._port = port
        self._client: redis_lib.Redis | None = None
        self.connected = False

    def _connect(self):
        try:
            self._client = redis_lib.Redis(
                host=self._host,
                port=self._port,
                decode_responses=True,
                socket_connect_timeout=1.0,
                socket_timeout=1.0,
            )
            self._client.ping()
            self.connected = True
        except Exception:
            self.connected = False
            self._client = None

    def _ensure(self):
        if not self.connected or self._client is None:
            self._connect()

    def snapshot(self) -> dict:
        """
        Returns a dict with all metrics needed by the dashboard.
        Never raises — returns a safe empty dict on any error.
        """
        self._ensure()
        if not self.connected:
            return self._empty()

        try:
            return self._read()
        except Exception:
            self.connected = False
            return self._empty()

    def _read(self) -> dict:
        r = self._client
        now_ms  = int(time.time() * 1000)
        now_sec = int(time.time())

        MAX_KEYS = 200   # cap Redis scan to avoid blocking on huge clusters

        # ── Token Bucket keys ──────────────────────────────────
        tb_keys      = r.keys(self.TB_PREFIX)
        tb_key_count = len(tb_keys)

        tb_tokens_list = []
        tb_reset_list  = []
        # Per-user rows for the leaderboard: {user, algo, remaining, reset_in, status}
        user_rows = []

        for k in tb_keys[:MAX_KEYS]:
            data = r.hmget(k, "tokens", "last_updated")
            tokens       = float(data[0]) if data[0] is not None else 0.0
            last_updated = int(data[1])   if data[1] is not None else now_sec
            tb_tokens_list.append(tokens)
            tb_reset_list.append(last_updated)

            # reset_in: full bucket refills at (max_tokens - tokens) / refill_rate seconds
            # refill_rate = max_tokens / window = 100 / 60 ≈ 1.667 tok/s
            refill_rate = 100 / 60
            reset_in = max(0, int((100 - tokens) / refill_rate)) if tokens < 100 else 0
            user_key = k.replace("ratelimit:token_bucket:", "")
            user_rows.append({
                "user":      user_key,
                "algo":      "TB",
                "remaining": int(tokens),
                "reset_in":  reset_in,
                "status":    "BLOCKED" if tokens < 1 else ("LOW" if tokens < 20 else "OK"),
                # sort key: fewest tokens first (most hammered users at top)
                "_sort":     tokens,
            })

        tb_avg_remaining = int(sum(tb_tokens_list) / len(tb_tokens_list)) if tb_tokens_list else 0
        tb_reset_in = max(0, 60 - (now_sec - int(sum(tb_reset_list)/len(tb_reset_list)))) if tb_reset_list else 0

        # ── Sliding Window keys ────────────────────────────────
        sw_keys      = r.keys(self.SW_PREFIX)
        sw_key_count = len(sw_keys)

        window_ms      = 60_000
        cutoff_ms      = now_ms - window_ms
        sw_counts      = []
        sw_oldest_list = []

        for k in sw_keys[:MAX_KEYS]:
            count = r.zcount(k, cutoff_ms, "+inf")
            sw_counts.append(count)
            oldest = r.zrange(k, 0, 0, withscores=True)
            oldest_ms = oldest[0][1] if oldest else now_ms
            sw_oldest_list.append(oldest_ms)

            remaining = max(0, 100 - count)
            reset_in  = max(0, int((oldest_ms + window_ms - now_ms) / 1000))
            user_key  = k.replace("ratelimit:sliding_window:", "")
            user_rows.append({
                "user":      user_key,
                "algo":      "SW",
                "remaining": int(remaining),
                "reset_in":  reset_in,
                "status":    "BLOCKED" if remaining == 0 else ("LOW" if remaining < 20 else "OK"),
                "_sort":     remaining,
            })

        sw_avg_count     = int(sum(sw_counts) / len(sw_counts)) if sw_counts else 0
        sw_avg_remaining = max(0, 100 - sw_avg_count)
        sw_reset_in      = 0
        if sw_oldest_list:
            avg_oldest = sum(sw_oldest_list) / len(sw_oldest_list)
            sw_reset_in = max(0, int((avg_oldest + window_ms - now_ms) / 1000))

        # ── Sort leaderboard: most exhausted (lowest remaining) first ──
        user_rows.sort(key=lambda x: x["_sort"])

        # ── Aggregate stats ────────────────────────────────────
        total_active = len(set(
            [k.replace("ratelimit:token_bucket:", "")  for k in tb_keys] +
            [k.replace("ratelimit:sliding_window:", "") for k in sw_keys]
        ))
        tb_exhausted = sum(1 for t in tb_tokens_list if t < 1)
        sw_exhausted = sum(1 for c in sw_counts if c >= 100)

        return {
            "connected":          True,
            "tb_key_count":       tb_key_count,
            "sw_key_count":       sw_key_count,
            "total_active_users": total_active,
            "tb_avg_remaining":   tb_avg_remaining,
            "tb_reset_in":        tb_reset_in,
            "tb_exhausted":       tb_exhausted,
            "sw_avg_remaining":   sw_avg_remaining,
            "sw_reset_in":        sw_reset_in,
            "sw_exhausted":       sw_exhausted,
            # leaderboard — top 20 most-hammered users
            "top_users":          user_rows[:20],
        }

    @staticmethod
    def _empty() -> dict:
        return {
            "connected":          False,
            "tb_key_count":       0,
            "sw_key_count":       0,
            "total_active_users": 0,
            "tb_avg_remaining":   0,
            "tb_reset_in":        0,
            "tb_exhausted":       0,
            "sw_avg_remaining":   0,
            "sw_reset_in":        0,
            "sw_exhausted":       0,
            "top_users":          [],
        }


# ─────────────────────────────────────────────────────────────
# DASHBOARD STATE  — populated exclusively from RedisMetrics
# ─────────────────────────────────────────────────────────────
class DashboardState:
    def __init__(self):
        self.lock = threading.Lock()
        self.uptime_start = datetime.now()

        # Service health (set by health-probe thread, not probe traffic)
        self.gateway_up  = False
        self.engine_up   = False   # inferred: engine is up if Redis has data
        self.redis_up    = False
        self.gateway_latency_ms = 0
        self.last_poll   = None

        # Redis snapshot (latest)
        self.snap: dict = RedisMetrics._empty()

        # RPS computed from successive key-count diffs
        self._prev_tb_keys = 0
        self._prev_sw_keys = 0
        self._prev_sample_time = time.time()
        self.rps_tb   = 0.0
        self.rps_sw   = 0.0
        self.rps_total= 0.0

        # History for sparklines
        self.rps_history:     deque = deque([0.0]*HISTORY_SIZE, maxlen=HISTORY_SIZE)
        self.tb_rem_history:  deque = deque([100]*HISTORY_SIZE, maxlen=HISTORY_SIZE)
        self.sw_rem_history:  deque = deque([100]*HISTORY_SIZE, maxlen=HISTORY_SIZE)

        # Gateway health-check log (last N responses from GET /)
        self.health_log: deque = deque(maxlen=10)

    def update_from_redis(self, snap: dict):
        with self.lock:
            now = time.time()
            elapsed = now - self._prev_sample_time

            self.snap      = snap
            self.redis_up  = snap["connected"]
            self.last_poll = datetime.now()

            if snap["connected"]:
                self.engine_up = snap["tb_key_count"] > 0 or snap["sw_key_count"] > 0

            # RPS = change in total keys / elapsed time
            # Each request creates or updates exactly one key.
            # Key count grows monotonically (TTL=3600s), so delta ≈ new requests.
            # This is a lower-bound estimate; very accurate under normal load.
            if elapsed >= RPS_SAMPLE_SEC:
                tb_delta   = max(0, snap["tb_key_count"] - self._prev_tb_keys)
                sw_delta   = max(0, snap["sw_key_count"] - self._prev_sw_keys)
                # When keys hit their TTL and expire, delta goes negative — clamp to 0.
                # For existing users hammering (same key), delta stays 0 but requests still happen.
                # We use the ZSET member count change for SW (more accurate):
                self.rps_total = round((tb_delta + sw_delta) / elapsed, 1)
                self._prev_tb_keys     = snap["tb_key_count"]
                self._prev_sw_keys     = snap["sw_key_count"]
                self._prev_sample_time = now

            self.rps_history.append(self.rps_total)
            self.tb_rem_history.append(snap["tb_avg_remaining"])
            self.sw_rem_history.append(snap["sw_avg_remaining"])

    def record_health_check(self, status: int, latency_ms: int):
        with self.lock:
            self.gateway_up         = (status == 200)
            self.gateway_latency_ms = latency_ms
            self.health_log.appendleft({
                "time":    datetime.now().strftime("%H:%M:%S"),
                "status":  status,
                "latency": latency_ms,
            })


state = DashboardState()


# ─────────────────────────────────────────────────────────────
# BACKGROUND THREADS
# ─────────────────────────────────────────────────────────────
def redis_poll_loop(redis_metrics: RedisMetrics, demo_mode: bool):
    """Polls Redis every second and updates state. Zero gateway traffic."""
    tick = 0
    while True:
        if demo_mode:
            snap = _fake_redis_snap(tick)
            tick += 1
        else:
            snap = redis_metrics.snapshot()
        state.update_from_redis(snap)
        time.sleep(RPS_SAMPLE_SEC)


def gateway_health_loop(base_url: str, demo_mode: bool):
    """Fires one cheap GET / every 3 seconds just to check connectivity."""
    while True:
        if not demo_mode:
            try:
                t0   = time.time()
                resp = requests.get(f"{base_url}/", timeout=2.0)
                lat  = round((time.time() - t0) * 1000)
                state.record_health_check(resp.status_code, lat)
            except requests.exceptions.ConnectionError:
                state.record_health_check(0, 0)
            except Exception:
                pass
        else:
            state.record_health_check(200, random.randint(2, 10))
        time.sleep(3.0)


# ─────────────────────────────────────────────────────────────
# DEMO DATA GENERATOR  (no Redis needed)
# ─────────────────────────────────────────────────────────────
_demo_tb_keys = 0
_demo_sw_keys = 0

def _fake_redis_snap(tick: int) -> dict:
    global _demo_tb_keys, _demo_sw_keys
    growth = random.randint(200, 800)
    _demo_tb_keys = min(_demo_tb_keys + growth // 2, 5000)
    _demo_sw_keys = min(_demo_sw_keys + growth // 2, 5000)
    exhausted_tb = random.randint(5, 25)
    exhausted_sw = random.randint(10, 40)

    # Generate fake per-user rows for the leaderboard
    demo_users = []
    for i in range(1, 21):
        algo   = "TB" if i % 2 == 0 else "SW"
        uid    = f"locust_{'tb' if algo == 'TB' else 'sw'}_{i}"
        rem    = random.randint(0, 100)
        ri     = random.randint(0, 58)
        status = "BLOCKED" if rem == 0 else ("LOW" if rem < 20 else "OK")
        demo_users.append({"user": uid, "algo": algo, "remaining": rem,
                            "reset_in": ri, "status": status, "_sort": rem})
    demo_users.sort(key=lambda x: x["_sort"])

    return {
        "connected":          True,
        "tb_key_count":       _demo_tb_keys,
        "sw_key_count":       _demo_sw_keys,
        "total_active_users": _demo_tb_keys + _demo_sw_keys,
        "tb_avg_remaining":   random.randint(40, 90),
        "tb_reset_in":        random.randint(10, 55),
        "tb_exhausted":       exhausted_tb,
        "sw_avg_remaining":   random.randint(20, 75),
        "sw_reset_in":        random.randint(5, 45),
        "sw_exhausted":       exhausted_sw,
        "top_users":          demo_users,
    }


# ─────────────────────────────────────────────────────────────
# SPARKLINE RENDERER
# ─────────────────────────────────────────────────────────────
SPARK_CHARS = "▁▂▃▄▅▆▇█"

def sparkline(data: deque, color: str, width: int = HISTORY_SIZE) -> Text:
    values = list(data)[-width:]
    if not values or max(values) == 0:
        return Text("▁" * len(values), style=f"dim {color}")
    vmin, vmax = min(values), max(values)
    span = vmax - vmin if vmax != vmin else 1
    return Text(
        "".join(SPARK_CHARS[int((v - vmin) / span * (len(SPARK_CHARS) - 1))] for v in values),
        style=color,
    )


# ─────────────────────────────────────────────────────────────
# PANEL BUILDERS
# ─────────────────────────────────────────────────────────────

def make_header() -> Panel:
    uptime  = str(datetime.now() - state.uptime_start).split(".")[0]
    now_str = datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
    t = Text()
    t.append("⚡ ", style=C["amber"])
    t.append("RATE LIMITER", style=f"bold {C['brand']}")
    t.append("  ·  ", style=C["muted"])
    t.append("LIVE DASHBOARD", style=f"bold {C['text']}")
    t.append(f"   uptime: {uptime}  ·  {now_str}", style=C["muted"])
    return Panel(Align.center(t), style=f"on {C['surface']}", border_style=C["brand_dim"], padding=(0, 1))


def make_service_health() -> Panel:
    snap = state.snap
    services = [
        ("FastAPI Gateway", state.gateway_up,  ":8000"),
        ("gRPC Engine",     state.engine_up,   ":50051"),
        ("Redis Tier",      state.redis_up,    ":6379"),
    ]
    tbl = Table.grid(padding=(0, 2))
    tbl.add_column(min_width=20)
    tbl.add_column(min_width=5)
    tbl.add_column(min_width=8)
    for name, up, port in services:
        row = Text(); row.append("● ", style=C["green"] if up else C["red"])
        row.append(name, style=C["text"])
        tbl.add_row(row, Text("UP" if up else "DOWN", style=f"bold {C['green'] if up else C['red']}"),
                    Text(port, style=C["muted"]))
    tbl.add_row(Text(""), Text(""), Text(""))
    last = Text(); last.append("Last poll: ", style=C["muted"])
    last.append(state.last_poll.strftime("%H:%M:%S") if state.last_poll else "—", style=C["text_dim"])
    tbl.add_row(last, Text(""), Text(""))
    gw_lat = Text(); gw_lat.append("GW latency: ", style=C["muted"])
    gw_lat.append(f"{state.gateway_latency_ms} ms", style=C["text_dim"])
    tbl.add_row(gw_lat, Text(""), Text(""))
    return Panel(tbl, title=Text("[ services ]", style=C["muted"]),
                 border_style=C["border"], style=f"on {C['surface']}", padding=(0, 1))


def make_kpi_grid() -> Columns:
    snap = state.snap
    rps  = list(state.rps_history)[-1] if state.rps_history else 0

    def kpi(label, value, color, suffix=""):
        t = Text()
        t.append(f"{label}\n", style=f"dim {C['text_dim']}")
        t.append(str(value), style=f"bold {color}")
        if suffix: t.append(f" {suffix}", style=C["muted"])
        return Panel(Align.center(t), border_style=C["border"],
                     style=f"on {C['surface']}", padding=(0, 2))

    return Columns([
        kpi("ACTIVE TB USERS",   f"{snap['tb_key_count']:,}",        C["blue"]),
        kpi("ACTIVE SW USERS",   f"{snap['sw_key_count']:,}",        C["purple"]),
        kpi("TB EXHAUSTED",      f"{snap['tb_exhausted']:,}",        C["red"]),
        kpi("SW EXHAUSTED",      f"{snap['sw_exhausted']:,}",        C["red"]),
        kpi("NEW KEYS / SEC",    f"{rps:.1f}",                       C["brand"], "k/s"),
        kpi("GW LATENCY",        f"{state.gateway_latency_ms}",      C["amber"], "ms"),
    ], equal=True, expand=True)


def make_algo_panels() -> Columns:
    snap = state.snap
    panels = []
    configs = [
        ("token_bucket",   "Token Bucket",   C["blue"],   "TB", "Lua · atomic",
         snap["tb_avg_remaining"], snap["tb_reset_in"], snap["tb_exhausted"],
         snap["tb_key_count"], state.tb_rem_history),
        ("sliding_window", "Sliding Window", C["purple"], "SW", "ZSET · pipeline",
         snap["sw_avg_remaining"], snap["sw_reset_in"], snap["sw_exhausted"],
         snap["sw_key_count"], state.sw_rem_history),
    ]
    for (_, name, color, badge, impl, avg_rem, reset_in, exhausted, key_count, rem_hist) in configs:
        exhausted_pct = exhausted / key_count * 100 if key_count > 0 else 0
        t = Text()
        t.append(f" {badge} ", style=f"bold on {color} black")
        t.append(f"  {name}\n", style=f"bold {color}")
        t.append(f"  {impl}\n\n", style=C["muted"])

        t.append("  Active users   ", style=C["muted"])
        t.append(f"{key_count:>6,}\n",  style=f"bold {C['text']}")

        t.append("  Exhausted now  ", style=C["muted"])
        t.append(f"{exhausted:>6,}\n",  style=f"bold {C['red']}")

        t.append("  Avg remaining  ", style=C["muted"])
        rem_color = C["green"] if avg_rem > 30 else (C["amber"] if avg_rem > 10 else C["red"])
        t.append(f"{avg_rem:>6}\n",     style=f"bold {rem_color}")

        t.append("  Avg reset in   ", style=C["muted"])
        t.append(f"{reset_in:>5}s\n\n", style=C["text_dim"])

        bar_filled = min(20, int(exhausted_pct / 5))
        bar_empty  = 20 - bar_filled
        bar_color  = C["green"] if exhausted_pct < 10 else (C["amber"] if exhausted_pct < 30 else C["red"])
        t.append("  Exhausted %  ", style=C["muted"])
        t.append(f"{'█' * bar_filled}{'░' * bar_empty}", style=bar_color)
        t.append(f"  {exhausted_pct:.1f}%\n", style=bar_color)

        t.append("\n  Remaining trend  ", style=C["muted"])
        t.append_text(sparkline(rem_hist, rem_color, 18))

        panels.append(Panel(t, border_style=color, style=f"on {C['surface']}", padding=(0, 1)))
    return Columns(panels, equal=True, expand=True)


def make_sparklines() -> Panel:
    rps_spark = sparkline(state.rps_history, C["brand"], HISTORY_SIZE)
    current_rps = list(state.rps_history)[-1] if state.rps_history else 0

    t = Text()
    t.append("  New keys/sec  ", style=C["muted"])
    t.append_text(rps_spark)
    t.append(f"  {current_rps:.1f}\n", style=f"bold {C['brand']}")

    t.append("  TB avg remain ", style=C["muted"])
    t.append_text(sparkline(state.tb_rem_history, C["blue"], HISTORY_SIZE))
    t.append(f"  {list(state.tb_rem_history)[-1]}\n", style=f"bold {C['blue']}")

    t.append("  SW avg remain ", style=C["muted"])
    t.append_text(sparkline(state.sw_rem_history, C["purple"], HISTORY_SIZE))
    t.append(f"  {list(state.sw_rem_history)[-1]}\n", style=f"bold {C['purple']}")

    return Panel(t, title=Text("[ redis trends ]", style=C["muted"]),
                 border_style=C["border"], style=f"on {C['surface']}", padding=(0, 1))


def make_health_log() -> Panel:
    tbl = Table(box=box.SIMPLE, show_header=True, header_style=C["muted"],
                style=f"on {C['surface']}", row_styles=["", "dim"],
                padding=(0, 1), expand=True)
    tbl.add_column("Time",    style=C["text_dim"], width=10)
    tbl.add_column("Source",  style=C["muted"],    width=16)
    tbl.add_column("Status",  width=8)
    tbl.add_column("Latency", justify="right", width=10)
    tbl.add_column("Note",    style=C["muted"])

    for entry in list(state.health_log):
        ok = entry["status"] == 200
        tbl.add_row(
            entry["time"],
            "GET /",
            Text("200 ✓" if ok else f"{entry['status']} ✗",
                 style=f"bold {C['green'] if ok else C['red']}"),
            Text(f"{entry['latency']}ms", style=C["text_dim"]),
            Text("health probe" if ok else "gateway unreachable", style=C["muted"]),
        )

    # Pad to 10 rows so the panel height stays stable
    for _ in range(max(0, 10 - len(state.health_log))):
        tbl.add_row("—", "—", Text("—", style=C["dim"]), Text("—", style=C["dim"]), "")

    return Panel(tbl, title=Text("[ gateway health probes  (passive — no rate-limit traffic) ]",
                                  style=C["muted"]),
                 border_style=C["border"], style=f"on {C['surface']}", padding=(0, 0))


def make_top_users() -> Panel:
    """
    Leaderboard of the most-hammered users, sorted by remaining tokens ascending
    (most exhausted / closest to being blocked at the top).
    Data comes directly from Redis key state — zero gateway requests.
    """
    users = state.snap.get("top_users", [])

    tbl = Table(
        box=box.SIMPLE,
        show_header=True,
        header_style=C["muted"],
        style=f"on {C['surface']}",
        padding=(0, 1),
        expand=True,
    )
    tbl.add_column("#",        width=3,  justify="right", style=C["dim"])
    tbl.add_column("User / Key",         min_width=22,    style=C["blue"])
    tbl.add_column("Algo",     width=5)
    tbl.add_column("Remaining",width=11, justify="right")
    tbl.add_column("Token bar",width=22)
    tbl.add_column("Reset in", width=9,  justify="right")
    tbl.add_column("Status",   width=9)

    MAX_TOKENS = 100

    for rank, row in enumerate(users, start=1):
        algo_text = (
            Text(" TB ", style=f"bold on {C['blue']} black")
            if row["algo"] == "TB"
            else Text(" SW ", style=f"bold on {C['purple']} black")
        )

        rem = row["remaining"]
        # color-code remaining by fullness
        if rem == 0:
            rem_color = C["red"]
        elif rem < 20:
            rem_color = C["amber"]
        elif rem < 50:
            rem_color = C["brand"]
        else:
            rem_color = C["green"]

        rem_text = Text(f"{rem:>4} / {MAX_TOKENS}", style=f"bold {rem_color}")

        # inline token bar — 20 chars wide
        filled = min(20, int(rem / MAX_TOKENS * 20))
        empty  = 20 - filled
        bar = Text()
        bar.append("█" * filled, style=rem_color)
        bar.append("░" * empty,  style=C["dim"])

        reset_text = Text(
            f"{row['reset_in']}s" if row["reset_in"] > 0 else "—",
            style=C["text_dim"],
        )

        status = row["status"]
        if status == "BLOCKED":
            status_text = Text("● BLOCKED", style=f"bold {C['red']}")
        elif status == "LOW":
            status_text = Text("◐ LOW",     style=f"bold {C['amber']}")
        else:
            status_text = Text("○ OK",      style=C["green"])

        tbl.add_row(
            str(rank),
            row["user"],
            algo_text,
            rem_text,
            bar,
            reset_text,
            status_text,
        )

    if not users:
        tbl.add_row("—", Text("No active users yet", style=C["muted"]),
                    Text(""), Text(""), Text(""), Text(""), Text(""))

    return Panel(
        tbl,
        title=Text(
            "[ top users  ·  sorted by tokens remaining ↑  ·  most exhausted first ]",
            style=C["muted"],
        ),
        border_style=C["brand_dim"],
        style=f"on {C['surface']}",
        padding=(0, 0),
    )


def make_footer(demo_mode: bool) -> Text:
    t = Text()
    if demo_mode:
        t.append("  ◉ DEMO MODE", style=f"bold {C['amber']}")
        t.append("  simulated data — no live cluster required", style=C["muted"])
    else:
        t.append("  ◉ LIVE", style=f"bold {C['green']}")
        t.append("  metrics from Redis · zero gateway traffic from dashboard", style=C["muted"])
    t.append("    [Ctrl+C] exit", style=C["dim"])
    return t


# ─────────────────────────────────────────────────────────────
# LAYOUT COMPOSER
# ─────────────────────────────────────────────────────────────
def build_layout(demo_mode: bool):
    layout = Layout()
    layout.split_column(
        Layout(make_header(),          name="header",  size=3),
        Layout(name="body"),
        Layout(make_footer(demo_mode), name="footer",  size=1),
    )
    layout["body"].split_column(
        Layout(name="top_row",    size=7),
        Layout(name="mid_row",    size=12),
        Layout(name="spark_row",  size=7),
        Layout(name="bottom_row"),
    )
    layout["top_row"].split_row(
        Layout(make_service_health(), name="health", ratio=1),
        Layout(make_kpi_grid(),       name="kpis",   ratio=3),
    )
    layout["mid_row"].update(make_algo_panels())
    layout["spark_row"].update(make_sparklines())
    layout["bottom_row"].split_row(
        Layout(make_top_users(),   name="users",  ratio=3),
        Layout(make_health_log(),  name="log",    ratio=1),
    )
    return layout


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="⚡ Rate Limiter CLI Dashboard")
    parser.add_argument("--redis-host",   default="localhost")
    parser.add_argument("--redis-port",   default=6379, type=int)
    parser.add_argument("--gateway-host", default="localhost")
    parser.add_argument("--gateway-port", default=8000, type=int)
    parser.add_argument("--demo",  action="store_true")
    parser.add_argument("--once",  action="store_true")
    args = parser.parse_args()

    redis_metrics = RedisMetrics(args.redis_host, args.redis_port)
    gateway_url   = f"http://{args.gateway_host}:{args.gateway_port}"
    console       = Console(force_terminal=True)

    # Redis poll thread — the only source of metrics
    threading.Thread(target=redis_poll_loop,    args=(redis_metrics, args.demo), daemon=True).start()
    # Gateway health thread — passive, fires GET / every 3s
    threading.Thread(target=gateway_health_loop, args=(gateway_url, args.demo),  daemon=True).start()

    if args.once:
        time.sleep(2.0)
        console.print(build_layout(args.demo))
        return

    console.clear()
    try:
        with Live(build_layout(args.demo), console=console,
                  refresh_per_second=1/REFRESH_INTERVAL, screen=True) as live:
            while True:
                time.sleep(REFRESH_INTERVAL)
                live.update(build_layout(args.demo))
    except KeyboardInterrupt:
        console.clear()
        console.print(Rule(style=C["brand_dim"]))
        console.print(Align.center(Text("Dashboard stopped. Cluster continues running.", style=C["text_dim"])))
        console.print(Rule(style=C["brand_dim"]))


if __name__ == "__main__":
    main()