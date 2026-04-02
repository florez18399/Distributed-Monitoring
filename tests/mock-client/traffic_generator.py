import asyncio
import aiohttp
import random
import time
import os
import glob
import json
import re
import logging
import uuid
from enum import Enum

# --- Logging ---
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("traffic-gen")

# --- Configuration ---
GATEWAY_URL = os.getenv("GATEWAY_URL", "http://main-api-gateway:81")
CONF_DIR = os.getenv("CONF_DIR", "/confs")
ZONE_MANIFEST_DIR = os.getenv("ZONE_MANIFEST_DIR", "/zones")
REQUEST_INTERVAL = float(os.getenv("REQUEST_INTERVAL", "0.5"))
MODES = [m.strip() for m in os.getenv("MODES", "personas").split(",")]
PERSONA_POOL_SIZE = int(os.getenv("PERSONA_POOL_SIZE", "10"))
TIME_ACCELERATION = float(os.getenv("TIME_ACCELERATION", "300"))

# Parse persona mix: "casual:0.5,power:0.2,window:0.3"
_mix_raw = os.getenv("PERSONA_MIX", "casual:0.5,power:0.2,window:0.3")
PERSONA_MIX = {}
for part in _mix_raw.split(","):
    k, v = part.strip().split(":")
    PERSONA_MIX[k.strip()] = float(v.strip())

# Parse JSON env vars
try:
    TEMPORAL_EVENTS = json.loads(os.getenv("TEMPORAL_EVENTS", "[]"))
except json.JSONDecodeError:
    TEMPORAL_EVENTS = []

try:
    ANOMALY_PROFILES = json.loads(os.getenv("ANOMALY_PROFILES", "[]"))
except json.JSONDecodeError:
    ANOMALY_PROFILES = []


# ============================================================
# Endpoint Discovery
# ============================================================

class EndpointDiscovery:
    """Discovers zones and apps from nginx confs and zone manifests."""

    def __init__(self, conf_dir, manifest_dir):
        self.conf_dir = conf_dir
        self.manifest_dir = manifest_dir
        self.zone_map = {}  # { "zone1": ["catalog-api", "orders-api", ...] }

    def discover(self):
        zones = self._discover_zones_from_confs()
        if not zones:
            return {}

        for zone_id in zones:
            apps = self._discover_apps_from_manifest(zone_id)
            if apps:
                self.zone_map[zone_id] = apps
            else:
                # Fallback: parse nginx conf for location blocks
                apps = self._discover_apps_from_conf(zone_id)
                if apps:
                    self.zone_map[zone_id] = apps

        return self.zone_map

    def _discover_zones_from_confs(self):
        zones = []
        conf_files = glob.glob(os.path.join(self.conf_dir, "*.conf"))
        for f in conf_files:
            name = os.path.basename(f).replace(".conf", "")
            if re.match(r"zone\d+", name):
                zones.append(name)
        return sorted(zones)

    def _discover_apps_from_manifest(self, zone_id):
        manifest_path = os.path.join(self.manifest_dir, f"{zone_id}.json")
        if not os.path.exists(manifest_path):
            return []
        try:
            with open(manifest_path) as f:
                data = json.load(f)
            return [app["name"] for app in data.get("apps", [])]
        except (json.JSONDecodeError, KeyError):
            return []

    def _discover_apps_from_conf(self, zone_id):
        conf_path = os.path.join(self.conf_dir, f"{zone_id}.conf")
        if not os.path.exists(conf_path):
            return []
        apps = []
        with open(conf_path) as f:
            for line in f:
                m = re.search(r"proxy_pass\s+http://[^/]+/(\S+?)-envoy", line)
                if m:
                    # Extract app name from container name pattern: zoneN-appname-envoy
                    full = m.group(0)
                    container_match = re.search(rf"{zone_id}-(.+?)-envoy", full)
                    if container_match:
                        apps.append(container_match.group(1))
        return apps


# ============================================================
# Rate Controller
# ============================================================

class RateController:
    """Shared rate multiplier between modes."""

    def __init__(self, base_interval):
        self.base_interval = base_interval
        self.multiplier = 1.0

    @property
    def current_interval(self):
        if self.multiplier <= 0.01:
            return self.base_interval * 100
        return self.base_interval / self.multiplier

    def current_pool_size(self, base_pool):
        return max(1, int(base_pool * self.multiplier))


# ============================================================
# Mode 1: State Machine Personas
# ============================================================

class State(Enum):
    IDLE = "IDLE"
    BROWSING = "BROWSING"
    SELECTING = "SELECTING"
    CHECKOUT = "CHECKOUT"
    VALIDATING = "VALIDATING"
    DONE = "DONE"


# Transition matrices: from_state -> [(to_state, probability), ...]
TRANSITION_MATRICES = {
    "casual": {
        State.IDLE:       [(State.BROWSING, 1.0)],
        State.BROWSING:   [(State.BROWSING, 0.6), (State.SELECTING, 0.2), (State.DONE, 0.2)],
        State.SELECTING:  [(State.BROWSING, 0.3), (State.SELECTING, 0.3), (State.CHECKOUT, 0.1), (State.DONE, 0.3)],
        State.CHECKOUT:   [(State.VALIDATING, 0.7), (State.DONE, 0.3)],
        State.VALIDATING: [(State.DONE, 1.0)],
    },
    "power": {
        State.IDLE:       [(State.BROWSING, 1.0)],
        State.BROWSING:   [(State.BROWSING, 0.1), (State.SELECTING, 0.7), (State.DONE, 0.2)],
        State.SELECTING:  [(State.SELECTING, 0.1), (State.CHECKOUT, 0.8), (State.DONE, 0.1)],
        State.CHECKOUT:   [(State.VALIDATING, 0.95), (State.DONE, 0.05)],
        State.VALIDATING: [(State.DONE, 1.0)],
    },
    "window": {
        State.IDLE:       [(State.BROWSING, 1.0)],
        State.BROWSING:   [(State.BROWSING, 0.7), (State.SELECTING, 0.1), (State.DONE, 0.2)],
        State.SELECTING:  [(State.BROWSING, 0.4), (State.SELECTING, 0.2), (State.CHECKOUT, 0.02), (State.DONE, 0.38)],
        State.CHECKOUT:   [(State.VALIDATING, 0.5), (State.DONE, 0.5)],
        State.VALIDATING: [(State.DONE, 1.0)],
    },
}

# State -> (app_name, endpoint_path)
STATE_ENDPOINTS = {
    State.BROWSING:   ("catalog-api", "/products"),
    State.SELECTING:  ("catalog-api", "/products"),
    State.CHECKOUT:   ("orders-api", "/checkout"),
    State.VALIDATING: ("auth-api", "/validate"),
}

# Speed factor per persona type (lower = faster)
PERSONA_SPEED = {"casual": 1.5, "power": 0.5, "window": 2.0}


class Persona:
    """A single user session following a state machine."""

    def __init__(self, persona_type, zone, session_id=None):
        self.persona_type = persona_type
        self.zone = zone
        self.session_id = session_id or uuid.uuid4().hex[:8]
        self.state = State.IDLE
        self.request_count = 0
        self.start_time = time.time()
        self.consumer_id = f"user-{persona_type}-{self.session_id}"

    def transition(self):
        matrix = TRANSITION_MATRICES[self.persona_type]
        transitions = matrix.get(self.state, [(State.DONE, 1.0)])
        states = [t[0] for t in transitions]
        weights = [t[1] for t in transitions]
        self.state = random.choices(states, weights=weights)[0]
        return self.state

    def get_endpoint(self):
        if self.state not in STATE_ENDPOINTS:
            return None
        app, path = STATE_ENDPOINTS[self.state]
        return f"/{self.zone}/{app}{path}"

    @property
    def speed_factor(self):
        return PERSONA_SPEED.get(self.persona_type, 1.0)


class PersonaEngine:
    """Spawns and manages persona state machine coroutines."""

    def __init__(self, zone_map, rate_controller, gateway_url):
        self.zone_map = zone_map
        self.zones = list(zone_map.keys())
        self.rate_controller = rate_controller
        self.gateway_url = gateway_url
        self.stats = RequestStats()

    async def run(self):
        log.info("[PERSONA-ENGINE] Starting with base pool=%d", PERSONA_POOL_SIZE)
        active_tasks = set()

        while True:
            target_pool = self.rate_controller.current_pool_size(PERSONA_POOL_SIZE)
            # Spawn new personas to fill pool
            while len(active_tasks) < target_pool:
                task = asyncio.create_task(self._run_persona())
                active_tasks.add(task)
                task.add_done_callback(active_tasks.discard)

            await asyncio.sleep(0.5)

    async def _run_persona(self):
        ptype = random.choices(
            list(PERSONA_MIX.keys()),
            weights=list(PERSONA_MIX.values())
        )[0]
        zone = random.choice(self.zones)
        persona = Persona(ptype, zone)

        log.info("[PERSONA %s-%s] %s | IDLE -> BROWSING", ptype, persona.session_id, zone)
        persona.transition()  # IDLE -> BROWSING

        async with aiohttp.ClientSession() as session:
            while persona.state != State.DONE:
                endpoint = persona.get_endpoint()
                if endpoint:
                    await self._make_request(session, endpoint, persona)

                sleep_time = self.rate_controller.current_interval * persona.speed_factor
                sleep_time *= random.uniform(0.7, 1.3)
                await asyncio.sleep(sleep_time)

                old_state = persona.state
                persona.transition()
                if persona.state != old_state:
                    log.info(
                        "[PERSONA %s-%s] %s | %s -> %s",
                        ptype, persona.session_id, zone,
                        old_state.value, persona.state.value
                    )

        duration = time.time() - persona.start_time
        log.info(
            "[PERSONA %s-%s] %s | SESSION DONE (%d requests, %.1fs)",
            ptype, persona.session_id, zone,
            persona.request_count, duration
        )

    async def _make_request(self, session, endpoint, persona):
        url = f"{self.gateway_url}{endpoint}"
        headers = {"X-CONSUMER-ID": persona.consumer_id}
        start = time.time()
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                await resp.read()
                duration_ms = (time.time() - start) * 1000
                persona.request_count += 1
                self.stats.record(resp.status, duration_ms)
                log.debug("[PERSONA %s-%s] GET %s -> %d (%.0fms)",
                          persona.persona_type, persona.session_id, endpoint, resp.status, duration_ms)
        except Exception as e:
            duration_ms = (time.time() - start) * 1000
            self.stats.record("ERR", duration_ms)
            log.debug("[PERSONA %s-%s] GET %s -> ERROR: %s",
                      persona.persona_type, persona.session_id, endpoint, str(e))


# ============================================================
# Mode 2: Temporal Workload Patterns
# ============================================================

# Daily traffic curve: (hour, multiplier)
DAILY_CURVE = [
    (0, 0.1), (5, 0.1), (8, 0.7), (12, 1.0),
    (13, 1.0), (17, 0.8), (20, 0.3), (24, 0.1)
]


class TemporalEngine:
    """Modulates RateController based on simulated time of day."""

    def __init__(self, rate_controller, acceleration=300, events=None):
        self.rate_controller = rate_controller
        self.acceleration = acceleration  # real seconds per simulated hour
        self.events = events or []
        self.start_time = time.time()

    def _simulated_hour(self):
        elapsed = time.time() - self.start_time
        sim_hours = (elapsed / self.acceleration) % 24
        return sim_hours

    def _base_multiplier(self, hour):
        for i in range(len(DAILY_CURVE) - 1):
            h0, m0 = DAILY_CURVE[i]
            h1, m1 = DAILY_CURVE[i + 1]
            if h0 <= hour < h1:
                t = (hour - h0) / (h1 - h0)
                return m0 + t * (m1 - m0)
        return 0.1

    def _active_event(self, hour):
        for evt in self.events:
            at = evt.get("at_hour", 0)
            dur = evt.get("duration_hours", 0)
            if at <= hour < at + dur:
                return evt
        return None

    def _phase_name(self, hour):
        if hour < 5: return "night"
        if hour < 8: return "morning_ramp"
        if hour < 12: return "morning_peak"
        if hour < 13: return "midday_peak"
        if hour < 17: return "afternoon"
        if hour < 20: return "evening_decline"
        return "night"

    async def run(self):
        log.info("[TEMPORAL] Starting (acceleration=%ds/sim-hour)", int(self.acceleration))
        while True:
            hour = self._simulated_hour()
            multiplier = self._base_multiplier(hour)
            phase = self._phase_name(hour)

            event = self._active_event(hour)
            event_label = ""
            if event:
                multiplier = event.get("multiplier", multiplier)
                event_label = f" ({event['type']} active)"

            self.rate_controller.multiplier = multiplier
            pool = self.rate_controller.current_pool_size(PERSONA_POOL_SIZE)

            log.info(
                "[TEMPORAL] Sim %02d:%02d | %s | Multiplier: %.2f | Pool: %d%s",
                int(hour), int((hour % 1) * 60),
                phase, multiplier, pool, event_label
            )
            await asyncio.sleep(5)


# ============================================================
# Mode 3: Anomaly Injection Profiles
# ============================================================

class AnomalyEngine:
    """Runs targeted anomaly profiles independently."""

    def __init__(self, profiles, gateway_url):
        self.profiles = profiles
        self.gateway_url = gateway_url

    async def run(self):
        if not self.profiles:
            log.info("[ANOMALY] No profiles configured")
            return

        log.info("[ANOMALY] Starting %d profile(s)", len(self.profiles))
        tasks = []
        for profile in self.profiles:
            tasks.append(asyncio.create_task(self._run_profile(profile)))
        await asyncio.gather(*tasks)
        log.info("[ANOMALY] All profiles completed")

    async def _run_profile(self, profile):
        ptype = profile.get("type", "unknown")
        delay = profile.get("start_delay_seconds", 0)
        if delay > 0:
            log.info("[ANOMALY %s] Waiting %ds before start", ptype, delay)
            await asyncio.sleep(delay)

        handler = {
            "cascade_failure": self._cascade_failure,
            "zone_brownout": self._zone_brownout,
            "scanner_attack": self._scanner_attack,
            "ddos": self._ddos,
        }.get(ptype)

        if handler:
            await handler(profile)
        else:
            log.warning("[ANOMALY] Unknown profile type: %s", ptype)

    async def _cascade_failure(self, profile):
        zone = profile.get("target_zone", "zone1")
        app = profile.get("target_app", "catalog-api")
        endpoint = profile.get("endpoint", "/products")
        concurrency = profile.get("concurrency", 50)
        duration = profile.get("duration_seconds", 120)

        url = f"{self.gateway_url}/{zone}/{app}{endpoint}"
        log.info("[ANOMALY cascade_failure] START %s/%s%s (%d concurrent, %ds)",
                 zone, app, endpoint, concurrency, duration)

        stats = {"total": 0, "ok": 0, "fail": 0}
        end_time = time.time() + duration

        async def worker():
            connector = aiohttp.TCPConnector(limit=0)
            async with aiohttp.ClientSession(connector=connector) as session:
                while time.time() < end_time:
                    try:
                        async with session.get(
                            url,
                            headers={"X-CONSUMER-ID": "anomaly-cascade"},
                            timeout=aiohttp.ClientTimeout(total=10)
                        ) as resp:
                            await resp.read()
                            stats["total"] += 1
                            if resp.status < 500:
                                stats["ok"] += 1
                            else:
                                stats["fail"] += 1
                    except Exception:
                        stats["total"] += 1
                        stats["fail"] += 1

        tasks = [asyncio.create_task(worker()) for _ in range(concurrency)]
        await asyncio.gather(*tasks)
        log.info("[ANOMALY cascade_failure] COMPLETE (%d total, %d failures)",
                 stats["total"], stats["fail"])

    async def _zone_brownout(self, profile):
        zone = profile.get("target_zone", "zone1")
        ramp_seconds = profile.get("ramp_seconds", 180)
        peak_rps = profile.get("peak_rps", 100)
        hold_seconds = profile.get("hold_seconds", 60)

        log.info("[ANOMALY zone_brownout] START %s (ramp %ds, peak %d rps, hold %ds)",
                 zone, ramp_seconds, peak_rps, hold_seconds)

        apps = ["catalog-api", "orders-api", "payments-api", "auth-api"]
        endpoints = ["/products", "/checkout", "/checkout", "/validate"]
        stats = {"total": 0}

        # Ramp phase
        start = time.time()
        active_workers = []

        async def burst_worker(rps, until):
            connector = aiohttp.TCPConnector(limit=0)
            async with aiohttp.ClientSession(connector=connector) as session:
                while time.time() < until:
                    idx = random.randrange(len(apps))
                    url = f"{self.gateway_url}/{zone}/{apps[idx]}{endpoints[idx]}"
                    try:
                        async with session.get(
                            url,
                            headers={"X-CONSUMER-ID": "anomaly-brownout"},
                            timeout=aiohttp.ClientTimeout(total=10)
                        ) as resp:
                            await resp.read()
                            stats["total"] += 1
                    except Exception:
                        stats["total"] += 1
                    await asyncio.sleep(1.0 / max(rps, 1))

        # Ramp: increase concurrency linearly
        ramp_end = start + ramp_seconds
        hold_end = ramp_end + hold_seconds
        step_interval = 5
        steps = max(1, ramp_seconds // step_interval)

        for step in range(steps):
            current_rps = max(1, int(peak_rps * (step + 1) / steps))
            step_end = min(start + (step + 1) * step_interval, hold_end)
            task = asyncio.create_task(burst_worker(current_rps, step_end))
            active_workers.append(task)
            await asyncio.sleep(step_interval)

        # Hold phase
        if time.time() < hold_end:
            task = asyncio.create_task(burst_worker(peak_rps, hold_end))
            active_workers.append(task)
            remaining = hold_end - time.time()
            if remaining > 0:
                await asyncio.sleep(remaining)

        for t in active_workers:
            t.cancel()

        log.info("[ANOMALY zone_brownout] COMPLETE (%d total requests)", stats["total"])

    async def _scanner_attack(self, profile):
        zone = profile.get("target_zone", "zone1")
        rps = profile.get("rps", 20)
        duration = profile.get("duration_seconds", 90)

        log.info("[ANOMALY scanner_attack] START %s (%d rps, %ds)", zone, rps, duration)

        scan_paths = [
            "/admin", "/wp-admin", "/login", "/.env", "/config.json",
            "/api/v1/users", "/debug", "/phpinfo", "/backup.sql",
            "/actuator/health", "/.git/config", "/server-status",
        ]
        apps = ["catalog-api", "orders-api", "payments-api", "auth-api"]
        stats = {"total": 0, "status_404": 0}
        end_time = time.time() + duration

        connector = aiohttp.TCPConnector(limit=0)
        async with aiohttp.ClientSession(connector=connector) as session:
            while time.time() < end_time:
                app = random.choice(apps)
                path = random.choice(scan_paths)
                url = f"{self.gateway_url}/{zone}/{app}{path}"
                try:
                    async with session.get(
                        url,
                        headers={"X-CONSUMER-ID": "anomaly-scanner"},
                        timeout=aiohttp.ClientTimeout(total=5)
                    ) as resp:
                        await resp.read()
                        stats["total"] += 1
                        if resp.status == 404:
                            stats["status_404"] += 1
                except Exception:
                    stats["total"] += 1
                await asyncio.sleep(1.0 / rps)

        log.info("[ANOMALY scanner_attack] COMPLETE (%d total, %d 404s)",
                 stats["total"], stats["status_404"])

    async def _ddos(self, profile):
        zone = profile.get("target_zone", "zone1")
        app = profile.get("target_app", "auth-api")
        endpoint = profile.get("endpoint", "/validate")
        concurrency = profile.get("concurrency", 200)
        duration = profile.get("duration_seconds", 60)

        url = f"{self.gateway_url}/{zone}/{app}{endpoint}"
        log.info("[ANOMALY ddos] START %s/%s%s (%d concurrent, %ds)",
                 zone, app, endpoint, concurrency, duration)

        stats = {"total": 0, "ok": 0, "fail": 0}
        end_time = time.time() + duration

        async def worker():
            connector = aiohttp.TCPConnector(limit=0)
            async with aiohttp.ClientSession(connector=connector) as session:
                while time.time() < end_time:
                    try:
                        async with session.get(
                            url,
                            headers={"X-CONSUMER-ID": "anomaly-ddos"},
                            timeout=aiohttp.ClientTimeout(total=10)
                        ) as resp:
                            await resp.read()
                            stats["total"] += 1
                            if resp.status < 500:
                                stats["ok"] += 1
                            else:
                                stats["fail"] += 1
                    except Exception:
                        stats["total"] += 1
                        stats["fail"] += 1

        tasks = [asyncio.create_task(worker()) for _ in range(concurrency)]
        await asyncio.gather(*tasks)
        log.info("[ANOMALY ddos] COMPLETE (%d total, %d ok, %d fail)",
                 stats["total"], stats["ok"], stats["fail"])


# ============================================================
# Request Stats
# ============================================================

class RequestStats:
    """Aggregates request statistics for periodic reporting."""

    def __init__(self):
        self.reset()
        self.last_report = time.time()

    def reset(self):
        self.counts = {"2xx": 0, "4xx": 0, "5xx": 0, "err": 0}
        self.total = 0
        self.total_ms = 0.0

    def record(self, status, duration_ms):
        self.total += 1
        self.total_ms += duration_ms
        if status == "ERR":
            self.counts["err"] += 1
        elif isinstance(status, int):
            if status < 300:
                self.counts["2xx"] += 1
            elif status < 500:
                self.counts["4xx"] += 1
            else:
                self.counts["5xx"] += 1

    def should_report(self, interval=30):
        return time.time() - self.last_report >= interval

    def report(self):
        if self.total == 0:
            return
        avg_ms = self.total_ms / self.total
        log.info(
            "[STATS] Last 30s: %d req | 2xx: %d (%.1f%%) | 4xx: %d | 5xx: %d (%.1f%%) | err: %d (%.1f%%) | avg: %.0fms",
            self.total,
            self.counts["2xx"], 100 * self.counts["2xx"] / self.total,
            self.counts["4xx"],
            self.counts["5xx"], 100 * self.counts["5xx"] / self.total,
            self.counts["err"], 100 * self.counts["err"] / self.total,
            avg_ms
        )
        self.reset()
        self.last_report = time.time()


# ============================================================
# Traffic Orchestrator
# ============================================================

class TrafficOrchestrator:
    """Main entry point. Discovers endpoints, starts enabled modes."""

    def __init__(self):
        self.discovery = EndpointDiscovery(CONF_DIR, ZONE_MANIFEST_DIR)
        self.rate_controller = RateController(REQUEST_INTERVAL)

    async def run(self):
        # Discovery with retry
        zone_map = {}
        for attempt in range(12):
            zone_map = self.discovery.discover()
            if zone_map:
                break
            wait = min(5 * (attempt + 1), 30)
            log.warning("[DISCOVERY] No zones found (attempt %d/12), retrying in %ds...", attempt + 1, wait)
            await asyncio.sleep(wait)

        if not zone_map:
            log.error("[DISCOVERY] No zones found after retries. Exiting.")
            return

        zone_summary = ", ".join(f"{z} ({len(a)} apps)" for z, a in zone_map.items())
        log.info("[DISCOVERY] Found %d zones: %s", len(zone_map), zone_summary)
        log.info("[ORCHESTRATOR] Active modes: %s", ", ".join(MODES))

        tasks = []

        # Stats reporter
        stats = None
        if "personas" in MODES:
            engine = PersonaEngine(zone_map, self.rate_controller, GATEWAY_URL)
            stats = engine.stats
            tasks.append(asyncio.create_task(engine.run()))

        if "temporal" in MODES:
            engine = TemporalEngine(self.rate_controller, TIME_ACCELERATION, TEMPORAL_EVENTS)
            tasks.append(asyncio.create_task(engine.run()))

        if "anomaly" in MODES:
            engine = AnomalyEngine(ANOMALY_PROFILES, GATEWAY_URL)
            tasks.append(asyncio.create_task(engine.run()))

        # Stats reporter coroutine
        if stats:
            tasks.append(asyncio.create_task(self._stats_reporter(stats)))

        await asyncio.gather(*tasks)

    async def _stats_reporter(self, stats):
        while True:
            await asyncio.sleep(30)
            stats.report()


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    try:
        orchestrator = TrafficOrchestrator()
        asyncio.run(orchestrator.run())
    except KeyboardInterrupt:
        log.info("Traffic generator stopped.")
