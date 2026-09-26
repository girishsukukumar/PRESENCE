"""
roi-analysis.py
---------------
End-to-end anomaly-detection AUDIT for the S3 central unit.

The question this harness answers: "If I inject a known anomaly into the
presence data stream, does the REAL central unit detect it?"  It is the
test counterpart to simulate_presence.py: same leaf payloads, same real-
time pacing (the S3 timestamps every message with ITS OWN clock on receipt,
so visit durations ARE the wall-clock gaps -- there is no fast-forward),
but it adds the half that the plain simulator is missing: it subscribes to
the ALERT topic, logs every alert the device publishes, and grades the run
against the scenario's expected alerts.

THE RUN
-------
Phase A  PRIME   publish `--prime-days` normal days first. A fresh device's
                 EWMA baselines / 24 hourly bins need ~2-3 days of history
                 before the checks are stable (see simulate_presence.py).
Phase B  TEST    publish ONE anomaly day (the --scenario), starting at a
                 wall-clock anchor chosen per scenario so the anomaly lands
                 where the scenario intends (e.g. wake-early must start
                 ~13:20 so the overnight BedRoom sleep ENDS ~06:00 local,
                 far from the trained wake window 16:30-21:40).
Phase C  EVAL    keep listening through the following daily rollover plus a
                 margin, then grade observed alerts vs. expected ones.

OUTPUTS  (under --run-dir, default roi_runs/<scenario>-<timestamp>/)
    verdict.csv   one row per expected alert: PASS / FAIL / OBSERVE
    alerts.csv    every alert captured during the run, flattened
    alerts.jsonl  raw audit trail of every alert received
    events.jsonl  raw audit trail of every MQTT message this script published
    state.json    checkpoint -- a killed run resumes with `--resume`

USAGE
-----
    pip install paho-mqtt
    python roi-analysis.py --scenario bathroom-long --prime-days 2 --plan-only
    python roi-analysis.py --scenario bathroom-long --prime-days 2
    python roi-analysis.py --resume --run-dir roi_runs/<scenario>-<timestamp>
    python roi-analysis.py --report --run-dir roi_runs/<scenario>-<timestamp>

NOTES
-----
* The S3 must be powered on, flashed with the current firmware, and able to
  reach the broker (config.json -> broker.hivemq.com:1883).
* The laptop must stay awake and online for the whole run (days, not hours).
* If the script dies (laptop sleep, power cut), restart with --resume: it
  re-syncs the S3's occupancy state and continues from the last checkpoint.
* Alerts received DURING priming are logged but NOT scored -- they are the
  expected growing pains of a warming-up baseline.
"""

import argparse
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta

try:
    import paho.mqtt.client as mqtt
except ImportError:
    sys.exit("paho-mqtt not installed. Run:  pip install paho-mqtt")

try:
    from simulate_presence import (
        SCENARIOS,
        build_payload,
        summarize,
        DEFAULT_TOPIC,
        expected_alerts as describe_expected,  # its prose, for the plan preview
    )
except SystemExit:  # simulate_presence exits if its own paho import fails
    sys.exit("paho-mqtt not installed. Run:  pip install paho-mqtt")

CONFIG_PATH = "basic_MQTT_SUB_claude/data/config.json"
DEFAULT_ALERT_TOPIC = "PRESENCE/BLR/MANSARVOVAR/A4562/ALERT"
ROLLOVER_MARGIN_H = 2.0          # keep listening this long past the rollover
ANCHOR_OFFSET_MIN = 2            # start segments this many minutes past the anchor
DEFAULT_RUN_ROOT = "roi_runs"

# --------------------------------------------------------------------------
# Expected alerts per scenario. Alert-type substrings match what process.ino
# passes to publishAlert() (visit_duration_sec, ongoing_visit_exceeds_baseline,
# visit_count_per_day_spike, total_duration_per_day_sec,
# max_duration_per_day_sec, multivariate_anomaly, visit_at_hour_03_unusual).
# "realtime" = the device fires while / just after the trigger visit;
# "rollover" = at the local-midnight daily rollover in EVAL phase.
# --------------------------------------------------------------------------
EXPECTED_ALERTS = {
    "bathroom-long":     [("visit_duration_sec", "realtime"),
                          ("ongoing_visit_exceeds_baseline", "realtime")],
    "bathroom-spike":    [("visit_count_per_day_spike", "rollover"),
                          ("total_duration_per_day_sec", "rollover"),
                          ("max_duration_per_day_sec", "rollover")],
    "wake-early":        [("multivariate_anomaly", "rollover")],
    "unusual-hour-visit": [("visit_at_hour_03_unusual", "realtime")],
    "normal":            [],
    "no-sleep":          [],
}

# Expected but genuinely borderline alerts: the magnitude only just breaches
# the primed baseline, so a missing one is a NOTE, not a FAIL.
OPTIONAL_ALERTS = {
    ("bathroom-spike", "max_duration_per_day_sec"),
}

# Wall-clock HH:MM the TEST day must start so the anomaly lands where the
# scenario intends.
TEST_DAY_ANCHOR_HHMM = {
    "normal":             "00:00",
    "no-sleep":           "00:00",
    "bathroom-long":      "00:00",
    "bathroom-spike":     "00:00",
    "unusual-hour-visit": "00:00",
    "wake-early":         "13:20",
}

ANCHOR_REASON = {
    "normal":             "keeps the daily counters attributed to one clean calendar date.",
    "no-sleep":           "keeps the daily counters attributed to one clean calendar date.",
    "bathroom-long":      "keeps the long visit inside one calendar day.",
    "bathroom-spike":     "keeps all ~26 visits counting toward a single daily total.",
    "unusual-hour-visit": "the three 03:00 toilet visits only land in the 03:00 bin when the plan starts at local midnight.",
    "wake-early":         "the overnight BedRoom sleep must END ~06:00 local (trained wake window is 16:30-21:40); "
                          "that requires starting at ~13:20.",
}

# Known firmware gaps that a scenario's verdict would trip over. Printed in the
# plan preview so a FAIL is understood as a product gap, not a harness bug.
FIRMWARE_NOTES = {
    "wake-early": "KNOWN GAP: process.ino computes bedLongestEndEpoch (the wake "
                  "candidate) but never assigns it to wakeTimeSecOfDayToday, so the "
                  "rollover's `!isnan(wakeTimeSecOfDayToday)` guard is always false and "
                  "the Isolation-Forest multivariate check never runs. Expect "
                  "`multivariate_anomaly` to FAIL until that handoff is added.",
}


# --------------------------------------------------------------------------
# Small time helpers (all local wall-clock -- the S3 runs IST wall-clock too).
# --------------------------------------------------------------------------
def now_iso():
    return datetime.now().isoformat(timespec="seconds")


def next_local_midnight(from_dt, offset_min=ANCHOR_OFFSET_MIN):
    """Next local midnight at or after `from_dt`, plus a small offset."""
    cand = (from_dt + timedelta(days=1)).replace(hour=0, minute=offset_min,
                                                 second=0, microsecond=0)
    return cand


def next_hhmm_after(hhmm, from_dt, offset_min=ANCHOR_OFFSET_MIN):
    """Next occurrence of wall-clock HH:MM at or after from_dt + offset."""
    h, m = (int(x) for x in hhmm.split(":"))
    cand = from_dt.replace(hour=h, minute=m, second=0, microsecond=0)
    if cand <= from_dt:
        cand += timedelta(days=1)
    return cand + timedelta(minutes=offset_min)


def read_config():
    """Returns the central unit's config.json as a dict, or {} if absent."""
    try:
        with open(CONFIG_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def parse_broker(arg):
    b = arg
    if b.startswith("mqtt://"):
        b = b[len("mqtt://"):].split("/")[0]
    if ":" in b:
        host, port = b.rsplit(":", 1)
        return host, int(port)
    return b, 1883


def dtfmt(epoch):
    return datetime.fromtimestamp(epoch).strftime("%Y-%m-%d %H:%M:%S")


# --------------------------------------------------------------------------
# The audit run.
# --------------------------------------------------------------------------
class Audit:
    def __init__(self, args):
        self.args = args
        self.rng = random.Random(args.seed)
        cfg = read_config()
        if args.broker:
            self.broker = args.broker
        else:
            self.broker = cfg.get("mqttbroker") or "localhost:1883"
        self.alert_topic = (args.alert_topic
                            or cfg.get("mqttPublishingtopic")
                            or DEFAULT_ALERT_TOPIC)
        self.status_topic = args.topic
        self.out_dir = args.run_dir or self.default_run_dir()
        # Only a LIVE run (or --resume) needs to create the run directory:
        # --plan-only must not leave artifact folders behind, and --report
        # reads an existing one.
        if not (args.plan_only or args.report):
            os.makedirs(self.out_dir, exist_ok=True)
        self.state_path = os.path.join(self.out_dir, "state.json")
        self.events_path = os.path.join(self.out_dir, "events.jsonl")
        self.alerts_path = os.path.join(self.out_dir, "alerts.jsonl")
        self.state = self._load_state()
        # A checkpoint is the source of truth for what the run actually was:
        # --report/--resume must grade the recorded scenario, never whatever
        # happens to be the CLI default. Warn when they disagree.
        if self.state.get("scenario") and self.state["scenario"] != args.scenario:
            print(f"NOTE: checkpoint scenario '{self.state['scenario']}' overrides "
                  f"CLI '{args.scenario}'.")
            args.scenario = self.state["scenario"]
        if self.state.get("prime_days") != args.prime_days:
            print(f"NOTE: checkpoint prime_days ({self.state['prime_days']}) "
                  f"overrides CLI ({args.prime_days}).")
            args.prime_days = self.state["prime_days"]
        self.client = None
        self.open_room = None
        self.published = 0

    # -- run directory / checkpoint --------------------------------------
    def default_run_dir(self):
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        return os.path.join(DEFAULT_RUN_ROOT, f"{self.args.scenario}-{stamp}")

    def _load_state(self):
        if os.path.exists(self.state_path):
            try:
                with open(self.state_path, encoding="utf-8-sig") as f:
                    return json.load(f)
            except Exception as e:
                # A checkpoint that won't parse must NEVER be silently thrown
                # away -- on a days-long run that would wipe all progress.
                sys.exit(f"FATAL: could not parse checkpoint {self.state_path}: {e}\n"
                         "Refusing to start a fresh run over it. Inspect/remove the "
                         "file manually (or pass a different --run-dir) and retry.")
        return {
            "run_id": os.path.basename(self.out_dir),
            "scenario": self.args.scenario,
            "prime_days": self.args.prime_days,
            "phase": "priming",              # priming | test | listen | done
            "prime_days_done": 0,
            "seg_index": 0,
            "seg_end_epoch": None,
            "open_room": None,
            "test_start_epoch": None,
            "test_end_epoch": None,
            "listen_until_epoch": None,
            "created_iso": now_iso(),
        }

    def save_state(self):
        tmp = self.state_path + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump(self.state, f, indent=2)
            os.replace(tmp, self.state_path)
        except Exception as e:
            print(f"WARNING: could not save checkpoint: {e}")

    # -- MQTT -------------------------------------------------------------
    def connect(self):
        host, port = parse_broker(self.broker)
        print(f"Connecting to MQTT broker {host}:{port} ...")
        client_id = f"roi_audit_{self.state['run_id'][:20]}_{uuid.uuid4().hex[:8]}"
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        c.on_connect = self._on_connect
        c.on_message = self._on_alert
        c.reconnect_delay_set(min_delay=1, max_delay=60)  # auto-reconnect in the bg loop
        try:
            c.connect(host, port, keepalive=60)
        except Exception as e:
            sys.exit(f"Could not reach MQTT broker at {host}:{port}: {e}\n"
                     "Is the broker reachable? Override with --broker.")
        c.loop_start()
        self.client = c
        time.sleep(2)  # give the subscribe handshake a moment

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        client.subscribe(self.alert_topic, qos=0)
        print(f"Subscribed to ALERT topic: {self.alert_topic}")

    def _on_alert(self, client, userdata, msg):
        try:
            doc = json.loads(msg.payload.decode("utf-8"))
        except Exception:
            return
        if not isinstance(doc, dict) or not doc.get("alert_type"):
            # ON_LINE / OFF_LINE / last-will heartbeats are not anomaly alerts.
            return
        rec = {
            "t": now_iso(),
            "wall_epoch": time.time(),
            "phase": self.state.get("phase", "?"),
            "room": doc.get("sensor_room"),
            "alert_type": doc.get("alert_type"),
            "value": doc.get("value"),
            "baseline_mean": doc.get("baseline_mean"),
            "s3_epoch": doc.get("epoch"),
            "kind": doc.get("kind"),
        }
        try:
            with open(self.alerts_path, "a") as f:
                f.write(json.dumps(rec) + "\n")
        except Exception as e:
            print(f"WARNING: could not write alert log: {e}")
        ts = self.state.get("test_start_epoch")
        since = f"  (latency since test start {int(time.time() - ts)}s)" if ts else ""
        print(f"[alert {rec['phase']:<7s}] {rec['alert_type']:<34s} "
              f"room={rec['room']}  value={rec['value']}  mean={rec['baseline_mean']}"
              f"{since}")

    # -- presence publishing ----------------------------------------------
    def journal_event(self, phase, day, room, status, extra=None):
        rec = {"t": now_iso(), "wall_epoch": time.time(), "phase": phase,
               "day": day, "room": room, "status": status}
        if extra:
            rec.update(extra)
        try:
            with open(self.events_path, "a") as f:
                f.write(json.dumps(rec) + "\n")
        except Exception as e:
            print(f"WARNING: could not write event log: {e}")

    def send(self, phase, day, room, status):
        payload = json.dumps(build_payload(room, status))
        self.client.publish(self.status_topic, payload, qos=0)
        self.published += 1
        self.journal_event(phase, day, room, status)
        print(f"[pub {phase:<6s}] {room:10s} {status:12s}")

    def re_sync_occupancy(self):
        """After a restart the S3 may still think `open_room` is occupied.
        Close it explicitly so resumed visits start from a clean state."""
        room = self.state.get("open_room")
        if room:
            print(f"[resume] closing previously open room {room} on the S3")
            self.send(self.state.get("phase", "priming"), 0, room, "NOT_DETECTED")
            self.state["open_room"] = None
            self.save_state()

    def play_segment(self, phase, day, room, duration_sec, seg_index):
        # Persist the planned absolute end BEFORE sleeping, so a restart
        # mid-segment knows exactly how much real time is left.
        seg_end = time.time() + duration_sec
        self.state["seg_index"] = seg_index
        self.state["seg_end_epoch"] = seg_end
        self.state["open_room"] = room
        self.open_room = room
        self.save_state()

        if seg_end - time.time() <= 0:
            # We were killed and only came back after the segment expired.
            # re_sync_occupancy() already closed the room, so skip rather
            # than fabricate a 0-second visit (which would poison the
            # per-visit baseline).
            print(f"[resume] segment {room} {duration_sec}s already elapsed -- skipping")
            self.journal_event(phase, day, room, "SKIPPED_SEG",
                               {"duration_sec": duration_sec})
            self.state.pop("seg_end_epoch", None)
            self.save_state()
            return

        self.send(phase, day, room, "DETECTED")
        while time.time() < seg_end:
            time.sleep(min(1.0, seg_end - time.time()))
            # occasional keep-alive heartbeat, like the real leaves
            if int(time.time()) % self.args.keepalive_sec == 0:
                self.send(phase, day, room, "KEEP_ALIVE")
        self.send(phase, day, room, "NOT_DETECTED")
        self.state.pop("seg_end_epoch", None)
        self.state["open_room"] = None
        self.open_room = None
        self.save_state()

    def play_day(self, segments, phase, day, seg_start=0):
        for i in range(seg_start, len(segments)):
            room, dur = segments[i]
            self.play_segment(phase, day, room, dur, i)
            if i < len(segments) - 1:
                time.sleep(self.rng.uniform(3, 25))  # short gap while "moving"
        self.state["seg_index"] = 0
        self.save_state()

    def wait_until(self, target_dt, label=""):
        wait = (target_dt - datetime.now()).total_seconds()
        if wait <= 0:
            return
        target_epoch = target_dt.timestamp()
        print(f"[wait] {label or 'waiting'} until {target_dt:%Y-%m-%d %H:%M} "
              f"({wait / 3600:.1f}h) -- the S3 grades by its own wall clock, "
              f"so pacing must be real.")
        n = 0
        while time.time() < target_epoch:
            time.sleep(min(60, max(1.0, target_epoch - time.time())))
            n += 1
            if n % 360 == 0:  # ~once every 6h, so a long wait looks alive
                print(f"[wait] still waiting until {target_dt:%H:%M} "
                      f"({(target_epoch - time.time()) / 3600:.1f}h left)")

    # -- phases -----------------------------------------------------------
    def run_priming(self):
        while self.state["prime_days_done"] < self.args.prime_days:
            day = self.state["prime_days_done"] + 1
            if self.state["seg_index"] == 0 and self.state["prime_days_done"] > 0:
                self.wait_until(next_local_midnight(datetime.now()),
                                f"prime day {day} (midnight anchor)")
            segs = SCENARIOS["normal"](self.rng)
            print(f"\n=== PRIME day {day}/{self.args.prime_days} starts "
                  f"{datetime.now():%Y-%m-%d %H:%M:%S} ===")
            self.play_day(segs, phase="prime", day=day,
                          seg_start=self.state["seg_index"])
            self.state["prime_days_done"] = day
            self.save_state()
        self.state["phase"] = "test"
        self.save_state()

    def run_test(self):
        anchor = TEST_DAY_ANCHOR_HHMM[self.args.scenario]
        self.wait_until(next_hhmm_after(anchor, datetime.now()),
                        f"TEST day start ({anchor} anchor)")
        if self.state["test_start_epoch"] is None:
            self.state["test_start_epoch"] = time.time()
            self.save_state()
        segs = SCENARIOS[self.args.scenario](self.rng)
        print(f"\n=== TEST day ({self.args.scenario}) starts "
              f"{datetime.now():%Y-%m-%d %H:%M:%S} ===")
        self.play_day(segs, phase="test", day=1,
                      seg_start=self.state["seg_index"])
        self.state["test_end_epoch"] = time.time()
        self.state["phase"] = "listen"
        self.state["listen_until_epoch"] = self.compute_listen_until()
        self.state["seg_index"] = 0
        self.save_state()
        self.run_listen()

    def compute_listen_until(self):
        """Keep listening until the daily rollover AFTER the last test
        segment, plus a margin for clock skew between laptop and S3."""
        end_dt = datetime.fromtimestamp(self.state["test_end_epoch"])
        return next_local_midnight(end_dt).timestamp() + ROLLOVER_MARGIN_H * 3600

    def run_listen(self):
        until = self.state["listen_until_epoch"]
        print(f"\n[listen] scenario finished; capturing rollover alerts until "
              f"{dtfmt(until)} ...")
        while time.time() < until:
            time.sleep(min(60, max(1.0, until - time.time())))
        self.state["phase"] = "done"
        self.save_state()
        self.finish_report()

    def finish_report(self):
        """Grade the run and write verdict.csv / alerts.csv, then print."""
        alerts = self.load_alerts()
        test_alerts = [a for a in alerts if a.get("phase") == "test"]
        overall, rows, unexpected = self.grade(test_alerts)
        self.write_verdict_csv(rows)
        self.write_alerts_csv(alerts)
        self.print_report(alerts, rows, unexpected, overall)

    # -- paper plans (no network) ----------------------------------------
    def plan_only(self):
        print("\n" + "=" * 74)
        print("ROI ANALYSIS -- PLAN ONLY (nothing is published)")
        print(f"scenario    : {self.args.scenario}   (seed {self.args.seed})")
        print(f"prime days  : {self.args.prime_days} normal day(s) first")
        print(f"status topic: {self.status_topic}")
        print(f"alert topic : {self.alert_topic}")
        print(f"broker      : {self.broker}  (from ./{CONFIG_PATH})")
        print("-" * 74)

        normal_feats = summarize(SCENARIOS["normal"](self.rng))
        print(f"PRIME day   : ~{normal_feats['total_plan_hours']:.1f}h of 'normal' "
              f"itinerary per day, repeated {self.args.prime_days}x")
        print("            : day 1 starts immediately; later days anchor to local "
              "midnight.")
        if self.args.prime_days > 0:
            print(f"            : daily features this primes: "
                  f"count={normal_feats['bathroom_visit_count']}, "
                  f"total={normal_feats['bathroom_total_duration_sec']}s, "
                  f"max={normal_feats['bathroom_max_duration_sec']}s -- all inside the "
                  f"trained normal ranges (3-7 / 700-2000 / 280-580), so a healthy "
                  f"device should NOT alert after warm-up.")

        test_feats = summarize(SCENARIOS[self.args.scenario](self.rng))
        anchor = TEST_DAY_ANCHOR_HHMM[self.args.scenario]
        print(f"TEST day    : {self.args.scenario} itinerary, ~{test_feats['total_plan_hours']:.1f}h")
        print(f"            : must START at {anchor} local -- {ANCHOR_REASON[self.args.scenario]}")
        print(f"            : triggers these expected alerts:")
        expected = EXPECTED_ALERTS[self.args.scenario]
        if expected:
            for etype, when in expected:
                opt = " (optional)" if (self.args.scenario, etype) in OPTIONAL_ALERTS else ""
                print(f"              - {etype:<34s} {when}{opt}")
        else:
            print(f"              - (none -- a quiet-day control run; any alert is a FAIL)")
        for line in describe_expected(test_feats):
            print(f"            : {line}")
        note = FIRMWARE_NOTES.get(self.args.scenario)
        if note:
            print(f"            : {note}")

        # Rough wall-clock budget (real time -- the S3 grades by wall clock).
        prime_h = self.args.prime_days * 24.0
        test_h = 24.0 + 26.0        # itinerary + anchor wait + rollover/margin
        print("-" * 74)
        print(f"WALL-CLOCK BUDGET: roughly {prime_h + test_h:.0f}h "
              f"({prime_h:.0f}h priming + up to 26h test day, anchor wait and rollover "
              f"listening). A short first run: --prime-days 1.")
        print("PREREQUISITES : S3 powered/flashed and online on the broker; laptop "
              "awake + online throughout.")
        print("=" * 74)

    # -- report-only ------------------------------------------------------
    def report_only(self):
        """Re-print the verdict of a finished run from its files alone."""
        if not os.path.exists(self.state_path):
            sys.exit(f"FATAL: {self.state_path} does not exist -- there is no run "
                     "to report. Did you mean --plan-only (preview) or a real run?")
        if self.state["phase"] != "done":
            print(f"WARNING: run is in phase '{self.state['phase']}', not 'done'. "
                  f"Showing alerts captured so far anyway.")
        self.finish_report()

    # -- reporting --------------------------------------------------------
    def load_alerts(self):
        if not os.path.exists(self.alerts_path):
            return []
        out = []
        try:
            with open(self.alerts_path) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            out.append(json.loads(line))
                        except ValueError:
                            continue
        except Exception:
            return []
        return out

    def grade(self, test_alerts):
        rows = []
        expected = EXPECTED_ALERTS[self.args.scenario]
        for etype, when in expected:
            obs = [a for a in test_alerts
                   if etype in (a.get("alert_type") or "")]
            optional = (self.args.scenario, etype) in OPTIONAL_ALERTS
            if obs:
                a = obs[0]
                lat = (int(a["wall_epoch"] - self.state.get("test_start_epoch", 0))
                       if self.state.get("test_start_epoch") and when == "realtime"
                       else None)
                rows.append({
                    "expected_alert_type": etype, "when": when,
                    "verdict": "PASS",
                    "value": a.get("value"), "baseline_mean": a.get("baseline_mean"),
                    "latency_sec": lat, "count": len(obs),
                })
            elif optional:
                rows.append({"expected_alert_type": etype, "when": when,
                             "verdict": "OBSERVE", "value": None,
                             "baseline_mean": None, "latency_sec": None, "count": 0})
            else:
                rows.append({"expected_alert_type": etype, "when": when,
                             "verdict": "FAIL", "value": None,
                             "baseline_mean": None, "latency_sec": None, "count": 0})
        expected_substrs = [e for e, _ in expected]
        unexpected = [a for a in test_alerts
                      if not any(e in (a.get("alert_type") or "") for e in expected_substrs)]
        required_missing = any(r["verdict"] == "FAIL" for r in rows)
        overall = "FAIL" if required_missing else "PASS"
        return overall, rows, unexpected

    def write_verdict_csv(self, rows):
        path = os.path.join(self.out_dir, "verdict.csv")
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                f.write("run_id,scenario,expected_alert_type,when,verdict,"
                        "observed,value,baseline_mean,latency_sec\n")
                for r in rows:
                    f.write(f"{self.state['run_id']},{self.args.scenario},"
                            f"{r['expected_alert_type']},{r['when']},{r['verdict']},"
                            f"{r['count']},{r['value']},{r['baseline_mean']},"
                            f"{r['latency_sec']}\n")
            print(f"Wrote {path}")
        except Exception as e:
            print(f"WARNING: could not write verdict.csv: {e}")

    def write_alerts_csv(self, alerts):
        path = os.path.join(self.out_dir, "alerts.csv")
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                f.write("run_id,t,phase,room,alert_type,value,baseline_mean,"
                        "wall_epoch,s3_epoch\n")
                for a in alerts:
                    f.write(f"{self.state['run_id']},{a.get('t')},{a.get('phase')},"
                            f"{a.get('room')},{a.get('alert_type')},{a.get('value')},"
                            f"{a.get('baseline_mean')},{a.get('wall_epoch')},"
                            f"{a.get('s3_epoch')}\n")
            print(f"Wrote {path}")
        except Exception as e:
            print(f"WARNING: could not write alerts.csv: {e}")

    def print_report(self, alerts, rows, unexpected, overall):
        print("\n" + "=" * 74)
        print(f"ROI ANALYSIS REPORT -- {self.args.scenario}")
        print(f"run dir : {self.out_dir}")
        print(f"alerts captured: {len(alerts)} total "
              f"({len([a for a in alerts if a.get('phase') == 'prime'])} during priming, "
              f"{len([a for a in alerts if a.get('phase') == 'test'])} during test)")
        print("-" * 74)
        if rows:
            print(f"{'expected alert':<36s}{'when':<10s}{'verdict':<9s}{'value':>8s}"
                  f"{'latency':>12s}")
            for r in rows:
                lat = (f"{r['latency_sec']}s" if r["latency_sec"] is not None
                       else ("at rollover" if r["when"] == "rollover" else "-"))
                print(f"{r['expected_alert_type']:<36s}{r['when']:<10s}"
                      f"{r['verdict']:<9s}{str(r['value']):>8s}{lat:>12s}")
            for r in rows:
                if r["verdict"] == "FAIL":
                    print(f"  !! NOT DETECTED: {r['expected_alert_type']} "
                          f"({r['when']})")
        if unexpected:
            print("-" * 74)
            print("Unexpected alerts during the TEST phase (not scored, for review):")
            for a in unexpected:
                print(f"  {a.get('alert_type')} room={a.get('room')} "
                      f"value={a.get('value')} mean={a.get('baseline_mean')} "
                      f"at {a.get('t')}")
        print("-" * 74)
        print(f"=== VERDICT: {overall} ===")


# --------------------------------------------------------------------------
def resolve_run_dir(args):
    """--run-dir wins; on --resume/--report pick the newest dir with a
    checkpoint. Never guess for a fresh run."""
    if args.run_dir:
        return args.run_dir
    if args.resume or args.report:
        if not os.path.isdir(DEFAULT_RUN_ROOT):
            sys.exit(f"No runs found under ./{DEFAULT_RUN_ROOT} -- nothing to "
                     f"{'resume' if args.resume else 'report'}.")
        runs = sorted(
            (d for d in os.listdir(DEFAULT_RUN_ROOT)
             if os.path.isfile(os.path.join(DEFAULT_RUN_ROOT, d, "state.json"))),
            reverse=True)
        if not runs:
            sys.exit(f"No checked-point runs found under ./{DEFAULT_RUN_ROOT}.")
        return os.path.join(DEFAULT_RUN_ROOT, runs[0])
    return None


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scenario", choices=sorted(EXPECTED_ALERTS),
                    default="bathroom-long",
                    help="Anomaly to inject in the TEST phase (default: bathroom-long, "
                         "which fires within minutes on a primed device)")
    ap.add_argument("--prime-days", type=int, default=2,
                    help="Normal days to publish first so the S3's baselines warm up "
                         "(2-3 recommended on a fresh device)")
    ap.add_argument("--broker", default=None,
                    help="MQTT broker [host[:port] or mqtt://host:port]. Default: "
                         "central unit's config.json mqttbroker, else localhost:1883")
    ap.add_argument("--topic", default=DEFAULT_TOPIC,
                    help="Status topic the leaves publish presence to")
    ap.add_argument("--alert-topic", default=None,
                    help="Topic the central unit publishes alerts on; default from "
                         "config.json mqttPublishingtopic")
    ap.add_argument("--run-dir", default=None,
                    help="Run directory (for a fresh run this is created; combine with "
                         "--resume/--report to target an existing one)")
    ap.add_argument("--resume", action="store_true",
                    help="Resume the newest checked-point run (or --run-dir)")
    ap.add_argument("--report", action="store_true",
                    help="Re-print the verdict of a finished run from its files "
                         "(no broker needed)")
    ap.add_argument("--plan-only", action="store_true",
                    help="Print the multi-day plan and expected alerts, then exit "
                         "(no broker, no publishing)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--keepalive-sec", type=int, default=45,
                    help="Heartbeat cadence (s), matching the real leaves")
    args = ap.parse_args()

    if args.prime_days < 1:
        sys.exit("--prime-days must be >= 1 (use at least 1, ideally 2-3, on a "
                 "fresh device).")
    if args.resume and args.report:
        sys.exit("--resume and --report are mutually exclusive.")

    args.run_dir = resolve_run_dir(args)
    sim = Audit(args)

    if args.report:
        sim.report_only()
        return

    if args.plan_only:
        sim.plan_only()
        return

    sim.connect()
    print(f"Run dir: {sim.out_dir}")

    try:
        if sim.state["phase"] == "priming":
            sim.run_priming()
        if sim.state["phase"] == "test":
            sim.run_test()
        if sim.state["phase"] == "listen":
            print(f"[resume] back in the listen phase; "
                  f"{max(0.0, sim.state['listen_until_epoch'] - time.time()) / 3600:.1f}h "
                  f"left until the report.")
            sim.run_listen()
        # phase 'done': report already printed when it finished.
    except KeyboardInterrupt:
        sim.save_state()
        print(f"\nInterrupted by user. Progress is checkpointed in {sim.out_dir}.")
        if sim.open_room is not None:
            try:
                sim.send(sim.state.get("phase", "?"), 1, sim.open_room, "NOT_DETECTED")
                sim.state["open_room"] = None
                sim.save_state()
            except Exception:
                pass  # network may be gone; state.json still says open_room
        print(f"Resume later with: python roi-analysis.py --resume --run-dir "
              f"{sim.out_dir}")
        sys.exit(130)
    finally:
        if sim.client is not None:
            try:
                sim.client.loop_stop()
                sim.client.disconnect()
            except Exception:
                pass
        print(f"Total presence messages published: {sim.published}")


if __name__ == "__main__":
    main()