"""
simulate_presence.py
--------------------
Test-data stub that mimics the leaf ESP32 nodes (ESP32 + LD2410c sensor)
publishing presence messages over MQTT to the S3 central unit, so you can
verify that the anomaly detection (EWMA baselines + Isolation Forest) is
actually firing.

Payloads are byte-for-byte the same shape the Arduino leaves send:

    {"sensor_room":"LivingRoom", "status":"DETECTED",
     "Moving Target":false, "Moving Target Dist":-1,
     "Stationary Target":true, "Stationary Target Dist":561}

CRITICAL TIMING CONSTRAINT
--------------------------
The S3 timestamps every message with ITS OWN clock on receipt and computes
each interval's duration as (time(NOT_DETECTED) - time(DETECTED)). That
means the stub MUST pace its publishes in real time -- the wall-clock gap
between a DETECTED and its NOT_DETECTED *is* the visit duration the device
sees. There is no fast-forward: speeding the script up would send
zero-second visits and the device would log 0 s intervals.

Anomaly alerts are computed DAILY (at local midnight rollover) for the
bathroom count / total / max / hourly-pattern / Isolation-Forest checks,
and IN REAL TIME for per-visit duration and ongoing-visit checks. So:

  * bathroom-long     -> fires within minutes (per-visit + ongoing checks)
  * bathroom-spike    -> fires at the end of the day (count spike)
  * wake-early        -> fires at the end of the day (IF multivariate)
  * unusual-hour-visit-> fires at the end of the day (hourly pattern)

PRIMING NOTE
------------
The firmware's per-visit EWMA ("bath_dur") is extremely sensitive until it
has seen some history (variance floor = 1 s, K_SIGMA = 2.5), so on a FRESH
device even a normal day's second visit can alert. For a clean demo, let a
normal scenario run for 2-3 real days first, then inject the anomaly
scenario. See --plan-only to preview.

Usage:
    pip install paho-mqtt
    python simulate_presence.py --scenario normal --days 2              # prime
    python simulate_presence.py --scenario bathroom-spike --days 1     # then spike
    python simulate_presence.py --scenario bathroom-long                # quick real-time test
    python simulate_presence.py --broker localhost:1883 --scenario wake-early --plan-only
"""

import argparse
import json
import random
import socket
import sys
import time
from datetime import datetime, timedelta

try:
    import paho.mqtt.client as mqtt
except ImportError:
    sys.exit("paho-mqtt not installed. Run:  pip install paho-mqtt")

DEFAULT_TOPIC = "PRESENCE/BLR/MANSARVOVAR/A4562/STATUS"
CONFIG_PATH = "basic_MQTT_SUB/data/config.json"

ROOMS = ["LivingRoom", "BedRoom", "Toilet-1", "Kitchen", "Hallway"]

# Trained Isolation Forest feature ranges (from the 2026-09-21..11-20 data).
# "normal" days below are tuned to land INSIDE these so a clean demo passes.
WAKE_RANGE = (int(16.6 * 3600), int(21.7 * 3600))   # wake_time_sec_of_day
BATH_COUNT_RANGE = (3, 7)
BATH_TOTAL_RANGE = (700, 2000)
BATH_MAX_RANGE = (280, 580)


# --------------------------------------------------------------------------
# Itinerary builders. Each returns a list of (room, duration_sec) segments
# plus a structured summary used by --plan-only / expected-alert printing.
# --------------------------------------------------------------------------
def normal_day(rng):
    """A 'normal' day: communicates with the central unit using feature
    values inside the trained distribution. The long BedRoom block ends
    ~20:20 local, which is inside the trained wake_time range."""
    layout = [
        ("LivingRoom", 3600),       # 01:00
        ("Toilet-1", 300),          # 01:05
        ("LivingRoom", 5400),       # 02:35
        ("Kitchen", 1200),          # 02:55
        ("LivingRoom", 7200),       # 05:30  (sits in the living room)
        ("Toilet-1", 300),          # 05:35
        ("LivingRoom", 4500),       # 06:50
        ("Toilet-1", 320),          # 06:55
        ("Kitchen", 3000),          # 07:45
        ("LivingRoom", 11700),      # 11:00
        ("Toilet-1", 330),          # 11:05
        ("LivingRoom", 4500),       # 12:20  <- BedRoom slot starts here
        ("BedRoom", 8 * 3600),      # 12:20 -> 20:20 (main sleep, wake ~20:20)
        ("Toilet-1", 300),          # 20:25
        ("Kitchen", 1500),          # 20:50
        ("LivingRoom", 7200),       # 22:50
        ("BedRoom", 2700),          # brief return to bed, not the day's longest
        ("Toilet-1", 250),          # 23:37
        ("LivingRoom", 1300),       # 23:59  wrap
    ]
    return layout


def bathroom_spike_day(rng):
    """~26 bathroom visits spread through the day (vs 3-6 normal) plus a big
    total duration -> the count / total-duration / max checks all fire at the
    day's rollover. NOTE: this itinerary is ~16h long in real time."""
    slots = []
    for h in range(0, 24, 2):
        slots.extend([(h, 10), (h, 55)])  # two visits per 2h window
    slots.sort()
    day = [("LivingRoom", 3000)]
    for h, m in slots:
        day.append(("Toilet-1", rng.randint(240, 420)))
        day.append(("LivingRoom", rng.randint(2400, 4200)))
        day.append(("Kitchen", rng.randint(300, 900)))
    day.append(("BedRoom", 8 * 3600))
    day.append(("Toilet-1", 280))
    day.append(("LivingRoom", 1200))
    return day


def bathroom_long_day(rng):
    """One normal-ish warmup visit, then a ~30 min bathroom visit. Trips the
    per-visit duration check (real time) and ongoing-visit check."""
    return [
        ("LivingRoom", 1800),
        ("Toilet-1", 300),          # seeds bath_dur baseline
        ("LivingRoom", 600),
        ("Toilet-1", 540),          # second normal-ish visit
        ("Kitchen", 1200),
        ("Toilet-1", 30 * 60),      # THE ANOMALY: 30 min in the bathroom
        ("LivingRoom", 3600),
        ("Toilet-1", 420),
        ("LivingRoom", 3600),
    ]


def wake_early_day(rng):
    """Normal bathroom pattern but the night-of sleep is overnight and ends
    at ~06:00 local. wake_time_sec_of_day ~ 21600, well outside the trained
    range -> IF multivariate alert at rollover."""
    return [
        ("LivingRoom", 3600),
        ("Toilet-1", 300),
        ("LivingRoom", 9000),
        ("Toilet-1", 320),
        ("Kitchen", 1800),
        ("LivingRoom", 3600),
        ("Toilet-1", 300),
        ("LivingRoom", 3600),
        ("Toilet-1", 300),
        ("Kitchen", 1500),
        ("LivingRoom", 3000),
        ("Toilet-1", 330),
        # overnight sleep 21:00 -> 06:00 next local day
        ("BedRoom", 9 * 3600),
        ("Toilet-1", 280),
        ("Kitchen", 1200),
        ("LivingRoom", 3600),
    ]


def unusual_hour_visit_day(rng):
    """Normal day, plus THREE toilet visits landing in the 03:00 wall-clock
    bin. If hourly-pattern baselines exist (bh_03 ~ 0, variance floor 1 s),
    a single visit scores (1-0)/1 = 1.0 < K_SIGMA(2.5) and would NOT fire;
    three visits score 3.0 and DO fire 'hour_03 unusual' at rollover."""
    day = normal_day(rng)
    # insert at index 4: the Kitchen(1200) ends ~02:55 local, so these three
    # visits land inside the 03:00 hour when the plan starts at ~midnight.
    day[4:4] = [("Toilet-1", 300), ("Toilet-1", 300), ("Toilet-1", 330)]
    return day


def no_sleep_day(rng):
    """No BedRoom session >= 3h. wake_time stays NaN -> IF check skipped
    (by design); use this to confirm that the EWMA-only checks still run."""
    return [seg for seg in normal_day(rng) if seg[0] != "BedRoom"]


SCENARIOS = {
    "normal": normal_day,
    "bathroom-spike": bathroom_spike_day,
    "bathroom-long": bathroom_long_day,
    "wake-early": wake_early_day,
    "unusual-hour-visit": unusual_hour_visit_day,
    "no-sleep": no_sleep_day,
}


# --------------------------------------------------------------------------
# Feature summary / expected-alert report
# --------------------------------------------------------------------------
def summarize(segments):
    """Compute the daily features the S3 would see from these segments, the
    way extract_features.py does (attribution by end-of-interval date, no
    merging for bathroom, longest *adjacent* BedRoom run >= 3h for wake)."""
    bath = [d for r, d in segments if r == "Toilet-1"]
    # BedRoom chains: consecutive BedRoom entries in the itinerary are only
    # ~3-25 s apart (<= the firmware's 300 s merge gap) so they form ONE
    # chain; a run interrupted by another room is a separate chain.
    chains, cur = [], 0
    for r, d in segments:
        if r == "BedRoom":
            cur += d
        else:
            if cur:
                chains.append(cur)
            cur = 0
    if cur:
        chains.append(cur)
    longest_bed = max(chains, default=0)

    # offset (s from plan start) of the END of the longest BedRoom chain:
    # ruled for recommending a start time whose wake lands in the trained range
    wake_off = None
    if longest_bed:
        cum, cur, best_dur = 0, 0, 0
        for r, d in segments:
            cum += d
            if r == "BedRoom":
                cur += d
                if cur > best_dur:
                    best_dur = cur
                    wake_off = cum
            else:
                cur = 0

    total = sum(d for _, d in segments)
    return {
        "bathroom_visit_count": len(bath),
        "bathroom_total_duration_sec": sum(bath),
        "bathroom_max_duration_sec": max(bath, default=0),
        "longest_bedroom_chain_min": longest_bed // 60,
        "wake_offset_sec_from_plan_start": wake_off,
        "total_plan_hours": round(total / 3600, 2),
    }


def expected_alerts(feat):
    out = []
    c, tot, mx = feat["bathroom_visit_count"], feat["bathroom_total_duration_sec"], feat["bathroom_max_duration_sec"]
    if not (BATH_COUNT_RANGE[0] <= c <= BATH_COUNT_RANGE[1]):
        out.append(f"bathroom visit count {c} OUT of normal {BATH_COUNT_RANGE} -> count spike/drop alert at rollover")
    if not (BATH_TOTAL_RANGE[0] <= tot <= BATH_TOTAL_RANGE[1]):
        out.append(f"bathroom total {tot}s OUT of normal {BATH_TOTAL_RANGE} -> total-duration alert at rollover")
    if not (BATH_MAX_RANGE[0] <= mx <= BATH_MAX_RANGE[1]):
        out.append(f"bathroom max {mx}s OUT of normal {BATH_MAX_RANGE} -> per-visit duration alert "
                   f"(fires in real time for a single long visit)")
    if feat["longest_bedroom_chain_min"] < 180:
        out.append(f"longest bedroom chain {feat['longest_bedroom_chain_min']}m < 3h -> wake_time NaN, "
                   "IF check skipped (by design)")
    else:
        out.append(f"longest bedroom chain {feat['longest_bedroom_chain_min']}m -> wake_time set from its end, "
                   "feed into IF at rollover")
    return out


# --------------------------------------------------------------------------
# MQTT machinery (paho-mqtt 2.x)
# --------------------------------------------------------------------------
def parse_broker(arg):
    b = arg
    if b.startswith("mqtt://"):
        b = b[len("mqtt://"):].split("/")[0]
    if ":" in b:
        host, port = b.rsplit(":", 1)
        return host, int(port)
    return b, 1883


def read_config_broker(path):
    import os
    if not os.path.exists(path):
        return None, None
    try:
        with open(path) as f:
            doc = json.load(f)
        return doc.get("mqttbroker"), None
    except Exception:
        return None, None


def build_payload(room, status):
    return {
        "sensor_room": room,
        "status": status,
        "Moving Target": False,
        "Moving Target Dist": -1,
        "Stationary Target": True,
        "Stationary Target Dist": 561,
    }


class PresenceSimulator:
    def __init__(self, args):
        self.args = args
        self.rng = random.Random(args.seed)
        self.room_itr = None          # iterator over (room, status, until_ts)
        self.open_room = None
        self.sent = 0

    # -- dry-run -------------------------------------------------------
    def send(self, room, status, wallclock_note=""):
        payload = build_payload(room, status)
        if self.args.dry_run:
            print(f"[dry] {datetime.now():%H:%M:%S} {room:10s} {status:12s} {json.dumps(payload)}")
        else:
            info = self.client.publish(self.args.topic, json.dumps(payload), qos=0)
            self.sent += 1
            print(f"[pub] {datetime.now():%H:%M:%S} {room:10s} {status:12s}")
        if self.args.log_file:
            with open(self.args.log_file, "a") as f:
                f.write(json.dumps({"t": datetime.now().isoformat(),
                                    "room": room, "status": status}) + "\n")

    # -- one itinerary -------------------------------------------------
    def play_segment(self, room, duration_sec):
        exit_ts = time.time() + duration_sec
        self.send(room, "DETECTED")
        self.open_room = room
        while time.time() < exit_ts:
            time.sleep(min(1.0, exit_ts - time.time()))
            # occasional keep-alive heartbeat on the current room
            if int(time.time()) % self.args.keepalive_sec == 0:
                self.send(room, "KEEP_ALIVE")
        self.send(room, "NOT_DETECTED")
        self.open_room = None

    def play_day(self, segments, first_day):
        if not first_day and self.args.gap_between_days > 0:
            time.sleep(self.args.gap_between_days)
        for room, dur in segments:
            self.play_segment(room, dur)
            time.sleep(self.rng.uniform(3, 25))  # short gap while "moving"

    def run(self):
        segments = SCENARIOS[self.args.scenario](self.rng)
        feats = summarize(segments)
        print("=" * 70)
        print(f"Scenario    : {self.args.scenario}  (seed {self.args.seed})")
        print(f"Topic       : {self.args.topic}")
        print(f"Broker      : {self.args.broker_display}")
        print(f"Plan length : {feats['total_plan_hours']}h  ->", feats)
        for a in expected_alerts(feats):
            print("  expect  :", a)
        if feats.get("wake_end_offset_sec") is not None:
            off = feats["wake_end_offset_sec"]
            lo = (WAKE_RANGE[0] - off) % 86400
            hi = (WAKE_RANGE[1] - off) % 86400
            f = lambda s: "%02d:%02d" % ((s // 3600) % 24, (s % 3600) // 60)
            print(f"Wake-slot    : start the plan between {f(lo)} and {f(hi)} local so wake "
                  f"lands in the trained range {f(WAKE_RANGE[0])}-{f(WAKE_RANGE[1])} "
                  "(pass --start-at at most 60 min past your target to be safe)")
        print("=" * 70)
        if self.args.plan_only:
            return

        # default anchor: start the first segment at local midnight so the
        # wall-clock alignment of the BedRoom slot is reproducible; override
        # with --start-at.
        if self.args.start_at is not None:
            now = datetime.now()
            anchor = now.replace(hour=self.args.start_at.hour,
                                 minute=self.args.start_at.minute,
                                 second=0, microsecond=0)
            if anchor <= now:
                anchor += timedelta(days=1)
            wait = (anchor - now).total_seconds()
            print(f"Starting first segment at {anchor:%Y-%m-%d %H:%M} "
                  f"(waiting {wait/60:.0f} min). Ctrl+C to stop.")
            if wait > 0:
                time.sleep(wait)

        for day in range(self.args.days):
            print(f"\n--- day {day+1}/{self.args.days} starts "
                  f"{datetime.now():%Y-%m-%d %H:%M:%S} ---")
            self.play_day(segments, first_day=(day == 0))
        print("Scenario finished. Device will evaluate today's metrics at rollover.")


# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scenario", choices=sorted(SCENARIOS), default="normal")
    ap.add_argument("--days", type=int, default=1, help="Repeat the scenario this many days")
    ap.add_argument("--broker", default=None,
                    help="MQTT broker [host[:port] or mqtt://host:port]. Default: central unit's "
                         "config.json mqttbroker, else localhost:1883")
    ap.add_argument("--topic", default=DEFAULT_TOPIC)
    ap.add_argument("--keepalive-sec", type=int, default=45, help="Heartbeat cadence (s)")
    ap.add_argument("--gap-between-days", type=int, default=60)
    ap.add_argument("--start-at", default=None,
                    help="HH:MM -- hold until this wall-clock time before starting the first "
                         "segment. Default 'now' = begin immediately; use HH:MM to land the "
                         "sleep slot in the trained wake window (see --plan-only).")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--dry-run", action="store_true",
                    help="Print the JSON that would be published; do not connect to the broker")
    ap.add_argument("--plan-only", action="store_true",
                    help="Print the itinerary summary and expected alerts, then exit")
    ap.add_argument("--log-file", default=None, help="Append every published event as JSON Lines")
    args = ap.parse_args()

    if args.start_at:
        if args.start_at.lower() == "now":
            args.start_at = None
        else:
            try:
                args.start_at = datetime.strptime(args.start_at, "%H:%M")
            except ValueError:
                sys.exit("--start-at must be HH:MM (e.g. 23:30) or 'now'")

    cfg_broker, _ = read_config_broker(CONFIG_PATH)
    if args.broker:
        hostport = args.broker
    elif cfg_broker:
        hostport = cfg_broker
    else:
        hostport = "localhost:1883"
    host, port = parse_broker(hostport)
    args.broker_display = f"{host}:{port}" + (f" (from {CONFIG_PATH})" if (cfg_broker and not args.broker) else "")

    # --start-at: "now" or HH:MM
    if args.start_at:
        if args.start_at.lower() == "now":
            args.start_at = None
        else:
            try:
                args.start_at = datetime.strptime(args.start_at, "%H:%M")
            except ValueError:
                sys.exit("--start-at must be HH:MM (e.g. 23:30) or 'now'")

    if args.start_at:                       # parse "HH:MM" (or "now")
        if args.start_at.lower() == "now":
            args.start_at = None
        else:
            try:
                args.start_at = datetime.strptime(args.start_at, "%H:%M")
            except ValueError:
                sys.exit("--start-at must be HH:MM (e.g. 23:30) or 'now'")

    if args.start_at:
        if args.start_at.lower() == "now":
            args.start_at = None
        else:
            try:
                args.start_at = datetime.strptime(args.start_at, "%H:%M")
            except ValueError:
                sys.exit("--start-at must be HH:MM (e.g. 23:30) or 'now'")

    # ---------------- --start-at parsing ----------------
    if args.start_at:
        if args.start_at.lower() == "now":
            args.start_at = None
        else:
            try:
                args.start_at = datetime.strptime(args.start_at, "%H:%M")
            except ValueError:
                sys.exit("--start-at must be HH:MM (e.g. 23:30) or 'now'")

    sim = PresenceSimulator(args)
    if args.dry_run or args.plan_only:
        # no broker connection needed for these modes
        args.topic_send = args.topic
        sim.client = None
        sim.run()
        return

    print(f"Connecting to {host}:{port} ...")
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id=f"sim_leaf_{args.scenario}_{socket.gethostname()}")
    try:
        client.connect(host, port, keepalive=60)
    except Exception as e:
        sys.exit(f"Could not reach MQTT broker at {host}:{port}: {e}\n"
                 "Is the broker running? Override with --broker, or use --dry-run / "
                 "--plan-only to exercise the stub without a broker.")
    client.loop_start()
    sim.client = client
    try:
        sim.run()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        if sim.open_room is not None:
            sim.send(sim.open_room, "NOT_DETECTED")  # leave the device state clean
    finally:
        client.loop_stop()
        client.disconnect()
        print(f"Total messages sent: {sim.sent}")


if __name__ == "__main__":
    main()