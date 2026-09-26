"""
extract_features.py
--------------------
Reads the daily interval CSV files logged by the S3's logIntervalToSD()
(one file per day, rows: room,start_epoch,end_epoch,duration_sec) and builds
a per-day feature table suitable for training an Isolation Forest:
    date,bathroom_visit_count,bathroom_total_duration_sec,
    bathroom_max_duration_sec,wake_time_sec_of_day

TIMEZONE NOTE (verified against the 2026-09-21..2026-11-20 dataset):
The ESP32-S3 calls configTime(gmtOffset_sec=5*3600+1800, 0, ntpServer).
On this firmware the system clock ITSELF is advanced by that offset, so the
epoch values that end up in the CSV files are LOCAL wall-clock seconds
(IST, GMT+5:30), not true UTC. Evidence: the daily file names are produced
with localtime(endEpoch), and the epochs inside each file line up with the
file-name date with no extra timezone math. Therefore this script must NOT
re-apply a timezone offset: treat each epoch as "wall-clock seconds" and
parse it with pd.to_datetime(..., unit="s"), which yields a naive datetime
whose hour/minute/second fields are the correct local wall-clock values.
Do NOT switch to datetime.fromtimestamp() unless you also change the
firmware to log true UTC.

ROOM NAMES (must match the leaf firmware exactly):
    LivingRoom, BedRoom, Toilet-1, Kitchen   (as found in the data)
There is no "BathRoom" in the data -- the bathroom sensor on the leaves is
the room named "Toilet-1", so --bathroom-room now defaults to "Toilet-1".

WAKE-TIME DEFINITION (kept consistent with process.ino / maybeRecordWakeTime):
Per calendar date, take the day's BedRoom intervals and merge back-to-back
fragments that are <= --sleep-merge-gap-sec apart (the LD2410c briefly
loses lock on a very still sleeping person, fragmenting one night into many
short intervals that none would pass a duration threshold on their own).
Then pick the LONGEST merged session that lasted at least
--min-sleep-duration-sec; wake time = seconds-of-day at the END of that
session, attributed to the session's END calendar date.
The old --sleep-start-hour-min/max bedtime window is kept for users who
want to restrict to night sessions, but it now DEFAULTS to the whole day
(0..24 = no restriction). Rationale: the provided training dataset was
generated with bedroom sessions starting between 12:00 and 20:59, so the
old 21:00-02:00 window matched nothing and wake_time_sec_of_day came out
100% NaN (which then made train_isolation_forest.py drop every row). The
"longest session of the day" rule is robust to any sleep schedule and is
what the on-device firmware implements.

Usage:
    pip install pandas
    python extract_features.py --logdir /path/to/sd_card/intervals --out features.csv
"""

import argparse
import glob
import os

import pandas as pd


def load_all_intervals(logdir: str) -> pd.DataFrame:
    rows = []
    for path in sorted(glob.glob(os.path.join(logdir, "*.csv"))):
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(",")
                if len(parts) != 4:
                    continue  # skip malformed lines rather than crash the whole run
                room, start_epoch, end_epoch, duration_sec = parts
                rows.append({
                    "room": room.strip(),
                    "start_epoch": int(start_epoch),
                    "end_epoch": int(end_epoch),
                    "duration_sec": int(duration_sec),
                })
    if not rows:
        raise RuntimeError(f"No interval rows found under {logdir}")
    df = pd.DataFrame(rows)

    # DIAGNOSTIC: print exactly what room name strings actually exist in the
    # data. If your bathroom/bedroom names aren't here verbatim, the filters
    # below (which are case/whitespace tolerant but otherwise exact matches)
    # will silently find nothing.
    print("Room name counts found in the data (check these match --bathroom-room / --bedroom-room):")
    print(df["room"].value_counts().to_string())
    print()

    # Epochs are LOCAL wall-clock seconds (see module docstring): build naive
    # datetimes whose fields ARE the local wall time. unit="s" keeps this
    # laptop-independent and avoids the deprecated datetime.utcfromtimestamp.
    df["start_dt"] = pd.to_datetime(df["start_epoch"], unit="s")
    df["end_dt"] = pd.to_datetime(df["end_epoch"], unit="s")
    # Attribute each interval to the calendar date on which it ENDED -- the
    # firmware also counts a visit on the day it ends (WhenRoomIsEmpty).
    df["date"] = df["end_dt"].dt.date
    return df


def _matches_room(series: pd.Series, target: str) -> pd.Series:
    """Case- and whitespace-tolerant room-name comparison."""
    return series.str.strip().str.casefold() == target.strip().casefold()


def merge_close_intervals(room_df: pd.DataFrame, max_gap_sec: int) -> pd.DataFrame:
    """
    Merges consecutive intervals of the SAME room into one continuous
    session when the gap between them is <= max_gap_sec. This exists
    because mmWave sensors like the LD2410c can briefly lose lock on a
    very still sleeping person, fragmenting one long overnight session
    into many short ones -- none of which individually looks like "sleep"
    to a simple duration threshold, even though the person never got up.
    Sessions are merged TRANSITIVELY (a chain of gaps each <= max_gap_sec
    becomes a single session), identical to the on-device chaining in
    process.ino's maybeRecordWakeTime().
    """
    if room_df.empty:
        return room_df
    sorted_df = room_df.sort_values("start_epoch").reset_index(drop=True)
    merged = []
    cur_start = sorted_df.loc[0, "start_epoch"]
    cur_end = sorted_df.loc[0, "end_epoch"]
    for i in range(1, len(sorted_df)):
        s, e = sorted_df.loc[i, "start_epoch"], sorted_df.loc[i, "end_epoch"]
        if s - cur_end <= max_gap_sec:
            cur_end = max(cur_end, e)  # same session, absorb the fragment
        else:
            merged.append((cur_start, cur_end))
            cur_start, cur_end = s, e
    merged.append((cur_start, cur_end))
    out = pd.DataFrame(merged, columns=["start_epoch", "end_epoch"])
    out["duration_sec"] = out["end_epoch"] - out["start_epoch"]
    out["start_dt"] = pd.to_datetime(out["start_epoch"], unit="s")
    out["end_dt"] = pd.to_datetime(out["end_epoch"], unit="s")
    return out


def compute_wake_times(df: pd.DataFrame, bedroom_name: str, merge_gap_sec: int,
                       min_sleep_duration_sec: int,
                       sleep_start_hour_min: int, sleep_start_hour_max: int) -> dict:
    """
    Computes one wake-time-of-day value per calendar date.

    Uses a GLOBAL merge across the whole dataset (not per-day) -- this
    correctly handles a session that starts before midnight and ends after
    it, which a per-day grouping would otherwise split in two. A session is
    attributed to its END date, matching how the firmware attributes
    everything to the day the interval ends.
    """
    bedroom_raw = df[_matches_room(df["room"], bedroom_name)]

    print(f"Raw {bedroom_name} interval durations (seconds) -- "
          f"if these are mostly short even though the person slept fine, "
          f"that's sensor flicker fragmenting the night into pieces:")
    print(bedroom_raw["duration_sec"].describe().to_string())
    print()

    merged = merge_close_intervals(bedroom_raw, merge_gap_sec)
    print(f"After merging fragments separated by <= {merge_gap_sec}s gaps, "
          f"{len(bedroom_raw)} raw intervals became {len(merged)} sessions. "
          f"Merged session durations (seconds):")
    print(merged["duration_sec"].describe().to_string())
    print()

    # Optional restriction to a bedtime window; whole-day (0..24) by default
    # because the current training data's bedroom sessions start midday.
    if sleep_start_hour_min < sleep_start_hour_max:
        in_window = (
            (merged["start_dt"].dt.hour >= sleep_start_hour_min)
            | (merged["start_dt"].dt.hour < sleep_start_hour_max)
        )
    else:  # degenerate/default window covers the whole day -- no restriction
        in_window = pd.Series(True, index=merged.index)

    candidates = merged[in_window & (merged["duration_sec"] >= min_sleep_duration_sec)]
    by_hour = candidates["start_dt"].dt.hour.value_counts().sort_index()
    print(f"{len(candidates)} merged sessions qualify as a sleep episode "
          f"(duration >= {min_sleep_duration_sec / 3600:g}h"
          + (f", start hour in [{sleep_start_hour_min},{sleep_start_hour_max})"
             if sleep_start_hour_min < sleep_start_hour_max else
             ", no start-hour restriction")
          + "). Start-hour distribution of qualifying sessions:")
    print(by_hour.to_string() if not by_hour.empty else "(none)")
    print()

    if candidates.empty:
        print("WARNING: zero qualifying sleep sessions -- wake_time_sec_of_day will be NaN "
              "for every date. Lower --min-sleep-duration-sec, or widen "
              "--sleep-start-hour-min/max, or check that --bedroom-room matches the data.\n")
        return {}

    # Wake time = seconds-of-day at the END of the day's LONGEST qualifying
    # merged session (the main sleep episode), attributed to the END date.
    # Kept in sync with the firmware: for every date, the longest qualifying
    # session's end becomes that day's wake time.
    wake_by_date = {}
    for _, row in candidates.sort_values("duration_sec", ascending=False).iterrows():
        date = row["end_dt"].date()
        if date in wake_by_date:
            continue  # keep the longest session for this date, first wins
        wake_sec = row["end_dt"].hour * 3600 + row["end_dt"].minute * 60 + row["end_dt"].second
        wake_by_date[date] = int(wake_sec)
    return wake_by_date


def build_features(df: pd.DataFrame, bathroom_name: str, bedroom_name: str,
                   merge_gap_sec: int, min_sleep_duration_sec: int,
                   sleep_start_hour_min: int, sleep_start_hour_max: int) -> pd.DataFrame:
    wake_by_date = compute_wake_times(
        df, bedroom_name, merge_gap_sec, min_sleep_duration_sec,
        sleep_start_hour_min, sleep_start_hour_max)

    records = []
    for date, day_df in df.groupby("date"):
        # NOTE: bathroom features are computed from the RAW intervals of that
        # day (no merging) -- this is exactly what the firmware does, since it
        # increments its per-day counters once per WhenRoomIsEmpty().
        bathroom = day_df[_matches_room(day_df["room"], bathroom_name)]
        record = {
            "date": date,
            "bathroom_visit_count": len(bathroom),
            "bathroom_total_duration_sec": int(bathroom["duration_sec"].sum()),
            "bathroom_max_duration_sec": int(bathroom["duration_sec"].max()) if len(bathroom) else 0,
            "wake_time_sec_of_day": wake_by_date.get(date, float("nan")),
            # EXTENSION POINT: once outing detection is added to the firmware
            # and logged (e.g. as a synthetic "Outing" room in the interval
            # log), add outing_count / outing_duration_sec / outing_occurred
            # columns here so training and runtime features stay in sync.
        }
        records.append(record)
    feat_df = pd.DataFrame(records).sort_values("date").reset_index(drop=True)
    return feat_df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--logdir", required=True, help="Folder containing the daily interval CSV files")
    ap.add_argument("--out", default="features.csv", help="Output feature table path")
    ap.add_argument("--bathroom-room", default="Toilet-1",
                    help="Exact room name your leaf firmware uses for the bathroom sensor. "
                         "The data uses 'Toilet-1' (there is no 'BathRoom'). "
                         "Matching is case/whitespace tolerant.")
    ap.add_argument("--bedroom-room", default="BedRoom",
                    help="Exact room name your leaf firmware uses for the bedroom sensor")
    ap.add_argument("--sleep-merge-gap-sec", type=int, default=300,
                    help="BedRoom intervals separated by a gap smaller than this (seconds) are "
                         "merged into one continuous sleep session, to compensate for the sensor "
                         "briefly losing lock on a very still sleeping person. Default 300 (5 min).")
    ap.add_argument("--min-sleep-duration-sec", type=int, default=3 * 3600,
                    help="A merged BedRoom session must last at least this long to count as a "
                         "sleep episode whose end is the wake time. Default 10800 (3 h).")
    ap.add_argument("--sleep-start-hour-min", type=int, default=0,
                    help="Optional bedtime window (24h): only sessions STARTING at or after "
                         "this hour qualify as sleep episodes. Default 0 = no restriction "
                         "(the training data's bedroom sessions start midday, so a night-only "
                         "window would match nothing).")
    ap.add_argument("--sleep-start-hour-max", type=int, default=24,
                    help="Optional bedtime window (24h): sessions starting before this hour also "
                         "count. Default 24 = no restriction. Set e.g. --sleep-start-hour-min 21 "
                         "--sleep-start-hour-max 2 for a strict night-time window.")
    args = ap.parse_args()

    df = load_all_intervals(args.logdir)
    feat_df = build_features(df, args.bathroom_room, args.bedroom_room,
                             args.sleep_merge_gap_sec, args.min_sleep_duration_sec,
                             args.sleep_start_hour_min, args.sleep_start_hour_max)
    feat_df.to_csv(args.out, index=False)
    print(f"Wrote {len(feat_df)} daily feature rows to {args.out}")
    print(feat_df.describe(include="all"))
    n_missing_wake = feat_df["wake_time_sec_of_day"].isna().sum()
    if n_missing_wake == len(feat_df):
        print("WARNING: wake_time_sec_of_day is NaN for EVERY date. The Isolation Forest will "
              "have nothing to learn from that feature; see the diagnostics above.")
    elif n_missing_wake:
        print(f"NOTE: wake_time_sec_of_day is NaN for {n_missing_wake}/{len(feat_df)} dates "
              f"(no qualifying sleep episode); those rows will be dropped at training time.")


if __name__ == "__main__":
    main()