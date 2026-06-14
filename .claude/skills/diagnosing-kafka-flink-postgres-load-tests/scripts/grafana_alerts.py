#!/usr/bin/env python3
"""Pair Grafana alert state transitions into fired/resolved episodes over a window.

Grafana's unified alerting writes one annotation row per state change (Normal -> Pending ->
Alerting/Error -> Normal) into its SQLite store, not a single fired/resolved record. This walks
those transitions per alert instance and pairs each departure from Normal (fired) with the next
return to Normal (resolved), so the bundle can show the resolve time next to the fire time.

Reads a copy of grafana.db (the caller copies it out of the container, read-only); it never
touches the live stack or needs Grafana credentials.
"""
import argparse
import re
import sqlite3
import sys
import time

# Grafana state strings can carry a parenthetical reason, e.g. "Normal (MissingSeries)".
# "Normal" is the only inactive state; everything else is an active (raised) state.
def is_normal(state):
    return (state or "").startswith("Normal")


# Peak severity to label an episode by: the worst state it reached between fire and resolve.
# Pending = condition true, waiting on `for`; Alerting = actually firing; Error/NoData = the
# DatasourceError / missing-data states Grafana raises when an evaluation can't be made.
SEVERITY = {"Pending": 1, "NoData": 2, "Error": 3, "Alerting": 4}


def base_state(state):
    return re.sub(r"\s*\(.*\)$", "", state or "").strip()


def alert_label(text):
    """Split the annotation text 'Alert name {k=v, ...} - <eval values>' into (name, labels).

    Only the leading name and first label set identify the instance; the trailing ' - ...' carries
    per-evaluation values that change every tick, so it is intentionally ignored (keying on it would
    stop a fire row from ever matching its resolve row)."""
    m = re.match(r"^(.*?)\s*\{(.*?)\}", text or "", re.S)
    if not m:
        return (text or "").strip(), ""
    name, body = m.group(1).strip(), m.group(2)
    drop = {"alertname", "grafana_folder"}
    parts = []
    for kv in body.split(","):
        if "=" not in kv:
            continue
        k, v = kv.split("=", 1)
        k, v = k.strip(), v.strip()
        if k and not k.startswith("__") and k not in drop:
            parts.append(f"{k}={v}")
    return name, ", ".join(parts)


def fmt(ms):
    return time.strftime("%H:%M:%S", time.localtime(ms / 1000))


def human(secs):
    secs = int(secs)
    if secs < 60:
        return f"{secs}s"
    if secs < 3600:
        return f"{secs // 60}m{secs % 60:02d}s"
    return f"{secs // 3600}h{(secs % 3600) // 60:02d}m"


def episodes(rows, window_start_ms):
    """Pair transition rows (ordered by epoch) into (fire, resolve, peak_state, name, labels)."""
    open_by_key = {}  # (name, labels) -> dict(fire, peak)
    out = []
    for r in rows:
        name, labels = alert_label(r["text"])
        key = (name, labels)
        new = r["new_state"]
        if is_normal(new):
            o = open_by_key.pop(key, None)
            if o:
                out.append((o["fire"], r["epoch"], o["peak"], name, labels))
            else:
                # Resolve with no in-window fire: the episode began before the window.
                out.append((None, r["epoch"], base_state(r["prev_state"]), name, labels))
        else:
            o = open_by_key.get(key)
            sev = base_state(new)
            if o is None:
                open_by_key[key] = {"fire": r["epoch"], "peak": sev}
            elif SEVERITY.get(sev, 0) > SEVERITY.get(o["peak"], 0):
                o["peak"] = sev
    for (name, labels), o in open_by_key.items():
        out.append((o["fire"], None, o["peak"], name, labels))
    out.sort(key=lambda e: (e[0] if e[0] is not None else window_start_ms))
    return out


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", required=True, help="path to a copy of grafana.db")
    p.add_argument("--since", type=int, default=10800, help="lookback window in seconds")
    args = p.parse_args()

    start_ms = int((time.time() - args.since) * 1000)
    try:
        db = sqlite3.connect(args.db)
    except sqlite3.Error as e:
        print(f"(could not open grafana db: {e})")
        return
    db.row_factory = sqlite3.Row
    try:
        rows = db.execute(
            "SELECT epoch, epoch_end, prev_state, new_state, text "
            "FROM annotation WHERE epoch >= ? ORDER BY epoch",
            (start_ms,),
        ).fetchall()
    except sqlite3.Error as e:
        print(f"(no alert annotations available: {e})")
        return

    eps = episodes(rows, start_ms)
    if not eps:
        print("(no alerts fired in window)")
        return

    for fire, resolve, peak, name, labels in eps:
        fired = fmt(fire) if fire else "<before window>"
        if resolve and fire:
            resolved = f"{fmt(resolve)}  ({human((resolve - fire) / 1000)})"
        elif resolve:
            resolved = fmt(resolve)
        else:
            resolved = "— still active"
        tail = f"  | {labels}" if labels else ""
        print(f"FIRED {fired:>15}   RESOLVED {resolved:<22} [{peak}]  {name}{tail}")


if __name__ == "__main__":
    main()
