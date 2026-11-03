#!/usr/bin/env python3
"""Headless check for the day-simulator upgrade in csms_live.py.

Connects one observer to /observe, sets speed 1440x (a full sim day in
~60 s real time), plays traffic, and checks:

  * SPEEDS: the world snapshot accepts and reports speed 1440
  * DAY TALLY RESET: when the sim date rolls over, snapshots carry the
    new date with a fresh tally, and that fresh tally exactly matches
    the (atSimDate-stamped) summaries billed after midnight
  * TALLY = SUMMARIES: the last tally of the starting day, minus the
    tally at join, exactly matches the summaries billed that day
    (sessions, kWh, revenue; cars served = distinct billed tags)
  * BAND MATH: every summary's cost equals the sum of its per-band
    breakdown, and each band's cost equals its kWh x its rate (so the
    large high-speed meter steps did not corrupt ToU billing)
  * PATTERNS: sim events show the day's shape — at least one morning
    departure (07:00-09:00 "left the building") and at least one
    evening-rush arrival/charge event (17:00-20:00)

Usage: test_day_sim.py   (exit 0 = all pass)
OBSERVE_URL env overrides ws://localhost:8901/observe.
DAY_TEST_TIMEOUT env (seconds, real time) defaults to 240.
"""

import asyncio
import json
import os
import sys
import time
from datetime import datetime

from websockets.asyncio.client import connect

OBSERVE_URL = os.environ.get("OBSERVE_URL", "ws://localhost:8901/observe")
TIMEOUT_S = float(os.environ.get("DAY_TEST_TIMEOUT", "240"))

results = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append(ok)
    print(f"[DAYTEST] {'PASS' if ok else 'FAIL'}: {name} {detail}",
          flush=True)


def sim_dt(world):
    if not world:
        return None
    ms = world.get("sim", {}).get("nowMs")
    return datetime.fromtimestamp(ms / 1000.0) if ms else None


def tally_delta_matches(t_from, t_to, summaries):
    """(t_to - t_from) == sums over `summaries`? Returns (ok, detail)."""
    exp_sess = len(summaries)
    exp_kwh = round(sum(s["kwh"] for s in summaries), 3)
    exp_rev = round(sum(s["cost"] for s in summaries), 2)
    got_sess = t_to.get("sessions", 0) - t_from.get("sessions", 0)
    got_kwh = round(t_to.get("kwh", 0) - t_from.get("kwh", 0), 3)
    got_rev = round(t_to.get("revenue", 0) - t_from.get("revenue", 0), 2)
    ok = (got_sess == exp_sess and abs(got_kwh - exp_kwh) < 0.011
          and abs(got_rev - exp_rev) < 0.011)
    return ok, (f"sessions {got_sess} vs {exp_sess}, "
                f"kwh {got_kwh} vs {exp_kwh}, "
                f"revenue {got_rev} vs {exp_rev}")


async def main() -> int:
    started = time.monotonic()
    summaries = []          # summary dicts (carry atSimDate from server)
    events = []             # (sim datetime, sim_event dict)
    state = {"world": None}

    async with connect(OBSERVE_URL, open_timeout=60) as ws:

        async def pump():
            try:
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    ev = msg.get("event")
                    if ev == "world":
                        state["world"] = msg
                    elif ev == "summary":
                        summaries.append(msg)
                    elif ev == "sim_event":
                        hour = None
                        at = msg.get("atSim") or ""
                        try:
                            hour = int(at.split(":")[0])
                        except (ValueError, IndexError):
                            hour = None
                        if hour is None:
                            d = sim_dt(state["world"])
                            hour = d.hour if d else None
                        events.append((hour, msg))
            except Exception:
                pass

        pump_task = asyncio.create_task(pump())

        deadline = time.monotonic() + 30
        while state["world"] is None and time.monotonic() < deadline:
            await asyncio.sleep(0.25)
        if state["world"] is None:
            check("join snapshot received", False, "no world in 30s")
            return 1
        start_dt = sim_dt(state["world"])
        start_date = start_dt.date().isoformat()
        t_join = dict(state["world"].get("dayTally") or {})
        print(f"[DAYTEST] joined at sim {start_dt} (tally at join: "
              f"{t_join}) — setting 1440x, playing traffic", flush=True)
        await ws.send(json.dumps({"control": "SimSpeed", "speed": 1440}))
        await ws.send(json.dumps({"control": "SimTraffic",
                                  "playing": True}))

        saw_1440 = False
        last_tally_by_date = {start_date: t_join}
        rollover = None       # (new_date, pre_tally, fresh_tally, sums)
        deadline = time.monotonic() + TIMEOUT_S
        while time.monotonic() < deadline:
            w = state["world"]
            if w:
                if w.get("sim", {}).get("speed") == 1440:
                    saw_1440 = True
                tally = dict(w.get("dayTally") or {})
                d = sim_dt(w)
                if tally.get("date") and d:
                    cur_date = tally["date"]
                    if cur_date != start_date and rollover is None:
                        rollover = (
                            cur_date,
                            dict(last_tally_by_date.get(start_date,
                                                        t_join)),
                            tally,
                            list(summaries))
                        print(f"[DAYTEST] sim midnight crossed: "
                              f"{start_date} -> {cur_date}; pre "
                              f"{rollover[1]}; fresh {tally} — "
                              f"continuing into the new morning",
                              flush=True)
                    last_tally_by_date[cur_date] = tally
            # Done once we have the rollover AND the new day's morning
            # departure window (07:00-09:00) has produced a leave event;
            # the evening rush belongs to the starting day either way.
            if rollover is not None and any(
                    h is not None and 7 <= h < 9
                    and e.get("kind") == "leave" for h, e in events):
                break
            await asyncio.sleep(0.25)
        await ws.send(json.dumps({"control": "SimTraffic",
                                   "playing": False}))
        await asyncio.sleep(0.5)
        pump_task.cancel()

    check("speed 1440x accepted and reported by the world", saw_1440)

    def day_sums(date_str, pool):
        return [s for s in pool if s.get("atSimDate") == date_str]

    # --- tally vs summaries ---------------------------------------------
    if rollover:
        new_date, pre_tally, fresh_tally, sums_at_roll = rollover
        ok1, det1 = tally_delta_matches(
            t_join, pre_tally, day_sums(start_date, sums_at_roll))
        cars_ok = True
        if t_join.get("sessions", 0) == 0:
            cars_ok = (
                pre_tally.get("carsServed")
                == len({s["idTag"]
                        for s in day_sums(start_date, sums_at_roll)}))
        check("day tally (starting day) matches its billed summaries",
              ok1 and cars_ok, det1 + f"; cars_ok={cars_ok}")
        fresh_sums = day_sums(new_date, sums_at_roll)
        ok2, det2 = tally_delta_matches(
            {"sessions": 0, "kwh": 0.0, "revenue": 0.0},
            fresh_tally, fresh_sums)
        fresh_cars = (
            fresh_tally.get("carsServed")
            == len({s["idTag"] for s in fresh_sums}))
        check("day tally resets at sim midnight (new date, fresh "
              "totals matching post-midnight summaries)",
              ok2 and fresh_cars and fresh_tally.get("date") == new_date,
              det2 + f"; cars_ok={fresh_cars}")
    else:
        final_tally = dict((state["world"] or {}).get("dayTally") or {})
        ok, det = tally_delta_matches(
            t_join, final_tally, day_sums(start_date, summaries))
        check("day tally matches its billed summaries (no rollover "
              "seen)", ok and final_tally.get("date") == start_date, det)
        check("day tally resets at sim midnight (new date, fresh "
              "totals matching post-midnight summaries)", False,
              "no sim-midnight rollover observed within timeout")

    check("fast-forward day produced billed sessions",
          len(summaries) >= 3,
          f"{len(summaries)} summaries, dates "
          f"{sorted({s.get('atSimDate') for s in summaries})}")

    # --- per-band billing math on every summary ---------------------------
    band_ok = bool(summaries)
    worst = ""
    for s in summaries:
        bands = s.get("bands") or []
        if not bands:
            band_ok, worst = False, f"tx={s.get('transactionId')} no bands"
            break
        bk = round(sum(b["kwh"] for b in bands), 3)
        bc = round(sum(b["cost"] for b in bands), 2)
        if abs(bk - s["kwh"]) > 0.011 or abs(bc - s["cost"]) > 0.011:
            band_ok = False
            worst = (f"tx={s.get('transactionId')} bands kwh={bk} "
                     f"cost={bc} vs summary kwh={s['kwh']} "
                     f"cost={s['cost']}")
            break
        for b in bands:
            # Band kWh is rounded to 3dp and cost to 2dp in the summary,
            # so kwh*rate can differ from cost by up to rate*0.0005 +
            # 0.005 (~0.014 at the peak rate) without any billing error.
            if abs(b["kwh"] * b["rate"] - b["cost"]) > 0.02:
                band_ok, worst = False, f"band {b} cost != kwh*rate"
                break
        if not band_ok:
            break
    check("every summary's cost/kWh equals its per-band breakdown "
          "(ToU intact at 1440x)", band_ok, worst)

    # --- time-of-day patterns ---------------------------------------------
    morning_dep = any(
        h is not None and 7 <= h < 9 and e.get("kind") == "leave"
        for h, e in events)
    evening_arr = any(
        h is not None and 17 <= h < 20
        and e.get("kind") in ("arrive", "charge_start", "wait", "dequeue")
        for h, e in events)
    check("pattern: morning departures seen (07:00-09:00 leaves)",
          morning_dep, f"events={len(events)}")
    check("pattern: evening return rush seen (17:00-20:00 arrivals)",
          evening_arr)

    ok = all(results) and len(results) >= 6
    print(f"[DAYTEST] RESULT: {'ALL PASS' if ok else 'FAILURES PRESENT'} "
          f"({sum(results)}/{len(results)} checks) "
          f"in {time.monotonic() - started:.0f}s real time", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
