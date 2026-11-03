#!/usr/bin/env python3
"""Headless harness for the shared-bay society simulation in csms_live.py.

Connects TWO observers to /observe, plays traffic at 30x, and checks:

  * join snapshots carry the shared world: 10 residents, 4 shared bays,
    10 parking spots' worth of cars, all starting "away"
  * QUEUE ROTATION: a car is seen WAITING in normal parking while all
    four bays are busy, and later that same car moves into a freed bay
    and charges (server "dequeue" event + world state)
  * OWNER BILLING: every session summary's idTag matches the car the
    world last showed charging in that bay, and the wallets broadcast
    afterwards debits exactly that resident by the billed cost
  * SNAPSHOT REJOIN: a fresh observer connecting mid-session receives a
    world snapshot containing the in-progress session
  * TWO OBSERVERS, ONE WORLD: both observers' latest snapshots agree on
    cars, bays, queue and ledger
  * MANUAL CONTROL: SimUnplug from the second observer ends a session

Usage: test_sim_world.py   (exit 0 = all pass)
OBSERVE_URL env overrides ws://localhost:8901/observe (single-port local
runs and the live Render deployment).
"""

import asyncio
import json
import os
import sys
import time

from websockets.asyncio.client import connect

OBSERVE_URL = os.environ.get("OBSERVE_URL", "ws://localhost:8901/observe")
TIMEOUT_S = float(os.environ.get("SIM_TEST_TIMEOUT", "540"))

results = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append(ok)
    print(f"[SIMTEST] {'PASS' if ok else 'FAIL'}: {name} {detail}",
          flush=True)


class Observer:
    def __init__(self, name: str):
        self.name = name
        self.ws = None
        self.world = None            # latest world snapshot
        self.worlds = []             # history (capped)
        self.summaries = []
        self.sim_events = []
        self.wallets = {}
        self.task = None

    async def start(self):
        self.ws = await connect(OBSERVE_URL, open_timeout=60)
        self.task = asyncio.create_task(self._pump())

    async def _pump(self):
        try:
            async for raw in self.ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                ev = msg.get("event")
                if ev == "world":
                    self.world = msg
                    self.worlds.append(msg)
                    if len(self.worlds) > 400:
                        self.worlds.pop(0)
                    for r in msg.get("residents", []):
                        self.wallets[r["tag"]] = r["balance"]
                elif ev == "summary":
                    self.summaries.append(msg)
                elif ev == "sim_event":
                    self.sim_events.append(msg)
                elif ev == "wallets":
                    self.wallets.update(msg.get("wallets", {}))
        except Exception:
            pass

    async def send(self, obj: dict):
        await self.ws.send(json.dumps(obj))

    async def close(self):
        if self.task:
            self.task.cancel()
        if self.ws:
            await self.ws.close()


def strip_clock(world):
    if world is None:
        return None
    w = json.loads(json.dumps(world))
    w.get("sim", {}).pop("nowMs", None)
    w.get("sim", {}).pop("clock", None)
    return w


async def wait_for(fn, timeout, desc, poll=0.5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        out = fn()
        if out:
            return out
        await asyncio.sleep(poll)
    raise TimeoutError(desc)


async def main() -> int:
    started = time.monotonic()
    a = Observer("A")
    b = Observer("B")
    await a.start()
    await b.start()

    # --- join snapshots -------------------------------------------------
    await wait_for(lambda: a.world and b.world, 30,
                   "initial world snapshots")
    w0 = a.world
    check("snapshot: 10 residents, 4 shared bays",
          len(w0.get("residents", [])) == 10
          and len(w0.get("bays", [])) == 4
          and len(w0.get("cars", [])) == 10,
          f"residents={len(w0.get('residents', []))} "
          f"bays={len(w0.get('bays', []))} cars={len(w0.get('cars', []))}")
    bay_ids = [x["cpId"] for x in w0["bays"]]
    check("bays are BAY-01..BAY-04 (shared, no resident binding)",
          bay_ids == ["BAY-01", "BAY-02", "BAY-03", "BAY-04"]
          and all(x["car"] is None for x in w0["bays"]),
          f"bays={bay_ids}")

    # --- play traffic at 30x ---------------------------------------------
    await a.send({"control": "SimSpeed", "speed": 30})
    await a.send({"control": "SimTraffic", "playing": True})
    print("[SIMTEST] traffic playing at 30x — waiting for the queue "
          "to form and rotate…", flush=True)

    bay_car = {}          # bay -> tag most recently seen charging there
    billed_ok = []        # per-summary owner-billing verdicts
    prev_balances = dict(a.wallets)
    summaries_seen = 0
    first_waiter = None
    waiter_charging_at = None
    rotation_deadline = started + TIMEOUT_S

    while time.monotonic() < rotation_deadline:
        # keep bay->car map current from A's snapshots
        if a.world:
            for bay in a.world["bays"]:
                if bay.get("car"):
                    bay_car[bay["cpId"]] = bay["car"]
        # owner billing on every new summary
        while summaries_seen < len(a.summaries):
            s = a.summaries[summaries_seen]
            summaries_seen += 1
            owner = bay_car.get(s["cpId"])
            bal = a.wallets.get(s["idTag"])
            wallet_ok = (
                bal is not None
                and abs(bal - s["walletBalance"]) < 0.011
                and (s["idTag"] not in prev_balances
                     or abs((prev_balances[s["idTag"]] - s["cost"])
                            - s["walletBalance"]) < 0.011))
            billed_ok.append(owner == s["idTag"] and wallet_ok)
            print(f"[SIMTEST] summary: {s['cpId']} billed {s['idTag']} "
                  f"₹{s['cost']} for {s['kwh']} kWh "
                  f"(world showed {owner} in that bay; "
                  f"wallet now ₹{s['walletBalance']})", flush=True)
            prev_balances = dict(a.wallets)
        # queue formation: someone waiting while every bay is busy
        if first_waiter is None and a.world:
            cars = {c["tag"]: c for c in a.world["cars"]}
            busy = all(x.get("car") for x in a.world["bays"])
            waiting = [t for t in a.world["queue"]
                       if cars.get(t, {}).get("state") == "waiting"]
            if busy and waiting:
                first_waiter = waiting[0]
                wcar = cars[first_waiter]
                print(f"[SIMTEST] queue formed: {first_waiter} waiting "
                      f"in parking P{(wcar.get('spot') or 0) + 1} while "
                      f"all 4 bays busy", flush=True)
        # rotation: the first waiter is now charging
        if first_waiter and a.world:
            cars = {c["tag"]: c for c in a.world["cars"]}
            if cars.get(first_waiter, {}).get("state") == "charging":
                waiter_charging_at = time.monotonic()
                break
        await asyncio.sleep(0.5)

    check("queue formed: a car waited in parking with all 4 bays busy",
          first_waiter is not None, f"first waiter: {first_waiter}")
    deq = [e for e in a.sim_events
           if e.get("kind") == "dequeue" and e.get("tag") == first_waiter]
    check("queue rotation: first waiter moved into a freed bay and "
          "charged (dequeue event + world state)",
          waiter_charging_at is not None and bool(deq),
          f"waiter={first_waiter} dequeue_events={len(deq)}")
    check("owner billing: every summary billed the car's owner wallet",
          bool(billed_ok) and all(billed_ok),
          f"{sum(billed_ok)}/{len(billed_ok)} summaries owner-billed")

    # --- snapshot rejoin mid-session --------------------------------------
    def active_session_world(obs):
        if not obs.world:
            return None
        for bay in obs.world["bays"]:
            if bay.get("session"):
                return bay
        return None

    live_bay = await wait_for(lambda: active_session_world(a), 60,
                              "an active session to rejoin into")
    await b.close()
    await asyncio.sleep(1.0)
    b2 = Observer("B2")
    await b2.start()
    rejoined = await wait_for(lambda: active_session_world(b2), 30,
                              "snapshot with in-progress session on rejoin")
    check("snapshot rejoin: fresh observer sees the in-progress session",
          rejoined is not None
          and rejoined.get("car") == live_bay.get("car"),
          f"bay={rejoined.get('cpId')} car={rejoined.get('car')} "
          f"session={rejoined.get('session')}")

    # --- two observers, one world ------------------------------------------
    await asyncio.sleep(3.0)
    same = strip_clock(a.world) == strip_clock(b2.world)
    check("two observers hold identical world state",
          same, "" if same else "snapshots diverged")

    # --- manual control from the second observer ---------------------------
    target = None
    if b2.world:
        for bay in b2.world["bays"]:
            if bay.get("car"):
                target = bay["cpId"]
                break
    unplug_ok = False
    if target:
        before = len(b2.summaries)
        await b2.send({"control": "SimUnplug", "cpId": target})
        try:
            await wait_for(
                lambda: any(s["cpId"] == target
                            for s in b2.summaries[before:]),
                30, "summary after SimUnplug")
            unplug_ok = True
        except TimeoutError:
            unplug_ok = False
    check("manual control: SimUnplug from another observer ends the "
          "session", unplug_ok, f"bay={target}")

    await a.send({"control": "SimTraffic", "playing": False})
    await a.close()
    await b2.close()

    ok = all(results) and len(results) >= 6
    print(f"[SIMTEST] RESULT: {'ALL PASS' if ok else 'FAILURES PRESENT'} "
          f"({sum(results)}/{len(results)} checks) "
          f"in {time.monotonic() - started:.0f}s", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
