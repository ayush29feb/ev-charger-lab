#!/usr/bin/env python3
"""Live OCPP 1.6J CSMS — the real protocol, no emulation.

Unlike the in-page web simulator (which only *pretends* to speak OCPP inside
one HTML file), this server speaks genuine OCPP 1.6J on a real WebSocket:

  OCPP endpoint     ws://0.0.0.0:9001/<chargePointId>   (subprotocol ocpp1.6)
  Observer endpoint ws://0.0.0.0:9001/observe
  Static files      http://0.0.0.0:8080/  (this folder, stdlib http.server)

Charge points connect to the OCPP endpoint and exchange real frames:

  [2, "<id>", "<Action>", { ... }]   CALL
  [3, "<id>", { ... }]               CALLRESULT
  [4, "<id>", "<err>", "<desc>", {}] CALLERROR

Handled charge-point -> CSMS calls: BootNotification, Authorize,
StartTransaction, MeterValues, StatusNotification, Heartbeat,
StopTransaction, DataTransfer, DiagnosticsStatusNotification,
FirmwareStatusNotification.

The CSMS can also *initiate* calls (RemoteStartTransaction,
RemoteStopTransaction, Reset, UnlockConnector, ChangeAvailability,
TriggerMessage, ...) — pending replies are matched by message id with
asyncio futures. Observer clients trigger these by sending control JSON
on the /observe socket, e.g.:

  {"control": "RemoteStartTransaction", "cpId": "SIM_001", "idTag": "AYUSH001"}
  {"control": "TriggerMessage", "cpId": "SIM_001", "requestedMessage": "BootNotification"}

Every raw frame in both directions is broadcast to all observers as:

  {"dir": "cp->csms" | "csms->cp", "cpId": "...", "raw": "<exact frame>"}

plus {"event": "state", ...} snapshots on connector status changes and
{"event": "summary", ...} + {"event": "wallets", ...} when a session is
billed. On top of the raw protocol sits a society simulation with SHARED
bays: ten residents (wallets belong to people, not bays) and four bays
whose charge points are loopback OCPP clients of this same CSMS. Cars
arrive, charge in any free bay, or park in ordinary parking and queue;
the whole world state is broadcast as {"event": "world", ...} snapshots
and replayed to every observer on join. Observers steer it with Sim*
controls (SimPlugIn, SimUnplug, SimTraffic, SimSpeed, SimReset).
Billing is time-of-use: each MeterValues interval's energy is
priced by the tariff band of that interval's own timestamp (Night
23:00-06:00 ₹10/kWh, Solar hours 09:00-16:00 ₹12/kWh, Evening peak
17:00-22:00 ₹18/kWh, all other hours ₹14/kWh), the final interval up to
meterStop is priced by the StopTransaction timestamp, and the summary
carries the total plus a per-band breakdown. Debited from an in-memory
wallet keyed by idTag.

Dependencies: Python 3.12 stdlib + `websockets` only (same venv as the lab).

The society simulation (shared bays, resident pool, queue, traffic) is
implemented further down — see the "Society simulation" section banner.
"""

import asyncio
import itertools
import json
import os
import random
import threading
import time
import uuid
from datetime import datetime, timezone
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from websockets.asyncio.client import connect as ws_connect
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

LIVE_DIR = Path(__file__).resolve().parent
WS_PORT = 9001
HTTP_PORT = 8080
HEARTBEAT_INTERVAL_S = 30

# In-memory wallets (₹). Any idTag not listed here is rejected as Invalid.
# Wallets belong to PEOPLE (residents), never to bays: the bays are
# shared, and a session bills whichever resident's car is plugged in.
WALLETS = {
    "DEMO1234": 500.00,
    "AYUSH001": 250.00,
    "PRIYA002": 300.00,
    "RAHUL003": 150.00,
    "MEERA004": 400.00,
    "ARJUN005": 350.00,
    "KAVYA006": 300.00,
    "ROHAN007": 275.00,
    "SNEHA008": 325.00,
    "VIKRAM009": 200.00,
}

# The resident pool: ten EV owners in the building. tag -> display info.
RESIDENTS = {
    "DEMO1234": {"name": "Demo",   "flat": "B-707",  "color": "#66707e"},
    "AYUSH001": {"name": "Ayush",  "flat": "A-302",  "color": "#7d8a99"},
    "PRIYA002": {"name": "Priya",  "flat": "C-1801", "color": "#6f8579"},
    "RAHUL003": {"name": "Rahul",  "flat": "A-503",  "color": "#8a7f6d"},
    "MEERA004": {"name": "Meera",  "flat": "B-1202", "color": "#71808f"},
    "ARJUN005": {"name": "Arjun",  "flat": "D-904",  "color": "#83748a"},
    "KAVYA006": {"name": "Kavya",  "flat": "C-1105", "color": "#7d7462"},
    "ROHAN007": {"name": "Rohan",  "flat": "A-1108", "color": "#5f7d84"},
    "SNEHA008": {"name": "Sneha",  "flat": "D-603",  "color": "#8a8296"},
    "VIKRAM009": {"name": "Vikram", "flat": "B-1509", "color": "#6d7f74"},
}

# Four SHARED charging bays and a row of ordinary parking spots.
SIM_BAYS = ["BAY-01", "BAY-02", "BAY-03", "BAY-04"]
PARKING_SPOTS = 10

# Server-side ledger + recent payments (also carried in world snapshots,
# so a rejoining observer sees the same history as everyone else).
LEDGER = {"sessions": 0, "kwh": 0.0, "revenue": 0.0}
PAYMENTS = []          # most recent first, capped at 12
PAYMENTS_CAP = 12


# --------------------------------------------------------------------------
# Time-of-use tariff
# --------------------------------------------------------------------------
# Resident price (₹/kWh) by the wall-clock hour *as written* in the energy
# interval's timestamp: a charger in India sending +05:30 timestamps is
# priced on Indian local time; the browser bays send their simulation
# clock with a Z suffix, so accelerated time prices in the right band.
def tariff_band(hour: int):
    """(key, label, ₹/kWh) for a wall-clock hour 0-23."""
    if hour >= 23 or hour < 6:
        return ("night", "Night", 10.0)
    if 9 <= hour < 16:
        return ("solar", "Solar hours", 12.0)
    if 17 <= hour < 22:
        return ("peak", "Evening peak", 18.0)
    return ("standard", "Standard", 14.0)


def parse_ts(value):
    """Parse an OCPP ISO-8601 timestamp; None if missing/unparseable."""
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def price_interval(session: dict, delta_wh: float, ts) -> None:
    """Accumulate one energy interval into the session's per-band totals,
    priced by the band of the interval's timestamp (falling back to the
    previous interval's timestamp, then to server receipt time)."""
    if delta_wh <= 0:
        return
    when = ts or session.get("last_ts") or datetime.now(timezone.utc)
    key, label, rate = tariff_band(when.hour)
    kwh = delta_wh / 1000.0
    band = session["bands"].setdefault(key, {
        "band": key, "label": label, "rate": rate, "kwh": 0.0, "cost": 0.0,
    })
    band["kwh"] += kwh
    band["cost"] += kwh * rate


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(tag: str, text: str) -> None:
    print(f"{now_stamp()} [{tag}] {text}", flush=True)


# --------------------------------------------------------------------------
# Shared state
# --------------------------------------------------------------------------

observers: set = set()               # observer websockets
connections: dict = {}               # cpId -> ChargePointConnection
connector_state: dict = {}           # cpId -> {connectorId, status, ...}
sessions: dict = {}                  # transactionId -> session dict
_tx_ids = itertools.count(1)


async def broadcast(obj: dict) -> None:
    if not observers:
        return
    line = json.dumps(obj)
    dead = []
    for ws in list(observers):
        try:
            await ws.send(line)
        except Exception:
            dead.append(ws)
    for ws in dead:
        observers.discard(ws)


async def broadcast_frame(direction: str, cp_id: str, raw: str) -> None:
    await broadcast({"dir": direction, "cpId": cp_id, "raw": raw})


async def broadcast_state(cp_id: str) -> None:
    state = connector_state.get(cp_id)
    if state is not None:
        await broadcast({"event": "state", "cpId": cp_id, **state})


async def broadcast_wallets() -> None:
    await broadcast({"event": "wallets", "wallets": dict(WALLETS)})


# --------------------------------------------------------------------------
# One OCPP connection (one charge point)
# --------------------------------------------------------------------------

class ChargePointConnection:
    def __init__(self, websocket, cp_id: str):
        self.ws = websocket
        self.id = cp_id
        self.pending: dict = {}      # message id -> asyncio.Future
        self.boot_info: dict = {}

    # -- low level ------------------------------------------------------
    async def _send_raw(self, raw: str) -> None:
        await self.ws.send(raw)
        log(f"CSMS:{self.id}", f"SEND {raw}")
        await broadcast_frame("csms->cp", self.id, raw)

    async def send_call(self, action: str, payload: dict, timeout: float = 15.0):
        """Send a genuine [2, ...] CALL and await the [3]/[4] reply."""
        msg_id = uuid.uuid4().hex[:16]
        frame = json.dumps([2, msg_id, action, payload])
        future = asyncio.get_running_loop().create_future()
        self.pending[msg_id] = future
        await self._send_raw(frame)
        try:
            return await asyncio.wait_for(future, timeout)
        finally:
            self.pending.pop(msg_id, None)

    # -- main receive loop ----------------------------------------------
    async def run(self) -> None:
        try:
            async for raw in self.ws:
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", "replace")
                log(f"CSMS:{self.id}", f"RECV {raw}")
                await broadcast_frame("cp->csms", self.id, raw)
                await self._handle_frame(raw)
        except ConnectionClosed:
            pass
        finally:
            for future in self.pending.values():
                if not future.done():
                    future.cancel()
            self.pending.clear()

    async def _handle_frame(self, raw: str) -> None:
        try:
            frame = json.loads(raw)
        except json.JSONDecodeError:
            return
        if not isinstance(frame, list) or len(frame) < 2:
            return
        msg_type, msg_id = frame[0], frame[1]

        if msg_type == 3:  # CALLRESULT for one of our calls
            future = self.pending.pop(msg_id, None)
            if future is not None and not future.done():
                future.set_result(frame[2] if len(frame) > 2 else {})
            return
        if msg_type == 4:  # CALLERROR
            future = self.pending.pop(msg_id, None)
            if future is not None and not future.done():
                future.set_result({
                    "__call_error__": True,
                    "error_code": frame[2] if len(frame) > 2 else "?",
                    "error_description": frame[3] if len(frame) > 3 else "",
                })
            return
        if msg_type != 2 or len(frame) < 4:
            return

        action, payload = frame[2], frame[3] or {}
        handler = getattr(self, f"on_{action}", None)
        if handler is None:
            reply = [4, msg_id, "NotImplemented",
                     f"Action {action} not implemented by this CSMS", {}]
            await self._send_raw(json.dumps(reply))
            return
        try:
            result = await handler(payload)
        except Exception as exc:  # never let one bad call kill the socket
            log(f"CSMS:{self.id}", f"ERROR handling {action}: {exc!r}")
            reply = [4, msg_id, "InternalError", str(exc), {}]
            await self._send_raw(json.dumps(reply))
            return
        await self._send_raw(json.dumps([3, msg_id, result]))

    # -- charge point -> CSMS handlers ----------------------------------
    async def on_BootNotification(self, payload: dict) -> dict:
        self.boot_info = payload
        connector_state[self.id] = {
            "connectorId": 1,
            "status": connector_state.get(self.id, {}).get("status", "Available"),
            "vendor": payload.get("chargePointVendor"),
            "model": payload.get("chargePointModel"),
            "firmware": payload.get("firmwareVersion"),
            "booted": True,
        }
        log(f"CSMS:{self.id}",
            f"BOOT {payload.get('chargePointVendor')} "
            f"{payload.get('chargePointModel')} "
            f"firmware={payload.get('firmwareVersion', 'n/a')} "
            f"-> Accepted, heartbeat {HEARTBEAT_INTERVAL_S}s")
        await broadcast_state(self.id)
        return {
            "currentTime": utc_now_iso(),
            "interval": HEARTBEAT_INTERVAL_S,
            "status": "Accepted",
        }

    async def on_Heartbeat(self, payload: dict) -> dict:
        return {"currentTime": utc_now_iso()}

    async def on_Authorize(self, payload: dict) -> dict:
        id_tag = payload.get("idTag", "")
        accepted = id_tag in WALLETS
        status = "Accepted" if accepted else "Invalid"
        log(f"CSMS:{self.id}", f"AUTHORIZE idTag={id_tag} -> {status}")
        return {"idTagInfo": {"status": status}}

    async def on_StartTransaction(self, payload: dict) -> dict:
        transaction_id = next(_tx_ids)
        sessions[transaction_id] = {
            "cp_id": self.id,
            "connector_id": payload.get("connectorId", 1),
            "id_tag": payload.get("idTag", ""),
            "meter_start": payload.get("meterStart", 0),
            "register": payload.get("meterStart", 0),
            "power_w": 0.0,
            "soc": None,
            "started_at": datetime.now(timezone.utc),
            # Time-of-use pricing state: energy is priced interval by
            # interval as MeterValues arrive, keyed off the register.
            "priced_register": payload.get("meterStart", 0),
            "last_ts": parse_ts(payload.get("timestamp")),
            "bands": {},
        }
        log(f"CSMS:{self.id}",
            f"START tx={transaction_id} "
            f"connector={payload.get('connectorId')} "
            f"idTag={payload.get('idTag')} "
            f"meterStart={payload.get('meterStart')} Wh")
        return {
            "transactionId": transaction_id,
            "idTagInfo": {"status": "Accepted"},
        }

    async def on_MeterValues(self, payload: dict) -> dict:
        transaction_id = payload.get("transactionId")
        session = sessions.get(transaction_id)
        register = power = soc = None
        for entry in payload.get("meterValue", []):
            entry_register = entry_power = entry_soc = None
            for sv in entry.get("sampledValue", []):
                measurand = sv.get("measurand", "Energy.Active.Import.Register")
                try:
                    value = float(sv.get("value", "nan"))
                except (TypeError, ValueError):
                    continue
                if measurand == "Energy.Active.Import.Register":
                    entry_register = value
                elif measurand == "Power.Active.Import":
                    entry_power = value
                elif measurand == "SoC":
                    entry_soc = value
            if entry_register is not None:
                register = entry_register
                if session is not None:
                    # Price this interval now, by its own timestamp, so a
                    # session spanning a band boundary bills each slice of
                    # energy at the rate in force while it was delivered.
                    entry_ts = parse_ts(entry.get("timestamp"))
                    price_interval(
                        session,
                        entry_register - session["priced_register"],
                        entry_ts)
                    session["priced_register"] = entry_register
                    if entry_ts is not None:
                        session["last_ts"] = entry_ts
            if entry_power is not None:
                power = entry_power
            if entry_soc is not None:
                soc = entry_soc
        if register is not None:
            if session is not None:
                session["register"] = register
                if power is not None:
                    session["power_w"] = power
                if soc is not None:
                    session["soc"] = soc
                delivered = (register - session["meter_start"]) / 1000.0
            else:
                delivered = float("nan")
            log(f"CSMS:{self.id}",
                f"METER tx={transaction_id} register={register:.1f} Wh "
                f"power={power} W -> {delivered:.3f} kWh so far")
            await broadcast({
                "event": "meter", "cpId": self.id,
                "transactionId": transaction_id,
                "register": register, "power": power, "soc": soc,
            })
        return {}

    async def on_StatusNotification(self, payload: dict) -> dict:
        state = connector_state.setdefault(self.id, {})
        state.update({
            "connectorId": payload.get("connectorId", 1),
            "status": payload.get("status", "Available"),
            "errorCode": payload.get("errorCode"),
            "timestamp": payload.get("timestamp"),
            "booted": True,
        })
        log(f"CSMS:{self.id}",
            f"STATUS connector={state['connectorId']} -> {state['status']}")
        await broadcast_state(self.id)
        return {}

    async def on_StopTransaction(self, payload: dict) -> dict:
        transaction_id = payload.get("transactionId")
        session = sessions.pop(transaction_id, None)
        meter_stop = payload.get("meterStop", 0)
        meter_start = session["meter_start"] if session else 0
        id_tag = (session or {}).get("id_tag") or payload.get("idTag", "")
        total_kwh = (meter_stop - meter_start) / 1000.0
        duration_s = (
            (datetime.now(timezone.utc) - session["started_at"]).total_seconds()
            if session else 0.0
        )
        # Bill the final interval (last priced register -> meterStop) at
        # the band of the StopTransaction timestamp, then total up the
        # per-band accumulation built from the MeterValues intervals.
        bands = []
        if session is not None:
            price_interval(session,
                           meter_stop - session["priced_register"],
                           parse_ts(payload.get("timestamp")))
            cost = round(sum(b["cost"] for b in session["bands"].values()), 2)
            bands = [{
                "band": b["band"], "label": b["label"], "rate": b["rate"],
                "kwh": round(b["kwh"], 3), "cost": round(b["cost"], 2),
            } for b in session["bands"].values() if b["kwh"] > 0]
        else:
            # Session unknown to this server instance (e.g. it restarted
            # mid-transaction): price the whole amount at the stop band.
            stop_ts = parse_ts(payload.get("timestamp")) \
                or datetime.now(timezone.utc)
            _, label, rate = tariff_band(stop_ts.hour)
            cost = round(total_kwh * rate, 2)
            if total_kwh > 0:
                bands = [{"band": tariff_band(stop_ts.hour)[0],
                          "label": label, "rate": rate,
                          "kwh": round(total_kwh, 3), "cost": cost}]
        balance = None
        if id_tag in WALLETS:
            WALLETS[id_tag] = round(WALLETS[id_tag] - cost, 2)
            balance = WALLETS[id_tag]
        band_text = ", ".join(
            f"{b['label']} {b['kwh']:.3f} kWh @ ₹{b['rate']:.0f} = ₹{b['cost']:.2f}"
            for b in bands) or "no energy"
        summary = (
            "SESSION SUMMARY "
            f"tx={transaction_id} idTag={id_tag} "
            f"meterStart={meter_start} Wh meterStop={meter_stop} Wh "
            f"total={total_kwh:.3f} kWh duration={duration_s:.1f}s "
            f"bands[{band_text}] cost=₹{cost:.2f} "
            f"wallet[{id_tag}]={balance}"
        )
        log(f"CSMS:{self.id}", summary)
        summary_event = {
            "event": "summary", "cpId": self.id,
            "transactionId": transaction_id, "idTag": id_tag,
            "meterStart": meter_start, "meterStop": meter_stop,
            "kwh": round(total_kwh, 3), "cost": cost, "bands": bands,
            "durationS": round(duration_s, 1), "walletBalance": balance,
        }
        world = globals().get("WORLD")
        if world is not None:
            summary_event["atSim"] = world.sim_clock_text()
        await broadcast(summary_event)
        LEDGER["sessions"] += 1
        LEDGER["kwh"] = round(LEDGER["kwh"] + summary_event["kwh"], 3)
        LEDGER["revenue"] = round(LEDGER["revenue"] + cost, 2)
        PAYMENTS.insert(0, {k: v for k, v in summary_event.items()
                            if k != "event"})
        del PAYMENTS[PAYMENTS_CAP:]
        if world is not None:
            await world.broadcast_world()
        await broadcast_wallets()
        return {"idTagInfo": {"status": "Accepted"}}

    async def on_DataTransfer(self, payload: dict) -> dict:
        return {"status": "Accepted"}

    async def on_DiagnosticsStatusNotification(self, payload: dict) -> dict:
        return {}

    async def on_FirmwareStatusNotification(self, payload: dict) -> dict:
        return {}


# --------------------------------------------------------------------------
# Society simulation — SHARED bays, a resident pool and a waiting queue
# --------------------------------------------------------------------------
# The four bays are shared infrastructure: any resident's car can use any
# bay. Each bay's charge point is simulated server-side but speaks genuine
# OCPP 1.6J over a loopback WebSocket to this very CSMS, so its frames,
# wallet billing and observer broadcasts are identical to a real charger
# (or the raw test client) connecting from outside. A traffic engine on a
# simulation clock moves cars gate -> bay, gate -> parking (queue) ->
# bay -> parking -> gate. All world state lives here, server-side, and a
# full snapshot is sent to every observer when it joins.

WORLD = None          # SimWorld, created in main()


def _taper_power_w(soc: float) -> float:
    """Charge power profile: full 7.4 kW until 80% SoC, then tapering."""
    if soc < 80.0:
        return 7400.0
    return 7400.0 * max(0.35, 1.0 - (soc - 80.0) / 20.0 * 0.65)


class SimChargePoint:
    """One shared bay's charge point (loopback OCPP client)."""

    def __init__(self, world, bay_id: str, base_url: str, register_seed: int):
        self.world = world
        self.id = bay_id
        self.url = f"{base_url}/{bay_id}"
        self.ws = None
        self.pending: dict = {}
        self.seq = 0
        self.register = float(register_seed)
        self.tx_id = None
        self.stop_requested = False
        self.connected = False
        self._send_lock = asyncio.Lock()

    # -- plumbing -------------------------------------------------------
    async def run_forever(self) -> None:
        while True:
            try:
                await self._connect_and_run()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log(f"SIMCP:{self.id}", f"loop error: {exc!r}; retry in 3s")
            self.connected = False
            self.ws = None
            await asyncio.sleep(3)

    async def _connect_and_run(self) -> None:
        async with ws_connect(self.url, subprotocols=["ocpp1.6"]) as ws:
            self.ws = ws
            self.connected = True
            reader = asyncio.create_task(self._reader())
            heartbeat = asyncio.create_task(self._heartbeat_loop())
            try:
                await self.call("BootNotification", {
                    "chargePointVendor": "Simplecharge",
                    "chargePointModel": "SocietyWallbox 7.4",
                    "chargePointSerialNumber": f"SN-{self.id}",
                    "firmwareVersion": "2.1.0",
                })
                await self._status("Available")
                await reader          # until the socket closes
            finally:
                reader.cancel()
                heartbeat.cancel()

    async def _reader(self) -> None:
        async for raw in self.ws:
            try:
                frame = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(frame, list) or len(frame) < 2:
                continue
            if frame[0] in (3, 4):
                future = self.pending.pop(frame[1], None)
                if future is not None and not future.done():
                    future.set_result(frame[2] if len(frame) > 2 else {})
            elif frame[0] == 2 and len(frame) >= 4:
                await self._handle_server_call(frame[1], frame[2],
                                               frame[3] or {})

    async def _handle_server_call(self, msg_id, action, payload) -> None:
        if action == "RemoteStopTransaction":
            self.stop_requested = True
            await self._reply(msg_id, {"status": "Accepted"})
        elif action == "RemoteStartTransaction":
            await self._reply(msg_id, {"status": "Accepted"})
            asyncio.create_task(
                self.world.remote_start(self.id, payload.get("idTag")))
        elif action in ("Reset", "UnlockConnector", "ChangeAvailability",
                        "TriggerMessage"):
            if action == "Reset":
                self.stop_requested = True
            await self._reply(msg_id, {"status": "Accepted"})
        else:
            await self._send_raw(json.dumps(
                [4, msg_id, "NotImplemented",
                 f"Action {action} not implemented by this charge point", {}]))

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(25)
            try:
                await self.call("Heartbeat", {})
            except Exception:
                return

    async def _send_raw(self, raw: str) -> None:
        async with self._send_lock:
            await self.ws.send(raw)

    async def _reply(self, msg_id, result: dict) -> None:
        await self._send_raw(json.dumps([3, msg_id, result]))

    async def call(self, action: str, payload: dict, timeout: float = 15.0):
        self.seq += 1
        msg_id = f"sim-{self.id}-{self.seq}"
        frame = json.dumps([2, msg_id, action, payload])
        future = asyncio.get_running_loop().create_future()
        self.pending[msg_id] = future
        await self._send_raw(frame)
        try:
            return await asyncio.wait_for(future, timeout)
        finally:
            self.pending.pop(msg_id, None)

    async def _status(self, status: str) -> None:
        await self.call("StatusNotification", {
            "connectorId": 1, "errorCode": "NoError", "status": status,
            "timestamp": self.world.sim_iso(),
        })

    # -- one charging session for one car -------------------------------
    async def run_session(self, car: dict) -> None:
        world = self.world
        gen = world.generation
        tag = car["tag"]
        self.stop_requested = False
        try:
            await self._status("Preparing")
            auth = await self.call("Authorize", {"idTag": tag})
            if (auth or {}).get("idTagInfo", {}).get("status") != "Accepted":
                log(f"SIMCP:{self.id}",
                    f"Authorize {tag} not accepted — session aborted")
                return
            meter_start = int(round(self.register))
            self.register = float(meter_start)
            start = await self.call("StartTransaction", {
                "connectorId": 1, "idTag": tag, "meterStart": meter_start,
                "timestamp": world.sim_iso(),
            })
            self.tx_id = (start or {}).get("transactionId")
            await self._status("Charging")
            await world.on_session_started(self.id, car, gen)
            last = time.monotonic()
            while not self.stop_requested:
                await asyncio.sleep(0.5)
                now = time.monotonic()
                dt_sim_s = (now - last) * world.speed
                last = now
                power = _taper_power_w(car["soc"])
                wh = power * dt_sim_s / 3600.0
                self.register += wh
                car["soc"] = min(
                    100.0, car["soc"] + wh / (car["battery_kwh"] * 10.0))
                await self.call("MeterValues", {
                    "connectorId": 1, "transactionId": self.tx_id,
                    "meterValue": [{
                        "timestamp": world.sim_iso(),
                        "sampledValue": [
                            {"value": f"{self.register:.1f}",
                             "measurand": "Energy.Active.Import.Register",
                             "unit": "Wh"},
                            {"value": f"{power:.0f}",
                             "measurand": "Power.Active.Import",
                             "unit": "W"},
                            {"value": f"{car['soc']:.1f}",
                             "measurand": "SoC", "unit": "Percent"},
                        ],
                    }],
                })
                if car["soc"] >= car["target_soc"]:
                    break
            meter_stop = int(round(self.register))
            self.register = float(meter_stop)
            await self.call("StopTransaction", {
                "transactionId": self.tx_id, "meterStop": meter_stop,
                "timestamp": world.sim_iso(),
                "reason": "Remote" if self.stop_requested else "Local",
            })
            self.tx_id = None
            await self._status("Finishing")
            await asyncio.sleep(0.8)
            await self._status("Available")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log(f"SIMCP:{self.id}", f"session error: {exc!r}")
            self.tx_id = None
        finally:
            await world.on_session_finished(self.id, car, gen)


class SimWorld:
    """Cars, shared bays, parking, the queue and the traffic engine."""

    SPEEDS = (1, 3, 10, 30)

    def __init__(self, base_url: str):
        self.speed = 1
        self.playing = False
        self.now_ms = time.time() * 1000.0
        self._last_tick = time.monotonic()
        self._last_clock_broadcast = 0.0
        self.generation = 0
        self.cars = {tag: self._fresh_car(tag) for tag in RESIDENTS}
        self.queue: list = []
        self.bays = {bay: {"car": None, "phase": "free"} for bay in SIM_BAYS}
        self.cps = {
            bay: SimChargePoint(self, bay, base_url,
                                100000 + i * 13777 + random.randint(0, 4000))
            for i, bay in enumerate(SIM_BAYS)
        }
        self.next_arrival_ms = None

    @staticmethod
    def _fresh_car(tag: str) -> dict:
        return {"tag": tag, "state": "away", "spot": None, "bay": None,
                "soc": 0.0, "target_soc": 90.0, "battery_kwh": 30.0,
                "dwell_until": None}

    # -- clock ------------------------------------------------------------
    def sim_datetime(self) -> datetime:
        return datetime.fromtimestamp(self.now_ms / 1000.0)

    def sim_iso(self) -> str:
        return self.sim_datetime().strftime("%Y-%m-%dT%H:%M:%SZ")

    def sim_clock_text(self) -> str:
        return self.sim_datetime().strftime("%H:%M:%S")

    def sim_band(self):
        return tariff_band(self.sim_datetime().hour)

    # -- snapshot ---------------------------------------------------------
    def _session_view(self, cp_id: str):
        for tx_id, s in sessions.items():
            if s["cp_id"] == cp_id:
                return {
                    "transactionId": tx_id, "idTag": s["id_tag"],
                    "kwh": round((s["register"] - s["meter_start"]) / 1000.0, 3),
                    "powerW": round(s["power_w"]),
                    "soc": (round(s["soc"], 1)
                            if s["soc"] is not None else None),
                    "costSoFar": round(
                        sum(b["cost"] for b in s["bands"].values()), 2),
                }
        return None

    def snapshot(self) -> dict:
        key, label, rate = self.sim_band()
        return {
            "event": "world",
            "sim": {"nowMs": int(self.now_ms), "speed": self.speed,
                    "playing": self.playing, "clock": self.sim_clock_text(),
                    "band": {"key": key, "label": label, "rate": rate}},
            "residents": [
                {"tag": tag, "name": r["name"], "flat": r["flat"],
                 "color": r["color"], "balance": WALLETS[tag]}
                for tag, r in RESIDENTS.items()
            ],
            "cars": [
                {"tag": c["tag"], "state": c["state"], "spot": c["spot"],
                 "bay": c["bay"], "soc": round(c["soc"], 1),
                 "targetSoc": round(c["target_soc"], 1),
                 "queuePos": (self.queue.index(c["tag"]) + 1
                              if c["tag"] in self.queue else None)}
                for c in self.cars.values()
            ],
            "bays": [
                {"cpId": bay, "phase": self.bays[bay]["phase"],
                 "car": self.bays[bay]["car"],
                 "status": connector_state.get(bay, {}).get(
                     "status", "Offline"),
                 "session": self._session_view(bay)}
                for bay in SIM_BAYS
            ],
            "queue": list(self.queue),
            "ledger": dict(LEDGER),
            "payments": list(PAYMENTS),
        }

    async def broadcast_world(self) -> None:
        await broadcast(self.snapshot())

    async def sim_event(self, kind: str, text: str, **fields) -> None:
        log("SIM", text)
        await broadcast({"event": "sim_event", "kind": kind,
                         "text": text, **fields})

    # -- helpers ------------------------------------------------------------
    def _free_spot(self):
        used = {c["spot"] for c in self.cars.values()
                if c["spot"] is not None}
        for i in range(PARKING_SPOTS):
            if i not in used:
                return i
        return None

    def _free_bay(self):
        for bay in SIM_BAYS:
            if self.bays[bay]["car"] is None:
                return bay
        return None

    @staticmethod
    def _who(tag: str) -> str:
        r = RESIDENTS[tag]
        return f"{r['name']} ({r['flat']})"

    # -- bay assignment / queue ---------------------------------------------
    async def assign_bay(self, bay_id: str, car: dict,
                         from_queue: bool = False) -> None:
        if car["tag"] in self.queue:
            self.queue.remove(car["tag"])
        car["spot"] = None
        car["state"] = "charging"
        car["bay"] = bay_id
        car["dwell_until"] = None
        self.bays[bay_id]["car"] = car["tag"]
        self.bays[bay_id]["phase"] = "starting"
        if from_queue:
            await self.sim_event(
                "dequeue",
                f"{self._who(car['tag'])} waited their turn — "
                f"now plugging into {bay_id}",
                tag=car["tag"], bay=bay_id)
        else:
            await self.sim_event(
                "charge_start",
                f"{self._who(car['tag'])} plugged into {bay_id} "
                f"({car['soc']:.0f}% → {car['target_soc']:.0f}%)",
                tag=car["tag"], bay=bay_id)
        asyncio.create_task(self.cps[bay_id].run_session(car))
        await self.broadcast_world()

    async def try_advance_queue(self) -> None:
        while self.queue:
            bay_id = self._free_bay()
            if bay_id is None:
                return
            car = self.cars[self.queue[0]]
            await self.assign_bay(bay_id, car, from_queue=True)

    async def on_session_started(self, bay_id: str, car: dict,
                                 gen: int) -> None:
        if gen != self.generation:
            return
        self.bays[bay_id]["phase"] = "charging"
        await self.broadcast_world()

    async def on_session_finished(self, bay_id: str, car: dict,
                                  gen: int) -> None:
        if gen != self.generation:
            return                      # a reset already placed this car
        if self.bays[bay_id]["car"] == car["tag"]:
            self.bays[bay_id]["car"] = None
            self.bays[bay_id]["phase"] = "free"
        if car["bay"] == bay_id:
            car["bay"] = None
            spot = self._free_spot()
            if spot is not None:
                car["spot"] = spot
                car["state"] = "parked"
                car["dwell_until"] = (
                    self.now_ms + random.uniform(25, 100) * 60000)
                await self.sim_event(
                    "charge_done",
                    f"{self._who(car['tag'])} finished at {bay_id} — "
                    f"moved to parking P{spot + 1}",
                    tag=car["tag"], bay=bay_id, spot=spot)
            else:
                car["state"] = "away"
                await self.sim_event(
                    "charge_done",
                    f"{self._who(car['tag'])} finished at {bay_id} and left "
                    f"(parking full)", tag=car["tag"], bay=bay_id)
        await self.try_advance_queue()
        await self.broadcast_world()

    # -- arrivals -------------------------------------------------------------
    async def arrive(self, tag: str, wants_charge: bool,
                     direct_bay: str = None) -> None:
        car = self.cars[tag]
        car["battery_kwh"] = round(random.uniform(24, 36), 1)
        if wants_charge:
            car["soc"] = round(random.uniform(55, 80), 1)
            car["target_soc"] = round(random.uniform(86, 93), 1)
        else:
            car["soc"] = round(random.uniform(30, 85), 1)
            car["target_soc"] = car["soc"]
        await self.sim_event(
            "arrive",
            f"{self._who(tag)} arrived at the gate"
            + (" looking to charge" if wants_charge else " (just home)"),
            tag=tag)
        if wants_charge:
            bay_id = None
            if direct_bay and self.bays[direct_bay]["car"] is None:
                bay_id = direct_bay
            elif not self.queue:
                bay_id = self._free_bay()
            if bay_id is not None:
                await self.assign_bay(bay_id, car)
                return
            spot = self._free_spot()
            if spot is None:
                car["state"] = "away"
                await self.sim_event(
                    "note", f"{self._who(tag)} found parking full and "
                    f"drove on", tag=tag)
                await self.broadcast_world()
                return
            car["spot"] = spot
            car["state"] = "waiting"
            self.queue.append(tag)
            await self.sim_event(
                "wait",
                f"all bays busy — {self._who(tag)} parked in "
                f"P{spot + 1} and joined the queue "
                f"(#{len(self.queue)} in line)",
                tag=tag, spot=spot, queuePos=len(self.queue))
            await self.broadcast_world()
            return
        spot = self._free_spot()
        if spot is None:
            car["state"] = "away"
            await self.broadcast_world()
            return
        car["spot"] = spot
        car["state"] = "parked"
        car["dwell_until"] = self.now_ms + random.uniform(30, 120) * 60000
        await self.sim_event(
            "park", f"{self._who(tag)} parked in P{spot + 1}",
            tag=tag, spot=spot)
        await self.broadcast_world()

    # -- traffic engine -------------------------------------------------------
    async def tick_loop(self) -> None:
        while True:
            await asyncio.sleep(0.25)
            now_real = time.monotonic()
            dt = now_real - self._last_tick
            self._last_tick = now_real
            if dt > 0:
                self.now_ms += dt * 1000.0 * self.speed
            if self.playing:
                if self.next_arrival_ms is None:
                    self.next_arrival_ms = (
                        self.now_ms + random.uniform(4, 12) * 60000)
                if self.now_ms >= self.next_arrival_ms:
                    self.next_arrival_ms = (
                        self.now_ms + random.uniform(4, 12) * 60000)
                    away = [t for t, c in self.cars.items()
                            if c["state"] == "away"]
                    if away:
                        await self.arrive(random.choice(away),
                                          wants_charge=random.random() < 0.8)
            for car in list(self.cars.values()):
                if (car["state"] == "parked"
                        and car["dwell_until"] is not None
                        and self.now_ms >= car["dwell_until"]):
                    car["state"] = "away"
                    car["spot"] = None
                    car["dwell_until"] = None
                    await self.sim_event(
                        "leave", f"{self._who(car['tag'])} left the building",
                        tag=car["tag"])
                    await self.broadcast_world()
            if now_real - self._last_clock_broadcast >= 2.0:
                self._last_clock_broadcast = now_real
                await broadcast({"event": "sim",
                                 "nowMs": int(self.now_ms),
                                 "speed": self.speed,
                                 "playing": self.playing})
                # Periodic full snapshot too: session energy / SoC only
                # change via MeterValues, which carry no world broadcast,
                # so without this the scene would freeze mid-session.
                await self.broadcast_world()

    # -- manual + remote controls ---------------------------------------------
    async def plug_in(self, bay_id: str) -> None:
        if bay_id not in self.bays:
            return
        if self.bays[bay_id]["car"] is not None:
            await self.sim_event(
                "note", f"{bay_id} is already in use", bay=bay_id)
            return
        if self.queue:
            car = self.cars[self.queue[0]]
            await self.assign_bay(bay_id, car, from_queue=True)
            return
        away = [t for t, c in self.cars.items() if c["state"] == "away"]
        if away:
            await self.arrive(random.choice(away), True,
                              direct_bay=bay_id)
            return
        parked = [c for c in self.cars.values() if c["state"] == "parked"]
        if parked:
            car = random.choice(parked)
            car["target_soc"] = round(random.uniform(86, 93), 1)
            if car["soc"] >= car["target_soc"]:
                car["target_soc"] = min(98.0, car["soc"] + 10)
            await self.sim_event(
                "summon",
                f"{self._who(car['tag'])} called over from parking "
                f"to {bay_id}", tag=car["tag"], bay=bay_id)
            await self.assign_bay(bay_id, car)
            return
        await self.sim_event(
            "note", "no car available to bring in", bay=bay_id)

    async def remote_start(self, bay_id: str, id_tag: str = None) -> None:
        if self.bays[bay_id]["car"] is not None:
            return
        if id_tag and id_tag in self.cars:
            car = self.cars[id_tag]
            if car["state"] == "waiting":
                await self.assign_bay(bay_id, car, from_queue=True)
                return
            if car["state"] == "away":
                await self.arrive(id_tag, True, direct_bay=bay_id)
                return
            if car["state"] == "parked":
                car["target_soc"] = round(random.uniform(86, 93), 1)
                if car["soc"] >= car["target_soc"]:
                    car["target_soc"] = min(98.0, car["soc"] + 10)
                await self.assign_bay(bay_id, car)
                return
        await self.plug_in(bay_id)

    async def reset(self) -> None:
        self.generation += 1
        for cp in self.cps.values():
            cp.stop_requested = True
        self.queue.clear()
        self.cars = {tag: self._fresh_car(tag) for tag in RESIDENTS}
        self.bays = {bay: {"car": None, "phase": "free"} for bay in SIM_BAYS}
        self.now_ms = time.time() * 1000.0
        self.next_arrival_ms = None
        LEDGER.update({"sessions": 0, "kwh": 0.0, "revenue": 0.0})
        PAYMENTS.clear()
        await self.sim_event("reset", "day reset — cars home, queue cleared")
        await broadcast_wallets()
        await self.broadcast_world()

    async def handle_sim_control(self, ctrl: dict) -> None:
        action = ctrl.get("control", "")
        bay_id = ctrl.get("cpId", "")
        if action == "SimTraffic":
            self.playing = bool(ctrl.get("playing"))
            await self.sim_event(
                "note", "traffic " + ("playing" if self.playing else "paused"))
        elif action == "SimSpeed":
            try:
                n = int(ctrl.get("speed", 1))
            except (TypeError, ValueError):
                n = 1
            if n in self.SPEEDS:
                self.speed = n
                await self.sim_event("note", f"simulation speed {n}×")
        elif action == "SimPlugIn":
            await self.plug_in(bay_id)
        elif action == "SimUnplug":
            cp = self.cps.get(bay_id)
            if cp is not None and self.bays[bay_id]["car"] is not None:
                cp.stop_requested = True
                await self.sim_event(
                    "note", f"unplug requested at {bay_id}", bay=bay_id)
        elif action == "SimReset":
            await self.reset()
        else:
            return
        await broadcast({"event": "control_result", "cpId": bay_id,
                         "control": action,
                         "result": {"status": "Accepted"}})
        await self.broadcast_world()


def start_world(port: int) -> None:
    global WORLD
    WORLD = SimWorld(f"ws://127.0.0.1:{port}")
    for cp in WORLD.cps.values():
        asyncio.create_task(cp.run_forever())
    asyncio.create_task(WORLD.tick_loop())
    log("SIM", f"society world live: {len(RESIDENTS)} residents, "
               f"{len(SIM_BAYS)} shared bays, {PARKING_SPOTS} parking spots")


# --------------------------------------------------------------------------
# Observer / control endpoint
# --------------------------------------------------------------------------

# control name -> (OCPP action, payload builder from the control message)
def _remote_stop_payload(ctrl: dict) -> dict:
    if "transactionId" in ctrl:
        return {"transactionId": ctrl["transactionId"]}
    for tx_id, session in sessions.items():
        if session["cp_id"] == ctrl.get("cpId"):
            return {"transactionId": tx_id}
    return {"transactionId": -1}


CONTROL_CALLS = {
    "RemoteStartTransaction": lambda c: {
        "connectorId": c.get("connectorId", 1),
        "idTag": c.get("idTag", "DEMO1234"),
    },
    "RemoteStopTransaction": _remote_stop_payload,
    "Reset": lambda c: {"type": c.get("type", "Soft")},
    "UnlockConnector": lambda c: {"connectorId": c.get("connectorId", 1)},
    "ChangeAvailability": lambda c: {
        "connectorId": c.get("connectorId", 0),
        "type": c.get("type", "Operative"),
    },
    "TriggerMessage": lambda c: {
        "requestedMessage": c.get("requestedMessage", "BootNotification"),
        "connectorId": c.get("connectorId", 1),
    },
}


async def handle_control(ctrl: dict) -> None:
    action = ctrl.get("control", "")
    if action.startswith("Sim"):
        if WORLD is not None:
            await WORLD.handle_sim_control(ctrl)
        return
    cp_id = ctrl.get("cpId", "")
    conn = connections.get(cp_id)
    if conn is None:
        log("CTRL", f"{action} for {cp_id}: charge point not connected")
        await broadcast({"event": "control_error", "cpId": cp_id,
                         "control": action,
                         "error": "charge point not connected"})
        return
    builder = CONTROL_CALLS.get(action)
    if builder is None:
        log("CTRL", f"unsupported control {action!r}")
        return
    payload = builder(ctrl)
    log("CTRL", f"-> {action} {payload} to {cp_id}")
    try:
        result = await conn.send_call(action, payload)
        log("CTRL", f"<- {action} result from {cp_id}: {result}")
        await broadcast({"event": "control_result", "cpId": cp_id,
                         "control": action, "result": result})
    except Exception as exc:
        log("CTRL", f"{action} to {cp_id} failed: {exc!r}")
        await broadcast({"event": "control_error", "cpId": cp_id,
                         "control": action, "error": str(exc)})


async def on_observer(websocket) -> None:
    observers.add(websocket)
    log("CSMS", f"OBSERVER connected from {websocket.remote_address} "
                f"({len(observers)} total)")
    # Bring the new observer up to date.
    await websocket.send(json.dumps({"event": "wallets",
                                     "wallets": dict(WALLETS)}))
    for cp_id in list(connector_state):
        state = connector_state[cp_id]
        await websocket.send(json.dumps({"event": "state",
                                         "cpId": cp_id, **state}))
    if WORLD is not None:
        # Full society snapshot: cars, queue, bays, ledger, payments —
        # a rejoining observer lands in exactly the world everyone
        # else is watching, mid-session included.
        await websocket.send(json.dumps(WORLD.snapshot()))
    try:
        async for raw in websocket:
            try:
                ctrl = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue
            if isinstance(ctrl, dict) and "control" in ctrl:
                asyncio.create_task(handle_control(ctrl))
    except ConnectionClosed:
        pass
    finally:
        observers.discard(websocket)
        log("CSMS", "OBSERVER disconnected")


# --------------------------------------------------------------------------
# WebSocket routing + HTTP static server
# --------------------------------------------------------------------------

async def on_connect(websocket) -> None:
    path = (websocket.request.path or "/").strip("/")
    if path == "observe":
        await on_observer(websocket)
        return
    cp_id = path.split("/")[0] or "unknown"
    log("CSMS", f"CONNECT charge point '{cp_id}' from {websocket.remote_address}")
    conn = ChargePointConnection(websocket, cp_id)
    connections[cp_id] = conn
    try:
        await conn.run()
    finally:
        if connections.get(cp_id) is conn:
            connections.pop(cp_id, None)
        state = connector_state.setdefault(cp_id, {})
        state.update({"status": "Offline", "booted": False})
        await broadcast_state(cp_id)
        log("CSMS", f"DISCONNECT charge point '{cp_id}'")


def start_http_server() -> ThreadingHTTPServer:
    handler = partial(SimpleHTTPRequestHandler, directory=str(LIVE_DIR))
    httpd = ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return httpd


def _select_subprotocol(connection, subprotocols):
    """Negotiate ocpp1.6 when offered, but still accept clients that offer
    no subprotocol at all (observers on /observe don't speak OCPP). The
    websockets default aborts such handshakes with HTTP 400."""
    if "ocpp1.6" in subprotocols:
        return "ocpp1.6"
    return None


def _single_port_http(connection, request, port: int):
    """Single-port mode (PORT env set, e.g. Render / Railway / Cloud Run):
    hosts that expose only ONE public port get plain HTTP requests answered
    here (the visualizer page, with its WebSocket target rewritten to the
    page's own public origin), while WebSocket upgrade requests fall through
    to the OCPP handler."""
    if (request.headers.get("Upgrade") or "").lower() == "websocket":
        return None
    path = (request.path or "/").split("?")[0]
    if path == "/healthz":
        return connection.respond(200, "ok")
    if path in ("/", "/visualizer.html"):
        html = (LIVE_DIR / "visualizer.html").read_text(encoding="utf-8")
        # The page must dial the WebSocket on the SAME public origin it was
        # loaded from (location.host) — NOT on `port`, which on PaaS hosts
        # like Render is the internal container port (e.g. 10000) and is not
        # reachable from the internet. (Bug found live 2026-11-03: the page
        # tried wss://<host>:10000 and the observer socket never connected.)
        html = html.replace('HOST + ":9001"', "location.host")
        resp = connection.respond(200, html)
        resp.headers["Content-Type"] = "text/html; charset=utf-8"
        return resp
    return connection.respond(404, "not found")


async def main() -> None:
    port_env = os.environ.get("PORT")
    if port_env:
        # Single-port mode: HTTP + OCPP WebSocket share one port (PaaS hosts
        # like Render/Railway/Cloud Run expose exactly one port per service).
        port = int(port_env)
        server = await serve(on_connect, "0.0.0.0", port,
                             subprotocols=["ocpp1.6"],
                             select_subprotocol=_select_subprotocol,
                             process_request=lambda c, r: _single_port_http(c, r, port))
        log("CSMS", f"SINGLE-PORT mode on {port}: http://0.0.0.0:{port}/visualizer.html + ws://0.0.0.0:{port}/<charge_point_id> (OCPP 1.6J) + /observe")
        start_world(port)
        await server.wait_closed()
        return
    start_http_server()
    server = await serve(on_connect, "0.0.0.0", WS_PORT,
                         subprotocols=["ocpp1.6"],
                         select_subprotocol=_select_subprotocol)
    start_world(WS_PORT)
    log("CSMS", f"LISTENING ws://0.0.0.0:{WS_PORT}/<charge_point_id> (OCPP 1.6J)")
    log("CSMS", f"LISTENING ws://0.0.0.0:{WS_PORT}/observe (observer/control)")
    log("CSMS", f"SERVING   http://0.0.0.0:{HTTP_PORT}/visualizer.html (static)")
    await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
