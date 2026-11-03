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
billed. Billing: kWh = (meterStop - meterStart) / 1000 at ₹18/kWh, debited
from an in-memory wallet keyed by idTag.

Dependencies: Python 3.12 stdlib + `websockets` only (same venv as the lab).
"""

import asyncio
import itertools
import json
import threading
import uuid
from datetime import datetime, timezone
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

LIVE_DIR = Path(__file__).resolve().parent
WS_PORT = 9001
HTTP_PORT = 8080
RATE_INR_PER_KWH = 18.0
HEARTBEAT_INTERVAL_S = 30

# In-memory wallets (₹). Any idTag not listed here is rejected as Invalid.
WALLETS = {
    "DEMO1234": 500.00,
    "AYUSH001": 250.00,
    "PRIYA002": 300.00,
    "RAHUL003": 150.00,
}


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
            "started_at": datetime.now(timezone.utc),
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
            for sv in entry.get("sampledValue", []):
                measurand = sv.get("measurand", "Energy.Active.Import.Register")
                try:
                    value = float(sv.get("value", "nan"))
                except (TypeError, ValueError):
                    continue
                if measurand == "Energy.Active.Import.Register":
                    register = value
                elif measurand == "Power.Active.Import":
                    power = value
                elif measurand == "SoC":
                    soc = value
        if register is not None:
            if session is not None:
                session["register"] = register
                if power is not None:
                    session["power_w"] = power
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
        cost = round(total_kwh * RATE_INR_PER_KWH, 2)
        balance = None
        if id_tag in WALLETS:
            WALLETS[id_tag] = round(WALLETS[id_tag] - cost, 2)
            balance = WALLETS[id_tag]
        summary = (
            "SESSION SUMMARY "
            f"tx={transaction_id} idTag={id_tag} "
            f"meterStart={meter_start} Wh meterStop={meter_stop} Wh "
            f"total={total_kwh:.3f} kWh duration={duration_s:.1f}s "
            f"rate=₹{RATE_INR_PER_KWH:.0f}/kWh cost=₹{cost:.2f} "
            f"wallet[{id_tag}]={balance}"
        )
        log(f"CSMS:{self.id}", summary)
        await broadcast({
            "event": "summary", "cpId": self.id,
            "transactionId": transaction_id, "idTag": id_tag,
            "meterStart": meter_start, "meterStop": meter_stop,
            "kwh": round(total_kwh, 3), "cost": cost,
            "durationS": round(duration_s, 1), "walletBalance": balance,
        })
        await broadcast_wallets()
        return {"idTagInfo": {"status": "Accepted"}}

    async def on_DataTransfer(self, payload: dict) -> dict:
        return {"status": "Accepted"}

    async def on_DiagnosticsStatusNotification(self, payload: dict) -> dict:
        return {}

    async def on_FirmwareStatusNotification(self, payload: dict) -> dict:
        return {}


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


async def main() -> None:
    start_http_server()
    server = await serve(on_connect, "0.0.0.0", WS_PORT,
                         subprotocols=["ocpp1.6"],
                         select_subprotocol=_select_subprotocol)
    log("CSMS", f"LISTENING ws://0.0.0.0:{WS_PORT}/<charge_point_id> (OCPP 1.6J)")
    log("CSMS", f"LISTENING ws://0.0.0.0:{WS_PORT}/observe (observer/control)")
    log("CSMS", f"SERVING   http://0.0.0.0:{HTTP_PORT}/visualizer.html (static)")
    await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
