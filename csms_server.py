#!/usr/bin/env python3
"""Minimal OCPP 1.6J CSMS (central system) for the EV charger test lab.

Listens on ws://localhost:9000/<charge_point_id> and implements just enough
of OCPP 1.6J to run a full charging session:

  BootNotification -> accept, heartbeat interval 30s
  Authorize        -> accept idTag "DEMO1234", reject everything else
  StartTransaction -> allocate a transaction id, remember meterStart
  MeterValues      -> log the energy register + kWh delivered so far
  StopTransaction  -> print a SESSION SUMMARY (kWh, duration, cost @ ₹18/kWh)

Every raw OCPP frame in both directions is appended, with a timestamp, to
transcript.log next to this script.

Note: the mobilityhouse `ocpp` 2.x library ships no separate CentralSystem
class for v16 — the central system is a ChargePoint subclass whose methods
*receive* calls instead of sending them (see the library's own examples).
This class is named CentralSystem after that role.
"""

import asyncio
import itertools
from datetime import datetime, timezone
from pathlib import Path

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from ocpp.routing import on
from ocpp.v16 import ChargePoint as BaseChargePoint
from ocpp.v16 import call_result
from ocpp.v16.datatypes import IdTagInfo
from ocpp.v16.enums import Action, AuthorizationStatus, RegistrationStatus

LAB_DIR = Path(__file__).resolve().parent
TRANSCRIPT = LAB_DIR / "transcript.log"
ACCEPTED_TAG = "DEMO1234"
RATE_INR_PER_KWH = 18.0


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def log_line(tag: str, text: str) -> None:
    line = f"{now_stamp()} [{tag}] {text}"
    print(line, flush=True)
    with TRANSCRIPT.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


class LoggingConnection:
    """Wrap a websocket so every raw OCPP frame lands in the transcript."""

    def __init__(self, websocket, tag: str):
        self._ws = websocket
        self._tag = tag

    async def recv(self) -> str:
        message = await self._ws.recv()
        log_line(self._tag, f"RECV {message}")
        return message

    async def send(self, message: str) -> None:
        log_line(self._tag, f"SEND {message}")
        await self._ws.send(message)

    async def close(self) -> None:
        await self._ws.close()


class CentralSystem(BaseChargePoint):
    _tx_ids = itertools.count(1)

    def __init__(self, charge_point_id: str, connection: LoggingConnection):
        super().__init__(charge_point_id, connection)
        self.sessions = {}  # transaction_id -> session dict

    @on(Action.boot_notification)
    def on_boot_notification(self, charge_point_vendor, charge_point_model, **kwargs):
        log_line(
            f"CSMS:{self.id}",
            f"BOOT {charge_point_vendor} {charge_point_model} "
            f"firmware={kwargs.get('firmware_version', 'n/a')} -> Accepted, heartbeat 30s",
        )
        return call_result.BootNotification(
            current_time=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            interval=30,
            status=RegistrationStatus.accepted,
        )

    @on(Action.authorize)
    def on_authorize(self, id_tag, **kwargs):
        accepted = id_tag == ACCEPTED_TAG
        status = (
            AuthorizationStatus.accepted if accepted else AuthorizationStatus.invalid
        )
        log_line(
            f"CSMS:{self.id}",
            f"AUTHORIZE idTag={id_tag} -> {status.value}",
        )
        return call_result.Authorize(id_tag_info=IdTagInfo(status=status))

    @on(Action.start_transaction)
    def on_start_transaction(self, connector_id, id_tag, meter_start, timestamp, **kwargs):
        transaction_id = next(self._tx_ids)
        self.sessions[transaction_id] = {
            "connector_id": connector_id,
            "id_tag": id_tag,
            "meter_start": meter_start,
            "register": meter_start,
            "started_at": datetime.now(timezone.utc),
        }
        log_line(
            f"CSMS:{self.id}",
            f"START tx={transaction_id} connector={connector_id} idTag={id_tag} "
            f"meterStart={meter_start} Wh",
        )
        return call_result.StartTransaction(
            transaction_id=transaction_id,
            id_tag_info=IdTagInfo(status=AuthorizationStatus.accepted),
        )

    @on(Action.meter_values)
    def on_meter_values(self, connector_id, meter_value, transaction_id=None, **kwargs):
        session = self.sessions.get(transaction_id)
        for entry in meter_value:
            values = {sv.get("measurand", "Energy.Active.Import.Register"): sv
                      for sv in entry["sampled_value"]}
            register = values.get("Energy.Active.Import.Register")
            if register is None:
                continue
            register_wh = float(register["value"])
            power = values.get("Power.Active.Import", {}).get("value", "?")
            current = values.get("Current.Import", {}).get("value", "?")
            if session is not None:
                session["register"] = register_wh
                delivered = (register_wh - session["meter_start"]) / 1000.0
            else:
                delivered = float("nan")
            log_line(
                f"CSMS:{self.id}",
                f"METER tx={transaction_id} register={register_wh:.1f} Wh "
                f"power={power} W current={current} A -> {delivered:.3f} kWh so far",
            )
        return call_result.MeterValues()

    @on(Action.stop_transaction)
    def on_stop_transaction(self, meter_stop, timestamp, transaction_id, **kwargs):
        session = self.sessions.pop(transaction_id, None)
        meter_start = session["meter_start"] if session else 0
        total_kwh = (meter_stop - meter_start) / 1000.0
        duration_s = (
            (datetime.now(timezone.utc) - session["started_at"]).total_seconds()
            if session
            else 0.0
        )
        cost = total_kwh * RATE_INR_PER_KWH
        summary = (
            "SESSION SUMMARY "
            f"tx={transaction_id} meterStart={meter_start} Wh meterStop={meter_stop} Wh "
            f"total={total_kwh:.3f} kWh duration={duration_s:.1f}s "
            f"rate=₹{RATE_INR_PER_KWH:.0f}/kWh cost=₹{cost:.2f}"
        )
        log_line(f"CSMS:{self.id}", summary)
        return call_result.StopTransaction(
            id_tag_info=IdTagInfo(status=AuthorizationStatus.accepted)
        )


async def on_connect(websocket):
    charge_point_id = websocket.request.path.strip("/")
    log_line("CSMS", f"CONNECT charge point '{charge_point_id}' from {websocket.remote_address}")
    connection = LoggingConnection(websocket, tag=f"CSMS:{charge_point_id}")
    cp = CentralSystem(charge_point_id, connection)
    try:
        await cp.start()
    except ConnectionClosed:
        log_line("CSMS", f"DISCONNECT charge point '{charge_point_id}'")


async def main():
    server = await serve(on_connect, "127.0.0.1", 9000, subprotocols=["ocpp1.6"])
    log_line("CSMS", "LISTENING on ws://localhost:9000/<charge_point_id> (OCPP 1.6J)")
    await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
