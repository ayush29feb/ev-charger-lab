#!/usr/bin/env python3
"""Simulated charge point for the EV charger test lab.

Connects to the CSMS at ws://localhost:9000/SIM_001 and runs one full
OCPP 1.6J session:

  BootNotification -> Authorize (idTag DEMO1234) -> StartTransaction
  -> MeterValues every ~3s for ~30s (7.4 kW: register climbs ~6.2 Wh per
     3 s sample; includes Power.Active.Import and Current.Import)
  -> StopTransaction

All raw OCPP frames are appended to transcript.log, same style as the CSMS.
"""

import asyncio
import os
import random
from datetime import datetime, timezone
from pathlib import Path

from websockets.asyncio.client import connect

from ocpp.v16 import ChargePoint as BaseChargePoint
from ocpp.v16 import call

LAB_DIR = Path(__file__).resolve().parent
TRANSCRIPT = LAB_DIR / "transcript.log"
CP_ID = "SIM_001"
# Default unchanged (port 9000, the original csms_server.py). Override with
# the CSMS_URL env var to point the sim at another CSMS, e.g. the live one:
#   CSMS_URL=ws://localhost:9001/SIM_001 ./venv/bin/python charge_point_sim.py
CSMS_URL = os.environ.get("CSMS_URL", f"ws://localhost:9000/{CP_ID}")
ID_TAG = "DEMO1234"

POWER_W = 7400.0          # simulated 7.4 kW wallbox
VOLTAGE_V = 230.0
METER_START_WH = 104_520  # plausible lifetime register reading
SAMPLE_INTERVAL_S = 3.0
SESSION_SECONDS = 30


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log_line(tag: str, text: str) -> None:
    line = f"{now_stamp()} [{tag}] {text}"
    print(line, flush=True)
    with TRANSCRIPT.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


class LoggingConnection:
    """Wrap the client websocket so raw frames land in the transcript."""

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


def status_of(id_tag_info) -> str:
    if isinstance(id_tag_info, dict):
        return id_tag_info.get("status", "?")
    status = getattr(id_tag_info, "status", "?")
    return getattr(status, "value", str(status))


async def run_session(cp: BaseChargePoint) -> None:
    tag = f"CP:{CP_ID}"

    response = await cp.call(
        call.BootNotification(
            charge_point_model="SimWallbox 7.4",
            charge_point_vendor="MuseLab",
            firmware_version="1.0.0",
        )
    )
    log_line(tag, f"BOOT accepted, heartbeat interval={response.interval}s")

    response = await cp.call(call.Authorize(id_tag=ID_TAG))
    auth_status = status_of(response.id_tag_info)
    log_line(tag, f"AUTHORIZE idTag={ID_TAG} -> {auth_status}")
    if auth_status != "Accepted":
        raise RuntimeError(f"Authorize rejected: {auth_status}")

    register_wh = float(METER_START_WH)
    response = await cp.call(
        call.StartTransaction(
            connector_id=1,
            id_tag=ID_TAG,
            meter_start=METER_START_WH,
            timestamp=utc_now_iso(),
        )
    )
    transaction_id = response.transaction_id
    log_line(tag, f"START transaction_id={transaction_id} meterStart={METER_START_WH} Wh")

    samples = int(SESSION_SECONDS / SAMPLE_INTERVAL_S)
    for i in range(1, samples + 1):
        await asyncio.sleep(SAMPLE_INTERVAL_S)
        power_w = POWER_W + random.uniform(-60, 60)  # small realistic jitter
        register_wh += power_w * SAMPLE_INTERVAL_S / 3600.0  # W * s -> Wh
        current_a = power_w / VOLTAGE_V
        await cp.call(
            call.MeterValues(
                connector_id=1,
                transaction_id=transaction_id,
                meter_value=[
                    {
                        "timestamp": utc_now_iso(),
                        "sampled_value": [
                            {
                                "value": f"{register_wh:.1f}",
                                "measurand": "Energy.Active.Import.Register",
                                "unit": "Wh",
                            },
                            {
                                "value": f"{power_w:.0f}",
                                "measurand": "Power.Active.Import",
                                "unit": "W",
                            },
                            {
                                "value": f"{current_a:.1f}",
                                "measurand": "Current.Import",
                                "unit": "A",
                            },
                            {
                                "value": f"{VOLTAGE_V:.0f}",
                                "measurand": "Voltage",
                                "unit": "V",
                            },
                        ],
                    }
                ],
            )
        )
        log_line(
            tag,
            f"METER sample {i}/{samples} register={register_wh:.1f} Wh "
            f"({(register_wh - METER_START_WH) / 1000.0:.3f} kWh delivered)",
        )

    meter_stop = int(round(register_wh))
    await cp.call(
        call.StopTransaction(
            transaction_id=transaction_id,
            meter_stop=meter_stop,
            timestamp=utc_now_iso(),
        )
    )
    log_line(
        tag,
        f"STOP tx={transaction_id} meterStop={meter_stop} Wh "
        f"total={(meter_stop - METER_START_WH) / 1000.0:.3f} kWh",
    )


async def main():
    async with connect(CSMS_URL, subprotocols=["ocpp1.6"]) as websocket:
        connection = LoggingConnection(websocket, tag=f"CP:{CP_ID}")
        cp = BaseChargePoint(CP_ID, connection)
        receiver = asyncio.create_task(cp.start())
        try:
            await run_session(cp)
        finally:
            receiver.cancel()
            await websocket.close()
    log_line(f"CP:{CP_ID}", "SESSION COMPLETE")


if __name__ == "__main__":
    asyncio.run(main())
