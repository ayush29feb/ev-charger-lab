#!/usr/bin/env python3
"""Second headless test for csms_live.py: a raw-frame mini charge point.

Speaks hand-built OCPP 1.6J frames over `websockets` exactly the way the
browser JS clients in visualizer.html do (same message shapes), and checks:

  * BootNotification -> Accepted
  * StatusNotification -> {"event": "state"} broadcast on /observe
  * Authorize GUEST009 -> Invalid (rejected), AYUSH001 -> Accepted
  * StartTransaction + MeterValues
  * observer control RemoteStopTransaction -> forwarded as a real [2] CALL,
    this CP replies [3] {"status": "Accepted"}, then sends StopTransaction
  * {"event": "summary"} bills AYUSH001's wallet with time-of-use pricing:
    all timestamps below are 07:40-07:41 UTC, which is the "Standard" band
    (₹14/kWh), so 1.000 kWh costs ₹14.00 and the summary must carry a
    per-band breakdown totalling the same

Usage: test_raw_cp.py   (exit 0 = all pass)
URLs default to ws://localhost:9001/... and can be overridden with the
CP_URL / OBSERVE_URL environment variables (single-port local runs and
the live Render deployment).
"""

import asyncio
import json
import os
import sys
import time

from websockets.asyncio.client import connect

CP_URL = os.environ.get("CP_URL", "ws://localhost:9001/SIM_777")
OBSERVE_URL = os.environ.get("OBSERVE_URL", "ws://localhost:9001/observe")

results = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append(ok)
    print(f"[RAWTEST] {'PASS' if ok else 'FAIL'}: {name} {detail}", flush=True)


async def main() -> int:
    observed = {"frames": [], "events": []}

    async with connect(OBSERVE_URL) as obs, \
            connect(CP_URL, subprotocols=["ocpp1.6"]) as cp:

        async def obs_pump():
            try:
                async for raw in obs:
                    msg = json.loads(raw)
                    if "raw" in msg:
                        observed["frames"].append(msg)
                    else:
                        observed["events"].append(msg)
            except Exception:
                pass

        pump = asyncio.create_task(obs_pump())
        seq = 0

        async def call(action, payload):
            nonlocal seq
            seq += 1
            mid = f"raw-{seq}"
            await cp.send(json.dumps([2, mid, action, payload]))
            while True:
                frame = json.loads(await cp.recv())
                if frame[0] in (3, 4) and frame[1] == mid:
                    return frame

        # Boot + status
        r = await call("BootNotification", {
            "chargePointVendor": "MuseLab", "chargePointModel": "SimWallbox 7.4",
            "chargePointSerialNumber": "SN-SIM_777", "firmwareVersion": "1.0.0"})
        check("BootNotification Accepted",
              r[0] == 3 and r[2].get("status") == "Accepted", json.dumps(r[2]))

        await call("StatusNotification", {
            "connectorId": 1, "errorCode": "NoError",
            "status": "Available", "timestamp": "2026-11-03T07:40:00Z"})

        # Rejected tag, then a wallet tag
        r = await call("Authorize", {"idTag": "GUEST009"})
        check("Authorize GUEST009 -> Invalid",
              r[2].get("idTagInfo", {}).get("status") == "Invalid",
              json.dumps(r[2]))
        r = await call("Authorize", {"idTag": "AYUSH001"})
        check("Authorize AYUSH001 -> Accepted",
              r[2].get("idTagInfo", {}).get("status") == "Accepted",
              json.dumps(r[2]))

        # Start + one meter value (register 50000 -> 51000 over the session)
        r = await call("StartTransaction", {
            "connectorId": 1, "idTag": "AYUSH001", "meterStart": 50000,
            "timestamp": "2026-11-03T07:40:05Z"})
        tx = r[2].get("transactionId")
        check("StartTransaction got transactionId", isinstance(tx, int),
              f"tx={tx}")
        await call("MeterValues", {
            "connectorId": 1, "transactionId": tx,
            "meterValue": [{"timestamp": "2026-11-03T07:40:10Z",
                            "sampledValue": [
                                {"value": "50500.0",
                                 "measurand": "Energy.Active.Import.Register",
                                 "unit": "Wh"},
                                {"value": "7400",
                                 "measurand": "Power.Active.Import",
                                 "unit": "W"}]}]})

        # Operator control via observer: RemoteStopTransaction
        await obs.send(json.dumps({"control": "RemoteStopTransaction",
                                   "cpId": "SIM_777"}))
        # The CSMS forwards it to this CP as a real CALL — answer it.
        frame = json.loads(await asyncio.wait_for(cp.recv(), timeout=10))
        is_remote_stop = (frame[0] == 2 and frame[2] == "RemoteStopTransaction"
                          and frame[3].get("transactionId") == tx)
        check("RemoteStopTransaction forwarded as real [2] CALL",
              is_remote_stop, json.dumps(frame)[:200])
        await cp.send(json.dumps([3, frame[1], {"status": "Accepted"}]))

        # CP ends the session: 51000 Wh stop => 1.000 kWh, all of it inside
        # the 07:xx UTC "Standard" band (₹14/kWh) => ₹14.00
        r = await call("StopTransaction", {
            "transactionId": tx, "meterStop": 51000,
            "timestamp": "2026-11-03T07:41:00Z", "reason": "Remote"})
        check("StopTransaction Accepted",
              r[2].get("idTagInfo", {}).get("status") == "Accepted",
              json.dumps(r[2]))

        await asyncio.sleep(1.0)  # let the pump drain broadcasts
        pump.cancel()

    states = [e for e in observed["events"] if e.get("event") == "state"
              and e.get("cpId") == "SIM_777"]
    check("state event broadcast for StatusNotification",
          any(e.get("status") == "Available" for e in states),
          json.dumps(states)[:200])
    summaries = [e for e in observed["events"] if e.get("event") == "summary"]
    s = summaries[0] if summaries else {}
    bands = s.get("bands") or []
    bands_ok = (
        len(bands) == 1 and bands[0].get("band") == "standard"
        and bands[0].get("rate") == 14.0
        and abs(bands[0].get("kwh", 0) - 1.0) < 0.001
        and abs(bands[0].get("cost", 0) - 14.0) < 0.01
    )
    check("summary bills 1.000 kWh / ₹14.00 (ToU Standard band) to AYUSH001",
          s.get("kwh") == 1.0 and s.get("cost") == 14.0
          and s.get("walletBalance") == 236.0 and bands_ok,
          json.dumps(s))
    ctrl = [e for e in observed["events"]
            if e.get("event") == "control_result"]
    check("control_result reports Accepted from the CP",
          any(e.get("result", {}).get("status") == "Accepted" for e in ctrl),
          json.dumps(ctrl)[:200])

    ok = all(results) and len(results) >= 8
    print(f"[RAWTEST] RESULT: {'ALL PASS' if ok else 'FAILURES PRESENT'} "
          f"({sum(results)}/{len(results)} checks)", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
