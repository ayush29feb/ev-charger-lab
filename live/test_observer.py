#!/usr/bin/env python3
"""Headless observer test for csms_live.py.

Connects to ws://localhost:9001/observe and records every broadcast while
the root charge_point_sim.py runs a full session against the live CSMS on
ws://localhost:9001/SIM_001. Mid-session it sends one control message
(TriggerMessage) through the observer channel and records the forwarded
OCPP CALL frame and the charge point's reply.

Usage:  test_observer.py [listen_seconds] [control_at_seconds]
Exit code 0 if all required actions were observed on the wire.
"""

import asyncio
import json
import sys
import time

from websockets.asyncio.client import connect

OBSERVE_URL = "ws://localhost:9001/observe"
REQUIRED_ACTIONS = [
    "BootNotification", "Authorize", "StartTransaction",
    "MeterValues", "StopTransaction",
]


async def main() -> int:
    listen_s = float(sys.argv[1]) if len(sys.argv) > 1 else 60.0
    control_at = float(sys.argv[2]) if len(sys.argv) > 2 else 14.0

    frames = []          # (t, dir, cpId, raw)
    events = []          # non-frame broadcast objects
    started = time.monotonic()
    control_sent = False
    control = {
        "control": "TriggerMessage",
        "cpId": "SIM_001",
        "requestedMessage": "MeterValues",
    }

    async with connect(OBSERVE_URL) as ws:
        print(f"[TEST] observer connected to {OBSERVE_URL}", flush=True)
        while time.monotonic() - started < listen_s:
            if not control_sent and time.monotonic() - started >= control_at:
                await ws.send(json.dumps(control))
                print(f"[TEST] sent control: {json.dumps(control)}", flush=True)
                control_sent = True
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            t = time.monotonic() - started
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if "raw" in msg:
                frames.append((t, msg.get("dir"), msg.get("cpId"), msg["raw"]))
                print(f"[TEST] t={t:5.1f}s frame {msg.get('dir')} "
                      f"{msg.get('cpId')}: {msg['raw'][:220]}", flush=True)
            else:
                events.append((t, msg))
                print(f"[TEST] t={t:5.1f}s event: {raw[:260]}", flush=True)

    # --- assertions -----------------------------------------------------
    seen_actions = set()
    for _, _, _, raw in frames:
        try:
            frame = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(frame, list) and len(frame) >= 3 and frame[0] == 2:
            seen_actions.add(frame[2])

    ok = True
    print("\n[TEST] ===== assertions =====", flush=True)
    for action in REQUIRED_ACTIONS:
        present = action in seen_actions
        ok = ok and present
        print(f"[TEST] {'PASS' if present else 'FAIL'}: raw CALL frame "
              f"for {action} observed on /observe", flush=True)

    summaries = [e for _, e in events if e.get("event") == "summary"]
    print(f"[TEST] {'PASS' if summaries else 'FAIL'}: session summary "
          f"event received ({len(summaries)}): "
          f"{json.dumps(summaries[0]) if summaries else 'none'}", flush=True)
    ok = ok and bool(summaries)

    forwarded = [r for _, d, _, r in frames
                 if d == "csms->cp" and '"TriggerMessage"' in r]
    replies = [r for _, d, _, r in frames
               if d == "cp->csms" and (r.startswith("[3") or r.startswith("[4"))]
    ctrl_results = [e for _, e in events if e.get("event") in
                    ("control_result", "control_error")]
    print(f"[TEST] {'PASS' if forwarded else 'FAIL'}: control forwarded "
          f"as a real OCPP CALL frame: {forwarded[0][:220] if forwarded else 'none'}",
          flush=True)
    ok = ok and bool(forwarded)
    print(f"[TEST] INFO: TriggerMessage reply frames from CP after forward: "
          f"{len(replies)} total [3]/[4] frames seen; control events: "
          f"{json.dumps(ctrl_results)[:300]}", flush=True)

    print(f"[TEST] total frames observed: {len(frames)}", flush=True)
    print(f"[TEST] RESULT: {'ALL PASS' if ok else 'FAILURES PRESENT'}", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
