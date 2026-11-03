# Live version — the original OCPP 1.6J protocol, end to end

Unlike `../index.html` (a self-contained in-browser reenactment), everything
here speaks **genuine OCPP 1.6J over a real WebSocket**:

- **`csms_live.py`** — a real Python CSMS (central system). OCPP endpoint:
  `ws://<host>:9001/<chargePointId>` (subprotocol `ocpp1.6`), observer feed on
  `ws://<host>:9001/observe`, and it serves this folder's static files on
  `http://<host>:8080/`. Handles BootNotification, Authorize (wallet check),
  StartTransaction, MeterValues, StatusNotification, Heartbeat, StopTransaction
  (bills against in-memory wallets with **time-of-use pricing**: each
  MeterValues interval is priced by the tariff band of its own timestamp —
  Night 23:00–06:00 ₹10/kWh, Solar hours 09:00–16:00 ₹12/kWh, Evening peak
  17:00–22:00 ₹18/kWh, all other hours ₹14/kWh — and the session summary
  carries a per-band breakdown), and forwards operator commands
  from observers as genuine server-initiated CALLs (RemoteStartTransaction,
  RemoteStopTransaction, Reset, UnlockConnector, ChangeAvailability,
  TriggerMessage). Every raw frame is broadcast to observers verbatim.
- **`visualizer.html`** — a housing-society simulator on top of the real
  server: an animated top-down map (building + rooftop solar, load-manager
  and transformer boxes, six bays) whose cars drive in, plug in, charge and
  leave. The six bays are **real OCPP clients written in JavaScript**: each
  opens its own WebSocket to the CSMS and sends actual
  `[2, id, "BootNotification", {...}]` frames, waits for `[3, ...]`
  replies, runs sessions, and answers server-initiated calls. A traffic
  engine (Play + 1×/3×/10×/30×) runs a simulation clock that drives the
  tariff band, the modelled solar curve and the meter timestamps, and
  generates resident arrivals that run genuine sessions through the
  server. The log shows the exact wire strings relayed by the CSMS
  observer feed — nothing is re-enacted.
- **`test_observer.py` / `test_raw_cp.py`** — headless checks: a raw Python
  charge point runs a full session while an observer client asserts the frames
  arrive on the feed. `test_raw_cp.py` expects the ToU "Standard" band
  (₹14/kWh) for its 07:40 UTC timestamps and asserts the summary's per-band
  breakdown; URLs can be overridden with `CP_URL` / `OBSERVE_URL`.

Demo tags: `DEMO1234` (wallet ₹500), `AYUSH001` (₹250), `PRIYA002` (₹300),
`RAHUL003` (₹150), `MEERA004` (₹400), `ARJUN005` (₹350) are accepted;
anything else (try `GUEST009`) is rejected as Invalid.

## Run locally

```bash
cd live
python csms_live.py            # needs: pip install websockets
# open http://localhost:8080/visualizer.html
```

The root lab's Python charger can also connect as a hardware stand-in:
`python ../charge_point_sim.py` (points at ws://localhost:9001/SIM_001).

## Other hosts (single-port mode)

If the `PORT` environment variable is set (Render, Railway, Cloud Run and
most PaaS hosts set it automatically and expose exactly **one** public port),
`csms_live.py` switches to single-port mode: the visualizer page, `/healthz`,
the OCPP WebSocket endpoint and `/observe` all share that one port, and the
visualizer is served with its WebSocket port rewritten to match. A
`render.yaml` Blueprint for Render is included at the repo root; the root
Dockerfile also works as-is on Railway / Koyeb / Northflank / any VPS
(`docker build . && docker run -e PORT=8080 -p 8080:8080 <image>` — or leave
`PORT` unset on a VPS and publish both 8080 and 9001 in two-port mode).

## Deploy to Fly.io

GitHub Pages cannot host this version (Pages is static-only; a real server
must run). From this folder:

```bash
fly launch --copy-config       # uses the included fly.toml + Dockerfile
fly deploy
fly apps open                  # https://<your-app>.fly.dev/
```

Notes:
- Fly app names are globally unique — if `ev-charger-lab` is taken, flyctl
  will offer another name; everything else in fly.toml still applies.
- Fly usually requires a card on file before the first deploy.
- Ports: page on 80/443 → internal 8080; OCPP/observer WebSocket on external
  9001 (TLS terminated at the edge) → internal 9001. The visualizer picks
  `wss://` automatically when the page itself is served over HTTPS.
- Region is `sin` (Singapore) in fly.toml; machines are set to never
  auto-stop because chargers hold persistent connections.
