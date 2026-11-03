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
  server, built on a **shared-bay model**: a pool of **ten residents**,
  each with their own wallet, and **four shared charging bays** — any car
  can use any bay. The whole world lives **server-side** in `csms_live.py`:
  each bay's charge point is a loopback OCPP client of the same CSMS
  (genuine `[2, id, "BootNotification", {...}]` frames, wallet Authorize,
  meter values stamped with the simulation clock), and a traffic engine
  (Play + 1×/3×/10×/30×) generates arrivals. A car that finds a free bay
  plugs in and runs a real session billed to **its owner's** wallet; if
  all bays are busy it parks in **normal parking and queues**, and when a
  session ends the longest-waiting car takes the freed bay, then moves
  back to parking when done. The page is a pure observer + remote
  control: it renders the server's `{"event": "world", ...}` snapshots
  (cars, queue, sessions, ledger, payments) on an animated top-down map
  (building + rooftop solar, load-manager and transformer boxes, four
  bays, a ten-spot parking row, gate), and steers the world with `Sim*`
  controls (`SimPlugIn` pulls the next waiting car — or a new arrival —
  into a free bay; `SimUnplug` ends a session). Everyone who opens the
  page shares one world, and a full snapshot on join restores it
  mid-session. The log shows the exact wire strings relayed by the CSMS
  observer feed — nothing is re-enacted.
- **`test_observer.py` / `test_raw_cp.py` / `test_sim_world.py`** —
  headless checks: a raw Python charge point runs a full session while an
  observer client asserts the frames arrive on the feed. `test_raw_cp.py`
  expects the ToU "Standard" band (₹14/kWh) for its 07:40 UTC timestamps
  and asserts the summary's per-band breakdown; URLs can be overridden
  with `CP_URL` / `OBSERVE_URL`. `test_sim_world.py` plays traffic at
  30× with two observers and proves the queue forms and rotates, sessions
  bill the car owner's wallet, a rejoining observer restores an
  in-progress session from the snapshot, and both observers hold
  identical world state (`OBSERVE_URL` override).

Demo tags (residents): `DEMO1234` ( ₹500), `AYUSH001` (₹250),
`PRIYA002` (₹300), `RAHUL003` (₹150), `MEERA004` (₹400), `ARJUN005` (₹350),
`KAVYA006` (₹300), `ROHAN007` (₹275), `SNEHA008` (₹325), `VIKRAM009` (₹200)
are accepted; anything else (try `GUEST009`) is rejected as Invalid.

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
