# Live version — the original OCPP 1.6J protocol, end to end

Unlike `../index.html` (a self-contained in-browser reenactment), everything
here speaks **genuine OCPP 1.6J over a real WebSocket**:

- **`csms_live.py`** — a real Python CSMS (central system). OCPP endpoint:
  `ws://<host>:9001/<chargePointId>` (subprotocol `ocpp1.6`), observer feed on
  `ws://<host>:9001/observe`, and it serves this folder's static files on
  `http://<host>:8080/`. Handles BootNotification, Authorize (wallet check),
  StartTransaction, MeterValues, StatusNotification, Heartbeat, StopTransaction
  (bills at ₹18/kWh against in-memory wallets), and forwards operator commands
  from observers as genuine server-initiated CALLs (RemoteStartTransaction,
  RemoteStopTransaction, Reset, UnlockConnector, ChangeAvailability,
  TriggerMessage). Every raw frame is broadcast to observers verbatim.
- **`visualizer.html`** — the chargers in the lot are **real OCPP clients
  written in JavaScript**: each opens its own WebSocket to the CSMS and sends
  actual `[2, id, "BootNotification", {...}]` frames, waits for `[3, ...]`
  replies, runs sessions, and answers server-initiated calls. The terminal
  shows the exact wire strings relayed by the CSMS observer feed — nothing is
  re-enacted. Operator buttons send control JSON on `/observe`, which the
  CSMS forwards as real OCPP CALLs.
- **`test_observer.py` / `test_raw_cp.py`** — headless checks: a raw Python
  charge point runs a full session while an observer client asserts the frames
  arrive on the feed. Verified locally 2026-11-03 (session billed 0.062 kWh,
  ₹1.12, wallet debited; all core frames observed).

Demo tags: `DEMO1234` (wallet ₹500), `AYUSH001`, `PRIYA002`, `RAHUL003` are
accepted; anything else (try `GUEST009`) is rejected as Invalid.

## Run locally

```bash
cd live
python csms_live.py            # needs: pip install websockets
# open http://localhost:8080/visualizer.html
```

The root lab's Python charger can also connect as a hardware stand-in:
`python ../charge_point_sim.py` (points at ws://localhost:9001/SIM_001).

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
