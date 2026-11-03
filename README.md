# EV Charger OCPP Test Lab

A minimal, working OCPP 1.6J charge point + CSMS pair, built with the
mobilityhouse [`ocpp`](https://github.com/mobilityhouse/ocpp) Python library.
It runs one complete charging session — boot, authorize, start, meter values,
stop, billing summary — with every raw OCPP frame logged.

## Files

| File | What it is |
|---|---|
| `csms_server.py` | The CSMS (central system). Listens on `ws://localhost:9000/<charge_point_id>`. |
| `charge_point_sim.py` | A simulated wallbox ("SIM_001") that runs one full 30 s session at 7.4 kW. |
| `transcript.log` / `transcript.txt` | Captured run: every raw OCPP JSON frame from both sides, timestamped, plus decoded notes and the server's SESSION SUMMARY. |
| `requirements.txt` | Pinned dependencies (`ocpp==2.0.0`, `websockets==17.4`). Pinned to 2.0.0 because 2.1.0 requires Python ≥3.11 and Fly's Buildpack builder used Python 3.10. |
| `venv/` | Python 3.12 virtualenv with the dependencies installed. |

## What the CSMS does

- **BootNotification** → Accepted, heartbeat interval 30 s
- **Authorize** → accepts idTag `DEMO1234`, rejects any other tag
- **StartTransaction** → allocates a transaction id, records `meterStart`
- **MeterValues** → logs the energy register, live power/current, and kWh delivered so far
- **StopTransaction** → logs a `SESSION SUMMARY` with total kWh, duration, and
  cost at a sample tariff of **₹18/kWh**

Note: `ocpp` 2.x has no separate `CentralSystem` class for OCPP 1.6 — the
central system is a `ChargePoint` subclass whose handlers *receive* calls
instead of sending them. `csms_server.py` names that class `CentralSystem`
after its role.

## Re-run it

```bash
cd ~/workspace/ev-charger-lab

# one-time setup (already done in this checkout)
python3 -m venv venv && ./venv/bin/pip install -r requirements.txt

# terminal 1 — start the CSMS (leave it running)
./venv/bin/python csms_server.py

# terminal 2 — run one simulated charging session (~35 s)
./venv/bin/python charge_point_sim.py
```

Both processes append to `transcript.log`. To capture a fresh transcript,
stop the server, delete `transcript.log`, and repeat the two steps above.

## Verified run (2026-11-03)

One clean session, first attempt, no errors:

```
BootNotification  -> Accepted (heartbeat 30 s)
Authorize         -> DEMO1234 Accepted
StartTransaction  -> transactionId 1, meterStart 104520 Wh
MeterValues x10   -> register 104526.2 ... 104581.5 Wh (~7.4 kW, ~32 A)
StopTransaction   -> meterStop 104582 Wh
SESSION SUMMARY   -> total 0.062 kWh, duration 30.1 s, cost ₹1.12 @ ₹18/kWh
```

The tiny energy figure is real physics, not a bug: 7.4 kW for 30 seconds is
only ~0.06 kWh. A real overnight session at this rate would deliver ~50 kWh.

## Web simulator

`index.html` is a single-file, game-style visualization of this lab: cars drive
into bays on one side while the OCPP 1.6J message exchange streams on the other
(BootNotification -> Authorize -> StartTransaction -> MeterValues ->
StopTransaction, billed at ₹18/kWh). No build step and no server — open the
file in a browser, or host it free with GitHub Pages: repo **Settings -> Pages
-> Deploy from a branch -> main / (root)**, then it is live at
`https://ayush29feb.github.io/ev-charger-lab/`.
