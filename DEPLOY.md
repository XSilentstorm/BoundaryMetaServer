# Boundary MetaServer — Deployment Notes

What needs to change to deploy this to a host, and why Render is a poor fit.

---

## TL;DR

`process.env.PORT` is **already** wired up for the HTTP API (`index.js:23`), but the server
also binds **two more TCP ports + a UDP socket**, which Render (and most PaaS) can't expose.
For a game backend like this, a **VPS** (or **Fly.io**) is a much better fit than Render.

---

## 1. Changes that are needed no matter where you deploy

### 1.1 `package.json`
- Add a `start` script (hosts run `npm start`):
  ```json
  "scripts": {
    "start": "node index.js",
    "test": "echo \"Error: no test specified\" && exit 1"
  }
  ```
- (optional) Pin the Node version so the host uses a matching runtime:
  ```json
  "engines": { "node": ">=18" }
  ```

### 1.2 `index.js` — make ports + host configurable
Currently hardcoded:
```js
const HTTP_PORT = process.env.PORT || 8000;      // OK already
const RPC_PORT = 6969;                           // hardcoded
const MATCHMAKING_PORT = 9000;                   // hardcoded
const MATCHMAKING_HOST = "127.0.0.1";            // hardcoded -> MUST change
```
Change to:
```js
const HTTP_PORT = process.env.PORT || 8000;
const RPC_PORT = Number(process.env.RPC_PORT || 6969);
const MATCHMAKING_PORT = Number(process.env.MATCHMAKING_PORT || 9000);
const MATCHMAKING_HOST = process.env.MATCHMAKING_HOST || "127.0.0.1";
```
`MATCHMAKING_HOST` is the most important one: it's echoed back to the client so it knows
where to connect for matchmaking. `127.0.0.1` only works when the client is on the same box.

### 1.3 Point the DLL at the deployment host
The injected DLL currently talks to the metaserver at `127.0.0.1:8000`. That hardcoded
address needs to become the public hostname/URL of wherever you deploy.

### 1.4 Data files
`data/<steamid>.json` are the per-player loadout files. Make sure the test player's file is
present in the deployed `data/` directory (they're committed, so this is automatic if you
deploy from git).

---

## 2. Why Render specifically is a poor fit

Render exposes exactly **one TCP port** via `process.env.PORT`, and does **not** route UDP.

This server needs three things:

| Service | Protocol | Port | Works on Render? |
|---|---|---|---|
| HTTP API (loadout JSON) | TCP | `process.env.PORT` | ✅ yes |
| RPC (`connectServer` handshake) | TCP | 6969 | ❌ not exposed |
| Matchmaking (TCP) | TCP | 9000 | ❌ not exposed |
| Matchmaking (QoS probe) | UDP | 9000 | ❌ no UDP routing |

So on Render you'd effectively get **HTTP-only** (the loadout JSON), and the game's
connect/matchmaking flow would break. If HTTP-only is all you need, Render works — but you'd
want to make the RPC/matchmaking servers fail *softly* (not crash) when their ports are
unreachable.

---

## 3. Better alternatives than Render

### 3.1 VPS — best fit for this architecture (recommended)
A cheap VPS gives you a public IP and full control over **all TCP ports + UDP**, which is
exactly what a multi-port game backend needs.

- **Hetzner**, **DigitalOcean**, **Vultr**, **Linode**, **OVH** (≈ $4–6/mo, some have free trials)
- Deploy: `git clone` → `npm install` → `node index.js` (or `pm2 start index.js`)
- Open ports `8000`, `6969/tcp`, `9000/tcp+udp` in the firewall
- Everything works as-is (after the §1 changes), including UDP matchmaking

### 3.2 Fly.io — the PaaS that actually fits
Fly.io supports **multiple TCP ports** and **UDP**, so it handles this server far better than
Render while staying close to a PaaS experience.

- `fly.toml` can expose `internal_port = 6969`, `9000`, etc., plus UDP
- Free tier available for a small test
- Still need the §1 changes (env-var ports + `start` script)

### 3.3 Cloudflare Tunnel — zero new server (fastest for an HTTP-only test)
If you only need the **HTTP loadout flow** and want to test *right now* without provisioning
anything, run the metaserver on your existing PC and expose it with `cloudflared`:

- `cloudflared tunnel --url http://localhost:8000` → gives you a public `*.trycloudflare.com` URL
- Point the DLL at that URL
- Limitation: **TCP only** — the UDP QoS probe won't work (fine if you don't need matchmaking QoS)

---

## 4. Decision table

| You need… | Use |
|---|---|
| Full flow (loadout + RPC + matchmaking + UDP) | **VPS** (or Fly.io) |
| Just the loadout JSON (HTTP) | Render, Railway, or Cloudflare Tunnel |
| Fastest zero-setup test of loadout JSON | **Cloudflare Tunnel** from your PC |
