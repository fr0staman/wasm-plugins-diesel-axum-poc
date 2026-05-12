# wasm-plugins-diesel-axum-poc

A proof-of-concept for a runtime plugin system built on the **WebAssembly Component Model**.
Plugins are compiled to `.wasm` once and loaded by an Axum host at startup. Each plugin:

- declares its HTTP / WebSocket / SSE routes and event subscriptions via an OpenAPI spec
- typed event system between host <-> plugin <-> plugin, own extendable event system for plugins
- runs in a sandboxed Wasmtime store with fuel limits and table-access validation
- can read and write its own Postgres tables via Diesel queries that cross the WASM boundary
- optionally requires JWT authentication on individual routes, declared in OpenAPI
- possible async work (need to wait wasm32-wasip3)

---

## Quick start

```bash
# 1. Prerequisites
rustup target add wasm32-wasip2

# 2. Build plugins
bash scripts/build-plugins.sh

# 3. Configure
cp .env.example .env          # edit DATABASE_URL, PLUGINS, JWT_SECRET

# 4. Run
cargo run -p host
```

The host starts on `http://localhost:3000` by default.

---

## Environment variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | yes | — | `postgresql://user:pass@host:5432/db` |
| `PLUGINS` | yes | — | Comma-separated plugin names, e.g. `bonus,push,stream,wsecho` |
| `JWT_SECRET` | yes | — | HMAC-SHA256 secret used to sign and verify JWTs |
| `WASM_PLUGIN_DIR` | no | `plugins` | Directory containing compiled `.wasm` files |
| `HOST_PORT` | no | `3000` | TCP port for the Axum server |
| `RUST_LOG` | no | `info` | Tracing filter, e.g. `debug`, `host=trace` |

---

## API

### Host endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/health` | — | Liveness check |
| `GET` | `/plugins` | — | List loaded plugins with routes and subscriptions |
| `POST` | `/events` | JWT | Emit a domain event into the plugin bus |
| `GET` | `/ws/push` | — | WebSocket — streams `reward_granted` events to subscribers |
| `GET` | `/api-docs/openapi.json` | — | Merged OpenAPI 3.0 spec (host + all plugins) |

### Users / Payments CRUD

All CRUD endpoints require `Authorization: Bearer <token>`.

```
GET  /users?tenant_id=    POST /users
GET  /users/{id}          PATCH /users/{id}          DELETE /users/{id}

GET  /payments?user_id=   POST /payments
GET  /payments/{id}                                  DELETE /payments/{id}
```

### Plugin passthrough

| Path | Description |
|------|-------------|
| `/p/{plugin}/{*path}` | HTTP request forwarded to plugin |
| `/ws/p/{plugin}/{*path}` | WebSocket upgrade to plugin |
| `/sse/p/{plugin}/{*path}` | SSE stream from plugin |

Unregistered routes return `404` before touching WASM. Routes annotated with
`bearerAuth` in the plugin's OpenAPI spec return `401` before the WASM boundary
is crossed.

---

## Bundled plugins

| Plugin | Routes | Events | Auth |
|--------|--------|--------|------|
| `bonus` | `GET /status`, `POST /calculate`, `GET /ledger/{user_id}` | subscribes `payment_made`, emits `reward_granted` | `POST /calculate` requires JWT |
| `push` | `GET /status`, `POST /send`, `GET /notifications/{user_id}` | subscribes `reward_granted` | open |
| `stream` | SSE `GET /generate?count=N` | — | open |
| `wsecho` | WS `GET /chat` | — | open |

### Example requests

```bash
# Issue a token (replace SECRET with JWT_SECRET from .env)
TOKEN=$(curl -s -X POST http://localhost:3000/auth/token \
  -H 'Content-Type: application/json' \
  -d '{"sub":"1","tenant_id":1,"role":"admin"}' | jq -r .token)

# Protected HTTP route
curl -X POST http://localhost:3000/p/bonus/calculate \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"user_id":1,"user_tier":"pro","amount_cents":5000}'

# SSE stream (open — no token needed)
curl -N http://localhost:3000/sse/p/stream/generate?count=5

# WebSocket echo (open)
wscat -c ws://localhost:3000/ws/p/wsecho/chat
```

---

## Project layout

```
wit/plugin.wit              ← authoritative WIT contract (host + all plugins generate from this)
crates/
  host/                     ← Axum server + Wasmtime runtime
  diesel-wasm-bridge/       ← Diesel helpers usable inside WASM
plugins/
  bonus/                    ← bonus ledger plugin
  push/                     ← push notification plugin
  stream/                   ← SSE demo plugin
  wsecho/                   ← WebSocket echo plugin
scripts/
  build-plugins.sh          ← build all plugins to wasm32-wasip2
```

See [CLAUDE.md](CLAUDE.md) for the full developer guide: architecture, WIT contract,
adding new plugins, and known limitations.
