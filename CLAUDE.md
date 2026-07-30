# wasm-plugins-diesel-axum-poc

Proof-of-concept for a host/plugin system where plugins are compiled to
WebAssembly (WASM Component Model), loaded at runtime by a Tokio/Axum host,
and given sandboxed access to a shared Postgres database.

---

## Architecture overview

```
┌──────────────────────────────────────────────────────────────────┐
│  Host (crates/host)  –  Axum HTTP server + Wasmtime runtime      │
│                                                                  │
│  ┌──────────────┐   ┌───────────────┐   ┌───────────────────┐   │
│  │ PluginRuntime│   │ PluginExecutor│   │    Dispatcher     │   │
│  │    (Arc)     │──▶│  (Arc clone)  │   │ (broadcast chan)  │   │
│  └──────────────┘   └───────────────┘   └───────────────────┘   │
│          │                  │                      │             │
│   stores PluginPre<T>  makes Store<T>      sends EventEnvelope   │
│                                                      │           │
│  ┌───────────────────────────────────────────────────▼─────┐    │
│  │  ws_tx  broadcast::Sender<String>  →  /ws/push clients  │    │
│  └─────────────────────────────────────────────────────────┘    │
└──────────┬──────────────────┬────────────────────────────────────┘
           │  WIT ABI         │
    ┌──────▼──────┐    ┌──────▼──────┐
    │ bonus.wasm  │    │  push.wasm  │
    └─────────────┘    └─────────────┘
```

### Key components

| Path | Role |
|------|------|
| `wit/plugin.wit` | **Single authoritative WIT contract** between host and all plugins |
| `crates/host/` | Axum server, Wasmtime runtime, DB pool, event bus |
| `crates/host/src/models.rs` | Diesel models for `users` and `payments` (host-owned tables) |
| `crates/host/src/repository.rs` | Async Diesel CRUD for users and payments |
| `crates/host/src/schema.rs` | Diesel `table!` macros for host-owned tables |
| `crates/diesel-wasm-bridge/` | `render_query`, `decode<T>`, `migration!` — Diesel helpers usable inside WASM |
| `plugins/bonus/` | Calculates per-user daily bonuses; subscribes to `payment_made` |
| `plugins/push/` | Sends push notifications; subscribes to `reward_granted` |
| `plugins/stream/` | SSE demo — streams N JSON items; no DB |
| `plugins/wsecho/` | WebSocket echo demo — uppercases every message |

### Runtime flow

1. **Load** — `load_plugin` pre-links the component once (`PluginPre`) then calls
   `manifest()` (one WASM instantiation) to retrieve all static metadata and run migrations.
   Routes and auth requirements are parsed from the plugin's OpenAPI spec (`manifest.openapi`).
2. **HTTP** — `prepare_http_with_auth` resolves route existence and the JWT-required flag in
   one pass; WASM runs lock-free via `PluginExecutor::call_http`. Protected routes require
   `Authorization: Bearer <token>`; validated claims are injected as `x-auth-*` headers.
3. **WS / SSE** — `prepare_websocket` / `prepare_sse` validate the path and return an auth flag.
   If protected, the JWT is verified on the initial HTTP upgrade/request before the connection
   is established. WS/SSE stores use `u64::MAX` fuel (long-lived; not subject to per-call budget).
4. **Events** — `collect_event_handlers` snapshots handlers, releases the lock,
   then `call_event` runs each WASM handler concurrently.
5. **DB** — Plugins build queries with Diesel (compile-time type-checked), render
   them to `RenderedQuery` via `diesel-wasm-bridge`, then cross the WASM boundary
   via `db_query` / `db_execute`. The host validates table names (`plugin_{name}_*`
   prefix) before executing.
6. **Structured data** — Plugins can also call typed host-API functions
   (`get_user`, `create_payment`, etc.) without writing any SQL.
7. **Push** — After a `reward_granted` event the runtime broadcasts a JSON message
   over `ws_tx`; WebSocket clients connected to `/ws/push` receive it in real time.

---

## WIT contract (`wit/plugin.wit`)

The WIT file is the only source of truth. Both sides generate from it:
- **Host** — `wasmtime::component::bindgen!` reads it at `cargo build` time.
- **Plugins** — `wit_bindgen::generate!` in `src/lib.rs` regenerates `bindings` at `cargo build` time.

### Key types

| WIT type | Purpose |
|----------|---------|
| `plugin-manifest` | All static metadata returned in one call at load time (`name`, `version`, `subscribed-events`, `migrations`, `openapi`) |
| `system-event` | Typed variant: `payment-made(payment-made-event)` or `reward-granted(reward-granted-event)` |
| `custom-event` | Plugin-introduced event: `name: string` + opaque `payload: list<u8>` |
| `event-payload` | Top-level union: `system(system-event)` or `custom(custom-event)` |
| `event-subscription` | What a plugin subscribes to: `system(system-event-kind)` or `custom(string)` |
| `event-envelope` | Full event wrapper with `id`, `emitted-at`, `chain-depth`, `source`, `payload` |
| `user-snapshot` | Read-only view of a user passed across the boundary in system events |
| `payment-snapshot` | Read-only view of a payment passed in `payment-made` events |
| `rendered-query` | SQL + binary bind values rendered by Diesel on the WASM side |
| `plugin-error` | `db-error \| invalid-input \| not-found \| internal` |

The `plugin-manifest.openapi` field is the single source of truth for routes **and** security.
The host parses the OpenAPI JSON to derive `http_routes`, `ws_routes`, `sse_routes`, and
`auth_required_routes`. A route is protected when its OpenAPI operation has a non-empty
`security` array (e.g. `security: [{"bearerAuth": []}]`).

### `host-api` functions

| Function | Description |
|----------|-------------|
| `db-query(rendered-query)` | Run a SELECT; returns raw row bytes |
| `db-execute(rendered-query)` | Run INSERT/UPDATE/DELETE; returns affected row count |
| `emit-event(event-payload)` | Publish an event onto the bus from within a plugin |
| `cache-get / cache-set` | In-process TTL key-value store shared across plugins |
| `log(level, msg)` | Write structured log output via the host's tracing subscriber |
| `get-user / list-users / create-user` | Typed access to the host's `users` table |
| `get-payment / list-user-payments / create-payment` | Typed access to the host's `payments` table |
| `ws-recv / ws-send` | Receive/send a message on the active WebSocket connection |
| `sse-yield(data)` | Push a chunk on the active SSE stream |

### Adding a new system event type

1. Add a variant to `system-event` and a case to `system-event-kind` in `wit/plugin.wit`.
2. Add the matching arm to `system_event_kind()` in `crates/host/src/runtime.rs`.
3. Add the matching arm to `event_name()` in `crates/host/src/runtime.rs`.
4. Update `emit_event` in `crates/host/src/api.rs` (`PostEventBody`) if it should be emittable via REST.
5. Rebuild plugins (`scripts/build-plugins.sh`) then the host (`cargo build -p host`).

---

## Build

### Prerequisites

```bash
rustup target add wasm32-wasip2
```

No `cargo-component` needed — plugins are compiled with plain `cargo build` using
`wit_bindgen::generate!` directly.

### Build everything & development

```bash
# 1. Build WASM plugins
bash scripts/build-plugins.sh

# 2. Build the host
cargo build -p host
```

> **Important:** always run `build-plugins.sh` before `cargo build -p host` after
> any WIT change. The host's `bindgen!` and the plugins' `wit_bindgen::generate!`
> both read the WIT at compile time.

### Run

```bash
# Copy and edit the env file
cp .env.example .env   # or create manually, see Environment below

# Start the host
cargo run -p host
```

---

## Environment

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | *(required)* | `postgresql://user:pass@host:5432/db` |
| `PLUGINS` | *(required)* | Comma-separated plugin names to load, e.g. `bonus,push` |
| `WASM_PLUGIN_DIR` | `plugins` | Root directory for compiled `.wasm` files |
| `HOST_PORT` | `3000` | TCP port the Axum server listens on |
| `RUST_LOG` | `info` | Tracing filter (e.g. `debug`, `host=trace`) |

Example `.env`:
```env
DATABASE_URL=postgresql://diesel_poc:dsl-pc-pswd@localhost:5432/diesel_db_poc
PLUGINS=bonus,push
HOST_PORT=3000
```

---

## API endpoints

### Host endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness check |
| `GET` | `/plugins` | List loaded plugins with name, version, routes, subscriptions |
| `POST` | `/events` | Emit a domain event into the plugin bus |
| `GET` | `/ws/push` | WebSocket — streams `reward_granted` events as JSON to subscribers |
| `GET` | `/api-docs/openapi.json` | Merged OpenAPI 3.0 spec (host + all plugins) |

### Users CRUD

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/users?tenant_id=` | List users (defaults to tenant 1) |
| `POST` | `/users` | Create a user |
| `GET` | `/users/{id}` | Get a user |
| `PATCH` | `/users/{id}` | Update `locale` and/or `tier` |
| `DELETE` | `/users/{id}` | Delete a user |

### Payments CRUD

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/payments?user_id=` | List payments for a user |
| `POST` | `/payments` | Record a new payment |
| `GET` | `/payments/{id}` | Get a payment |
| `DELETE` | `/payments/{id}` | Delete a payment |

### Plugin passthrough

| Method | Path | Description |
|--------|------|-------------|
| `ANY` | `/p/{plugin}/{*path}` | Forward HTTP request to a plugin |
| `GET` | `/ws/p/{plugin}/{*path}` | WebSocket upgrade passthrough to a plugin |
| `GET` | `/sse/p/{plugin}/{*path}` | SSE stream passthrough to a plugin |

Routes are pre-validated from the plugin's OpenAPI spec before WASM is invoked.
Unregistered routes return `404` without touching WASM.
Routes annotated with `security: [{"bearerAuth": []}]` in the OpenAPI spec require
`Authorization: Bearer <JWT>` — 401 is returned before the WASM boundary is crossed.

---

## Adding a new plugin

1. Create `plugins/{name}/` following the structure of `plugins/bonus/`.
2. Set `[package.metadata.component] target = { path = "../../wit" }` in `Cargo.toml`
   and `crate-type = ["cdylib"]`.
3. In `src/lib.rs`, generate bindings with `wit_bindgen::generate!({ path: "./wit/plugin.wit" })`.
   Do **not** pass `async: true` — that async-lifts every export, including the ones the WIT
   declares sync, and wasmtime 47 rejects such a component at load time with
   "the `async` canonical option requires an async function type". Asynchrony is declared in
   the WIT (`async func`) and the generator follows it.
4. Implement the `Guest` trait methods: `manifest`, `init`, `handle_event`, `handle_http`
   (and optionally `handle_websocket`, `handle_sse`).
5. Build routes with `utoipa-axum`'s `OpenApiRouter` + `#[utoipa::path]` attributes.
   OpenAPI is the only route registry — the host derives routes and auth from it.
6. To protect a route with JWT, add `security(("bearerAuth" = []))` to its `#[utoipa::path]`
   attribute and add a `BearerAuth` modifier to your `ApiDoc`.
7. Declare event subscriptions in `events::subscribed_events()`.
8. Add DB migrations; embed them with `diesel_wasm_bridge::migration!()` in `migrations.rs`.
9. Add DB operations in `repository.rs` using `diesel_wasm_bridge::render_query` + `db::execute/query`.
10. Add the plugin name to `PLUGINS` in `.env`.
11. Run `scripts/build-plugins.sh`.

### Plugin module layout

```
plugins/{name}/src/
  lib.rs          ← Guest impl: manifest(), init(), handle_event(), handle_http(),
                    handle_websocket(), handle_sse()
  router.rs       ← utoipa-axum OpenApiRouter + openapi_json(); BearerAuth modifier if needed
  handlers.rs     ← HTTP handler functions with #[utoipa::path] attributes
  events.rs       ← subscribed_events() + event dispatch
  repository.rs   ← DB operations via diesel-wasm-bridge
  db.rs           ← render/execute/query wrappers over host_api calls
  schema.rs       ← Diesel table! macros (all prefixed plugin_{name}_)
  types.rs        ← Request/response DTOs
  error.rs        ← AppError type + IntoResponse
  migrations.rs   ← Migration list, embedded with migration!()
```

### Plugin isolation guarantees

- **Table access** — SQL is parsed at query time; only tables prefixed `plugin_{name}_` are allowed.
  `__diesel_migrations_*` is also permitted. Any other table name returns `invalid-input`.
- **Postgres role** — `SET LOCAL ROLE` is prepared in the host's `db.rs` but currently 
  commented out (see Known limitations).
- **Event loops** — A plugin cannot receive the event it just emitted (emitter filter).
  Chain depth is capped at `MAX_CHAIN_DEPTH = 8` (`dispatcher.rs`) to prevent infinite loops.
- **Fuel** — HTTP and event stores get `10_000_000` fuel units per invocation; runaway
  computation traps. WS and SSE stores use `u64::MAX` (no per-call limit) because they are
  long-lived and would exhaust any fixed budget.
- **Authentication** — Route protection is declared entirely through the OpenAPI spec:
  add `security(("bearerAuth" = []))` to a `#[utoipa::path]` attribute and the host will
  enforce `Authorization: Bearer <JWT>` before the WASM boundary is crossed. This applies
  to HTTP, WS, and SSE routes. Routes without a `security` annotation are always open.

---

## Codebase map

```
wit/plugin.wit                     ← authoritative WIT (one source of truth)
crates/
  host/src/
    main.rs                        ← startup: discover .wasm files, load plugins, serve
    runtime.rs                     ← PluginRuntime, PluginExecutor, LoadedPlugin, event loop
    api.rs                         ← Axum routes, OpenAPI annotations, users/payments CRUD
    host_api.rs                    ← Host trait impl: db_query/execute, emit_event, cache, log,
                                     get_user, list_users, create_user, get_payment, …
    db.rs                          ← DbPool, inline_binds, decode_bind
    migrations.rs                  ← Core (embedded) + plugin migration runner
    dispatcher.rs                  ← Broadcast-channel event bus, MAX_CHAIN_DEPTH guard
    context.rs                     ← SharedCache (in-process TTL cache, lazy eviction)
    validation.rs                  ← SQL table-access guard + ValidationCache
    models.rs                      ← Diesel models: User, NewUser, PatchUser, Payment, NewPayment
    repository.rs                  ← Async Diesel CRUD for users and payments
    schema.rs                      ← Diesel table! macros for users and payments
    bindings.rs                    ← Auto-generated by wasmtime bindgen! (do not edit)
  host/migrations/
    2025-01-01-000001_users/       ← up.sql / down.sql for users table
    2025-01-01-000002_payments/    ← up.sql / down.sql for payments table
  diesel-wasm-bridge/src/
    lib.rs                         ← render_query, decode<T>, migration! macro
    row.rs                         ← RawRow: Diesel Row impl for wire bytes
plugins/
  {name}/
    src/lib.rs                     ← Guest impl
    src/router.rs                  ← utoipa-axum OpenApiRouter + openapi_json()
    src/handlers.rs                ← Route handler functions with #[utoipa::path] attributes
    src/events.rs                  ← subscribed_events() + event dispatch
    src/repository.rs              ← DB operations using diesel-wasm-bridge
    src/db.rs                      ← render/execute/query wrappers
    src/schema.rs                  ← Diesel table! macros (plugin_{name}_ prefixed)
    src/types.rs                   ← Request/response DTOs
    src/error.rs                   ← AppError + IntoResponse
    src/migrations.rs              ← Migration list via migration!()
    migrations/V####__name/        ← up.sql / down.sql
    wit/plugin.wit                 ← Symlink/copy of root WIT (used by wit_bindgen)
scripts/
  build-plugins.sh                 ← cargo build --release --target wasm32-wasip2 for each plugin
```

---

## Known limitations / future work

- `SharedCache` has no background eviction; entries accumulate until process restart.
- `SET LOCAL ROLE "plugin_{name}"` is commented out in `db.rs` (`execute_raw` and `query_raw`);
  Postgres row-level isolation is not enforced at runtime yet.
- `query_raw` inlines bind parameters as SQL literals (text protocol). Diesel's `FromSql` impls
  for `TIMESTAMPTZ` and `DATE` expect the Postgres binary wire format, so those column types
  will decode incorrectly. Integer and text columns work. Fix: switch `query_raw` to the
  binary protocol or add per-OID text→binary conversion in `decode_bind`.
