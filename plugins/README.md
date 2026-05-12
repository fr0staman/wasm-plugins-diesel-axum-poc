# Plugin development guide

Each plugin is a Rust crate compiled to a WebAssembly Component (`wasm32-wasip2`).
The host loads the `.wasm` binary at startup, calls `manifest()` once to collect routes
and metadata, then dispatches requests and events without re-instantiating.

---

## How a plugin works

```
Plugin binary (.wasm)
  manifest()          → name, version, openapi JSON, event subscriptions, migrations
  handle_http(req)    → HTTP request → HTTP response
  handle_websocket()  → long-lived WS connection via ws_recv / ws_send host calls
  handle_sse()        → long-lived SSE stream via sse_yield host calls
  handle_event(env)   → process a domain event
```

The host derives everything it needs from `manifest.openapi`:
- which paths are HTTP routes, WS upgrade routes, or SSE routes
- which routes require JWT authentication (`security: [{"bearerAuth": []}]`)

There is no separate route list — OpenAPI is the single source of truth.

---

## Bundled plugins

| Plugin | Type | Description |
|--------|------|-------------|
| `bonus` | HTTP + events | Per-user daily bonus ledger; `POST /calculate` is JWT-protected |
| `push` | HTTP + events | Push notification log; all routes open |
| `stream` | SSE | Streams N JSON items; `GET /generate?count=N` |
| `wsecho` | WebSocket | Echoes messages uppercased; `GET /chat` |

---

## Creating a new plugin

### 1. Scaffold

Copy an existing plugin or create the directory manually:

```
plugins/{name}/
  Cargo.toml
  wit/plugin.wit        ← copy from the root wit/ directory
  src/
    lib.rs
    router.rs
    handlers.rs         ← omit if WS/SSE only
    events.rs
    migrations.rs
```

**`Cargo.toml`** minimum:

```toml
[package]
name = "{name}"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[package.metadata.component]
target = { path = "../../wit" }

[dependencies]
wit-bindgen = { workspace = true }
utoipa = { workspace = true }
utoipa-axum = { workspace = true }
axum = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
```

### 2. Generate bindings

```rust
// src/lib.rs
mod bindings {
    wit_bindgen::generate!({ path: "./wit/plugin.wit", async: true });
    use super::Component;
    export!(Component);
}
```

### 3. Implement `Guest`

```rust
impl Guest for Component {
    async fn manifest() -> PluginManifest {
        PluginManifest {
            name: env!("CARGO_PKG_NAME").to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            subscribed_events: events::subscribed_events(),
            migrations: migrations::all(),
            openapi: router::openapi_json(),
        }
    }

    async fn init() -> Result<(), PluginError> { Ok(()) }

    async fn handle_http(req: HttpRequest) -> Result<HttpResponse, PluginError> {
        router::dispatch(req).await
    }

    async fn handle_event(evt: EventEnvelope) -> Result<(), PluginError> {
        events::dispatch(evt).await
    }

    // Only needed for WS plugins:
    async fn handle_websocket(path: String, conn_id: u64) -> Result<(), PluginError> {
        while let Some(msg) = host_api::ws_recv().await {
            host_api::ws_send(msg).await.ok();
        }
        Ok(())
    }

    // Only needed for SSE plugins:
    async fn handle_sse(path: String, _conn_id: u64) -> Result<(), PluginError> {
        host_api::sse_yield(b"hello\n".to_vec()).await.ok();
        Ok(())
    }
}
```

### 4. Declare routes with utoipa

```rust
// src/router.rs
use std::sync::LazyLock;
use axum::Router;
use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};

#[derive(OpenApi)]
#[openapi(tags((name = "{name}", description = "...")))]
struct ApiDoc;

static ROUTER: LazyLock<(Router, utoipa::openapi::OpenApi)> = LazyLock::new(|| {
    OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(handlers::get_status))
        .routes(routes!(handlers::post_create))
        .split_for_parts()
});

pub async fn dispatch(req: HttpRequest) -> Result<HttpResponse, PluginError> { /* ... */ }

pub fn openapi_json() -> String {
    serde_json::to_string(&ROUTER.1).unwrap_or_default()
}
```

Each handler needs a `#[utoipa::path]` attribute:

```rust
// src/handlers.rs
#[utoipa::path(
    post,
    path = "/create",
    tag = "{name}",
    request_body = CreateRequest,
    responses((status = 200, body = CreateResponse))
)]
pub async fn post_create(Json(body): Json<CreateRequest>) -> impl IntoResponse { /* ... */ }
```

### 5. Protect a route with JWT

Add a `BearerAuth` modifier to `ApiDoc` and annotate the route:

```rust
// src/router.rs
use utoipa::{Modify, OpenApi, openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme}};

struct BearerAuth;

impl Modify for BearerAuth {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "bearerAuth",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("JWT")
                    .build(),
            ),
        );
    }
}

#[derive(OpenApi)]
#[openapi(
    tags((name = "{name}", description = "...")),
    modifiers(&BearerAuth)
)]
struct ApiDoc;
```

```rust
// src/handlers.rs — on a protected handler:
#[utoipa::path(
    post,
    path = "/admin",
    tag = "{name}",
    security(("bearerAuth" = [])),   // ← this line makes the host enforce JWT
    responses((status = 200, body = AdminResponse))
)]
pub async fn post_admin(...) { /* ... */ }
```

The host validates the JWT and injects `x-auth-user-id`, `x-auth-tenant-id`, and
`x-auth-role` headers into the forwarded request before calling `handle_http`.

The same `security` annotation works on WS/SSE stub handlers — the host validates
the token on the initial HTTP upgrade request.

### 6. Declare database migrations

```rust
// src/migrations.rs
use diesel_wasm_bridge::migration;

pub fn all() -> Vec<bindings::myapp::plugin::types::Migration> {
    vec![migration!("migrations/V0001__create_table")]
}
```

```sql
-- migrations/V0001__create_table/up.sql
CREATE TABLE plugin_{name}_items (
    id BIGSERIAL PRIMARY KEY,
    data TEXT NOT NULL
);
```

Table names **must** start with `plugin_{name}_`. Any other prefix is rejected.

### 7. Register in `.env`

```env
PLUGINS=bonus,push,{name}
```

Then run:

```bash
bash scripts/build-plugins.sh # compiles to wasm32-wasip2
cargo run -p host
```

---

## Host API reference

Calls available inside any plugin via `host_api::*`:

| Function | Description |
|----------|-------------|
| `db_query(rendered_query)` | Run a SELECT; returns raw row bytes |
| `db_execute(rendered_query)` | Run INSERT/UPDATE/DELETE; returns affected row count |
| `emit_event(payload)` | Publish a domain event onto the bus |
| `cache_get(key)` / `cache_set(key, value, ttl)` | In-process TTL key-value store |
| `log(level, msg)` | Structured log via host's tracing subscriber |
| `get_user(id)` / `list_users(tenant_id)` / `create_user(...)` | Host users table |
| `get_payment(id)` / `list_user_payments(user_id)` / `create_payment(...)` | Host payments table |
| `ws_recv()` | Receive next message from active WebSocket (returns `None` on disconnect) |
| `ws_send(msg)` | Send a message over active WebSocket |
| `sse_yield(data)` | Push a chunk to the active SSE stream |

---

## Isolation guarantees

| Guarantee | Mechanism |
|-----------|-----------|
| Table access | SQL parsed at query time; only `plugin_{name}_*` tables allowed |
| Event loops | Plugins can't receive their own events; chain depth capped at 8 |
| Compute budget | HTTP/event calls: 10M fuel units. WS/SSE: unlimited (long-lived) |
| Authentication | JWT enforced by host before WASM is invoked when `security` is declared in OpenAPI |
