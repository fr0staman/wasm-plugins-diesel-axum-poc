use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use wasmtime::component::{
    Access, Accessor, Component, FutureReader, HasSelf, Linker, ResourceTable, StreamReader,
};
use wasmtime::{
    AsContextMut, Config, Engine, InstanceAllocationStrategy, PoolingAllocationConfig, Store,
};
use wasmtime_wasi::WasiCtxBuilder;

use crate::bindings::myapp::plugin::types::{
    EventEnvelope, EventPayload, EventSource, EventSubscription, HttpRequest,
    HttpHeader, SystemEvent, SystemEventKind, WsMessage,
};
use crate::bindings::{Plugin, PluginPre};
use crate::streams::{
    ByteChannelConsumer, ChannelConsumer, ChannelProducer, OneshotConsumer,
};
use crate::context::SharedCache;
use crate::db::DbPool;
use crate::dispatcher::Dispatcher;
use crate::host_api::PluginState;
use crate::migrations::{run_core_migrations, run_plugin_migrations};
use crate::validation::{ValidationCache, new_validation_cache};

// ── Loaded plugin ─────────────────────────────────────────────────────────────

pub struct LoadedPlugin {
    pub name: String,
    pub version: String,
    pub subscribed_events: Vec<EventSubscription>,
    pub http_routes: Vec<String>,
    pub ws_routes: Vec<String>,
    pub sse_routes: Vec<String>,
    pub auth_required_routes: Vec<String>,
    pub openapi_json: String,
    /// Pre-linked component — instantiation is ~10-100× cheaper than full link+instantiate.
    instance_pre: PluginPre<PluginState>,
    /// Per-method matchit routers built from `http_routes` at load time.
    route_matchers: HashMap<String, matchit::Router<()>>,
}

impl LoadedPlugin {
    /// `method_upper` must already be ASCII-uppercased by the caller.
    pub fn matches_route(&self, method_upper: &str, path: &str) -> bool {
        self.route_matchers
            .get(method_upper)
            .map(|r| r.at(path).is_ok())
            .unwrap_or(false)
    }

    /// Returns true if this route is listed in `auth_required_routes`.
    /// `method_upper` must already be ASCII-uppercased by the caller.
    fn is_route_protected(&self, method_upper: &str, path: &str) -> bool {
        http_route_is_protected(&self.auth_required_routes, method_upper, path)
    }

    /// Returns true if a WS or SSE route (stored as bare path) requires auth.
    fn is_ws_sse_route_protected(&self, path: &str) -> bool {
        ws_sse_route_is_protected(&self.auth_required_routes, path)
    }
}

/// Checks whether a "METHOD /path" string is in the auth list.
/// Entries are stored as `"POST /calculate"` — no heap allocation on the hot path.
fn http_route_is_protected(auth_routes: &[String], method_upper: &str, path: &str) -> bool {
    let mlen = method_upper.len();
    auth_routes.iter().any(|r| {
        r.len() > mlen
            && r.as_bytes()[mlen] == b' '
            && r.starts_with(method_upper)
            && &r[mlen + 1..] == path
    })
}

/// Checks whether a bare path (e.g. `"/chat"`) is in the auth list.
/// WS/SSE entries are stored without a method prefix.
fn ws_sse_route_is_protected(auth_routes: &[String], path: &str) -> bool {
    auth_routes.iter().any(|r| r == path)
}

// ── Executor ──────────────────────────────────────────────────────────────────

/// Holds the Arc-cloneable pieces needed to create stores and call into plugins.
/// Extracted from PluginRuntime so WASM execution can happen without holding the runtime lock.
pub struct PluginExecutor {
    engine: Engine,
    pub db: Option<Arc<DbPool>>,
    pub cache: SharedCache,
    pub dispatcher: Dispatcher,
    pub validation_cache: ValidationCache,
    /// Broadcast channel for WebSocket clients subscribed to push events.
    pub ws_tx: broadcast::Sender<String>,
}

/// Awaits a plugin's terminal `future<result<_, plugin-error>>`.
///
/// Host futures are consumed by piping, not awaiting, so the value is routed
/// through a oneshot. A dropped sender means the plugin never resolved the
/// future — treated as a clean close, since the streams are already finished by
/// the time we get here.
async fn await_terminal(
    accessor: &Accessor<PluginState>,
    done: FutureReader<Result<(), crate::bindings::myapp::plugin::types::PluginError>>,
) -> Result<(), String> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    accessor
        .with(|mut store: Access<'_, PluginState>| {
            done.pipe(store.as_context_mut(), OneshotConsumer::new(tx))
        })
        .map_err(|e| e.to_string())?;

    match rx.await {
        Ok(Ok(())) | Err(_) => Ok(()),
        Ok(Err(e)) => Err(format!("{e:?}")),
    }
}

/// Per-invocation fuel for HTTP and event handlers. Runaway computation traps.
const HTTP_FUEL: u64 = 10_000_000;

/// Depth of the host-side frame buffer for a wasi:http response body.
const STREAM_BUFFER: usize = 32;

impl PluginExecutor {
    fn make_store(&self, plugin_name: &str, chain_depth: u8) -> Store<PluginState> {
        let wasi = WasiCtxBuilder::new().inherit_stderr().build();
        let state = PluginState {
            wasi,
            table: ResourceTable::new(),
            db: self.db.clone(),
            cache: self.cache.clone(),
            dispatcher: self.dispatcher.clone(),
            http: wasmtime_wasi_http::WasiHttpCtx::default(),
            http_hooks: Default::default(),
            plugin_name: plugin_name.to_string(),
            validation_cache: self.validation_cache.clone(),
            current_chain_depth: chain_depth,
        };
        let mut store = Store::new(&self.engine, state);
        store.set_fuel(HTTP_FUEL).ok();
        store
    }

    /// Store for a streaming invocation. `fuel` is the per-invocation budget:
    /// `u64::MAX` for WS/SSE, which run for the lifetime of a connection and
    /// would exhaust any fixed budget, but a real limit for HTTP — that budget
    /// is a sandboxing control, and handing HTTP an unlimited one would quietly
    /// remove it.
    fn make_store_streaming(&self, plugin_name: &str, fuel: u64) -> Store<PluginState> {
        let wasi = WasiCtxBuilder::new().inherit_stderr().build();
        let state = PluginState {
            wasi,
            table: ResourceTable::new(),
            db: self.db.clone(),
            cache: self.cache.clone(),
            dispatcher: self.dispatcher.clone(),
            http: wasmtime_wasi_http::WasiHttpCtx::default(),
            http_hooks: Default::default(),
            plugin_name: plugin_name.to_string(),
            validation_cache: self.validation_cache.clone(),
            current_chain_depth: 0,
        };
        let mut store = Store::new(&self.engine, state);
        store.set_fuel(fuel).ok();
        store
    }

    /// Runs a WebSocket connection. `inbound` carries client frames into the
    /// plugin; replies come back on the stream the plugin returns and are
    /// forwarded to `outbound`, which is bounded so a slow client applies
    /// backpressure to the plugin instead of growing a host-side buffer.
    ///
    /// Returns when the plugin's reply stream closes and its terminal future
    /// resolves.
    pub async fn call_websocket(
        &self,
        plugin_name: &str,
        pre: &PluginPre<PluginState>,
        path: String,
        conn_id: u64,
        inbound: mpsc::Receiver<WsMessage>,
        outbound: mpsc::Sender<Vec<WsMessage>>,
    ) -> anyhow::Result<()> {
        let mut store = self.make_store_streaming(plugin_name, u64::MAX);
        let instance = pre.instantiate_async(&mut store).await?;

        store
            .run_concurrent(async move |accessor: &Accessor<PluginState>| -> anyhow::Result<()> {
                let incoming = accessor.with(|mut store: Access<'_, PluginState>| {
                    StreamReader::new(store.as_context_mut(), ChannelProducer::new(inbound))
                })?;

                let (replies, done) = instance
                    .myapp_plugin_plugin_api()
                    .call_handle_websocket(accessor, path, conn_id, incoming)
                    .await?;

                accessor.with(|mut store: Access<'_, PluginState>| {
                    replies.pipe(store.as_context_mut(), ChannelConsumer::new(outbound))
                })?;

                await_terminal(accessor, done).await.map_err(|e| anyhow::anyhow!("ws plugin error: {e}"))
            })
            .await?
    }

    /// Runs an SSE stream. Each item the plugin writes becomes one `data:` line.
    /// `outbound` is bounded for the same backpressure reason as WS.
    pub async fn call_sse(
        &self,
        plugin_name: &str,
        pre: &PluginPre<PluginState>,
        path: String,
        conn_id: u64,
        outbound: mpsc::Sender<Vec<Vec<u8>>>,
    ) -> anyhow::Result<()> {
        let mut store = self.make_store_streaming(plugin_name, u64::MAX);
        let instance = pre.instantiate_async(&mut store).await?;

        store
            .run_concurrent(async move |accessor: &Accessor<PluginState>| -> anyhow::Result<()> {
                let (chunks, done) = instance
                    .myapp_plugin_plugin_api()
                    .call_handle_sse(accessor, path, conn_id)
                    .await?;

                accessor.with(|mut store: Access<'_, PluginState>| {
                    chunks.pipe(store.as_context_mut(), ChannelConsumer::new(outbound))
                })?;

                await_terminal(accessor, done).await.map_err(|e| anyhow::anyhow!("sse plugin error: {e}"))
            })
            .await?
    }

    /// Runs a plugin HTTP request.
    ///
    /// The request body is fed in as a stream, so a handler that never reads it
    /// costs nothing — the bytes are only lowered into guest memory on demand.
    /// Status and headers come back as soon as the guest returns them; the
    /// response body keeps flowing on `body_tx` while this future runs, so the
    /// caller must drive both concurrently.
    pub async fn call_http(
        &self,
        plugin_name: &str,
        pre: &PluginPre<PluginState>,
        method: String,
        uri: String,
        headers: Vec<HttpHeader>,
        body: Vec<u8>,
        head_tx: tokio::sync::oneshot::Sender<(u16, Vec<HttpHeader>)>,
        body_tx: mpsc::Sender<Vec<u8>>,
    ) -> Result<()> {
        let mut store = self.make_store_streaming(plugin_name, HTTP_FUEL);
        let instance = pre.instantiate_async(&mut store).await?;

        store
            .run_concurrent(async move |accessor: &Accessor<PluginState>| -> Result<()> {
                // axum already handed us the whole request body, so feed the Vec
                // straight in as the stream producer. Routing it through an mpsc
                // channel (as this did) allocated a 32-slot ring and a send per
                // request to move bytes we already had.
                let body = accessor.with(|mut store: Access<'_, PluginState>| {
                    StreamReader::new(store.as_context_mut(), body)
                })?;

                let req = HttpRequest {
                    method,
                    uri,
                    headers,
                    body,
                };
                let resp = instance
                    .myapp_plugin_plugin_api()
                    .call_handle_http(accessor, req)
                    .await?
                    .map_err(|e| anyhow!("plugin error: {:?}", e))?;

                // Hand the caller status+headers immediately so axum can start
                // the response while the guest is still producing the body.
                let _ = head_tx.send((resp.status, resp.headers));

                let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                accessor.with(|mut store: Access<'_, PluginState>| {
                    resp.body.pipe(
                        store.as_context_mut(),
                        ByteChannelConsumer::new(body_tx, done_tx),
                    )
                })?;

                // Stay inside run_concurrent until the guest has written the
                // whole body, otherwise the store is torn down mid-write and
                // the client gets an empty response.
                let _ = done_rx.await;
                Ok(())
            })
            .await?
    }

    pub async fn call_event(
        &self,
        plugin_name: &str,
        pre: &PluginPre<PluginState>,
        envelope: &EventEnvelope,
    ) {
        tracing::info!(
            plugin = %plugin_name,
            event = %event_name(&envelope.payload),
            chain_depth = envelope.chain_depth,
            "handling event"
        );
        let mut store = self.make_store(plugin_name, envelope.chain_depth);
        match pre.instantiate_async(&mut store).await {
            Ok(instance) => {
                let envelope = envelope.clone();
                let result = store
                    .run_concurrent(async move |accessor| {
                        instance
                            .myapp_plugin_plugin_api()
                            .call_handle_event(accessor, envelope)
                            .await
                    })
                    .await;
                match result {
                    Ok(Ok(Ok(()))) => {}
                    Ok(Ok(Err(e))) => tracing::error!(error = ?e, "plugin returned error"),
                    Ok(Err(e)) | Err(e) => tracing::error!(error = %e, "plugin trap"),
                }
            }
            Err(e) => tracing::error!(error = %e, "failed to instantiate plugin"),
        }
    }
}

// ── Runtime ───────────────────────────────────────────────────────────────────

pub struct PluginRuntime {
    engine: Engine,
    linker: Linker<PluginState>,
    pub plugins: Vec<LoadedPlugin>,
    /// Pre-built executor shared across all plugins and the event loop.
    /// Cloning it is a single atomic increment rather than 5 field clones.
    executor: Arc<PluginExecutor>,
    db: Option<Arc<DbPool>>,
    pub dispatcher: Dispatcher,
    /// Broadcast channel for push-event WebSocket clients. Capacity 128; lagging receivers drop messages.
    pub ws_tx: broadcast::Sender<String>,
}

/// `PluginRuntime` is only mutated at startup (plugin loading); after `Arc::new` it is immutable,
/// so a plain `Arc` replaces the `Arc<RwLock<…>>` that was used before.
pub type SharedRuntime = Arc<PluginRuntime>;

impl PluginRuntime {
    pub async fn new(database_url: &str) -> Result<(Self, broadcast::Receiver<EventEnvelope>)> {
        let mut config = Config::new();
        config.wasm_component_model(true);
        config.wasm_component_model_async(true);
        config.consume_fuel(true);

        let mut pool = PoolingAllocationConfig::default();
        // These cap *concurrent* instances, so they are a hard RPS ceiling:
        // past this many in-flight plugin calls, instantiation fails and the
        // request 502s. 64 was reachable with keep-alive at c=64.
        pool.total_memories(1024);
        pool.total_tables(1024);
        pool.max_memories_per_component(1);
        // Instantiation is the dominant per-request cost, so bias the pool for
        // slot reuse over RSS: prefer affine (same-module) slots, and reset
        // small regions with memset instead of madvise so a reused slot does
        // not fault its pages back in.
        pool.max_unused_warm_slots(0);
        // Keep this at 0. `keep_resident` trades one madvise on instance teardown
        // for a memset of that many bytes, and the memset loses badly once it is
        // more than a few pages — measured on /p/bonus/status at c=16:
        //
        //        0 -> 370 us CPU/req,  ~10k RPS
        //    64 KiB -> 379 us,         ~10k RPS
        //   256 KiB -> 431 us,          9.2k RPS
        //     1 MiB -> 810 us,          5.5k RPS
        //
        // An earlier 2 MiB setting here halved throughput. It looked like a 23%
        // win in a single-threaded microbenchmark, because memsetting the slot
        // keeps its pages warm for the very next instantiation in a tight loop —
        // the opposite of what happens when many slots cycle concurrently.
        pool.linear_memory_keep_resident(0);
        pool.table_keep_resident(0);

        config.allocation_strategy(InstanceAllocationStrategy::Pooling(pool));

        let engine = Engine::new(&config)?;

        let mut linker = Linker::new(&engine);
        // Both are needed. Guests are built for wasm32-wasip2, so their std pulls
        // wasi:*@0.2.x; p3 covers any interface a plugin imports at 0.3.0 directly.
        // Dropping the p2 line makes every plugin fail to link with
        // "component imports instance `wasi:io/poll@0.2.x`".
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
        wasmtime_wasi::p3::add_to_linker(&mut linker)?;
        // SPIKE: serve wasi:http types so plugins can export wasi:http/handler.
        wasmtime_wasi_http::p3::add_to_linker(&mut linker)?;
        Plugin::add_to_linker::<_, HasSelf<_>>(&mut linker, |s| s)?;

        let db = match DbPool::new(database_url).await {
            Ok(pool) => {
                tracing::info!("connected to database");
                let pool = Arc::new(pool);
                match run_core_migrations(database_url).await {
                    Ok(n) if n > 0 => tracing::info!(count = n, "ran core migrations"),
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "core migration failed"),
                }
                Some(pool)
            }
            Err(e) => {
                tracing::warn!(error = %e, "running without database");
                None
            }
        };

        let cache = SharedCache::new();
        let (dispatcher, event_rx) = Dispatcher::new();
        let validation_cache = new_validation_cache();
        let (ws_tx, _) = broadcast::channel(128);

        let executor = Arc::new(PluginExecutor {
            engine: engine.clone(),
            db: db.clone(),
            cache,
            dispatcher: dispatcher.clone(),
            validation_cache,
            ws_tx: ws_tx.clone(),
        });

        Ok((
            Self {
                engine,
                linker,
                plugins: Vec::new(),
                executor,
                db,
                dispatcher,
                ws_tx,
            },
            event_rx,
        ))
    }

    pub fn db(&self) -> Option<Arc<DbPool>> {
        self.db.clone()
    }

    /// Subscribe to the push-event WebSocket broadcast channel.
    pub fn subscribe_push_events(&self) -> broadcast::Receiver<String> {
        self.ws_tx.subscribe()
    }

    pub async fn load_plugin(&mut self, path: &str) -> Result<String> {
        let component = Component::from_file(&self.engine, path)
            .map_err(|e| anyhow!("loading component from {}: {}", path, e))?;

        let raw_pre = self.linker.instantiate_pre(&component)?;
        let plugin_pre = PluginPre::new(raw_pre)?;

        // Single instantiation reads all metadata via manifest(); http routes are derived from the openapi field.
        let mut store = self.executor.make_store("__loading__", 0);
        let instance = plugin_pre.instantiate_async(&mut store).await?;
        let manifest = store
            .run_concurrent(async move |accessor| {
                instance.myapp_plugin_plugin_api().call_manifest(accessor).await
            })
            .await??;

        let parsed = routes_from_openapi(&manifest.openapi);

        tracing::info!(
            plugin = %manifest.name,
            version = %manifest.version,
            subscribed = ?manifest.subscribed_events.iter().map(event_subscription_name).collect::<Vec<_>>(),
            http = ?parsed.http,
            ws   = ?parsed.ws,
            sse  = ?parsed.sse,
            auth = ?parsed.auth,
            "loaded plugin"
        );

        if let Some(pool) = &self.db {
            match pool.get_conn().await {
                Ok(mut conn) => {
                    match run_plugin_migrations(&mut *conn, &manifest.name, &manifest.migrations)
                        .await
                    {
                        Ok(n) if n > 0 => {
                            tracing::info!(plugin = %manifest.name, count = n, "ran migrations")
                        }
                        Ok(_) => {}
                        Err(e) => {
                            tracing::warn!(plugin = %manifest.name, error = %e, "migration failed")
                        }
                    }
                }
                Err(e) => tracing::warn!(error = %e, "could not open migration connection"),
            }
        }

        let route_matchers = build_route_matchers(&parsed.http);
        let name = manifest.name.clone();

        self.plugins.push(LoadedPlugin {
            name: manifest.name,
            version: manifest.version,
            subscribed_events: manifest.subscribed_events,
            http_routes: parsed.http,
            ws_routes: parsed.ws,
            sse_routes: parsed.sse,
            auth_required_routes: parsed.auth,
            openapi_json: manifest.openapi,
            instance_pre: plugin_pre,
            route_matchers,
        });

        Ok(name)
    }

    pub fn dispatch(&self, envelope: EventEnvelope) {
        self.dispatcher.send(envelope);
    }

    /// Single plugin lookup that resolves route existence and auth in one pass.
    /// Returns `None` if the route is not registered (caller should 404 immediately,
    /// before allocating request headers). `method_upper` must be ASCII-uppercased.
    pub fn prepare_http_with_auth(
        &self,
        plugin_name: &str,
        method_upper: &str,
        path: &str,
    ) -> Option<(Arc<PluginExecutor>, PluginPre<PluginState>, bool)> {
        let plugin = self.plugins.iter().find(|p| p.name == plugin_name)?;
        if !plugin.matches_route(method_upper, path) {
            return None;
        }
        let is_protected = plugin.is_route_protected(method_upper, path);
        Some((
            Arc::clone(&self.executor),
            plugin.instance_pre.clone(),
            is_protected,
        ))
    }

    /// SPIKE: look a plugin up by name only. A `wasi:http` service routes its own
    /// requests, so the host does not pre-validate the path the way
    /// `prepare_http_with_auth` does for `handle-http`.
    pub fn prepare_wasi_http(
        &self,
        plugin_name: &str,
    ) -> Option<(Arc<PluginExecutor>, PluginPre<PluginState>)> {
        let plugin = self.plugins.iter().find(|p| p.name == plugin_name)?;
        Some((Arc::clone(&self.executor), plugin.instance_pre.clone()))
    }

    /// Returns executor + pre-linked instance + auth flag for a WebSocket upgrade,
    /// or `None` if the path is not in the plugin's declared ws_routes.
    pub fn prepare_websocket(
        &self,
        plugin_name: &str,
        path: &str,
    ) -> Option<(Arc<PluginExecutor>, PluginPre<PluginState>, bool)> {
        let plugin = self.plugins.iter().find(|p| p.name == plugin_name)?;
        if !plugin.ws_routes.iter().any(|r| r == path) {
            return None;
        }
        let is_protected = plugin.is_ws_sse_route_protected(path);
        Some((
            Arc::clone(&self.executor),
            plugin.instance_pre.clone(),
            is_protected,
        ))
    }

    /// Returns executor + pre-linked instance + auth flag for an SSE connection,
    /// or `None` if the path is not in the plugin's declared sse_routes.
    pub fn prepare_sse(
        &self,
        plugin_name: &str,
        path: &str,
    ) -> Option<(Arc<PluginExecutor>, PluginPre<PluginState>, bool)> {
        let plugin = self.plugins.iter().find(|p| p.name == plugin_name)?;
        if !plugin.sse_routes.iter().any(|r| r == path) {
            return None;
        }
        let is_protected = plugin.is_ws_sse_route_protected(path);
        Some((
            Arc::clone(&self.executor),
            plugin.instance_pre.clone(),
            is_protected,
        ))
    }

    /// Collects (name, pre) pairs for every plugin that handles this event.
    /// The caller can release the lock immediately after.
    pub fn collect_event_handlers(
        &self,
        envelope: &EventEnvelope,
    ) -> Vec<(String, PluginPre<PluginState>)> {
        let emitter = plugin_emitter(&envelope.source);
        self.plugins
            .iter()
            .filter(|p| {
                p.subscribed_events
                    .iter()
                    .any(|s| subscription_matches(&envelope.payload, s))
                    && emitter.as_deref() != Some(p.name.as_str())
            })
            .map(|p| (p.name.clone(), p.instance_pre.clone()))
            .collect()
    }
}

// ── Event loop ────────────────────────────────────────────────────────────────

pub async fn run_event_loop(runtime: SharedRuntime, mut rx: broadcast::Receiver<EventEnvelope>) {
    use tokio::sync::broadcast::error::RecvError;
    loop {
        match rx.recv().await {
            Ok(envelope) => {
                process_event_unlocked(&runtime, envelope).await;
                loop {
                    match rx.try_recv() {
                        Ok(e) => process_event_unlocked(&runtime, e).await,
                        Err(broadcast::error::TryRecvError::Lagged(n)) => {
                            tracing::warn!(dropped = n, "event channel lagged");
                        }
                        Err(_) => break,
                    }
                }
            }
            Err(RecvError::Lagged(n)) => {
                tracing::warn!(dropped = n, "event channel lagged");
            }
            Err(RecvError::Closed) => break,
        }
    }
}

async fn process_event_unlocked(runtime: &SharedRuntime, envelope: EventEnvelope) {
    let executor = Arc::clone(&runtime.executor);
    let handlers = runtime.collect_event_handlers(&envelope);
    for (name, pre) in handlers {
        executor.call_event(&name, &pre, &envelope).await;
    }
    if let EventPayload::System(SystemEvent::RewardGranted(ref ev)) = envelope.payload {
        let msg = format!(
            r#"{{"type":"reward_granted","user_id":{},"reward_cents":{},"emitted_at":{}}}"#,
            ev.user.id, ev.reward_cents, envelope.emitted_at
        );
        let _ = executor.ws_tx.send(msg);
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

const METHODS: [&str; 8] = [
    "get", "post", "put", "delete", "patch", "head", "options", "trace",
];

struct ParsedRoutes {
    http: Vec<String>,
    ws: Vec<String>,
    sse: Vec<String>,
    /// Protected routes: "METHOD /path" for HTTP; bare "/path" for WS/SSE.
    auth: Vec<String>,
}

fn op_is_protected(op: &serde_json::Map<String, serde_json::Value>) -> bool {
    op.get("security")
        .and_then(|s| s.as_array())
        .map(|arr| !arr.is_empty())
        .unwrap_or(false)
}

fn routes_from_openapi(openapi_json: &str) -> ParsedRoutes {
    let mut out = ParsedRoutes {
        http: vec![],
        ws: vec![],
        sse: vec![],
        auth: vec![],
    };
    if openapi_json.is_empty() {
        return out;
    }
    let Ok(spec) = serde_json::from_str::<serde_json::Value>(openapi_json) else {
        return out;
    };
    let Some(paths) = spec["paths"].as_object() else {
        return out;
    };

    for (path, item) in paths {
        // WS: GET with a 101 response.
        // SSE: GET with a 200 response whose content includes text/event-stream.
        // Everything else → HTTP.
        if let Some(get_op) = item["get"].as_object() {
            let responses = &get_op["responses"];
            if responses["101"].is_object() {
                if op_is_protected(get_op) {
                    out.auth.push(path.clone());
                }
                out.ws.push(path.clone());
                continue;
            }
            if responses["200"]["content"]["text/event-stream"].is_object() {
                if op_is_protected(get_op) {
                    out.auth.push(path.clone());
                }
                out.sse.push(path.clone());
                continue;
            }
        }
        for method in METHODS {
            if let Some(op) = item[method].as_object() {
                let route = format!("{} {}", method.to_uppercase(), path);
                if op_is_protected(op) {
                    out.auth.push(route.clone());
                }
                out.http.push(route);
            }
        }
    }
    out
}

fn build_route_matchers(routes: &[String]) -> HashMap<String, matchit::Router<()>> {
    let mut map: HashMap<String, matchit::Router<()>> = HashMap::new();
    for route in routes {
        let mut parts = route.splitn(2, ' ');
        let (Some(method), Some(path)) = (parts.next(), parts.next()) else {
            tracing::warn!(route = %route, "skipping malformed route declaration");
            continue;
        };
        let router = map.entry(method.to_uppercase()).or_default();
        if let Err(e) = router.insert(path, ()) {
            tracing::warn!(route = %route, error = %e, "duplicate or invalid route pattern");
        }
    }
    map
}

pub fn event_name(payload: &EventPayload) -> String {
    match payload {
        EventPayload::System(sys) => match sys {
            SystemEvent::PaymentMade(_) => "payment_made".into(),
            SystemEvent::RewardGranted(_) => "reward_granted".into(),
        },
        EventPayload::Custom(ev) => ev.name.clone(),
    }
}

pub fn event_subscription_name(sub: &EventSubscription) -> String {
    match sub {
        EventSubscription::System(kind) => match kind {
            SystemEventKind::PaymentMade => "payment_made".into(),
            SystemEventKind::RewardGranted => "reward_granted".into(),
        },
        EventSubscription::Custom(name) => format!("custom:{name}"),
    }
}

fn subscription_matches(payload: &EventPayload, sub: &EventSubscription) -> bool {
    match (payload, sub) {
        (EventPayload::System(sys), EventSubscription::System(kind)) => {
            system_event_kind(sys) == *kind
        }
        (EventPayload::Custom(ev), EventSubscription::Custom(name)) => ev.name == *name,
        _ => false,
    }
}

fn system_event_kind(sys: &SystemEvent) -> SystemEventKind {
    match sys {
        SystemEvent::PaymentMade(_) => SystemEventKind::PaymentMade,
        SystemEvent::RewardGranted(_) => SystemEventKind::RewardGranted,
    }
}

fn plugin_emitter(source: &EventSource) -> Option<String> {
    match source {
        EventSource::Host => None,
        EventSource::Plugin(name) => Some(name.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── op_is_protected ───────────────────────────────────────────────────────

    fn obj(v: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
        v.as_object().cloned().unwrap()
    }

    #[test]
    fn op_protected_no_security_key() {
        assert!(!op_is_protected(&obj(json!({"responses": {"200": {}}}))));
    }

    #[test]
    fn op_protected_security_null() {
        assert!(!op_is_protected(&obj(json!({"security": null}))));
    }

    #[test]
    fn op_protected_empty_array_is_not_protected() {
        // security: [] at operation level means "explicitly no security" in OpenAPI
        assert!(!op_is_protected(&obj(json!({"security": []}))));
    }

    #[test]
    fn op_protected_single_scheme() {
        assert!(op_is_protected(&obj(
            json!({"security": [{"bearerAuth": []}]})
        )));
    }

    #[test]
    fn op_protected_multiple_schemes() {
        assert!(op_is_protected(&obj(
            json!({"security": [{"bearerAuth": []}, {"apiKey": []}]})
        )));
    }

    // ── routes_from_openapi — input edge cases ────────────────────────────────

    #[test]
    fn routes_empty_string() {
        let r = routes_from_openapi("");
        assert!(r.http.is_empty() && r.ws.is_empty() && r.sse.is_empty() && r.auth.is_empty());
    }

    #[test]
    fn routes_invalid_json() {
        let r = routes_from_openapi("{not json}");
        assert!(r.http.is_empty() && r.ws.is_empty() && r.sse.is_empty() && r.auth.is_empty());
    }

    #[test]
    fn routes_no_paths_key() {
        let r = routes_from_openapi(&json!({"info": {"title": "x"}}).to_string());
        assert!(r.http.is_empty() && r.ws.is_empty() && r.sse.is_empty() && r.auth.is_empty());
    }

    // ── routes_from_openapi — HTTP routes ─────────────────────────────────────

    #[test]
    fn routes_http_open_route_not_in_auth() {
        let spec = json!({
            "paths": {
                "/status": {"get": {"responses": {"200": {"description": "ok"}}}}
            }
        });
        let r = routes_from_openapi(&spec.to_string());
        assert!(r.http.contains(&"GET /status".to_string()));
        assert!(r.auth.is_empty());
        assert!(r.ws.is_empty());
        assert!(r.sse.is_empty());
    }

    #[test]
    fn routes_http_protected_route_in_both_http_and_auth() {
        let spec = json!({
            "paths": {
                "/calculate": {
                    "post": {
                        "security": [{"bearerAuth": []}],
                        "responses": {"200": {"description": "ok"}}
                    }
                }
            }
        });
        let r = routes_from_openapi(&spec.to_string());
        assert!(r.http.contains(&"POST /calculate".to_string()));
        assert!(r.auth.contains(&"POST /calculate".to_string()));
    }

    #[test]
    fn routes_http_multiple_methods_only_protected_one_in_auth() {
        let spec = json!({
            "paths": {
                "/items": {
                    "get": {"responses": {"200": {}}},
                    "post": {
                        "security": [{"bearerAuth": []}],
                        "responses": {"201": {}}
                    }
                }
            }
        });
        let r = routes_from_openapi(&spec.to_string());
        assert!(r.http.contains(&"GET /items".to_string()));
        assert!(r.http.contains(&"POST /items".to_string()));
        assert!(!r.auth.contains(&"GET /items".to_string()));
        assert!(r.auth.contains(&"POST /items".to_string()));
    }

    #[test]
    fn routes_http_security_empty_array_not_in_auth() {
        let spec = json!({
            "paths": {
                "/public": {
                    "post": {
                        "security": [],
                        "responses": {"200": {}}
                    }
                }
            }
        });
        let r = routes_from_openapi(&spec.to_string());
        assert!(r.http.contains(&"POST /public".to_string()));
        assert!(r.auth.is_empty());
    }

    // ── routes_from_openapi — WebSocket routes ────────────────────────────────

    #[test]
    fn routes_ws_open_route_not_in_auth() {
        let spec = json!({
            "paths": {
                "/chat": {"get": {"responses": {"101": {"description": "ws upgrade"}}}}
            }
        });
        let r = routes_from_openapi(&spec.to_string());
        assert!(r.ws.contains(&"/chat".to_string()));
        assert!(r.http.is_empty());
        assert!(r.auth.is_empty());
    }

    #[test]
    fn routes_ws_protected_in_both_ws_and_auth() {
        let spec = json!({
            "paths": {
                "/secure-chat": {
                    "get": {
                        "security": [{"bearerAuth": []}],
                        "responses": {"101": {"description": "ws"}}
                    }
                }
            }
        });
        let r = routes_from_openapi(&spec.to_string());
        assert!(r.ws.contains(&"/secure-chat".to_string()));
        assert!(r.auth.contains(&"/secure-chat".to_string()));
        assert!(r.http.is_empty());
    }

    #[test]
    fn routes_ws_auth_entry_is_bare_path_not_method_prefixed() {
        let spec = json!({
            "paths": {
                "/ws": {
                    "get": {
                        "security": [{"bearerAuth": []}],
                        "responses": {"101": {}}
                    }
                }
            }
        });
        let r = routes_from_openapi(&spec.to_string());
        assert!(r.auth.contains(&"/ws".to_string()));
        assert!(!r.auth.iter().any(|e| e.starts_with("GET")));
    }

    // ── routes_from_openapi — SSE routes ──────────────────────────────────────

    #[test]
    fn routes_sse_open_route_not_in_auth() {
        let spec = json!({
            "paths": {
                "/generate": {
                    "get": {
                        "responses": {
                            "200": {"content": {"text/event-stream": {"schema": {}}}}
                        }
                    }
                }
            }
        });
        let r = routes_from_openapi(&spec.to_string());
        assert!(r.sse.contains(&"/generate".to_string()));
        assert!(r.http.is_empty());
        assert!(r.auth.is_empty());
    }

    #[test]
    fn routes_sse_protected_in_both_sse_and_auth() {
        let spec = json!({
            "paths": {
                "/private-stream": {
                    "get": {
                        "security": [{"bearerAuth": []}],
                        "responses": {
                            "200": {"content": {"text/event-stream": {"schema": {}}}}
                        }
                    }
                }
            }
        });
        let r = routes_from_openapi(&spec.to_string());
        assert!(r.sse.contains(&"/private-stream".to_string()));
        assert!(r.auth.contains(&"/private-stream".to_string()));
        assert!(r.http.is_empty());
    }

    #[test]
    fn routes_sse_auth_entry_is_bare_path() {
        let spec = json!({
            "paths": {
                "/stream": {
                    "get": {
                        "security": [{"bearerAuth": []}],
                        "responses": {
                            "200": {"content": {"text/event-stream": {"schema": {}}}}
                        }
                    }
                }
            }
        });
        let r = routes_from_openapi(&spec.to_string());
        assert!(r.auth.contains(&"/stream".to_string()));
        assert!(!r.auth.iter().any(|e| e.starts_with("GET")));
    }

    // ── routes_from_openapi — mixed bag ───────────────────────────────────────

    #[test]
    fn routes_mixed_all_categories() {
        let spec = json!({
            "paths": {
                "/open": {"get": {"responses": {"200": {}}}},
                "/protected": {
                    "post": {
                        "security": [{"bearerAuth": []}],
                        "responses": {"200": {}}
                    }
                },
                "/chat": {"get": {"responses": {"101": {}}}},
                "/secure-chat": {
                    "get": {
                        "security": [{"bearerAuth": []}],
                        "responses": {"101": {}}
                    }
                },
                "/stream": {
                    "get": {
                        "responses": {"200": {"content": {"text/event-stream": {"schema": {}}}}}
                    }
                },
                "/secure-stream": {
                    "get": {
                        "security": [{"bearerAuth": []}],
                        "responses": {"200": {"content": {"text/event-stream": {"schema": {}}}}}
                    }
                }
            }
        });
        let r = routes_from_openapi(&spec.to_string());

        assert!(r.http.contains(&"GET /open".to_string()));
        assert!(r.http.contains(&"POST /protected".to_string()));
        assert!(r.ws.contains(&"/chat".to_string()));
        assert!(r.ws.contains(&"/secure-chat".to_string()));
        assert!(r.sse.contains(&"/stream".to_string()));
        assert!(r.sse.contains(&"/secure-stream".to_string()));

        assert!(!r.auth.contains(&"GET /open".to_string()));
        assert!(r.auth.contains(&"POST /protected".to_string()));
        assert!(!r.auth.contains(&"/chat".to_string()));
        assert!(r.auth.contains(&"/secure-chat".to_string()));
        assert!(!r.auth.contains(&"/stream".to_string()));
        assert!(r.auth.contains(&"/secure-stream".to_string()));

        assert_eq!(r.auth.len(), 3);
    }

    // ── http_route_is_protected ───────────────────────────────────────────────

    #[test]
    fn http_protected_exact_match() {
        let routes = vec!["POST /calculate".to_string()];
        assert!(http_route_is_protected(&routes, "POST", "/calculate"));
    }

    #[test]
    fn http_protected_method_mismatch() {
        let routes = vec!["POST /calculate".to_string()];
        assert!(!http_route_is_protected(&routes, "GET", "/calculate"));
    }

    #[test]
    fn http_protected_path_mismatch() {
        let routes = vec!["POST /calculate".to_string()];
        assert!(!http_route_is_protected(&routes, "POST", "/other"));
    }

    #[test]
    fn http_protected_path_prefix_does_not_match() {
        let routes = vec!["POST /calc".to_string()];
        assert!(!http_route_is_protected(&routes, "POST", "/calculate"));
    }

    #[test]
    fn http_protected_multiple_routes_one_matches() {
        let routes = vec![
            "GET /status".to_string(),
            "POST /calculate".to_string(),
            "GET /ledger".to_string(),
        ];
        assert!(http_route_is_protected(&routes, "POST", "/calculate"));
        assert!(!http_route_is_protected(&routes, "DELETE", "/calculate"));
    }

    #[test]
    fn http_protected_empty_list() {
        assert!(!http_route_is_protected(&[], "POST", "/calculate"));
    }

    // ── ws_sse_route_is_protected ─────────────────────────────────────────────

    #[test]
    fn ws_sse_protected_exact_match() {
        let routes = vec!["/chat".to_string()];
        assert!(ws_sse_route_is_protected(&routes, "/chat"));
    }

    #[test]
    fn ws_sse_protected_path_mismatch() {
        let routes = vec!["/chat".to_string()];
        assert!(!ws_sse_route_is_protected(&routes, "/other"));
    }

    #[test]
    fn ws_sse_protected_http_entry_does_not_match_bare_path() {
        // HTTP-style entries like "GET /chat" must NOT match bare "/chat" lookups
        let routes = vec!["GET /chat".to_string()];
        assert!(!ws_sse_route_is_protected(&routes, "/chat"));
    }

    #[test]
    fn ws_sse_protected_empty_list() {
        assert!(!ws_sse_route_is_protected(&[], "/chat"));
    }
}

// ── SPIKE: wasi:http/handler call path ────────────────────────────────────────

/// Calls a plugin's standard `wasi:http/handler` export.
///
/// wasmtime-wasi-http supplies the request/response conversions, but not the
/// store lifetime: `into_http` hands back a body that reads from the component,
/// so `run_concurrent` has to stay alive until that body is drained. The wasm
/// side therefore runs on its own task, streaming frames into a bounded channel,
/// while the caller returns as soon as status and headers are known.
pub async fn call_wasi_http(
    executor: &PluginExecutor,
    plugin_name: &str,
    pre: &PluginPre<PluginState>,
    req: axum::http::Request<axum::body::Body>,
) -> Result<axum::http::Response<axum::body::Body>> {
    use http_body_util::BodyExt as _;
    use wasmtime_wasi_http::p3::bindings::ServicePre;
    use wasmtime_wasi_http::p3::bindings::http::types::ErrorCode;
    use wasmtime_wasi_http::p3::{Request as WasiRequest, Response as WasiResponse};

    let mut store = executor.make_store_streaming(plugin_name, HTTP_FUEL);
    let service_pre = ServicePre::new(pre.instance_pre().clone())?;
    let service = service_pre.instantiate_async(&mut store).await?;

    // axum's body error type is its own; wasi:http wants ErrorCode.
    let (parts, body) = req.into_parts();
    let body = body.map_err(|e| ErrorCode::InternalError(Some(e.to_string())));
    let req = axum::http::Request::from_parts(parts, body);
    let (wasi_req, req_io) = WasiRequest::from_http(req);

    let (head_tx, head_rx) = tokio::sync::oneshot::channel();
    let (frame_tx, mut frame_rx) = mpsc::channel::<axum::body::Bytes>(STREAM_BUFFER);
    let plugin = plugin_name.to_string();

    let task = tokio::spawn(async move {
        store
            .run_concurrent(async move |accessor: &Accessor<PluginState>| -> Result<()> {
                let resp: WasiResponse = service
                    .handle(accessor, wasi_req)
                    .await?
                    .map_err(|e| anyhow!("wasi:http plugin error: {e:?}"))?;

                let http = accessor.with(|mut store: Access<'_, PluginState>| {
                    resp.into_http(store.as_context_mut(), req_io)
                })?;

                let (parts, mut body) = http.into_parts();
                let _ = head_tx.send(parts);

                // Draining here is what keeps the store alive; the channel is
                // bounded, so a slow client backs up into the plugin.
                while let Some(frame) = body.frame().await {
                    let frame = frame.map_err(|e| anyhow!("wasi:http body error: {e:?}"))?;
                    if let Ok(data) = frame.into_data()
                        && frame_tx.send(data).await.is_err()
                    {
                        break;
                    }
                }
                Ok(())
            })
            .await?
    });

    let Ok(parts) = head_rx.await else {
        let msg = match task.await {
            Ok(Err(e)) => e.to_string(),
            Ok(Ok(())) => "plugin returned no response".to_string(),
            Err(e) => e.to_string(),
        };
        anyhow::bail!(msg);
    };

    let stream = async_stream::stream! {
        while let Some(chunk) = frame_rx.recv().await {
            yield Ok::<_, std::convert::Infallible>(chunk);
        }
        if let Ok(Err(e)) = task.await {
            tracing::error!(plugin = %plugin, error = %e, "wasi:http plugin error");
        }
    };

    Ok(axum::http::Response::from_parts(
        parts,
        axum::body::Body::from_stream(stream),
    ))
}
