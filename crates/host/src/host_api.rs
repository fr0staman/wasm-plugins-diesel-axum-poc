use crate::bindings::myapp::plugin::host_api::{Host, HostWithStore};
use crate::bindings::myapp::plugin::types::Host as TypesHost;
use crate::bindings::myapp::plugin::types::{
    EventPayload, LogLevel, PaymentSnapshot, PluginError, RenderedQuery, UserSnapshot, UserTier,
};
use crate::context::SharedCache;
use crate::db::DbPool;
use crate::dispatcher::Dispatcher;
use crate::validation::{ValidationCache, validate_table_access_cached};
use std::future::Future;
use std::sync::Arc;
use wasmtime::component::{Accessor, HasSelf, ResourceTable};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

pub struct PluginState {
    pub wasi: WasiCtx,
    /// SPIKE: state for the wasi:http host implementation.
    pub http: wasmtime_wasi_http::WasiHttpCtx,
    /// Hooks are only consulted for *outbound* requests, which plugins cannot
    /// make — the `client` interface is unused here. Implementing the trait
    /// directly avoids the `default-send-request` feature and its TLS stack.
    pub http_hooks: NoHttpHooks,
    pub table: ResourceTable,
    pub db: Option<Arc<DbPool>>,
    pub cache: SharedCache,
    pub dispatcher: Dispatcher,
    pub plugin_name: String,
    pub validation_cache: ValidationCache,
    /// Chain depth of the event currently being handled; 0 for HTTP/WS/SSE invocations.
    /// Used by `emit_event` to set the outgoing envelope's `chain_depth`.
    pub current_chain_depth: u8,
}

pub fn new_conn_id() -> u64 {
    crate::util::rand_id()
}

// Empty marker trait required by Plugin::add_to_linker
impl TypesHost for PluginState {}

/// No-op `wasi:http` hooks; every trait method has a default.
#[derive(Default)]
pub struct NoHttpHooks;
impl wasmtime_wasi_http::p3::WasiHttpHooks for NoHttpHooks {
    fn send_request(
        &mut self,
        _req: axum::http::Request<
            http_body_util::combinators::UnsyncBoxBody<
                axum::body::Bytes,
                wasmtime_wasi_http::p3::bindings::http::types::ErrorCode,
            >,
        >,
        _opts: Option<wasmtime_wasi_http::p3::RequestOptions>,
        _fut: Box<dyn Future<Output = Result<(), wasmtime_wasi_http::p3::bindings::http::types::ErrorCode>> + Send + 'static>,
    ) -> Box<
        dyn Future<
                Output = Result<
                    (
                        axum::http::Response<
                            http_body_util::combinators::UnsyncBoxBody<
                                axum::body::Bytes,
                                wasmtime_wasi_http::p3::bindings::http::types::ErrorCode,
                            >,
                        >,
                        Box<dyn Future<Output = Result<(), wasmtime_wasi_http::p3::bindings::http::types::ErrorCode>> + Send + 'static>,
                    ),
                    wasmtime_wasi::TrappableError<
                        wasmtime_wasi_http::p3::bindings::http::types::ErrorCode,
                    >,
                >,
            > + Send
            + 'static,
    > {
        // Plugins are servers, not clients: outbound HTTP is deliberately denied.
        Box::new(async {
            Err(wasmtime_wasi_http::p3::bindings::http::types::ErrorCode::HttpRequestDenied.into())
        })
    }
}

impl wasmtime_wasi_http::p3::WasiHttpView for PluginState {
    fn http(&mut self) -> wasmtime_wasi_http::p3::WasiHttpCtxView<'_> {
        wasmtime_wasi_http::p3::WasiHttpCtxView {
            hooks: &mut self.http_hooks,
            table: &mut self.table,
            ctx: &mut self.http,
        }
    }
}

impl WasiView for PluginState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

/// Snapshot of the store state needed to run a DB call without holding a borrow
/// across an await. `Accessor::with` gives synchronous access only, so every
/// `HostWithStore` method pulls what it needs out first, then awaits freely.
struct DbCtx {
    db: Option<Arc<DbPool>>,
    plugin_name: String,
    validation_cache: ValidationCache,
}

fn db_ctx<U>(store: &Accessor<U, HasSelf<PluginState>>) -> DbCtx {
    store.with(|mut view| {
        let s = view.get();
        DbCtx {
            db: s.db.clone(),
            plugin_name: s.plugin_name.clone(),
            validation_cache: s.validation_cache.clone(),
        }
    })
}

fn require_db(ctx: &DbCtx) -> Result<&Arc<DbPool>, PluginError> {
    ctx.db
        .as_ref()
        .ok_or_else(|| PluginError::Internal("no db pool".into()))
}

// ── `async func` imports ──────────────────────────────────────────────────────
// Declared `async func` in the WIT, so bindgen puts them on `HostWithStore`
// as associated functions over an `Accessor` rather than `&mut self` methods.
impl<U> HostWithStore<U> for HasSelf<PluginState> {
    async fn db_query(
        store: &Accessor<U, Self>,
        q: RenderedQuery,
    ) -> Result<Vec<Vec<Vec<u8>>>, PluginError> {
        let ctx = db_ctx(store);
        if let Err(e) =
            validate_table_access_cached(&ctx.validation_cache, &q.sql, &ctx.plugin_name)
        {
            return Err(PluginError::InvalidInput(e.to_string()));
        }

        require_db(&ctx)?
            .query_raw(&q.sql, &ctx.plugin_name, q.binds, q.bind_types)
            .await
            .map_err(|e| PluginError::DbError(e.to_string()))
    }

    async fn db_execute(store: &Accessor<U, Self>, q: RenderedQuery) -> Result<u64, PluginError> {
        let ctx = db_ctx(store);
        if let Err(e) =
            validate_table_access_cached(&ctx.validation_cache, &q.sql, &ctx.plugin_name)
        {
            return Err(PluginError::InvalidInput(e.to_string()));
        }

        require_db(&ctx)?
            .execute_raw(&q.sql, &ctx.plugin_name, q.binds, q.bind_types)
            .await
            .map_err(|e| PluginError::DbError(e.to_string()))
    }

    async fn get_user(
        store: &Accessor<U, Self>,
        user_id: u64,
    ) -> Result<UserSnapshot, PluginError> {
        let ctx = db_ctx(store);
        crate::repository::find_user(require_db(&ctx)?, user_id as i64)
            .await
            .map_err(|e| PluginError::DbError(e.to_string()))?
            .map(user_to_snapshot)
            .ok_or(PluginError::NotFound)
    }

    async fn list_users(
        store: &Accessor<U, Self>,
        tenant_id: u64,
    ) -> Result<Vec<UserSnapshot>, PluginError> {
        let ctx = db_ctx(store);
        crate::repository::list_users(require_db(&ctx)?, tenant_id as i64)
            .await
            .map(|v| v.into_iter().map(user_to_snapshot).collect())
            .map_err(|e| PluginError::DbError(e.to_string()))
    }

    async fn create_user(
        store: &Accessor<U, Self>,
        tenant_id: u64,
        email: String,
        locale: String,
        tier: UserTier,
    ) -> Result<UserSnapshot, PluginError> {
        let ctx = db_ctx(store);
        let new = crate::models::NewUser {
            tenant_id: tenant_id as i64,
            email: &email,
            locale: &locale,
            tier: tier_to_str(tier),
        };
        crate::repository::create_user(require_db(&ctx)?, new)
            .await
            .map(user_to_snapshot)
            .map_err(|e| PluginError::DbError(e.to_string()))
    }

    async fn get_payment(
        store: &Accessor<U, Self>,
        payment_id: u64,
    ) -> Result<PaymentSnapshot, PluginError> {
        let ctx = db_ctx(store);
        crate::repository::find_payment(require_db(&ctx)?, payment_id as i64)
            .await
            .map_err(|e| PluginError::DbError(e.to_string()))?
            .map(payment_to_snapshot)
            .ok_or(PluginError::NotFound)
    }

    async fn list_user_payments(
        store: &Accessor<U, Self>,
        user_id: u64,
    ) -> Result<Vec<PaymentSnapshot>, PluginError> {
        let ctx = db_ctx(store);
        crate::repository::list_user_payments(require_db(&ctx)?, user_id as i64)
            .await
            .map(|v| v.into_iter().map(payment_to_snapshot).collect())
            .map_err(|e| PluginError::DbError(e.to_string()))
    }

    async fn create_payment(
        store: &Accessor<U, Self>,
        user_id: u64,
        amount_cents: i64,
        currency: String,
        method: String,
    ) -> Result<PaymentSnapshot, PluginError> {
        let ctx = db_ctx(store);
        let new = crate::models::NewPayment {
            user_id: user_id as i64,
            amount_cents,
            currency: &currency,
            method: &method,
        };
        crate::repository::create_payment(require_db(&ctx)?, new)
            .await
            .map(payment_to_snapshot)
            .map_err(|e| PluginError::DbError(e.to_string()))
    }

}

// ── plain `func` imports ──────────────────────────────────────────────────────
// Not declared async in the WIT: the host completes them without awaiting, so
// they stay `&mut self` methods on `Host`.
impl Host for PluginState {
    async fn emit_event(&mut self, payload: EventPayload) -> Result<(), PluginError> {
        use crate::bindings::myapp::plugin::types::{EventEnvelope, EventSource};

        let envelope = EventEnvelope {
            id: crate::util::rand_id(),
            emitted_at: crate::util::unix_now(),
            chain_depth: self.current_chain_depth + 1,
            source: EventSource::Plugin(self.plugin_name.clone()),
            payload,
        };
        self.dispatcher.send(envelope);
        Ok(())
    }

    async fn cache_get(&mut self, key: String) -> Option<Vec<u8>> {
        self.cache.get(&key)
    }

    async fn cache_set(&mut self, key: String, value: Vec<u8>, ttl_secs: u32) {
        self.cache.set(key, value, ttl_secs);
    }

    async fn log(&mut self, level: LogLevel, msg: String) {
        match level {
            LogLevel::Error => tracing::error!(plugin = %self.plugin_name, "{}", msg),
            LogLevel::Warn => tracing::warn!(plugin = %self.plugin_name, "{}", msg),
            LogLevel::Info => tracing::info!(plugin = %self.plugin_name, "{}", msg),
            LogLevel::Debug => tracing::debug!(plugin = %self.plugin_name, "{}", msg),
        }
    }

}

fn user_to_snapshot(u: crate::models::User) -> UserSnapshot {
    UserSnapshot {
        id: u.id as u64,
        tenant_id: u.tenant_id as u64,
        email: u.email,
        locale: u.locale,
        tier: match u.tier.as_str() {
            "pro" => UserTier::Pro,
            "enterprise" => UserTier::Enterprise,
            _ => UserTier::Free,
        },
    }
}

fn payment_to_snapshot(p: crate::models::Payment) -> PaymentSnapshot {
    PaymentSnapshot {
        id: p.id as u64,
        amount_cents: p.amount_cents,
        currency: p.currency,
        method: p.method,
        created_at: p.created_at.timestamp() as u64,
    }
}

fn tier_to_str(tier: UserTier) -> &'static str {
    match tier {
        UserTier::Free => "free",
        UserTier::Pro => "pro",
        UserTier::Enterprise => "enterprise",
    }
}
