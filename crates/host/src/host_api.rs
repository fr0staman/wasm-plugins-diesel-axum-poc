use crate::bindings::myapp::plugin::host_api::Host;
use crate::bindings::myapp::plugin::types::Host as TypesHost;
use crate::bindings::myapp::plugin::types::{
    EventPayload, LogLevel, PaymentSnapshot, PluginError, RenderedQuery, UserSnapshot, UserTier,
    WsMessage,
};
use crate::context::SharedCache;
use crate::db::DbPool;
use crate::dispatcher::Dispatcher;
use crate::validation::{ValidationCache, validate_table_access_cached};
use std::sync::Arc;
use tokio::sync::mpsc;
use wasmtime::component::ResourceTable;
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

/// Channels that bridge an active WebSocket connection to a plugin store.
/// Created by the Axum WebSocket handler and moved into PluginState.
pub struct WsInFlight {
    pub inbound: mpsc::Receiver<WsMessage>,
    pub outbound: mpsc::UnboundedSender<axum::extract::ws::Message>,
}

/// Channel that bridges an active SSE connection to a plugin store.
/// The plugin pushes chunks via sse_yield; the host forwards them as data: lines.
pub struct SseInFlight {
    pub outbound: mpsc::UnboundedSender<axum::body::Bytes>,
}

pub struct PluginState {
    pub wasi: WasiCtx,
    pub table: ResourceTable,
    pub db: Option<Arc<DbPool>>,
    pub cache: SharedCache,
    pub dispatcher: Dispatcher,
    pub plugin_name: String,
    pub validation_cache: ValidationCache,
    /// Chain depth of the event currently being handled; 0 for HTTP/WS/SSE invocations.
    /// Used by `emit_event` to set the outgoing envelope's `chain_depth`.
    pub current_chain_depth: u8,
    /// Present only during a `handle_websocket` invocation.
    pub ws: Option<WsInFlight>,
    /// Present only during a `handle_sse` invocation.
    pub sse: Option<SseInFlight>,
}

pub fn new_conn_id() -> u64 {
    crate::util::rand_id()
}

// Empty marker trait required by Plugin::add_to_linker
impl TypesHost for PluginState {}

impl WasiView for PluginState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl Host for PluginState {
    async fn db_query(&mut self, q: RenderedQuery) -> Result<Vec<Vec<Vec<u8>>>, PluginError> {
        if let Err(e) =
            validate_table_access_cached(&self.validation_cache, &q.sql, &self.plugin_name)
        {
            return Err(PluginError::InvalidInput(e.to_string()));
        }

        let Some(db) = &self.db else {
            return Err(PluginError::Internal("no db pool".into()));
        };

        db.query_raw(&q.sql, &self.plugin_name, q.binds, q.bind_types)
            .await
            .map_err(|e| PluginError::DbError(e.to_string()))
    }

    async fn db_execute(&mut self, q: RenderedQuery) -> Result<u64, PluginError> {
        if let Err(e) =
            validate_table_access_cached(&self.validation_cache, &q.sql, &self.plugin_name)
        {
            return Err(PluginError::InvalidInput(e.to_string()));
        }

        let Some(db) = &self.db else {
            return Err(PluginError::Internal("no db pool".into()));
        };

        db.execute_raw(&q.sql, &self.plugin_name, q.binds, q.bind_types)
            .await
            .map_err(|e| PluginError::DbError(e.to_string()))
    }

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

    async fn get_user(&mut self, user_id: u64) -> Result<UserSnapshot, PluginError> {
        let db = self
            .db
            .as_deref()
            .ok_or_else(|| PluginError::Internal("no db".into()))?;
        crate::repository::find_user(db, user_id as i64)
            .await
            .map_err(|e| PluginError::DbError(e.to_string()))?
            .map(user_to_snapshot)
            .ok_or(PluginError::NotFound)
    }

    async fn list_users(&mut self, tenant_id: u64) -> Result<Vec<UserSnapshot>, PluginError> {
        let db = self
            .db
            .as_deref()
            .ok_or_else(|| PluginError::Internal("no db".into()))?;
        crate::repository::list_users(db, tenant_id as i64)
            .await
            .map(|v| v.into_iter().map(user_to_snapshot).collect())
            .map_err(|e| PluginError::DbError(e.to_string()))
    }

    async fn create_user(
        &mut self,
        tenant_id: u64,
        email: String,
        locale: String,
        tier: UserTier,
    ) -> Result<UserSnapshot, PluginError> {
        let db = self
            .db
            .as_deref()
            .ok_or_else(|| PluginError::Internal("no db".into()))?;
        let new = crate::models::NewUser {
            tenant_id: tenant_id as i64,
            email: &email,
            locale: &locale,
            tier: tier_to_str(tier),
        };
        crate::repository::create_user(db, new)
            .await
            .map(user_to_snapshot)
            .map_err(|e| PluginError::DbError(e.to_string()))
    }

    async fn get_payment(&mut self, payment_id: u64) -> Result<PaymentSnapshot, PluginError> {
        let db = self
            .db
            .as_deref()
            .ok_or_else(|| PluginError::Internal("no db".into()))?;
        crate::repository::find_payment(db, payment_id as i64)
            .await
            .map_err(|e| PluginError::DbError(e.to_string()))?
            .map(payment_to_snapshot)
            .ok_or(PluginError::NotFound)
    }

    async fn list_user_payments(
        &mut self,
        user_id: u64,
    ) -> Result<Vec<PaymentSnapshot>, PluginError> {
        let db = self
            .db
            .as_deref()
            .ok_or_else(|| PluginError::Internal("no db".into()))?;
        crate::repository::list_user_payments(db, user_id as i64)
            .await
            .map(|v| v.into_iter().map(payment_to_snapshot).collect())
            .map_err(|e| PluginError::DbError(e.to_string()))
    }

    async fn ws_recv(&mut self) -> Option<WsMessage> {
        self.ws.as_mut()?.inbound.recv().await
    }

    async fn ws_send(&mut self, msg: WsMessage) -> Result<(), PluginError> {
        use axum::extract::ws::Message as AxumMsg;
        let axum_msg = match msg {
            WsMessage::Text(t) => AxumMsg::Text(t.into()),
            WsMessage::Binary(b) => AxumMsg::Binary(b.into()),
            WsMessage::Close(reason) => {
                AxumMsg::Close(reason.map(|r| axum::extract::ws::CloseFrame {
                    code: 1000,
                    reason: r.into(),
                }))
            }
        };
        self.ws
            .as_ref()
            .ok_or_else(|| PluginError::Internal("not in a ws context".into()))?
            .outbound
            .send(axum_msg)
            .map_err(|_| PluginError::Internal("ws closed".into()))
    }

    async fn sse_yield(&mut self, data: Vec<u8>) -> Result<(), PluginError> {
        self.sse
            .as_ref()
            .ok_or_else(|| PluginError::Internal("not in an sse context".into()))?
            .outbound
            .send(axum::body::Bytes::from(data))
            .map_err(|_| PluginError::Internal("sse stream closed".into()))
    }

    async fn create_payment(
        &mut self,
        user_id: u64,
        amount_cents: i64,
        currency: String,
        method: String,
    ) -> Result<PaymentSnapshot, PluginError> {
        let db = self
            .db
            .as_deref()
            .ok_or_else(|| PluginError::Internal("no db".into()))?;
        let new = crate::models::NewPayment {
            user_id: user_id as i64,
            amount_cents,
            currency: &currency,
            method: &method,
        };
        crate::repository::create_payment(db, new)
            .await
            .map(payment_to_snapshot)
            .map_err(|e| PluginError::DbError(e.to_string()))
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
