mod bindings {
    wit_bindgen::generate!({
        path: "./wit/plugin.wit",
    });

    use super::Component;
    export!(Component);
}

mod events;
mod migrations;
mod router;

use bindings::exports::myapp::plugin::plugin_api::Guest;
use bindings::{wit_future, wit_stream};
use bindings::myapp::plugin::types::{
    EventEnvelope, HttpRequest, HttpResponse, PluginError, PluginManifest, WsMessage,
};

/// Items per boundary crossing on the SSE stream.
const SSE_BATCH: usize = 32;

struct Component;

impl Guest for Component {
    fn manifest() -> PluginManifest {
        PluginManifest {
            name: env!("CARGO_PKG_NAME").to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            subscribed_events: events::subscribed_events(),
            migrations: migrations::all(),
            openapi: router::openapi_json(),
        }
    }

    async fn init() -> Result<(), PluginError> {
        Ok(())
    }

    async fn handle_event(evt: EventEnvelope) -> Result<(), PluginError> {
        events::dispatch(evt).await
    }

    async fn handle_http(_req: HttpRequest) -> Result<HttpResponse, PluginError> {
        Err(PluginError::NotFound)
    }

    async fn handle_websocket(
        _path: String,
        _conn_id: u64,
        _incoming: wit_bindgen::StreamReader<WsMessage>,
    ) -> (
        wit_bindgen::StreamReader<WsMessage>,
        wit_bindgen::FutureReader<Result<(), PluginError>>,
    ) {
        let (tx, rx) = wit_stream::new::<WsMessage>();
        let (done_tx, done_rx) = wit_future::new::<Result<(), PluginError>>(|| Ok(()));
        drop(tx);
        wit_bindgen::spawn_local(async move {
            done_tx.write(Ok(())).await;
        });
        (rx, done_rx)
    }

    /// Returns the chunk stream immediately; items are produced in a spawned
    /// task. `write_all` blocks once the host-side buffer fills, so a slow
    /// client throttles this loop instead of piling up in the host.
    async fn handle_sse(
        path: String,
        _conn_id: u64,
    ) -> (
        wit_bindgen::StreamReader<Vec<u8>>,
        wit_bindgen::FutureReader<Result<(), PluginError>>,
    ) {
        // path is e.g. "/generate?count=10"
        let count: u32 = path
            .split_once('?')
            .and_then(|(_, qs)| {
                qs.split('&')
                    .find_map(|kv| kv.strip_prefix("count="))
                    .and_then(|v| v.parse().ok())
            })
            .unwrap_or(5)
            .min(100);

        let (mut chunks_tx, chunks_rx) = wit_stream::new::<Vec<u8>>();
        let (done_tx, done_rx) = wit_future::new::<Result<(), PluginError>>(|| {
            Err(PluginError::Internal("sse handler dropped".into()))
        });

        wit_bindgen::spawn_local(async move {
            // Batch writes: one `write_all` is one boundary crossing regardless
            // of how many items it carries, so filling the batch amortises the
            // per-crossing cost across chunks.
            let mut batch = Vec::with_capacity(SSE_BATCH);
            for i in 0..count {
                batch.push(format!(r#"{{"index":{},"data":"item_{}"}}"#, i, i).into_bytes());
                if batch.len() == SSE_BATCH {
                    chunks_tx.write_all(core::mem::take(&mut batch)).await;
                    batch.reserve(SSE_BATCH);
                }
            }
            if !batch.is_empty() {
                chunks_tx.write_all(batch).await;
            }
            drop(chunks_tx);
            done_tx.write(Ok(())).await;
        });

        (chunks_rx, done_rx)
    }
}
