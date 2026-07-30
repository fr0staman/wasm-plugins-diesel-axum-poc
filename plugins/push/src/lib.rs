mod bindings {
    wit_bindgen::generate!({
        // Directory, not the bare file, so `wit/deps` resolves.
        path: "./wit",
        world: "plugin-guest",
        generate_all,
    });

    use super::Component;
    export!(Component);
}

mod db;
mod error;
mod events;
mod handlers;
mod migrations;
mod models;
mod repository;
mod router;
mod schema;
mod types;

use bindings::exports::myapp::plugin::plugin_api::Guest;
use bindings::{wit_future, wit_stream};
use bindings::myapp::plugin::types::{
    EventEnvelope, HttpRequest, HttpResponse, PluginError, PluginManifest, WsMessage,
};

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

    async fn handle_http(req: HttpRequest) -> Result<HttpResponse, PluginError> {
        router::dispatch(req).await
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

    async fn handle_sse(
        _path: String,
        _conn_id: u64,
    ) -> (
        wit_bindgen::StreamReader<Vec<u8>>,
        wit_bindgen::FutureReader<Result<(), PluginError>>,
    ) {
        let (tx, rx) = wit_stream::new::<Vec<u8>>();
        let (done_tx, done_rx) = wit_future::new::<Result<(), PluginError>>(|| Ok(()));
        drop(tx);
        wit_bindgen::spawn_local(async move {
            done_tx.write(Ok(())).await;
        });
        (rx, done_rx)
    }
}
