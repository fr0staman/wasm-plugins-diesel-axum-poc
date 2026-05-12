mod bindings {
    wit_bindgen::generate!({
        path: "./wit/plugin.wit",
        async: true,
    });

    use super::Component;
    export!(Component);
}

mod events;
mod migrations;
mod router;

use bindings::exports::myapp::plugin::plugin_api::Guest;
use bindings::myapp::plugin::host_api;
use bindings::myapp::plugin::types::{
    EventEnvelope, HttpRequest, HttpResponse, LogLevel, PluginError, PluginManifest, WsMessage,
};

struct Component;

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

    async fn init() -> Result<(), PluginError> {
        Ok(())
    }

    async fn handle_event(_evt: EventEnvelope) -> Result<(), PluginError> {
        Ok(())
    }

    async fn handle_http(_req: HttpRequest) -> Result<HttpResponse, PluginError> {
        Err(PluginError::NotFound)
    }

    async fn handle_websocket(path: String, conn_id: u64) -> Result<(), PluginError> {
        host_api::log(
            LogLevel::Info,
            format!("ws connected: path={path} conn={conn_id}"),
        )
        .await;

        while let Some(msg) = host_api::ws_recv().await {
            let reply = match msg {
                WsMessage::Text(t) => WsMessage::Text(t.to_uppercase()),
                WsMessage::Binary(b) => {
                    WsMessage::Binary(String::from_utf8_lossy(&b).to_uppercase().into_bytes())
                }
                WsMessage::Close(r) => {
                    host_api::ws_send(WsMessage::Close(r)).await.ok();
                    break;
                }
            };
            host_api::ws_send(reply)
                .await
                .map_err(|e| PluginError::Internal(format!("{e:?}")))?;
        }

        host_api::log(LogLevel::Info, format!("ws disconnected: conn={conn_id}")).await;
        Ok(())
    }

    async fn handle_sse(_path: String, _conn_id: u64) -> Result<(), PluginError> {
        Ok(())
    }
}
