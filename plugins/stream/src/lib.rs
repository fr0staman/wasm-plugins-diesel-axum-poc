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
    EventEnvelope, HttpRequest, HttpResponse, PluginError, PluginManifest,
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

    async fn handle_event(evt: EventEnvelope) -> Result<(), PluginError> {
        events::dispatch(evt).await
    }

    async fn handle_http(_req: HttpRequest) -> Result<HttpResponse, PluginError> {
        Err(PluginError::NotFound)
    }

    async fn handle_websocket(_path: String, _conn_id: u64) -> Result<(), PluginError> {
        Ok(())
    }

    async fn handle_sse(path: String, _conn_id: u64) -> Result<(), PluginError> {
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

        for i in 0..count {
            let chunk = format!(r#"{{"index":{},"data":"item_{}"}}"#, i, i);
            host_api::sse_yield(chunk.into_bytes())
                .await
                .map_err(|e| PluginError::Internal(format!("{e:?}")))?;
        }
        Ok(())
    }
}
