mod bindings {
    wit_bindgen::generate!({
        path: "./wit/plugin.wit",
        async: true,
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

    async fn handle_http(req: HttpRequest) -> Result<HttpResponse, PluginError> {
        router::dispatch(req).await
    }

    async fn handle_websocket(_path: String, _conn_id: u64) -> Result<(), PluginError> {
        Ok(())
    }

    async fn handle_sse(_path: String, _conn_id: u64) -> Result<(), PluginError> {
        Ok(())
    }
}
