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

mod events;
mod migrations;
mod router;

use bindings::exports::myapp::plugin::plugin_api::Guest;
use bindings::myapp::plugin::host_api;
use bindings::{wit_future, wit_stream};
use bindings::myapp::plugin::types::{
    EventEnvelope, HttpRequest, HttpResponse, LogLevel, PluginError, PluginManifest, WsMessage,
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

    async fn handle_event(_evt: EventEnvelope) -> Result<(), PluginError> {
        Ok(())
    }

    async fn handle_http(_req: HttpRequest) -> Result<HttpResponse, PluginError> {
        Err(PluginError::NotFound)
    }

    /// Returns immediately with the reply stream; the echo loop runs in a
    /// spawned task, reading `incoming` and writing replies until either side
    /// closes. Dropping `replies` ends the stream, after which `done` resolves.
    async fn handle_websocket(
        path: String,
        conn_id: u64,
        incoming: wit_bindgen::StreamReader<WsMessage>,
    ) -> (
        wit_bindgen::StreamReader<WsMessage>,
        wit_bindgen::FutureReader<Result<(), PluginError>>,
    ) {
        host_api::log(
            LogLevel::Info,
            &format!("ws connected: path={path} conn={conn_id}"),
        );

        let (mut replies_tx, replies_rx) = wit_stream::new::<WsMessage>();
        let (done_tx, done_rx) = wit_future::new::<Result<(), PluginError>>(|| {
            Err(PluginError::Internal("ws handler dropped".into()))
        });

        wit_bindgen::spawn_local(async move {
            let mut incoming = incoming;
            let mut buf: Vec<WsMessage> = Vec::with_capacity(8);

            loop {
                buf.clear();
                let (result, returned) = incoming.read(core::mem::take(&mut buf)).await;
                buf = returned;
                if matches!(result, wit_bindgen::StreamResult::Dropped) && buf.is_empty() {
                    break;
                }

                // Reply to the whole batch in one crossing rather than one
                // `write_all` per frame.
                let mut closed = false;
                let mut replies = Vec::with_capacity(buf.len());
                for msg in buf.drain(..) {
                    replies.push(match msg {
                        WsMessage::Text(t) => WsMessage::Text(t.to_uppercase()),
                        WsMessage::Binary(b) => WsMessage::Binary(
                            String::from_utf8_lossy(&b).to_uppercase().into_bytes(),
                        ),
                        WsMessage::Close(r) => {
                            closed = true;
                            WsMessage::Close(r)
                        }
                    });
                }
                if !replies.is_empty() {
                    replies_tx.write_all(replies).await;
                }
                if closed {
                    break;
                }
            }

            host_api::log(
                LogLevel::Info,
                &format!("ws disconnected: conn={conn_id}"),
            );
            drop(replies_tx);
            done_tx.write(Ok(())).await;
        });

        (replies_rx, done_rx)
    }

    async fn handle_sse(
        _path: String,
        _conn_id: u64,
    ) -> (
        wit_bindgen::StreamReader<Vec<u8>>,
        wit_bindgen::FutureReader<Result<(), PluginError>>,
    ) {
        let (chunks_tx, chunks_rx) = wit_stream::new::<Vec<u8>>();
        let (done_tx, done_rx) = wit_future::new::<Result<(), PluginError>>(|| Ok(()));
        drop(chunks_tx);
        wit_bindgen::spawn_local(async move {
            done_tx.write(Ok(())).await;
        });
        (chunks_rx, done_rx)
    }
}
