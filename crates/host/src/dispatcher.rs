use crate::bindings::myapp::plugin::types::EventEnvelope;
use tokio::sync::broadcast;

const CHANNEL_CAPACITY: usize = 256;
pub const MAX_CHAIN_DEPTH: u8 = 8;

#[derive(Clone)]
pub struct Dispatcher {
    tx: broadcast::Sender<EventEnvelope>,
}

impl Dispatcher {
    pub fn new() -> (Self, broadcast::Receiver<EventEnvelope>) {
        let (tx, rx) = broadcast::channel(CHANNEL_CAPACITY);
        (Self { tx }, rx)
    }

    pub fn send(&self, envelope: EventEnvelope) {
        if envelope.chain_depth >= MAX_CHAIN_DEPTH {
            tracing::warn!(
                chain_depth = envelope.chain_depth,
                "dropping event: exceeded max chain depth"
            );
            return;
        }
        // Ignore send errors (no receivers yet is fine)
        let _ = self.tx.send(envelope);
    }
}

impl Default for Dispatcher {
    fn default() -> Self {
        Self::new().0
    }
}
