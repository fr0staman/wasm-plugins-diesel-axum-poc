use crate::bindings::myapp::plugin::types::{EventEnvelope, EventSubscription, PluginError};

pub async fn dispatch(_evt: EventEnvelope) -> Result<(), PluginError> {
    Ok(())
}

pub fn subscribed_events() -> Vec<EventSubscription> {
    vec![]
}
