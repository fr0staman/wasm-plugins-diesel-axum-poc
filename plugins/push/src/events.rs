use crate::bindings::myapp::plugin::host_api;
use crate::bindings::myapp::plugin::types::{
    EventEnvelope, EventPayload, EventSubscription, LogLevel, PluginError, RewardGrantedEvent,
    SystemEvent, SystemEventKind,
};

pub async fn dispatch(evt: EventEnvelope) -> Result<(), PluginError> {
    if let EventPayload::System(SystemEvent::RewardGranted(RewardGrantedEvent {
        user,
        reward_cents,
        ..
    })) = evt.payload
    {
        host_api::log(
            LogLevel::Info,
            format!(
                "would send push to user={} \"You earned {} cents\"",
                user.id, reward_cents
            ),
        )
        .await;
    }
    Ok(())
}

pub fn subscribed_events() -> Vec<EventSubscription> {
    vec![EventSubscription::System(SystemEventKind::RewardGranted)]
}
