use crate::bindings::myapp::plugin::host_api;
use crate::bindings::myapp::plugin::types::{
    EventEnvelope, EventPayload, EventSubscription, LogLevel, PaymentMadeEvent, PluginError,
    SystemEvent, SystemEventKind,
};

pub async fn dispatch(evt: EventEnvelope) -> Result<(), PluginError> {
    if let EventPayload::System(SystemEvent::PaymentMade(PaymentMadeEvent {
        user, payment, ..
    })) = evt.payload
    {
        host_api::log(
            LogLevel::Info,
            &format!(
                "would send bonus to user={} \"You earned {} cents bonus\"",
                user.id, payment.amount_cents
            ),
        );
    }
    Ok(())
}

pub fn subscribed_events() -> Vec<EventSubscription> {
    vec![EventSubscription::System(SystemEventKind::PaymentMade)]
}
