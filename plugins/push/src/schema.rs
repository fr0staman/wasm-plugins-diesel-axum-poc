diesel::table! {
    plugin_push_push_notifications (id) {
        id                -> BigInt,
        user_id           -> BigInt,
        channel           -> VarChar,
        notification_type -> VarChar,
        sent_date         -> Date,
        created_at        -> Timestamptz,
    }
}
