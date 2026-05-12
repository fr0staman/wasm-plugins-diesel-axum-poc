diesel::table! {
    plugin_bonus_bonus_ledger (id) {
        id              -> BigInt,
        user_id         -> BigInt,
        bonus_cents     -> BigInt,
        calculated_date -> Date,
        created_at      -> Timestamptz,
    }
}
