diesel::table! {
    users (id) {
        id         -> BigInt,
        tenant_id  -> BigInt,
        email      -> Text,
        locale     -> VarChar,
        tier       -> VarChar,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    payments (id) {
        id           -> BigInt,
        user_id      -> BigInt,
        amount_cents -> BigInt,
        currency     -> VarChar,
        method       -> Text,
        created_at   -> Timestamptz,
    }
}

diesel::joinable!(payments -> users (user_id));
diesel::allow_tables_to_appear_in_same_query!(users, payments);
