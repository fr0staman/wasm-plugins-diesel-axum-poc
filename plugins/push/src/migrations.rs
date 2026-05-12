use crate::bindings::myapp::plugin::types::Migration;
use diesel_wasm_bridge::MigrationSpec;

static SPECS: &[MigrationSpec] = &[
    diesel_wasm_bridge::migration!(
        "V0001__push_notifications",
        "../migrations/V0001__push_notifications/up.sql",
        "../migrations/V0001__push_notifications/down.sql",
    ),
    diesel_wasm_bridge::migration!(
        "V0002__push_notifications",
        "../migrations/V0002__push_notifications/up.sql",
        "../migrations/V0002__push_notifications/down.sql",
    ),
];

pub fn all() -> Vec<Migration> {
    SPECS
        .iter()
        .map(|s| Migration {
            version: s.version.to_string(),
            up_sql: s.up_sql.to_string(),
            down_sql: s.down_sql.to_string(),
        })
        .collect()
}
