use crate::bindings::myapp::plugin::types::Migration;
use diesel_wasm_bridge::MigrationSpec;

static SPECS: &[MigrationSpec] = &[
    diesel_wasm_bridge::migration!(
        "V0001__bonus_ledger",
        "../migrations/V0001__bonus_ledger/up.sql",
        "../migrations/V0001__bonus_ledger/down.sql",
    ),
    diesel_wasm_bridge::migration!(
        "V0002__bonus_ledger",
        "../migrations/V0002__bonus_ledger/up.sql",
        "../migrations/V0002__bonus_ledger/down.sql",
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
