use chrono::{DateTime, Datelike, NaiveDate, Utc};
use diesel::prelude::*;

use crate::schema::*;
use crate::types::LedgerEntry;

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = plugin_bonus_bonus_ledger)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct BonusLedger {
    pub id: i64,
    pub user_id: i64,
    pub bonus_cents: i64,
    pub calculated_date: NaiveDate,
    pub created_at: DateTime<Utc>,
}

impl From<BonusLedger> for LedgerEntry {
    fn from(m: BonusLedger) -> Self {
        let d = m.calculated_date;
        LedgerEntry {
            id: m.id,
            bonus_cents: m.bonus_cents,
            calculated_date: format!("{}-{:02}-{:02}", d.year(), d.month(), d.day()),
        }
    }
}
