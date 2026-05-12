use diesel::dsl::sql;
use diesel::prelude::*;
use diesel::sql_types::Date;

use crate::db;
use crate::error::AppError;
use crate::models::BonusLedger;
use crate::schema::plugin_bonus_bonus_ledger as bonus_ledger;

pub struct BonusRepository;

impl BonusRepository {
    /// Insert a daily bonus for `uid`.
    /// Returns the number of affected rows; `0` means the conflict clause fired
    /// (a bonus was already recorded for this user today).
    pub async fn insert_daily_bonus(uid: i64, bonus_cents: i64) -> Result<u64, AppError> {
        let query = diesel::insert_into(bonus_ledger::table)
            .values((
                bonus_ledger::user_id.eq(uid),
                bonus_ledger::bonus_cents.eq(bonus_cents),
                bonus_ledger::calculated_date.eq(sql::<Date>("CURRENT_DATE")),
            ))
            .on_conflict((bonus_ledger::user_id, bonus_ledger::calculated_date))
            .do_nothing();

        db::execute(query).await
    }

    /// Fetch all bonus ledger entries for `uid`.
    pub async fn find_by_user(uid: i64) -> Result<Vec<BonusLedger>, AppError> {
        let query = bonus_ledger::table.filter(bonus_ledger::user_id.eq(uid));

        Ok(db::query(query).await?)
    }
}
