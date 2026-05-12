use anyhow::Result;
use diesel::Connection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};

use crate::bindings::myapp::plugin::types::Migration as WitMigration;

const CORE_MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

pub async fn run_core_migrations(database_url: &str) -> Result<usize> {
    let url = database_url.to_owned();
    let count = tokio::task::spawn_blocking(move || {
        let mut conn = diesel_async::async_connection_wrapper::AsyncConnectionWrapper::<
            AsyncPgConnection,
        >::establish(&url)
        .map_err(|e| anyhow::anyhow!("migration connection: {e}"))?;
        let applied = conn
            .run_pending_migrations(CORE_MIGRATIONS)
            .map_err(|e| anyhow::anyhow!("core migration failed: {e}"))?;
        Ok::<usize, anyhow::Error>(applied.len())
    })
    .await??;

    Ok(count)
}

pub async fn run_plugin_migrations(
    conn: &mut AsyncPgConnection,
    plugin_name: &str,
    migrations: &[WitMigration],
) -> Result<usize> {
    let tracking_table = format!("__diesel_migrations_{}", plugin_name);

    diesel::sql_query(format!(
        "CREATE TABLE IF NOT EXISTS \"{}\" (
            version VARCHAR PRIMARY KEY,
            run_on  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
        tracking_table
    ))
    .execute(conn)
    .await?;

    let already_run: Vec<String> = diesel::sql_query(format!(
        "SELECT version FROM \"{}\" ORDER BY version",
        tracking_table
    ))
    .load::<VersionRow>(conn)
    .await?
    .into_iter()
    .map(|r| r.version)
    .collect();

    let mut ran = 0;
    for mig in migrations {
        if already_run.contains(&mig.version) {
            continue;
        }
        tracing::info!(plugin = %plugin_name, version = %mig.version, "running migration");
        diesel::sql_query(mig.up_sql.as_str())
            .execute(conn)
            .await
            .map_err(|e| anyhow::anyhow!("migration {} failed: {}", mig.version, e))?;
        diesel::sql_query(format!(
            "INSERT INTO \"{}\" (version) VALUES ($1)",
            tracking_table
        ))
        .bind::<diesel::sql_types::Text, _>(mig.version.as_str())
        .execute(conn)
        .await?;
        ran += 1;
    }

    Ok(ran)
}

#[derive(diesel::QueryableByName)]
struct VersionRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    version: String,
}
