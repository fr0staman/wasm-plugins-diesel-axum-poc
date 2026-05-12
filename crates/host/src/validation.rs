use anyhow::{Result, bail};
use dashmap::DashMap;
use sqlparser::ast::visit_relations;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::ops::ControlFlow;
use std::sync::Arc;

/// Shared cache for SQL validation results.
/// Key: hash of `(plugin_name, sql)`, value: `None` = valid, `Some(msg)` = rejected.
/// Keyed by `u64` to avoid a heap allocation on every cache lookup.
pub type ValidationCache = Arc<DashMap<u64, Option<String>>>;

pub fn new_validation_cache() -> ValidationCache {
    Arc::new(DashMap::new())
}

#[inline]
fn validation_key(plugin_name: &str, sql: &str) -> u64 {
    let mut h = DefaultHasher::new();
    plugin_name.hash(&mut h);
    0u8.hash(&mut h); // separator so ("ab", "c") ≠ ("a", "bc")
    sql.hash(&mut h);
    h.finish()
}

/// Like `validate_table_access` but caches results — repeated identical SQL skips re-parsing.
pub fn validate_table_access_cached(
    cache: &ValidationCache,
    sql: &str,
    plugin_name: &str,
) -> Result<()> {
    let key = validation_key(plugin_name, sql);
    if let Some(entry) = cache.get(&key) {
        return match entry.value() {
            None => Ok(()),
            Some(msg) => bail!("{msg}"),
        };
    }
    let result = validate_table_access(sql, plugin_name);
    cache.insert(key, result.as_ref().err().map(|e| e.to_string()));
    result
}

pub fn validate_table_access(sql: &str, plugin_name: &str) -> Result<()> {
    let prefix = format!("plugin_{}_", plugin_name);
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;

    for stmt in &statements {
        let mut violation: Option<String> = None;
        let _ = visit_relations(stmt, |relation| {
            let s = relation.to_string();
            let s = s.strip_prefix('"').unwrap_or(&s);
            let name = s.strip_suffix('"').unwrap_or(s).to_string();

            if !name.starts_with("__diesel_migrations") && !name.starts_with(&prefix) {
                violation = Some(name);
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        });
        if let Some(table) = violation {
            bail!(
                "plugin '{}' attempted access to table '{}' which lacks required prefix '{}'",
                plugin_name,
                table,
                prefix
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_prefixed_table() {
        assert!(
            validate_table_access(
                "SELECT id FROM plugin_bonus_grants WHERE user_id = $1",
                "bonus"
            )
            .is_ok()
        );
    }

    #[test]
    fn rejects_unprefixed_table() {
        assert!(validate_table_access("SELECT * FROM users", "bonus").is_err());
    }

    #[test]
    fn allows_migration_table() {
        assert!(validate_table_access("SELECT * FROM __diesel_migrations_bonus", "bonus").is_ok());
    }
}
