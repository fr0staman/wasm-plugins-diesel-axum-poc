mod api;
mod auth;
mod bindings;
mod config;
mod context;
mod db;
mod dispatcher;
mod host_api;
mod migrations;
mod models;
mod repository;
mod routes;
mod runtime;
mod schema;
mod types;
mod util;
mod validation;

use std::sync::Arc;

use anyhow::Result;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::config::AppConfig;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".parse().unwrap()),
        )
        .init();

    let AppConfig {
        database_url,
        plugins,
        jwt_secret,
        wasm_plugins_dir,
        host_port,
    } = AppConfig::new().unwrap();

    let (mut runtime_inner, event_rx) = runtime::PluginRuntime::new(&database_url).await?;

    let available_plugins = read_wasm_plugins(&wasm_plugins_dir).unwrap_or_default();

    for (plugin_name, path) in available_plugins {
        if plugins.contains(&plugin_name) {
            match runtime_inner.load_plugin(&path.as_str()).await {
                Ok(_) => {}
                Err(e) => tracing::warn!(plugin = %plugin_name, error = %e, "skipping plugin"),
            }
        } else {
            tracing::warn!(plugin = %plugin_name, "disabled")
        }
    }

    // Freeze into an immutable Arc — plugins are never added or removed at runtime.
    let runtime: runtime::SharedRuntime = Arc::new(runtime_inner);

    // Background task: drive the plugin event loop.
    tokio::spawn(runtime::run_event_loop(Arc::clone(&runtime), event_rx));

    let addr = format!("0.0.0.0:{host_port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!(addr = %addr, "HTTP server listening");

    let jwt_secret = jwt_secret.unwrap_or_else(|| {
        tracing::warn!(
            "JWT_SECRET not set — using insecure default; tokens won't survive restarts"
        );
        "change-me-in-production".to_string()
    });

    let auth = Arc::new(auth::AuthConfig::from_secret(&jwt_secret));
    axum::serve(listener, api::router(runtime, auth)).await?;

    Ok(())
}

fn read_wasm_plugins(path: &str) -> Option<Vec<(String, String)>> {
    let files = std::fs::read_dir(&path);

    let plugins_dir = files.ok()?;

    let mut res = vec![];
    for entry in plugins_dir {
        let path = entry.ok().map(|v| v.path());

        if let Some(path) = path
            && path.is_file()
        {
            if let Some(ext) = path.extension()
                && ext == "wasm"
            {
                let name = path.file_stem().unwrap().to_str().unwrap().to_string();
                let path = path.to_str().unwrap().to_string();
                res.push((name, path));
            }
        }
    }

    Some(res)
}
