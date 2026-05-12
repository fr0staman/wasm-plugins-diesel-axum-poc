use config::{Config, Environment};
use serde::Deserialize;

pub const DEFAULT_WASM_PLUGINS_DIR: &str = "plugins";
pub const DEFAULT_HOST_PORT: u16 = 3000;

#[derive(Deserialize)]
pub struct AppConfig {
    pub plugins: Vec<String>,
    pub jwt_secret: Option<String>,
    pub database_url: String,
    pub wasm_plugins_dir: String,
    pub host_port: u16,
}

impl AppConfig {
    pub fn new() -> anyhow::Result<AppConfig> {
        Config::builder()
            .add_source(
                Environment::default()
                    .list_separator(",")
                    .with_list_parse_key("plugins")
                    .try_parsing(true),
            )
            .set_default("wasm_plugins_dir", DEFAULT_WASM_PLUGINS_DIR)
            .and_then(|v| v.set_default("host_port", DEFAULT_HOST_PORT))
            .and_then(|v| v.build())
            .and_then(|v| v.try_deserialize())
            .map_err(|e| anyhow::anyhow!("Error building config: {e}"))
    }
}
