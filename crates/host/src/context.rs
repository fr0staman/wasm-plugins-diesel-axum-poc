use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

struct Entry {
    value: Vec<u8>,
    expires_at: Instant,
}

#[derive(Clone, Default)]
pub struct SharedCache {
    inner: Arc<DashMap<String, Entry>>,
}

impl SharedCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let entry = self.inner.get(key)?;
        if entry.expires_at < Instant::now() {
            drop(entry);
            self.inner.remove(key);
            return None;
        }
        Some(entry.value.clone())
    }

    pub fn set(&self, key: String, value: Vec<u8>, ttl_secs: u32) {
        let expires_at = Instant::now() + Duration::from_secs(ttl_secs as u64);
        self.inner.insert(key, Entry { value, expires_at });
    }
}
