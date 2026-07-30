use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

struct Entry {
    value: Vec<u8>,
    expires_at: Instant,
}

#[derive(Clone, Default)]
pub struct SharedCache {
    inner: Arc<RwLock<HashMap<String, Entry>>>,
}

impl SharedCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        // Take the value under a read guard; only an expired entry needs the
        // write lock, and the guard is released before upgrading.
        let expired = {
            let map = self.inner.read().expect("cache poisoned");
            let entry = map.get(key)?;
            if entry.expires_at >= Instant::now() {
                return Some(entry.value.clone());
            }
            true
        };
        if expired {
            self.inner.write().expect("cache poisoned").remove(key);
        }
        None
    }

    pub fn set(&self, key: String, value: Vec<u8>, ttl_secs: u32) {
        let expires_at = Instant::now() + Duration::from_secs(ttl_secs as u64);
        self.inner
            .write()
            .expect("cache poisoned")
            .insert(key, Entry { value, expires_at });
    }
}
