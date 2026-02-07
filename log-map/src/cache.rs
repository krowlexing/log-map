//! Thread-safe in-memory cache for key-value pairs.

use std::collections::HashMap;
use std::sync::RwLock;

pub struct Cache {
    inner: RwLock<HashMap<i64, String>>,
}

impl Cache {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    pub fn get(&self, key: &i64) -> Option<String> {
        self.inner.read().ok()?.get(key).cloned()
    }

    pub fn insert(&self, key: i64, value: String) {
        if let Ok(mut guard) = self.inner.write() {
            guard.insert(key, value);
        }
    }

    pub fn remove(&self, key: &i64) {
        if let Ok(mut guard) = self.inner.write() {
            guard.remove(key);
        }
    }

    pub fn contains_key(&self, key: &i64) -> bool {
        self.inner.read().map(|g| g.contains_key(key)).unwrap_or(false)
    }

    pub fn len(&self) -> usize {
        self.inner.read().map(|g| g.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().map(|g| g.is_empty()).unwrap_or(true)
    }
}

impl Default for Cache {
    fn default() -> Self {
        Self::new()
    }
}
