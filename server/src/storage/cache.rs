use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use thiserror::Error;

pub struct InnerMapCache {
    cache: HashMap<String, i64>,
}

#[derive(Error, Debug)]
pub enum UpdateError {
    #[error("This key was already updated")]
    TooEarly,
}

impl InnerMapCache {
    pub fn update(&mut self, ordinal: i64, key: String) -> Result<(), UpdateError> {
        let last_update = self.cache.get(&key).cloned().unwrap_or(0);

        if last_update >= ordinal {
            return Err(UpdateError::TooEarly);
        }

        self.cache.insert(key, ordinal);

        return Ok(());
    }

    fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }
}

#[derive(Clone)]
pub struct MapCache {
    handle: Arc<Mutex<InnerMapCache>>,
}

impl MapCache {
    pub fn new() -> Self {
        Self {
            handle: Arc::new(Mutex::new(InnerMapCache::new())),
        }
    }

    pub async fn update(&self, key: String, ordinal: i64) -> Result<(), UpdateError> {
        let mut map = self.handle.lock().unwrap();

        map.update(ordinal, key)?;

        Ok(())
    }
}
