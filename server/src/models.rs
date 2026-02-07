use chrono::Utc;

#[derive(Debug, Clone)]
pub struct Record {
    pub ordinal: u64,
    pub key: String,
    pub value: Vec<u8>,
    pub timestamp: i64,
}

impl Record {
    pub fn new(key: String, value: Vec<u8>, ordinal: u64) -> Self {
        Self {
            ordinal,
            key,
            value,
            timestamp: Utc::now().timestamp_millis(),
        }
    }
}
