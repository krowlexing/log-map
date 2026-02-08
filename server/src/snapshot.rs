use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
struct SnapshotEntries {
    tmap: Option<PathBuf>,
    bmap: Option<PathBuf>,
}

const BMAP_MAGIC: &[u8; 4] = b"BMAP";
const BMAP_VERSION: u32 = 1;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    InvalidMagic(String),
    InvalidVersion(u32),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(e) => write!(f, "IO error: {}", e),
            Error::InvalidMagic(s) => write!(f, "Invalid magic: {}", s),
            Error::InvalidVersion(v) => write!(f, "Invalid version: {}", v),
        }
    }
}

impl std::error::Error for Error {}

pub struct Snapshot {
    snapshot_dir: PathBuf,
    snapshot_interval: u64,
    last_snapshot_ordinal: AtomicU64,
}

impl Snapshot {
    pub fn new(dir: &str, interval: u64) -> Result<Self, Error> {
        let snapshot_dir = PathBuf::from(dir);
        std::fs::create_dir_all(&snapshot_dir)?;
        Ok(Self {
            snapshot_dir,
            snapshot_interval: interval,
            last_snapshot_ordinal: AtomicU64::new(0),
        })
    }

    pub fn should_snapshot(&self, current_ordinal: u64) -> bool {
        if current_ordinal == 0 {
            return false;
        }
        let last = self.last_snapshot_ordinal.load(Ordering::Relaxed);
        if current_ordinal - last >= self.snapshot_interval {
            self.last_snapshot_ordinal.store(current_ordinal, Ordering::Relaxed);
            return true;
        }
        false
    }

    fn snapshot_path(&self, ordinal: u64, extension: &str) -> PathBuf {
        self.snapshot_dir.join(format!("snapshot_{}.{}", ordinal, extension))
    }

    pub async fn save_text(&self, records: &[(String, Vec<u8>)]) -> Result<(), Error> {
        let ordinal = records.len() as u64;
        let path = self.snapshot_path(ordinal, "tmap");
        let mut content = String::new();

        for (key, value) in records {
            let value_str = String::from_utf8_lossy(value);
            content.push_str(&format!("{}: {}\n", key, value_str));
        }

        tokio::fs::write(path, content).await?;
        Ok(())
    }

    pub async fn save_binary(&self, records: &[(String, Vec<u8>)]) -> Result<(), Error> {
        let ordinal = records.len() as u64;
        let path = self.snapshot_path(ordinal, "bmap");

        let mut buf = Vec::new();

        buf.extend_from_slice(BMAP_MAGIC);
        buf.extend_from_slice(&BMAP_VERSION.to_le_bytes());
        buf.extend_from_slice(&(records.len() as u32).to_le_bytes());

        for (key, value) in records {
            let key_bytes = key.as_bytes();
            let key_len = key_bytes.len() as u16;
            buf.extend_from_slice(&key_len.to_le_bytes());
            buf.extend_from_slice(key_bytes);

            let value_len = value.len() as u32;
            buf.extend_from_slice(&value_len.to_le_bytes());
            buf.extend_from_slice(value);
        }

        tokio::fs::write(path, buf).await?;
        Ok(())
    }

    pub async fn load_text(&self) -> Result<Vec<(String, Vec<u8>)>, Error> {
        let mut result = Vec::new();
        let entries = self.read_snapshot_entries()?;

        if let Some(path) = entries.tmap {
            let content = tokio::fs::read_to_string(path).await?;
            for line in content.lines() {
                if let Some((key, value)) = line.split_once(": ") {
                    result.push((key.to_string(), value.as_bytes().to_vec()));
                }
            }
        }

        Ok(result)
    }

    pub async fn load_binary(&self) -> Result<Vec<(String, Vec<u8>)>, Error> {
        let entries = self.read_snapshot_entries()?;

        if let Some(path) = entries.bmap {
            let data = tokio::fs::read(path).await?;

            if &data[0..4] != BMAP_MAGIC {
                return Err(Error::InvalidMagic(String::from_utf8_lossy(&data[0..4]).to_string()));
            }

            let version = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
            if version != BMAP_VERSION {
                return Err(Error::InvalidVersion(version));
            }

            let count = u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize;
            let mut result = Vec::with_capacity(count);
            let mut offset = 12;

            for _ in 0..count {
                let key_len = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
                offset += 2;
                let key = String::from_utf8_lossy(&data[offset..offset + key_len]).to_string();
                offset += key_len;

                let value_len = u32::from_le_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]) as usize;
                offset += 4;
                let value = data[offset..offset + value_len].to_vec();
                offset += value_len;

                result.push((key, value));
            }

            return Ok(result);
        }

        Ok(Vec::new())
    }

    fn read_snapshot_entries(&self) -> Result<SnapshotEntries, Error> {
        let mut tmap = None;
        let mut bmap = None;
        let mut max_ordinal = 0u64;

        for entry in std::fs::read_dir(&self.snapshot_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            if let Some(rest) = name_str.strip_prefix("snapshot_") {
                if let Some(ordinal_str) = rest.split('_').next() {
                    if let Ok(ordinal) = ordinal_str.parse::<u64>() {
                        if ordinal > max_ordinal {
                            max_ordinal = ordinal;
                            tmap = None;
                            bmap = None;
                        }

                        if ordinal == max_ordinal {
                            if name_str.ends_with(".tmap") {
                                tmap = Some(entry.path());
                            } else if name_str.ends_with(".bmap") {
                                bmap = Some(entry.path());
                            }
                        }
                    }
                }
            }
        }

        Ok(SnapshotEntries { tmap, bmap })
    }
}
