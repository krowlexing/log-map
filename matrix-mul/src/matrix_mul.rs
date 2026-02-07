//! Distributed matrix multiplication implementation.

use rand::Rng;
use std::time::Duration;

use crate::Error;

const START_KEY: i64 = 0;
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Distributed matrix multiplication coordinator.
///
/// `MatrixMul` loads matrices into the log-map and coordinates
/// distributed computation where clients pick random tasks.
pub struct MatrixMul {
    map: log_map::LogMap,
    m: usize,
    n: usize,
    p: usize,
}

impl MatrixMul {
    /// Connects to a log-server and creates a new `MatrixMul` instance.
    pub async fn connect(addr: impl Into<log_map::ServerAddr>) -> Result<Self, Error> {
        let map = log_map::LogMap::connect(addr).await?;
        Ok(Self {
            map,
            m: 0,
            n: 0,
            p: 0,
        })
    }

    pub fn set_size(&mut self, m: usize, n: usize, p: usize) {
        self.m = m;
        self.n = n;
        self.p = p;
    }

    /// Loads matrices A and B into the log-map.
    ///
    /// A is m×n, B is n×p.
    /// A rows stored at -1, -2, ..., -m
    /// B rows stored at -(m+1), -(m+2), ..., -(m+n)
    pub async fn load_matrices(&mut self, a: Vec<Vec<f64>>, b: Vec<Vec<f64>>) -> Result<(), Error> {
        let m = a.len();
        let n = a.get(0).map_or(0, |row| row.len());
        let b_n = b.len();
        let p = b.get(0).map_or(0, |row| row.len());

        if n != b_n {
            return Err(Error::DimensionMismatch(m, n, b_n, p));
        }

        self.m = m;
        self.n = n;
        self.p = p;

        for (i, row) in a.into_iter().enumerate() {
            let key = -(i as i64 + 1);
            let value = row
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            self.map.insert(key, value).await?;
        }

        for (j, row) in b.into_iter().enumerate() {
            let key = -(m as i64 + j as i64 + 1);
            let value = row
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            self.map.insert(key, value).await?;
        }

        Ok(())
    }

    /// Signals the start of computation by writing "start" to key 0.
    pub async fn start(&self) -> Result<(), Error> {
        self.map.insert(START_KEY, "start".to_string()).await?;
        Ok(())
    }

    /// Runs the worker loop: pick random tasks and compute until complete.
    pub async fn work(&self) -> Result<(), Error> {
        let mut tasks_computed = 0;
        loop {
            if self.is_complete()? {
                println!("Work complete! Computed {} tasks", tasks_computed);
                return Ok(());
            }

            let task_id = self.pick_random_task();
            if let Some((i, j)) = task_id {
                match self.try_compute_task(i, j).await {
                    Ok(_) => {
                        tasks_computed += 1;
                        println!("Computed C[{}][{}]", i, j);
                    }
                    Err(e) => {
                        println!("Failed to compute C[{}][{}]: {}", i, j, e);
                    }
                }
            }

            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// Waits for the computation to complete (polls for all result keys).
    pub async fn wait_for_completion(&self, m: usize, p: usize) -> Result<(), Error> {
        let total = m * p;
        let mut last_count = 0;
        loop {
            let mut count = 0;
            for idx in 1..=total {
                let key = idx as i64;
                if self.map.contains_key(key) {
                    count += 1;
                }
            }
            if count != last_count {
                println!("Progress: {}/{} elements computed", count, total);
                last_count = count;
            }
            if count == total {
                return Ok(());
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// Retrieves the complete result matrix.
    pub async fn get_result(&self, m: usize, p: usize) -> Result<Vec<Vec<f64>>, Error> {
        let mut result = vec![vec![0.0; p]; m];
        for i in 0..m {
            for j in 0..p {
                let key = (i * p + j + 1) as i64;
                let value = self
                    .map
                    .get(key)
                    .await?
                    .ok_or(Error::MissingMatrixData(key))?;
                result[i][j] = value.parse()?;
            }
        }
        Ok(result)
    }

    /// Checks if all result keys are present.
    fn is_complete(&self) -> Result<bool, Error> {
        let total = self.m * self.p;
        if total == 0 {
            return Ok(false);
        }
        let mut count = 0;
        for idx in 1..=total {
            if self.map.contains_key(idx as i64) {
                count += 1;
            }
        }
        Ok(count == total)
    }

    /// Picks a random task (i, j) that hasn't been computed yet.
    fn pick_random_task(&self) -> Option<(usize, usize)> {
        if self.m == 0 || self.p == 0 {
            println!("none");
            return None;
        }

        let mut rng = rand::thread_rng();
        let i = rng.gen_range(0..self.m);
        let j = rng.gen_range(0..self.p);
        let key = (i * self.p + j + 1) as i64;

        if self.map.contains_key(key) {
            None
        } else {
            Some((i, j))
        }
    }

    /// Attempts to compute a single element C[i][j] and write it to the map.
    async fn try_compute_task(&self, i: usize, j: usize) -> Result<(), Error> {
        let mut row_a = Vec::new();
        let mut col_b = Vec::new();

        let a_key = -(i as i64 + 1);
        if let Some(value) = self.map.get(a_key).await? {
            row_a = value
                .split(',')
                .map(|s| s.parse::<f64>())
                .collect::<Result<Vec<_>, _>>()?;
        }

        for k in 0..self.n {
            let b_key = -(self.m as i64 + k as i64 + 1);
            if let Some(value) = self.map.get(b_key).await? {
                let row: Vec<f64> = value
                    .split(',')
                    .map(|s| s.parse::<f64>())
                    .collect::<Result<Vec<_>, _>>()?;
                if let Some(&val) = row.get(j) {
                    col_b.push(val);
                }
            }
        }

        let mut sum = 0.0;
        for k in 0..row_a.len().min(col_b.len()) {
            sum += row_a[k] * col_b[k];
        }

        let key = (i * self.p + j + 1) as i64;
        println!("  Writing C[{}][{}] = {} to key {}", i, j, sum, key);
        self.map.insert(key, sum.to_string()).await?;

        Ok(())
    }
}
