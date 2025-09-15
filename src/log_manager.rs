use anyhow::Result;
use chrono::{DateTime, Utc as ChronoUtc};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use tokio::time::{interval, Duration};
use tracing::{debug, info, warn};

/// Log retention and cleanup functionality for trade CSV files
pub struct LogManager {
    log_dir: PathBuf,
    retention_days: u32,
    cleanup_interval_hours: u32,
}

impl LogManager {
    /// Create a new LogManager with specified settings
    pub fn new(log_dir: &str, retention_days: u32, cleanup_interval_hours: u32) -> Self {
        Self {
            log_dir: PathBuf::from(log_dir),
            retention_days,
            cleanup_interval_hours,
        }
    }

    /// Create LogManager from sniper config with defaults
    pub fn from_sniper_config(sniper_cfg: &crate::config::SniperSettings) -> Self {
        let log_dir =
            std::env::var("IRONCRAB_TRADE_LOG_DIR").unwrap_or_else(|_| "trade_logs".to_string());
        let retention_days = sniper_cfg.log_retention_days.unwrap_or(30);
        let cleanup_interval_hours = sniper_cfg.log_cleanup_interval_hours.unwrap_or(24);

        Self::new(&log_dir, retention_days, cleanup_interval_hours)
    }

    /// Start the periodic cleanup task
    pub async fn start_cleanup_task(self) -> Result<()> {
        let mut interval_timer = interval(Duration::from_secs(
            self.cleanup_interval_hours as u64 * 3600,
        ));

        info!(
            dir = %self.log_dir.display(),
            retention_days = self.retention_days,
            interval_hours = self.cleanup_interval_hours,
            "Starting log cleanup task"
        );

        loop {
            interval_timer.tick().await;

            if let Err(e) = self.cleanup_old_logs().await {
                warn!(error = %e, "Failed to cleanup old logs");
            }

            // Update current log info metrics after cleanup
            if let Ok(info) = self.get_log_info().await {
                crate::metrics::LOG_FILES_CURRENT_COUNT
                    .store(info.total_files as u64, Ordering::Relaxed);
                crate::metrics::LOG_FILES_CURRENT_SIZE_BYTES
                    .store(info.total_size_bytes, Ordering::Relaxed);
            }
        }
    }

    /// Perform a single cleanup operation
    pub async fn cleanup_old_logs(&self) -> Result<()> {
        if !self.log_dir.exists() {
            debug!(dir = %self.log_dir.display(), "Log directory does not exist, skipping cleanup");
            return Ok(());
        }

        let cutoff_date = ChronoUtc::now() - chrono::Duration::days(self.retention_days as i64);
        let mut files_removed = 0;
        let mut total_size_removed = 0u64;

        debug!(
            dir = %self.log_dir.display(),
            cutoff_date = %cutoff_date.format("%Y-%m-%d"),
            "Starting log cleanup"
        );

        let mut read_dir = tokio::fs::read_dir(&self.log_dir).await?;

        while let Some(entry) = read_dir.next_entry().await? {
            let path = entry.path();

            // Only process CSV files that match the trade log pattern
            if !path.is_file() || path.extension().is_none_or(|ext| ext != "csv") {
                continue;
            }

            let filename = match path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name,
                None => continue,
            };

            // Check if it matches the trades-YYYYMMDD.csv pattern
            if !filename.starts_with("trades-") || filename.len() != 19 {
                continue;
            }

            // Extract the date from filename (trades-YYYYMMDD.csv)
            let date_str = &filename[7..15]; // Extract YYYYMMDD part

            if let Ok(file_date) = parse_date_from_filename(date_str) {
                if file_date < cutoff_date {
                    // File is older than retention period, remove it
                    match self.remove_log_file(&path).await {
                        Ok(size) => {
                            files_removed += 1;
                            total_size_removed += size;
                            info!(
                                file = %path.display(),
                                date = %file_date.format("%Y-%m-%d"),
                                size_kb = size / 1024,
                                "Removed old trade log file"
                            );
                        }
                        Err(e) => {
                            warn!(
                                file = %path.display(),
                                error = %e,
                                "Failed to remove old trade log file"
                            );
                        }
                    }
                }
            } else {
                debug!(
                    filename = filename,
                    "Skipping file with invalid date format"
                );
            }
        }

        if files_removed > 0 {
            // Update cleanup metrics
            crate::metrics::LOG_FILES_CLEANED_TOTAL.fetch_add(files_removed, Ordering::Relaxed);
            crate::metrics::LOG_CLEANUP_SIZE_BYTES_TOTAL
                .fetch_add(total_size_removed, Ordering::Relaxed);

            info!(
                files_removed = files_removed,
                total_size_kb = total_size_removed / 1024,
                retention_days = self.retention_days,
                "Completed log cleanup"
            );
        } else {
            debug!("No old log files found to remove");
        }

        Ok(())
    }

    /// Remove a single log file and return its size
    async fn remove_log_file(&self, path: &Path) -> Result<u64> {
        let metadata = tokio::fs::metadata(path).await?;
        let size = metadata.len();
        tokio::fs::remove_file(path).await?;
        Ok(size)
    }

    /// Get information about current log files
    pub async fn get_log_info(&self) -> Result<LogInfo> {
        if !self.log_dir.exists() {
            return Ok(LogInfo::default());
        }

        let mut total_files = 0;
        let mut total_size = 0u64;
        let mut oldest_date: Option<DateTime<ChronoUtc>> = None;
        let mut newest_date: Option<DateTime<ChronoUtc>> = None;

        let mut read_dir = tokio::fs::read_dir(&self.log_dir).await?;

        while let Some(entry) = read_dir.next_entry().await? {
            let path = entry.path();

            // Only process CSV files that match the trade log pattern
            if !path.is_file() || path.extension().is_none_or(|ext| ext != "csv") {
                continue;
            }

            let filename = match path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name,
                None => continue,
            };

            // Check if it matches the trades-YYYYMMDD.csv pattern
            if !filename.starts_with("trades-") || filename.len() != 19 {
                continue;
            }

            // Extract the date from filename
            let date_str = &filename[7..15];
            if let Ok(file_date) = parse_date_from_filename(date_str) {
                total_files += 1;

                let metadata = tokio::fs::metadata(&path).await?;
                total_size += metadata.len();

                if oldest_date.is_none() || file_date < oldest_date.unwrap() {
                    oldest_date = Some(file_date);
                }
                if newest_date.is_none() || file_date > newest_date.unwrap() {
                    newest_date = Some(file_date);
                }
            }
        }

        Ok(LogInfo {
            total_files,
            total_size_bytes: total_size,
            oldest_date,
            newest_date,
        })
    }
}

/// Information about current log files
#[derive(Debug, Default)]
pub struct LogInfo {
    pub total_files: usize,
    pub total_size_bytes: u64,
    pub oldest_date: Option<DateTime<ChronoUtc>>,
    pub newest_date: Option<DateTime<ChronoUtc>>,
}

/// Parse date from filename format YYYYMMDD
fn parse_date_from_filename(date_str: &str) -> Result<DateTime<ChronoUtc>, chrono::ParseError> {
    let naive_date = chrono::NaiveDate::parse_from_str(date_str, "%Y%m%d")?;
    let naive_datetime = naive_date.and_hms_opt(0, 0, 0).unwrap();
    Ok(DateTime::from_naive_utc_and_offset(
        naive_datetime,
        ChronoUtc,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::fs;

    #[test]
    fn test_parse_date_from_filename() {
        let date = parse_date_from_filename("20231215").unwrap();
        assert_eq!(date.format("%Y-%m-%d").to_string(), "2023-12-15");

        assert!(parse_date_from_filename("invalid").is_err());
        assert!(parse_date_from_filename("20231301").is_err()); // Invalid month
    }

    #[tokio::test]
    async fn test_log_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let log_manager = LogManager::new(temp_dir.path().to_str().unwrap(), 7, 24);

        // Create some test files
        let old_date = ChronoUtc::now() - chrono::Duration::days(10);
        let recent_date = ChronoUtc::now() - chrono::Duration::days(3);

        let old_filename = format!("trades-{}.csv", old_date.format("%Y%m%d"));
        let recent_filename = format!("trades-{}.csv", recent_date.format("%Y%m%d"));

        let old_path = temp_dir.path().join(&old_filename);
        let recent_path = temp_dir.path().join(&recent_filename);

        // Create test files
        fs::write(&old_path, "test,data\n1,2\n").await.unwrap();
        fs::write(&recent_path, "test,data\n3,4\n").await.unwrap();

        // Also create a non-matching file that should be ignored
        let other_path = temp_dir.path().join("other.csv");
        fs::write(&other_path, "other,data\n").await.unwrap();

        // Run cleanup
        log_manager.cleanup_old_logs().await.unwrap();

        // Check results
        assert!(!old_path.exists(), "Old file should be removed");
        assert!(recent_path.exists(), "Recent file should remain");
        assert!(other_path.exists(), "Non-matching file should remain");
    }

    #[tokio::test]
    async fn test_log_info() {
        let temp_dir = TempDir::new().unwrap();
        let log_manager = LogManager::new(temp_dir.path().to_str().unwrap(), 7, 24);

        // Create test files
        let date1 = ChronoUtc::now() - chrono::Duration::days(5);
        let date2 = ChronoUtc::now() - chrono::Duration::days(2);

        let filename1 = format!("trades-{}.csv", date1.format("%Y%m%d"));
        let filename2 = format!("trades-{}.csv", date2.format("%Y%m%d"));

        let path1 = temp_dir.path().join(&filename1);
        let path2 = temp_dir.path().join(&filename2);

        fs::write(&path1, "test,data\n1,2\n").await.unwrap();
        fs::write(&path2, "test,data\n3,4,5\n").await.unwrap();

        let info = log_manager.get_log_info().await.unwrap();

        assert_eq!(info.total_files, 2);
        assert!(info.total_size_bytes > 0);
        assert!(info.oldest_date.is_some());
        assert!(info.newest_date.is_some());
    }
}
