#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
    use ironcrab::log_manager::LogManager;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_log_cleanup_removes_old_files() {
        // Create a temporary directory for test files
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap();

        // Create test log files with different dates
        let today = Utc::now().date_naive();
        let old_date = today - Duration::days(8);
        let recent_date = today - Duration::days(2);

        // Create old file (should be removed)
        let old_filename = format!("trades-{}.csv", old_date.format("%Y%m%d"));
        let old_file_path = temp_dir.path().join(&old_filename);
        let mut old_file = File::create(&old_file_path).unwrap();
        writeln!(old_file, "timestamp,symbol,side,price,quantity").unwrap();

        // Create recent file (should be kept)
        let recent_filename = format!("trades-{}.csv", recent_date.format("%Y%m%d"));
        let recent_file_path = temp_dir.path().join(&recent_filename);
        let mut recent_file = File::create(&recent_file_path).unwrap();
        writeln!(recent_file, "timestamp,symbol,side,price,quantity").unwrap();

        // Create today's file (should be kept)
        let today_filename = format!("trades-{}.csv", today.format("%Y%m%d"));
        let today_file_path = temp_dir.path().join(&today_filename);
        let mut today_file = File::create(&today_file_path).unwrap();
        writeln!(today_file, "timestamp,symbol,side,price,quantity").unwrap();

        // Verify all files exist before cleanup
        assert!(old_file_path.exists());
        assert!(recent_file_path.exists());
        assert!(today_file_path.exists());

        // Create LogManager with 7-day retention, 24-hour cleanup interval
        let log_manager = LogManager::new(log_dir, 7, 24);

        // Run cleanup
        let _ = log_manager.cleanup_old_logs().await;

        // Verify old file was removed, recent files kept
        assert!(!old_file_path.exists(), "Old file should be removed");
        assert!(recent_file_path.exists(), "Recent file should be kept");
        assert!(today_file_path.exists(), "Today's file should be kept");
    }

    #[tokio::test]
    async fn test_log_cleanup_handles_empty_directory() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap();

        let log_manager = LogManager::new(log_dir, 7, 24);

        // Should not panic or error on empty directory
        let _ = log_manager.cleanup_old_logs().await;
    }

    #[tokio::test]
    async fn test_log_cleanup_ignores_non_trade_files() {
        let temp_dir = TempDir::new().unwrap();
        let log_dir = temp_dir.path().to_str().unwrap();

        // Create non-trade files that should be ignored
        let other_file = temp_dir.path().join("other-20240101.csv");
        File::create(&other_file).unwrap();

        let config_file = temp_dir.path().join("config.toml");
        File::create(&config_file).unwrap();

        let log_manager = LogManager::new(log_dir, 7, 24);
        let _ = log_manager.cleanup_old_logs().await;

        // Non-trade files should remain untouched
        assert!(other_file.exists());
        assert!(config_file.exists());
    }
}
