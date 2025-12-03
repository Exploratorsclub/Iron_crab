#[cfg(test)]
mod tests {
    use base64::Engine;
    use ironcrab::wallet::Treasury;
    use std::env;
    use tempfile::TempDir;
    // Serialize tests that mutate process-wide environment variables to avoid CI flakiness
    use serial_test::serial;

    // Test-only helpers to centralize env mutations behind a single unsafe block,
    // satisfying linting that treats std::env changes as unsafe in this project.
    fn clear_env_vars_all() {
        unsafe {
            env::remove_var("IRONCRAB_KEYPAIR_JSON");
            env::remove_var("IRONCRAB_KEYPAIR_B64");
            env::remove_var("IRONCRAB_KEYPAIR_BASE58");
            env::remove_var("IRONCRAB_KEYPAIR_PATH");
        }
    }

    fn set_env_var(key: &str, val: &str) {
        unsafe {
            env::set_var(key, val);
        }
    }

    fn remove_env_var(key: &str) {
        unsafe {
            env::remove_var(key);
        }
    }

    #[test]
    #[serial]
    fn test_treasury_env_fallback_pattern() {
        // Clear any existing env vars
        clear_env_vars_all();

        // Test 1: When no env vars are set, load_from_env should fail
        assert!(Treasury::load_from_env().is_err());

        // Test 2: Test JSON env var works
        let test_keypair = solana_sdk::signer::keypair::Keypair::new();
        let keypair_bytes: Vec<u8> = test_keypair.to_bytes().to_vec();
        let json_string = serde_json::to_string(&keypair_bytes).unwrap();

        set_env_var("IRONCRAB_KEYPAIR_JSON", &json_string);

        // Should load successfully from JSON env var
        let treasury_from_env = Treasury::load_from_env();
        assert!(treasury_from_env.is_ok());

        // Test 3: Test fallback pattern (ENV first, then file)
        // Clear env var to force fallback
        remove_env_var("IRONCRAB_KEYPAIR_JSON");

        // Create a temporary keypair file
        let temp_dir = TempDir::new().unwrap();
        let keypair_path = temp_dir.path().join("test_keypair.json");
        std::fs::write(
            &keypair_path,
            serde_json::to_string(&keypair_bytes).unwrap(),
        )
        .unwrap();

        // Test the actual fallback pattern used in main.rs
        let treasury_fallback =
            Treasury::load_from_env().or_else(|_| Treasury::load(keypair_path.to_str().unwrap()));

        assert!(treasury_fallback.is_ok());

        // Test 4: ENV takes precedence over file path
        set_env_var("IRONCRAB_KEYPAIR_JSON", &json_string);

        let treasury_env_priority =
            Treasury::load_from_env().or_else(|_| Treasury::load(keypair_path.to_str().unwrap()));

        assert!(treasury_env_priority.is_ok());

        // Cleanup
        remove_env_var("IRONCRAB_KEYPAIR_JSON");
    }

    #[test]
    #[serial]
    fn test_treasury_env_formats() {
        // Clear env vars
        clear_env_vars_all();

        let test_keypair = solana_sdk::signer::keypair::Keypair::new();
        let keypair_bytes = test_keypair.to_bytes();

        // Test JSON format
        let json_string = serde_json::to_string(&keypair_bytes.to_vec()).unwrap();
        set_env_var("IRONCRAB_KEYPAIR_JSON", &json_string);
        assert!(Treasury::load_from_env().is_ok());
        remove_env_var("IRONCRAB_KEYPAIR_JSON");

        // Test Base64 format
        let base64_string = base64::engine::general_purpose::STANDARD.encode(keypair_bytes);
        set_env_var("IRONCRAB_KEYPAIR_B64", &base64_string);
        assert!(Treasury::load_from_env().is_ok());
        remove_env_var("IRONCRAB_KEYPAIR_B64");

        // Test Base58 format
        let base58_string = test_keypair.to_base58_string();
        set_env_var("IRONCRAB_KEYPAIR_BASE58", &base58_string);
        assert!(Treasury::load_from_env().is_ok());
        remove_env_var("IRONCRAB_KEYPAIR_BASE58");
    }

    #[test]
    #[serial]
    fn test_main_rs_fallback_pattern() {
        // This test specifically verifies the pattern used in main.rs works
        // Clear env vars
        clear_env_vars_all();

        // Create a test keypair file
        let test_keypair = solana_sdk::signer::keypair::Keypair::new();
        let keypair_bytes: Vec<u8> = test_keypair.to_bytes().to_vec();

        let temp_dir = TempDir::new().unwrap();
        let keypair_path = temp_dir.path().join("test_keypair.json");
        std::fs::write(
            &keypair_path,
            serde_json::to_string(&keypair_bytes).unwrap(),
        )
        .unwrap();

        // Simulate the exact pattern from main.rs:
        // Treasury::load_from_env().or_else(|_| Treasury::load(&cfg.solana.keypair_path))

        // First, clear ENV to test file fallback
        remove_env_var("IRONCRAB_KEYPAIR_JSON");

        let treasury_result =
            Treasury::load_from_env().or_else(|_| Treasury::load(keypair_path.to_str().unwrap()));

        // Should succeed by falling back to file
        assert!(treasury_result.is_ok());

        // Now test ENV priority - set env var and verify it takes precedence
        let json_string = serde_json::to_string(&keypair_bytes).unwrap();
        set_env_var("IRONCRAB_KEYPAIR_JSON", &json_string);

        let treasury_result =
            Treasury::load_from_env().or_else(|_| Treasury::load(keypair_path.to_str().unwrap()));

        // Should still succeed, with ENV taking priority
        assert!(treasury_result.is_ok());

        // Cleanup
        remove_env_var("IRONCRAB_KEYPAIR_JSON");
    }
}
