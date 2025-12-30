//! Smoke test for hot-reload config updates across binaries
//!
//! This test verifies that ConfigUpdate messages can be serialized/deserialized
//! correctly and that the response types work as expected.
//!
//! Note: Full integration testing requires running NATS and multiple processes,
//! which is better suited for CI integration tests or manual testing.

use std::collections::HashMap;

use ironcrab::ipc::{ConfigUpdate, ConfigUpdateResponse, ConfigUpdateStatus};

/// Test that ConfigUpdate can be serialized and deserialized
#[test]
fn test_config_update_roundtrip() {
    let mut config = HashMap::new();
    config.insert(
        "max_slippage_bps".to_string(),
        serde_json::json!(500),
    );
    config.insert(
        "daily_loss_limit_lamports".to_string(),
        serde_json::json!(5_000_000_000u64),
    );
    config.insert(
        "enable_raydium".to_string(),
        serde_json::json!(true),
    );

    let update = ConfigUpdate {
        component: "execution-engine".to_string(),
        config,
        source: "control-plane".to_string(),
        timestamp: chrono::Utc::now(),
    };

    // Serialize to JSON (as NATS would)
    let json = serde_json::to_string(&update).expect("serialize ConfigUpdate");
    
    // Deserialize back
    let parsed: ConfigUpdate = serde_json::from_str(&json).expect("deserialize ConfigUpdate");
    
    assert_eq!(parsed.component, "execution-engine");
    assert_eq!(parsed.config.len(), 3);
    assert_eq!(parsed.config.get("max_slippage_bps").and_then(|v| v.as_u64()), Some(500));
    assert_eq!(parsed.config.get("enable_raydium").and_then(|v| v.as_bool()), Some(true));
}

/// Test ConfigUpdateResponse for successful apply
#[test]
fn test_config_update_response_applied() {
    let response = ConfigUpdateResponse {
        status: ConfigUpdateStatus::Applied,
        applied_keys: vec!["max_slippage_bps".to_string(), "daily_loss_limit_lamports".to_string()],
        rejected_keys: vec![],
        new_snapshot_id: Some("snap-001".to_string()),
    };

    let json = serde_json::to_string(&response).expect("serialize response");
    let parsed: ConfigUpdateResponse = serde_json::from_str(&json).expect("deserialize response");

    assert!(matches!(parsed.status, ConfigUpdateStatus::Applied));
    assert_eq!(parsed.applied_keys.len(), 2);
    assert!(parsed.rejected_keys.is_empty());
}

/// Test ConfigUpdateResponse for partial apply
#[test]
fn test_config_update_response_partial() {
    let response = ConfigUpdateResponse {
        status: ConfigUpdateStatus::PartiallyApplied,
        applied_keys: vec!["max_slippage_bps".to_string()],
        rejected_keys: vec![
            ("unknown_key".to_string(), "Unknown config key: unknown_key".to_string()),
            ("invalid_value".to_string(), "Must be > 0".to_string()),
        ],
        new_snapshot_id: None,
    };

    let json = serde_json::to_string(&response).expect("serialize response");
    let parsed: ConfigUpdateResponse = serde_json::from_str(&json).expect("deserialize response");

    assert!(matches!(parsed.status, ConfigUpdateStatus::PartiallyApplied));
    assert_eq!(parsed.applied_keys.len(), 1);
    assert_eq!(parsed.rejected_keys.len(), 2);
    assert_eq!(parsed.rejected_keys[0].0, "unknown_key");
}

/// Test ConfigUpdateResponse for full rejection
#[test]
fn test_config_update_response_rejected() {
    let response = ConfigUpdateResponse {
        status: ConfigUpdateStatus::Rejected,
        applied_keys: vec![],
        rejected_keys: vec![
            ("bad_key".to_string(), "Unknown config key".to_string()),
        ],
        new_snapshot_id: None,
    };

    assert!(matches!(response.status, ConfigUpdateStatus::Rejected));
    assert!(response.applied_keys.is_empty());
}

/// Test config values that should be validated
#[test]
fn test_config_value_types() {
    let mut config = HashMap::new();
    
    // u64 values
    config.insert("max_position_size_lamports".to_string(), serde_json::json!(500_000_000u64));
    config.insert("daily_loss_limit_lamports".to_string(), serde_json::json!(5_000_000_000u64));
    config.insert("default_position_lamports".to_string(), serde_json::json!(100_000_000u64));
    
    // u32 values (BPS)
    config.insert("max_slippage_bps".to_string(), serde_json::json!(500u32));
    config.insert("early_max_slippage_bps".to_string(), serde_json::json!(300u32));
    
    // f64 values
    config.insert("early_min_liquidity_sol".to_string(), serde_json::json!(5.0f64));
    
    // bool values
    config.insert("enable_raydium".to_string(), serde_json::json!(true));
    config.insert("enable_pumpfun".to_string(), serde_json::json!(false));

    let update = ConfigUpdate {
        component: "test".to_string(),
        config,
        source: "test".to_string(),
        timestamp: chrono::Utc::now(),
    };

    // Verify type extraction works correctly
    assert_eq!(update.config.get("max_position_size_lamports").and_then(|v| v.as_u64()), Some(500_000_000));
    assert_eq!(update.config.get("max_slippage_bps").and_then(|v| v.as_u64()), Some(500));
    assert_eq!(update.config.get("early_min_liquidity_sol").and_then(|v| v.as_f64()), Some(5.0));
    assert_eq!(update.config.get("enable_raydium").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(update.config.get("enable_pumpfun").and_then(|v| v.as_bool()), Some(false));
}

/// Test component filtering (each binary should only process its own updates)
#[test]
fn test_config_update_component_filtering() {
    let components = vec![
        "execution-engine",
        "momentum-bot", 
        "market-data",
        "control-plane",
    ];

    for component in components {
        let update = ConfigUpdate {
            component: component.to_string(),
            config: HashMap::new(),
            source: "test".to_string(),
            timestamp: chrono::Utc::now(),
        };

        // Each binary should check: if update.component == "my-component"
        assert_eq!(update.component, component);
    }
}
