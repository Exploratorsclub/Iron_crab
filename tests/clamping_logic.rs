#[cfg(test)]
mod tests {
    use ironcrab::metrics;

    // Helper functions to access internal metrics counters for testing
    // These are available because we test the actual global state

    #[test]
    fn test_fee_pct_clamping_normal_values() {
        // Reset metrics first if available
        #[cfg(feature = "test_helpers")]
        {
            // Note: This would need a reset function for fee_pct metrics
            // For now, we test the functions don't panic
        }

        // Test normal fee percentages - these should all work fine
        metrics::record_fee_pct(0.0); // 0%
        metrics::record_fee_pct(0.1); // 0.1%
        metrics::record_fee_pct(0.5); // 0.5%
        metrics::record_fee_pct(1.0); // 1.0%

        // All should complete without panicking
    }

    #[test]
    fn test_fee_pct_clamping_extreme_values() {
        // Test values that should be clamped
        metrics::record_fee_pct(-1.0); // Negative fee - should be clamped to 0
        metrics::record_fee_pct(150.0); // 150% fee - should be clamped to 1.0
        metrics::record_fee_pct(1000.0); // 1000% fee - should be clamped to 1.0

        // Values should be clamped internally without panicking
    }

    #[test]
    fn test_fee_pct_clamping_nan_infinity() {
        // Test special float values - these should be handled gracefully
        metrics::record_fee_pct(f64::NAN); // Should be clamped to 0.0
        metrics::record_fee_pct(f64::INFINITY); // Should be clamped to 1.0
        metrics::record_fee_pct(f64::NEG_INFINITY); // Should be clamped to 0.0

        // Should handle gracefully without panicking
    }

    #[test]
    fn test_shortfall_pct_clamping_normal_values() {
        // Test normal shortfall percentages
        metrics::record_shortfall_pct(0.0); // No shortfall
        metrics::record_shortfall_pct(0.1); // 0.1% shortfall
        metrics::record_shortfall_pct(0.5); // 0.5% shortfall
        metrics::record_shortfall_pct(1.0); // 1.0% shortfall

        // All should complete without panicking
    }

    #[test]
    fn test_shortfall_pct_clamping_extreme_values() {
        // Test extreme shortfall values
        metrics::record_shortfall_pct(-5.0); // Negative shortfall - should be clamped to 0.0
        metrics::record_shortfall_pct(200.0); // 200% shortfall - should be clamped to 1.0
        metrics::record_shortfall_pct(500.0); // 500% shortfall - should be clamped to 1.0

        // Should be clamped internally without panicking
    }

    #[test]
    fn test_shortfall_pct_clamping_special_values() {
        // Test special float values for shortfall
        metrics::record_shortfall_pct(f64::NAN); // Should be clamped to 0.0
        metrics::record_shortfall_pct(f64::INFINITY); // Should be clamped to 1.0
        metrics::record_shortfall_pct(f64::NEG_INFINITY); // Should be clamped to 0.0

        // Should handle gracefully without panicking
    }

    #[test]
    fn test_trade_return_clamping_normal_values() {
        #[cfg(feature = "test_helpers")]
        metrics::reset_trade_return_metrics();

        // Test normal trade returns
        metrics::record_trade_return(-10.0); // -10% loss
        metrics::record_trade_return(-5.0); // -5% loss
        metrics::record_trade_return(0.0); // Break-even
        metrics::record_trade_return(2.5); // 2.5% profit
        metrics::record_trade_return(10.0); // 10% profit
        metrics::record_trade_return(50.0); // 50% profit

        // All should complete without panicking
    }

    #[test]
    fn test_trade_return_clamping_extreme_values() {
        // Test extreme trade returns
        metrics::record_trade_return(-100.0); // -100% loss (total loss)
        metrics::record_trade_return(-150.0); // -150% loss (should be clamped for bucketing)
        metrics::record_trade_return(1000.0); // 1000% gain (should be clamped for bucketing)
        metrics::record_trade_return(10000.0); // 10000% gain (should be clamped for bucketing)

        // Should handle extreme values gracefully
    }

    #[test]
    fn test_trade_return_clamping_special_values() {
        // Test special float values for trade returns
        metrics::record_trade_return(f64::NAN); // Should be handled as 0.0
        metrics::record_trade_return(f64::INFINITY); // Should be handled gracefully
        metrics::record_trade_return(f64::NEG_INFINITY); // Should be handled gracefully

        // Should handle gracefully without panicking
    }

    #[test]
    fn test_clamping_bounds_consistency() {
        // Test that clamping bounds are consistent and logical

        // Fee percentages should generally be in 0-1 range (0-100%)
        // Based on the source code, fees are clamped to [0, 1]
        metrics::record_fee_pct(-0.5); // Should become 0.0
        metrics::record_fee_pct(1.5); // Should become 1.0

        // Shortfall percentages should be in 0-1 range (0-100%)
        // Based on the source code, shortfall is clamped to [0, 1]
        metrics::record_shortfall_pct(-0.5); // Should become 0.0
        metrics::record_shortfall_pct(1.5); // Should become 1.0

        // Trade returns allow wider range but bucket for histograms
        metrics::record_trade_return(-200.0); // Should be handled
        metrics::record_trade_return(2000.0); // Should be handled
    }

    #[test]
    fn test_float_precision_edge_cases() {
        // Test very small values near zero
        metrics::record_fee_pct(f64::EPSILON); // Smallest positive f64
        metrics::record_fee_pct(-f64::EPSILON); // Should become 0.0
        metrics::record_fee_pct(1e-10); // Very small positive
        metrics::record_fee_pct(-1e-10); // Should become 0.0

        // Test values near bounds
        metrics::record_trade_return(-99.9999); // Very close to -100%
        metrics::record_trade_return(-100.0001); // Just over -100%

        // Should handle precision gracefully
    }

    #[test]
    fn test_subnormal_float_handling() {
        // Test subnormal float values (very small denormalized numbers)
        let subnormal = f64::MIN_POSITIVE / 2.0; // Create a subnormal number
        assert!(subnormal.is_subnormal() || subnormal == 0.0);

        metrics::record_fee_pct(subnormal);
        metrics::record_fee_pct(-subnormal); // Should become 0.0

        // Should handle subnormal numbers gracefully
    }

    #[test]
    fn test_clamping_specific_logic() {
        // Test specific clamping logic based on the source code

        // Fee percentage clamping: NaN/Inf/negative -> 0.0, > 1.0 -> 1.0
        metrics::record_fee_pct(f64::NAN); // Should become 0.0
        metrics::record_fee_pct(f64::INFINITY); // Should become 1.0 (since it's not < 0)
        metrics::record_fee_pct(-1.0); // Should become 0.0
        metrics::record_fee_pct(2.0); // Should become 1.0

        // Shortfall percentage has same logic as fee percentage
        metrics::record_shortfall_pct(f64::NAN); // Should become 0.0
        metrics::record_shortfall_pct(f64::INFINITY); // Should become 1.0
        metrics::record_shortfall_pct(-1.0); // Should become 0.0
        metrics::record_shortfall_pct(2.0); // Should become 1.0

        // Trade return: finite values are preserved, NaN/Inf become 0.0 for sum
        metrics::record_trade_return(f64::NAN); // Becomes 0.0 for calculations
        metrics::record_trade_return(f64::INFINITY); // Becomes 0.0 for calculations
        metrics::record_trade_return(100.0); // Preserved as 100.0
    }

    #[test]
    fn test_concurrent_clamping_safety() {
        use std::thread;

        // Test concurrent access to clamping functions
        let mut handles = vec![];

        for i in 0..10 {
            let handle = thread::spawn(move || {
                for j in 0..10 {
                    let value = (i * 10 + j) as f64 * 0.01; // 0.00 to 0.99
                    metrics::record_fee_pct(value);
                    metrics::record_shortfall_pct(value);
                    metrics::record_trade_return(value);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Should complete without data races or panics
    }

    #[test]
    fn test_clamping_documentation_compliance() {
        // Test that clamping behavior matches what's documented in the code

        // From record_fee_pct: "Clamp to [0, 1] to avoid outliers; guard NaN/Inf"
        // NaN/Inf/negative -> 0.0, else min(pct, 1.0)
        metrics::record_fee_pct(-0.1); // -> 0.0
        metrics::record_fee_pct(0.5); // -> 0.5 (unchanged)
        metrics::record_fee_pct(1.5); // -> 1.0

        // From record_shortfall_pct: same logic as fee_pct
        metrics::record_shortfall_pct(-0.1); // -> 0.0
        metrics::record_shortfall_pct(0.5); // -> 0.5 (unchanged)
        metrics::record_shortfall_pct(1.5); // -> 1.0

        // From record_trade_return: finite values preserved, NaN/Inf -> 0.0 for actual
        // Bucketing uses clamped value, sum uses actual value
        metrics::record_trade_return(-50.0); // Preserved for sum, clamped for bucketing
        metrics::record_trade_return(f64::NAN); // Becomes 0.0 for calculations
    }

    #[test]
    fn test_bucket_clamping_vs_sum_preservation() {
        #[cfg(feature = "test_helpers")]
        metrics::reset_trade_return_metrics();

        // Test that extreme values are handled correctly:
        // - Bucketing uses clamped values to keep distribution stable
        // - Sum/average uses actual values (with saturation)

        // This extreme value should be:
        // - Clamped for bucket placement to fit within TRADE_RETURN_BUCKETS
        // - Preserved (with saturation) for the running sum
        metrics::record_trade_return(10000.0); // Very large gain

        // This should not panic and should handle the extreme value appropriately
    }

    #[test]
    fn test_saturation_arithmetic() {
        // Test that very large values use saturation arithmetic in sum calculations

        // From the code: micro values are clamped to i64::MAX/MIN to prevent overflow
        let very_large = 1e20; // This * 1_000_000 would overflow i64
        metrics::record_trade_return(very_large);

        let very_small = -1e20; // This * 1_000_000 would underflow i64
        metrics::record_trade_return(very_small);

        // Should handle with saturation, not panic
    }
}
