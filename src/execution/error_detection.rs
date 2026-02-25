//! Error detection utilities for execution flows.
//!
//! Used to recognize specific on-chain error codes (e.g. PumpFun BondingCurveComplete)
//! for retry logic in liquidation and similar cold-path flows.

/// Detects BondingCurveComplete (6005) in error messages.
///
/// PumpFun bonding curve returns Custom(6005) when the curve has migrated to PumpSwap AMM.
/// This function checks for typical string representations:
/// - `"6005"` (decimal)
/// - `"0x1775"` (hex: 6005)
/// - `"InstructionError(1, Custom(6005))"`
/// - `"Custom(6005)"`
///
/// Returns `true` if the error appears to indicate BondingCurveComplete, `false` otherwise.
pub fn is_6005_bonding_curve_complete(err: &impl std::fmt::Display) -> bool {
    let s = format!("{err}");
    s.contains("6005") || s.contains("0x1775")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_6005_decimal() {
        assert!(is_6005_bonding_curve_complete(&"6005"));
    }

    #[test]
    fn detects_6005_hex() {
        assert!(is_6005_bonding_curve_complete(&"0x1775"));
    }

    #[test]
    fn detects_instruction_error_custom_6005() {
        assert!(is_6005_bonding_curve_complete(
            &"InstructionError(1, Custom(6005))"
        ));
    }

    #[test]
    fn detects_custom_6005() {
        assert!(is_6005_bonding_curve_complete(&"Custom(6005)"));
    }

    #[test]
    fn rejects_custom_6023() {
        assert!(!is_6005_bonding_curve_complete(&"Custom(6023)"));
    }

    #[test]
    fn rejects_other_simulation_failure() {
        assert!(!is_6005_bonding_curve_complete(&"Simulation failed: other"));
    }
}
