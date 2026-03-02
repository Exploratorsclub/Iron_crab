//! Position-related utilities (INVARIANTS.md I-13, FIX-38).
//!
//! Pool-matching logic for position price updates.

/// Pool-Matching für Position-Preis-Updates (INVARIANTS.md I-13, FIX-38).
///
/// Liefert `true`, wenn das Update angewendet werden soll.
/// Update wird übersprungen, wenn source_pool != position.pool (und position.pool nicht leer).
pub fn should_apply_position_price_update(position_pool: &str, source_pool: Option<&str>) -> bool {
    match source_pool {
        None => true,
        Some(pool) => position_pool.is_empty() || position_pool == pool,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_when_source_pool_none() {
        assert!(should_apply_position_price_update("pool123", None));
        assert!(should_apply_position_price_update("", None));
    }

    #[test]
    fn apply_when_position_pool_empty() {
        assert!(should_apply_position_price_update("", Some("any_pool")));
    }

    #[test]
    fn apply_when_pools_match() {
        assert!(should_apply_position_price_update(
            "pool123",
            Some("pool123")
        ));
    }

    #[test]
    fn skip_when_pools_differ() {
        assert!(!should_apply_position_price_update(
            "pool123",
            Some("other_pool")
        ));
    }
}
