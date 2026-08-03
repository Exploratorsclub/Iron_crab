//! DLMM bin-array publish filtering (C1g: active-bin touch policy).
//! STOP-CHECK: no RPC; Geyser-only publish shaping.

use crate::ipc::BinData;
use crate::metrics::inc_market_data_dlmm_bin_emit_active_zero_touch_total;
use crate::solana::dex::meteora_bin_array_layout::Bin;

/// Bins per Meteora DLMM array (must match `BinArray::BINS_PER_ARRAY` and quote flatten).
pub const DLMM_BINS_PER_ARRAY: i64 = 70;

/// Offset of `active_id` within `bin_array_index`, if the active bin lies in this array.
pub fn active_bin_offset_in_array(active_id: i32, bin_array_index: i64) -> Option<u8> {
    let base = bin_array_index * DLMM_BINS_PER_ARRAY;
    let offset = i64::from(active_id) - base;
    if (0..DLMM_BINS_PER_ARRAY).contains(&offset) {
        Some(offset as u8)
    } else {
        None
    }
}

/// Filter parsed bins for publish: non-zero liquidity only, but always retain the active bin
/// (even zero-liquidity) so arb/momentum quotes can walk adjacent bins (C1g H2).
pub fn filter_dlmm_bins_for_publish(
    parsed_bins: &[Bin],
    bin_array_index: i64,
    active_id: Option<i32>,
) -> Vec<BinData> {
    let mut out: Vec<BinData> = parsed_bins
        .iter()
        .enumerate()
        .filter(|(_, bin)| bin.amount_x > 0 || bin.amount_y > 0)
        .map(|(offset, bin)| BinData {
            offset: offset as u8,
            amount_x: bin.amount_x,
            amount_y: bin.amount_y,
        })
        .collect();

    if let Some(active_id) = active_id {
        if let Some(active_offset) = active_bin_offset_in_array(active_id, bin_array_index) {
            let already_present = out.iter().any(|b| b.offset == active_offset);
            if !already_present {
                let idx = active_offset as usize;
                if let Some(bin) = parsed_bins.get(idx) {
                    out.push(BinData {
                        offset: active_offset,
                        amount_x: bin.amount_x,
                        amount_y: bin.amount_y,
                    });
                    out.sort_by_key(|b| b.offset);
                    inc_market_data_dlmm_bin_emit_active_zero_touch_total();
                }
            }
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_bins() -> Vec<Bin> {
        (0..DLMM_BINS_PER_ARRAY as usize)
            .map(|_| Bin {
                amount_x: 0,
                amount_y: 0,
                price: 0.0,
            })
            .collect()
    }

    #[test]
    fn active_bin_offset_in_array_positive_and_negative() {
        assert_eq!(active_bin_offset_in_array(0, 0), Some(0));
        assert_eq!(active_bin_offset_in_array(69, 0), Some(69));
        assert_eq!(active_bin_offset_in_array(70, 1), Some(0));
        assert_eq!(active_bin_offset_in_array(-1, -1), Some(69));
        assert_eq!(active_bin_offset_in_array(100, 0), None);
    }

    #[test]
    fn filter_includes_zero_liquidity_active_bin() {
        let mut bins = empty_bins();
        bins[5].amount_x = 1_000;
        bins[5].amount_y = 2_000;
        let active_id = 5i32;
        let filtered = filter_dlmm_bins_for_publish(&bins, 0, Some(active_id));
        assert!(filtered
            .iter()
            .any(|b| b.offset == 5 && b.amount_x == 1_000));
    }

    #[test]
    fn filter_touches_empty_active_bin_for_quote_walker() {
        let bins = empty_bins();
        let active_id = 10i32;
        let filtered = filter_dlmm_bins_for_publish(&bins, 0, Some(active_id));
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].offset, 10);
        assert_eq!(filtered[0].amount_x, 0);
        assert_eq!(filtered[0].amount_y, 0);
    }

    #[test]
    fn filter_skips_active_touch_when_active_not_in_array() {
        let bins = empty_bins();
        let filtered = filter_dlmm_bins_for_publish(&bins, 0, Some(200));
        assert!(filtered.is_empty());
    }
}
