//! JSONL kind filter — excludes high-volume noise from disk logging.

use crate::ipc::MarketEventKind;

/// PR165: high-volume noise kinds excluded from JSONL (NATS core may still receive them).
pub fn market_event_should_jsonl(kind: &MarketEventKind) -> bool {
    !matches!(
        kind,
        MarketEventKind::AccountUpdate { .. }
            | MarketEventKind::TransactionDetected { .. }
            | MarketEventKind::WalletActivity { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ipc::{WalletAction, WalletType};

    #[test]
    fn wallet_activity_excluded_from_jsonl() {
        let kind = MarketEventKind::WalletActivity {
            wallet: "w".into(),
            wallet_type: WalletType::Whale,
            action: WalletAction::Buy,
            mint: "m".into(),
            amount_sol: 1,
            amount_tokens: 1,
            signature: "sig".into(),
            wallet_win_rate: None,
        };
        assert!(!market_event_should_jsonl(&kind));
    }

    #[test]
    fn trade_still_written_to_jsonl() {
        let kind = MarketEventKind::Trade {
            pool_address: "pool".into(),
            mint: "m".into(),
            quote_mint: "So11111111111111111111111111111111111111112".into(),
            trader: "t".into(),
            is_buy: true,
            sol_amount: 1,
            token_amount: 1,
            token_decimals: 9,
            signature: Some("sig".into()),
            dex: "raydium".into(),
            creator: None,
            token_program: None,
        };
        assert!(market_event_should_jsonl(&kind));
    }
}
