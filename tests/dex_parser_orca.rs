use ironcrab::solana::dex_parser::{
    parse_transaction_update_with_pool_lookup, OrcaPoolInfo, SOL_MINT,
};
use ironcrab::solana::geyser_listener::{GeyserTransactionUpdate, TokenAmount, TokenBalance};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::time::Instant;

const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const ORCA_SWAP: [u8; 8] = [0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8];

fn build_orca_swap_data(amount: u64, amount_specified_is_input: bool, a_to_b: bool) -> Vec<u8> {
    let mut data = Vec::with_capacity(42);
    data.extend_from_slice(&ORCA_SWAP);
    data.extend_from_slice(&amount.to_le_bytes());
    data.extend_from_slice(&0u64.to_le_bytes()); // other_amount_threshold
    data.extend_from_slice(&0u128.to_le_bytes()); // sqrt_price_limit
    data.push(if amount_specified_is_input { 1 } else { 0 });
    data.push(if a_to_b { 1 } else { 0 });
    data
}

fn token_balance(account_index: u8, mint: &Pubkey, amount: u64, decimals: u8) -> TokenBalance {
    TokenBalance {
        account_index,
        mint: mint.to_string(),
        ui_token_amount: TokenAmount {
            ui_amount: Some(amount as f64 / 10f64.powi(decimals as i32)),
            decimals,
            amount: amount.to_string(),
        },
        program_id: None,
    }
}

#[test]
fn parse_orca_buy_from_sol_uses_pool_mints_and_offsets() {
    let trader = Pubkey::new_unique();
    let pool = Pubkey::new_unique();
    let orca_program = Pubkey::from_str(ORCA_WHIRLPOOL).unwrap();
    let sol_mint = Pubkey::from_str(SOL_MINT).unwrap();
    let token_mint = Pubkey::new_unique();

    let instruction_data = build_orca_swap_data(5_000, true, true);

    let update = GeyserTransactionUpdate {
        signature: "sig_buy".to_string(),
        slot: 1,
        account_keys: vec![trader, orca_program],
        instruction_accounts: vec![Pubkey::new_unique(), Pubkey::new_unique(), pool, trader],
        instruction_data,
        inner_instructions: vec![],
        pre_token_balances: vec![token_balance(1, &token_mint, 0, 6)],
        post_token_balances: vec![token_balance(1, &token_mint, 100, 6)],
        pre_balances: vec![10_000],
        post_balances: vec![5_000],
        fee_lamports: 0,
        compute_units_consumed: None,
        grpc_recv_at: Instant::now(),
    };

    let lookup = |pool_key: &Pubkey| {
        if pool_key == &pool {
            Some(OrcaPoolInfo {
                token_mint_a: sol_mint,
                token_mint_b: token_mint,
                token_vault_a: Pubkey::new_unique(),
                token_vault_b: Pubkey::new_unique(),
                tick_current_index: Some(0),
                tick_spacing: Some(64),
                token_a_program: None,
                token_b_program: None,
            })
        } else {
            None
        }
    };

    let parsed = parse_transaction_update_with_pool_lookup(&update, Some(&lookup))
        .expect("expected orca trade");

    let (mint, quote_mint, is_buy, sol_amount, token_amount) = match parsed {
        ironcrab::solana::dex_parser::ParsedDexEvent::Trade {
            mint,
            quote_mint,
            is_buy,
            sol_amount,
            token_amount,
            ..
        } => (mint, quote_mint, is_buy, sol_amount, token_amount),
        _ => panic!("expected trade event"),
    };

    assert_eq!(mint, token_mint);
    assert_eq!(quote_mint, sol_mint);
    assert!(is_buy);
    assert_eq!(sol_amount, 5_000);
    assert_eq!(token_amount, 100);
}

#[test]
fn parse_orca_sell_to_sol_uses_pool_mints_and_offsets() {
    let trader = Pubkey::new_unique();
    let pool = Pubkey::new_unique();
    let orca_program = Pubkey::from_str(ORCA_WHIRLPOOL).unwrap();
    let sol_mint = Pubkey::from_str(SOL_MINT).unwrap();
    let token_mint = Pubkey::new_unique();

    let instruction_data = build_orca_swap_data(200, true, true);

    let update = GeyserTransactionUpdate {
        signature: "sig_sell".to_string(),
        slot: 1,
        account_keys: vec![trader, orca_program],
        instruction_accounts: vec![Pubkey::new_unique(), Pubkey::new_unique(), pool, trader],
        instruction_data,
        inner_instructions: vec![],
        pre_token_balances: vec![token_balance(1, &token_mint, 300, 6)],
        post_token_balances: vec![token_balance(1, &token_mint, 100, 6)],
        pre_balances: vec![5_000],
        post_balances: vec![12_000],
        fee_lamports: 0,
        compute_units_consumed: None,
        grpc_recv_at: Instant::now(),
    };

    let lookup = |pool_key: &Pubkey| {
        if pool_key == &pool {
            Some(OrcaPoolInfo {
                token_mint_a: token_mint,
                token_mint_b: sol_mint,
                token_vault_a: Pubkey::new_unique(),
                token_vault_b: Pubkey::new_unique(),
                tick_current_index: Some(0),
                tick_spacing: Some(64),
                token_a_program: None,
                token_b_program: None,
            })
        } else {
            None
        }
    };

    let parsed = parse_transaction_update_with_pool_lookup(&update, Some(&lookup))
        .expect("expected orca trade");

    let (mint, quote_mint, is_buy, sol_amount, token_amount) = match parsed {
        ironcrab::solana::dex_parser::ParsedDexEvent::Trade {
            mint,
            quote_mint,
            is_buy,
            sol_amount,
            token_amount,
            ..
        } => (mint, quote_mint, is_buy, sol_amount, token_amount),
        _ => panic!("expected trade event"),
    };

    assert_eq!(mint, token_mint);
    assert_eq!(quote_mint, sol_mint);
    assert!(!is_buy);
    assert_eq!(sol_amount, 7_000);
    assert_eq!(token_amount, 200);
}
