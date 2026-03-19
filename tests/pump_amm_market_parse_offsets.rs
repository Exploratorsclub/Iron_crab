//! Regression: PumpSwap market account layout + PDAs used by try_parse_pool_static_from_market_account_inner.
//! Mainnet fixtures (no RPC in tests).

use base64::Engine;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

const PUMPFUN_AMM_PROGRAM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const PUMPFUN_AMM_FEE_CONFIG: &str = "5PHirr8joyTMp9JMm6nW7hNDVyEYdkzDqazxPD7RaTjx";
const PUMPFUN_AMM_MARKET_CREATOR_SEED_OFFSET: usize = 211;

#[test]
fn pump_amm_singleton_global_volume_accumulator_pda_matches_mainnet() {
    let amm = Pubkey::from_str(PUMPFUN_AMM_PROGRAM).unwrap();
    let (gva, _bump) = Pubkey::find_program_address(&[b"global_volume_accumulator"], &amm);
    let expected = Pubkey::from_str("C2aFPdENg4A2HQsmrd5rTw5TaYBX5Ku887cWjbFKtZpw").unwrap();
    assert_eq!(
        gva, expected,
        "singleton global_volume_accumulator PDA must match live PumpSwap program"
    );
}

#[test]
fn pump_amm_fee_config_constant_matches_build_swap_convention() {
    let pk = Pubkey::from_str(PUMPFUN_AMM_FEE_CONFIG).unwrap();
    assert!(!pk.eq(&Pubkey::default()));
}

#[test]
fn pump_amm_creator_vault_pda_from_offset_211_gw_qj_pool() {
    // Pool 5rNMGrJ3V2vUY3GAuxiVKZmKCn6c5N6n7Ld5EWvgceVX — market account 301 bytes.
    let amm = Pubkey::from_str(PUMPFUN_AMM_PROGRAM).unwrap();
    let b64 = "8ZptBBGxbbz/AACai0BBbzRLepxzNQv4fUEdt0Z2lNdFokjimACHtmiIUezPgLlU+9lM8/Ofzs8lfBxr9pDv6iI3kBOq+3hRFgQABpuIV/6rgYT7aH9jRhjANdrEOdwa6ztVmKDwAAAAAAGWj0C+EyxnXaJ/J6wS8ZkBFTzAFbSlvzoqwlqpLOgAOhS8Icfsr7Rys+wmeOWG/XW+SS7OvdfUke7AU6C7b8m//53rxyt/TB98HUM6fdo3aHU7XRG4VOYrejj5U7Rg/U3SGWxZ0AMAAAjzdrbioivkPftheEW+zQSxRJ/PoEPyIxTj79Xhx75xAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==";
    let raw = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .unwrap();
    assert!(raw.len() >= PUMPFUN_AMM_MARKET_CREATOR_SEED_OFFSET + 32);
    let creator_seed = Pubkey::new_from_array(
        raw[PUMPFUN_AMM_MARKET_CREATOR_SEED_OFFSET..PUMPFUN_AMM_MARKET_CREATOR_SEED_OFFSET + 32]
            .try_into()
            .unwrap(),
    );
    let (authority, _) =
        Pubkey::find_program_address(&[b"creator_vault", creator_seed.as_ref()], &amm);
    let expected = Pubkey::from_str("6tkGUcYBJJ2c1pdtMQayUpEFEpyP3QqY8Lf6pvvpF5Fq").unwrap();
    assert_eq!(
        authority, expected,
        "creator_vault PDA from offset-211 seed must match swap ix account[18] on mainnet"
    );
}

#[test]
fn pump_amm_creator_vault_pda_from_offset_211_e7_ua_pool() {
    // Pool B8bvg3KzXzGAq51QjirhPTw5ChhiZWn2kNwvQd3YZFN8
    let amm = Pubkey::from_str(PUMPFUN_AMM_PROGRAM).unwrap();
    let b64 = "8ZptBBGxbbz+AADFaNoRa55klcAW7+DWqWYaL4wXt/vmI+Z60yGbPtS2fcLQmOioUSn0Ypyh/1QR45c8Rumr0I+K69gWfHjXic+/BpuIV/6rgYT7aH9jRhjANdrEOdwa6ztVmKDwAAAAAAEg8VWn3M0V6+sFAV7d7g8VL6SuPdq4xGsV69FYbfNd7OzOMZuzuU1VcAv9CBCss2frCZSgMN3cGvZBJkT47xn0LP95BZ/EPpystn/DXn8e8qAukAKYLaaesYiLvHOoT+WUFWxZ0AMAAFo9zhD4w2iy35YoI+u6fUkjqSSXsJGKfeA6Wz13nqmUAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==";
    let raw = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .unwrap();
    assert!(raw.len() >= PUMPFUN_AMM_MARKET_CREATOR_SEED_OFFSET + 32);
    let creator_seed = Pubkey::new_from_array(
        raw[PUMPFUN_AMM_MARKET_CREATOR_SEED_OFFSET..PUMPFUN_AMM_MARKET_CREATOR_SEED_OFFSET + 32]
            .try_into()
            .unwrap(),
    );
    let (authority, _) =
        Pubkey::find_program_address(&[b"creator_vault", creator_seed.as_ref()], &amm);
    let expected = Pubkey::from_str("AyJm7rZGQLKEAEQ8vHHhip9ycAgGG6arHxXGNwJKvDeu").unwrap();
    assert_eq!(
        authority, expected,
        "creator_vault PDA from offset-211 seed must match swap ix account[18] on mainnet"
    );
}
