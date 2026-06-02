use crate::execution::live_pool_cache::LivePoolCache;
use crate::solana::dex::{Dex, Quote};
use crate::solana::rpc::{ErrorClass, SolanaRpc};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use bs58;
use dashmap::DashMap;
use serde_json::{json, Value};
use solana_account_decoder::UiAccountEncoding;
use solana_client::client_error::ClientError;
use solana_client::rpc_config::{
    RpcAccountInfoConfig, RpcProgramAccountsConfig, RpcTransactionConfig,
};
use solana_client::rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType};
use solana_sdk::hash::hash;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::UiTransactionEncoding;
use spl_token::solana_program::pubkey::Pubkey as SplProgramPubkey;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{debug, info, warn};

const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const PUMPFUN_AMM_PROGRAM_ID: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
// Observed on-chain in PumpSwap/Pump.fun AMM swaps: `fee_program` is this program id.
const PUMPFUN_AMM_FEE_PROGRAM_ID: &str = "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ";
// Global Fee Program fee_config PDA — same for PumpSwap SELL ix meta #19 on successful mainnet txs.
// v14 `pool_accounts[12]` may carry a different pubkey (cache/market row artifact); SELL must still use this constant (Scope 59).
const PUMPFUN_AMM_FEE_CONFIG: &str = "5PHirr8joyTMp9JMm6nW7hNDVyEYdkzDqazxPD7RaTjx";

/// Same strings as `build_swap_ix_from_pool_accounts` uses for fee metas (diagnostics / downstream).
pub const PUMPFUN_AMM_BUILD_SWAP_FEE_CONFIG_STR: &str = PUMPFUN_AMM_FEE_CONFIG;
pub const PUMPFUN_AMM_BUILD_SWAP_FEE_PROGRAM_STR: &str = PUMPFUN_AMM_FEE_PROGRAM_ID;
/// Global config account — same for **all** PumpSwap pools (swap instruction account #2).
/// Verified from successful mainnet SELL/BUY txs; must not be read from market account bytes at
/// misaligned offsets (see incident: wrong bytes → pubkey with no account → Anchor 3012 on `global_config`).
const PUMPFUN_AMM_GLOBAL_CONFIG: &str = "ADyA8hdefvWN2dbGGWFotbzWxrAvLW83WG6QCVXvJKqw";

/// Fixed layout: `protocol_fee_recipient` pubkey inside PumpSwap `global_config` account data.
///
/// Real swaps pass this wallet as ix account #9 (`protocol_fee_recipient`). It is **not** embedded
/// in typical 301-byte pool markets that only expose lp_mint + pool vaults + creator_seed; the
/// previous heuristic (second quote TA in the market, or token accounts reachable from a pubkey
/// scan of `global_config`) then fails. Verified on mainnet: `global_config` bytes `[57..89]` equal
/// account #9 from a finalized PumpSwap tx for pool `5rNMGrJ3…` (Scope 41).
///
/// This is **not** a pool-specific override: it is the same field for all pools sharing this
/// `global_config` (Bug #35: we still do not substitute a guessed per-pool constant).
const PUMPFUN_AMM_GLOBAL_CONFIG_PROTOCOL_FEE_RECIPIENT_OFFSET: usize = 57;

/// Canonical PumpSwap `event_authority` PDA for swap instructions (Anchor seed `__event_authority`).
///
/// Same for all pools under `PUMPFUN_AMM_PROGRAM_ID`. DexPoolAccounts / cache slot [8] may carry a
/// wrong pubkey (mis-parse or stale layout); the program validates this account with seeds →
/// `ConstraintSeeds` (2006) if the passed key is not this PDA. See `dex_parser.rs` (SELL ix account 15).
fn pump_amm_canonical_event_authority(program_id: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"__event_authority"], program_id).0
}

/// Singleton `global_volume_accumulator` PDA — same address for all PumpSwap pools under the AMM program.
///
/// When market-byte heuristics find no AMM-owned candidate and PDA probes fail (e.g. account owner
/// differs from `program_id`), we still need this account for BUY; see `tests/pump_amm_market_parse_offsets.rs`.
fn pump_amm_singleton_global_volume_accumulator(pump_amm_program: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"global_volume_accumulator"], pump_amm_program).0
}

/// Resolve swap metas #9/#10 from observed pool/parser/cache values.
///
/// `protocol_fee_recipient` is **pool- and observation-specific** (not a global constant). If both
/// fee pubkeys are missing (`Pubkey::default()`), we return an error instead of substituting a
/// guessed mainnet recipient — that would violate Geyser-first / no-fake-truth invariants (I-4, I-12).
/// When the recipient is known but only the fee ATA is missing, derive the ATA from that recipient +
/// quote mint + quote token program (same on-chain derivation as observed swaps).
fn pump_amm_resolve_protocol_fee_accounts(
    protocol_fee_recipient: Pubkey,
    protocol_fee_recipient_ta: Pubkey,
    quote_mint: Pubkey,
    quote_token_program: Pubkey,
) -> Result<(Pubkey, Pubkey)> {
    match (
        protocol_fee_recipient == Pubkey::default(),
        protocol_fee_recipient_ta == Pubkey::default(),
    ) {
        (false, false) => Ok((protocol_fee_recipient, protocol_fee_recipient_ta)),
        (false, true) => Ok((
            protocol_fee_recipient,
            PumpFunAmmDex::derive_ata_with_program(
                protocol_fee_recipient,
                quote_mint,
                quote_token_program,
            ),
        )),
        (true, false) => Err(anyhow!(
            "pump_amm: protocol_fee_recipient missing but protocol_fee_recipient_ta is set (invalid fee metadata)"
        )),
        (true, true) => Err(anyhow!(
            "pump_amm: protocol_fee_recipient and protocol_fee_recipient_ta missing — cannot build swap ix without observed fee accounts"
        )),
    }
}

// Best-effort: observed Pump.fun AMM "market" account layout contains
// - base_mint at byte offset 43
// - quote_mint at byte offset 75
// Using `getProgramAccounts` with memcmp filters avoids reliance on tx-history on pruned RPC.
const PUMPFUN_AMM_MARKET_BASE_MINT_OFFSET: u64 = 43;
const PUMPFUN_AMM_MARKET_QUOTE_MINT_OFFSET: u64 = 75;
/// Minimum market account data length to read base_mint + quote_mint at fixed offsets.
const PUMPFUN_AMM_MARKET_MIN_DATA_LEN: usize = PUMPFUN_AMM_MARKET_QUOTE_MINT_OFFSET as usize + 32;

// PumpSwap AMM market account (301 bytes on mainnet): seed pubkey for `creator_vault` PDA lives here.
// Distinct from bonding-curve `creator-vault` (hyphen) — AMM uses underscore `creator_vault` + this seed.
const PUMPFUN_AMM_MARKET_CREATOR_SEED_OFFSET: usize = 211;

// Observed on-chain: buy_exact_quote_in fee fields sum to 125 bps (lp 2 + protocol 93 + creator 30).
// We use that as a conservative default for quoting.
const DEFAULT_TOTAL_FEE_BPS: u32 = 125;

fn anchor_disc(ix_name: &str) -> [u8; 8] {
    let out = hash(format!("global:{ix_name}").as_bytes());
    let mut disc = [0u8; 8];
    disc.copy_from_slice(&out.as_ref()[..8]);
    disc
}

/// Anchor discriminator for PumpSwap `sell` (IDL `global:sell`).
pub fn pump_amm_sell_ix_discriminator() -> [u8; 8] {
    anchor_disc("sell")
}

/// PumpSwap `sell` uses 21 base account metas; some pools require three trailing accounts (Pump cashback / volume tracking), total 24.
/// Verified on mainnet (e.g. sig `2CCmRDScAErjuBLnVJbGEyV3jsWbuNZpniZ5iTLSwZoE84nmyf285hqJXjRStMHJUaJ9Ex7EvL9fgwAVM83qGd3o`).
pub const PUMPFUN_AMM_SELL_EXTENDED_TOTAL_ACCOUNTS: usize = 24;

/// Cashback-enabled `sell` after Pump fee-recipient upgrade: 21 base + volume metas #21/#22 + `pool-v2` #23 + fee pair #24/#25.
/// See Pump `@pump_tech_updates` (2026): non-cashback `sell` = 24 accounts (`pool-v2` at #21); cashback `sell` = 26.
pub const PUMPFUN_AMM_SELL_CASHBACK_TOTAL_ACCOUNTS: usize = 26;

/// Extended cashback `sell` with two Pump-owned pre-fee metas before global FeeConfig/FeeProgram (mainnet ref
/// `3XPKr7ynZzRSwvwiVvWpDr58pFCUVUqwySBiUwPMqwUTDGETFWaZXpYWm84DnAuSet4rQRmcwUwsfZ8Vg8gJqeae`, pool `GrgDaBg4…`).
pub const PUMPFUN_AMM_SELL_EXTENDED_V2_TOTAL_ACCOUNTS: usize = 27;

/// Extended SELL tail indices (shared by legacy 24-account cashback and 26-account layouts).
pub const PUMPFUN_AMM_SELL_EXT_TAIL_0_IX: usize = 21;
pub const PUMPFUN_AMM_SELL_EXT_TAIL_1_IX: usize = 22;
pub const PUMPFUN_AMM_SELL_EXT_THIRD_META_IX: usize = 23;
/// Cashback `sell` fee-recipient pair after post-upgrade `pool-v2` meta (#23).
pub const PUMPFUN_AMM_SELL_FEE_TAIL_0_IX: usize = 24;
pub const PUMPFUN_AMM_SELL_FEE_TAIL_1_IX: usize = 25;

/// 27-account extended SELL: pre-fee metas before global fee pair; fee + trailing indices shifted by +2.
pub const PUMPFUN_AMM_SELL_PRE_FEE_0_IX_V2: usize = 19;
pub const PUMPFUN_AMM_SELL_PRE_FEE_1_IX_V2: usize = 20;
pub const PUMPFUN_AMM_SELL_FEE_CONFIG_IX_V2: usize = 21;
pub const PUMPFUN_AMM_SELL_FEE_PROGRAM_IX_V2: usize = 22;
pub const PUMPFUN_AMM_SELL_EXT_TAIL_0_IX_V2: usize = 23;
pub const PUMPFUN_AMM_SELL_EXT_TAIL_1_IX_V2: usize = 24;
pub const PUMPFUN_AMM_SELL_EXT_THIRD_META_IX_V2: usize = 25;
pub const PUMPFUN_AMM_SELL_FEE_TAIL_0_IX_V2: usize = 26;

/// Inputs for extended PumpSwap SELL layout readiness (JetStream publish + SLAVE gate).
#[derive(Debug, Clone, Copy)]
pub struct PumpAmmSellExtendedReadinessParams {
    pub sell_requires_extended: bool,
    pub third_meta: Option<Pubkey>,
    pub fee_tail_0: Option<Pubkey>,
    pub fee_tail_1: Option<Pubkey>,
    pub sell_requires_fee_tail: bool,
    pub sell_requires_pre_fee_metas: bool,
    pub sell_pre_fee_meta_1: Option<Pubkey>,
}

/// Whether an extended PumpSwap SELL layout has the pool-specific metas required before build.
///
/// Volume tails at ix #21/#22 are intent-user derivable at build time and are not part of this gate.
#[must_use]
pub fn pump_amm_sell_extended_layout_ready(params: PumpAmmSellExtendedReadinessParams) -> bool {
    if !params.sell_requires_extended {
        return true;
    }
    let third_ok = params
        .third_meta
        .filter(|p| *p != Pubkey::default())
        .is_some();
    let pre_fee_ok = if params.sell_requires_pre_fee_metas {
        params
            .sell_pre_fee_meta_1
            .filter(|p| *p != Pubkey::default())
            .is_some()
    } else {
        true
    };
    let needs_fee_tail =
        params.sell_requires_fee_tail || params.fee_tail_0.is_some() || params.fee_tail_1.is_some();
    let fee_ok = if params.sell_requires_pre_fee_metas {
        params
            .fee_tail_0
            .filter(|p| *p != Pubkey::default())
            .is_some()
    } else if needs_fee_tail {
        params
            .fee_tail_0
            .filter(|p| *p != Pubkey::default())
            .is_some()
            && params
                .fee_tail_1
                .filter(|p| *p != Pubkey::default())
                .is_some()
    } else {
        true
    };
    third_ok && pre_fee_ok && fee_ok
}

/// `pool-v2` PDA appended on post-upgrade PumpSwap swaps (`["pool-v2", base_mint]`).
#[must_use]
pub fn pump_amm_pool_v2_pda(base_mint: &Pubkey, program_id: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"pool-v2", base_mint.as_ref()], program_id).0
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PumpAmmSellExtendedFields {
    pub requires_extended: bool,
    /// 27-account extended `sell`: two readonly pre-fee metas at #19/#20 before global FeeConfig/FeeProgram.
    pub requires_pre_fee_metas: bool,
    /// Cashback `sell` (26 accounts): fee-recipient pair at ix #24/#25.
    pub requires_fee_tail: bool,
    pub pre_fee_meta_0: Option<Pubkey>,
    pub pre_fee_meta_1: Option<Pubkey>,
    pub tail_0: Option<Pubkey>,
    pub tail_1: Option<Pubkey>,
    pub third_meta: Option<Pubkey>,
    pub fee_tail_0: Option<Pubkey>,
    pub fee_tail_1: Option<Pubkey>,
}

#[must_use]
pub fn pump_amm_sell_ix_account_len_supported(n: usize) -> bool {
    matches!(
        n,
        21 | PUMPFUN_AMM_SELL_EXTENDED_TOTAL_ACCOUNTS
            | PUMPFUN_AMM_SELL_CASHBACK_TOTAL_ACCOUNTS
            | PUMPFUN_AMM_SELL_EXTENDED_V2_TOTAL_ACCOUNTS
    )
}

#[must_use]
pub fn pump_amm_sell_ix_uses_global_fee_at(n: usize) -> Option<(usize, usize)> {
    match n {
        PUMPFUN_AMM_SELL_EXTENDED_V2_TOTAL_ACCOUNTS => Some((
            PUMPFUN_AMM_SELL_FEE_CONFIG_IX_V2,
            PUMPFUN_AMM_SELL_FEE_PROGRAM_IX_V2,
        )),
        21
        | PUMPFUN_AMM_SELL_EXTENDED_TOTAL_ACCOUNTS
        | PUMPFUN_AMM_SELL_CASHBACK_TOTAL_ACCOUNTS => Some((19, 20)),
        _ => None,
    }
}

/// Map observed `sell` instruction accounts to extended-layout fields (Geyser + RPC history).
pub fn pump_amm_sell_extended_fields_from_ix_accounts(
    instruction_accounts: &[Pubkey],
) -> Option<PumpAmmSellExtendedFields> {
    let n = instruction_accounts.len();
    if !pump_amm_sell_ix_account_len_supported(n) {
        return None;
    }
    let base_mint = *instruction_accounts.get(3)?;
    let program_id = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID).ok()?;
    let pool_v2 = pump_amm_pool_v2_pda(&base_mint, &program_id);

    let global_fee_config = Pubkey::from_str(PUMPFUN_AMM_FEE_CONFIG).ok()?;
    let global_fee_program = Pubkey::from_str(PUMPFUN_AMM_FEE_PROGRAM_ID).ok()?;

    match n {
        21 => Some(PumpAmmSellExtendedFields {
            requires_extended: false,
            requires_pre_fee_metas: false,
            requires_fee_tail: false,
            pre_fee_meta_0: None,
            pre_fee_meta_1: None,
            tail_0: None,
            tail_1: None,
            third_meta: None,
            fee_tail_0: None,
            fee_tail_1: None,
        }),
        PUMPFUN_AMM_SELL_EXTENDED_V2_TOTAL_ACCOUNTS => {
            let fee_at_21 = *instruction_accounts.get(PUMPFUN_AMM_SELL_FEE_CONFIG_IX_V2)?;
            let fee_at_22 = *instruction_accounts.get(PUMPFUN_AMM_SELL_FEE_PROGRAM_IX_V2)?;
            if fee_at_21 != global_fee_config || fee_at_22 != global_fee_program {
                return None;
            }
            Some(PumpAmmSellExtendedFields {
                requires_extended: true,
                requires_pre_fee_metas: true,
                requires_fee_tail: false,
                pre_fee_meta_0: instruction_accounts
                    .get(PUMPFUN_AMM_SELL_PRE_FEE_0_IX_V2)
                    .copied(),
                pre_fee_meta_1: instruction_accounts
                    .get(PUMPFUN_AMM_SELL_PRE_FEE_1_IX_V2)
                    .copied(),
                tail_0: instruction_accounts
                    .get(PUMPFUN_AMM_SELL_EXT_TAIL_0_IX_V2)
                    .copied(),
                tail_1: instruction_accounts
                    .get(PUMPFUN_AMM_SELL_EXT_TAIL_1_IX_V2)
                    .copied(),
                third_meta: instruction_accounts
                    .get(PUMPFUN_AMM_SELL_EXT_THIRD_META_IX_V2)
                    .copied(),
                fee_tail_0: instruction_accounts
                    .get(PUMPFUN_AMM_SELL_FEE_TAIL_0_IX_V2)
                    .copied(),
                fee_tail_1: None,
            })
        }
        PUMPFUN_AMM_SELL_EXTENDED_TOTAL_ACCOUNTS => {
            let at_21 = *instruction_accounts.get(PUMPFUN_AMM_SELL_EXT_TAIL_0_IX)?;
            if at_21 == pool_v2 {
                // Post-upgrade non-cashback: #21 pool-v2, #22/#23 protocol fee recipient pair.
                Some(PumpAmmSellExtendedFields {
                    requires_extended: true,
                    requires_pre_fee_metas: false,
                    requires_fee_tail: false,
                    pre_fee_meta_0: None,
                    pre_fee_meta_1: None,
                    tail_0: Some(at_21),
                    tail_1: instruction_accounts
                        .get(PUMPFUN_AMM_SELL_EXT_TAIL_1_IX)
                        .copied(),
                    third_meta: instruction_accounts
                        .get(PUMPFUN_AMM_SELL_EXT_THIRD_META_IX)
                        .copied(),
                    fee_tail_0: None,
                    fee_tail_1: None,
                })
            } else {
                // Legacy extended cashback: #21/#22 volume metas, #23 third (often pool-v2).
                Some(PumpAmmSellExtendedFields {
                    requires_extended: true,
                    requires_pre_fee_metas: false,
                    requires_fee_tail: false,
                    pre_fee_meta_0: None,
                    pre_fee_meta_1: None,
                    tail_0: instruction_accounts
                        .get(PUMPFUN_AMM_SELL_EXT_TAIL_0_IX)
                        .copied(),
                    tail_1: instruction_accounts
                        .get(PUMPFUN_AMM_SELL_EXT_TAIL_1_IX)
                        .copied(),
                    third_meta: instruction_accounts
                        .get(PUMPFUN_AMM_SELL_EXT_THIRD_META_IX)
                        .copied(),
                    fee_tail_0: None,
                    fee_tail_1: None,
                })
            }
        }
        PUMPFUN_AMM_SELL_CASHBACK_TOTAL_ACCOUNTS => Some(PumpAmmSellExtendedFields {
            requires_extended: true,
            requires_pre_fee_metas: false,
            requires_fee_tail: true,
            pre_fee_meta_0: None,
            pre_fee_meta_1: None,
            tail_0: instruction_accounts
                .get(PUMPFUN_AMM_SELL_EXT_TAIL_0_IX)
                .copied(),
            tail_1: instruction_accounts
                .get(PUMPFUN_AMM_SELL_EXT_TAIL_1_IX)
                .copied(),
            third_meta: instruction_accounts
                .get(PUMPFUN_AMM_SELL_EXT_THIRD_META_IX)
                .copied(),
            fee_tail_0: instruction_accounts
                .get(PUMPFUN_AMM_SELL_FEE_TAIL_0_IX)
                .copied(),
            fee_tail_1: instruction_accounts
                .get(PUMPFUN_AMM_SELL_FEE_TAIL_1_IX)
                .copied(),
        }),
        _ => None,
    }
}

/// Stable, structured reason when local (validator) market-account parsing cannot build `PumpAmmPoolStatic`.
/// Used by `market-data` cold-path logging (Scope 42); not a global canonicalization of fee recipients.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PumpAmmLocalParseFailReason {
    PoolMarketNotFound,
    PoolMarketOwnerMismatch,
    PoolMarketDataTooShort,
    BaseOrQuoteMintMismatch,
    NoBaseVaultTokenAccount,
    NoQuoteVaultTokenAccount,
    ProtocolFeeRecipientUnresolved,
    ProtocolFeeRecipientTokenAccountMissing,
    CreatorVaultMissing,
    FeeConfigMissing,
    GlobalVolumeAccumulatorMissing,
    RpcGetMultipleAccountsFailed,
    /// Cold-path authoritative parse: protocol fee accounts are not in `global_config` at the
    /// fixed offset (would require embedded-market or scan heuristics / tx-history).
    AuthoritativeProtocolFeeUnresolved,
    /// Cold-path authoritative parse: creator vault cannot be derived from market offset 211
    /// (would require embedded-base-TA, authority search, or multi-seed PDA probing).
    AuthoritativeCreatorVaultUnresolved,
    /// Cold-path authoritative parse: multiple fee-program-owned accounts match market-derived
    /// candidates — cannot pick one without heuristics.
    AuthoritativeFeeConfigAmbiguous,
    /// Cold-path authoritative parse: more than one quote-mint token account besides the pool vault
    /// — cannot disambiguate without balance/list ordering heuristics.
    AuthoritativeEmbeddedQuoteTokenAccountsAmbiguous,
    /// Cold-path authoritative parse: more than one base-mint token account besides the pool vault.
    AuthoritativeEmbeddedBaseTokenAccountsAmbiguous,
}

impl std::fmt::Display for PumpAmmLocalParseFailReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::PoolMarketNotFound => "pool_market_not_found",
            Self::PoolMarketOwnerMismatch => "pool_market_owner_mismatch",
            Self::PoolMarketDataTooShort => "pool_market_layout_mismatch",
            Self::BaseOrQuoteMintMismatch => "base_quote_mint_mismatch",
            Self::NoBaseVaultTokenAccount => "no_base_vault_token_account",
            Self::NoQuoteVaultTokenAccount => "no_quote_vault_token_account",
            Self::ProtocolFeeRecipientUnresolved => "protocol_fee_recipient_missing",
            Self::ProtocolFeeRecipientTokenAccountMissing => "protocol_fee_recipient_ta_missing",
            Self::CreatorVaultMissing => "creator_vault_missing",
            Self::FeeConfigMissing => "fee_config_missing",
            Self::GlobalVolumeAccumulatorMissing => "global_volume_accumulator_missing",
            Self::RpcGetMultipleAccountsFailed => "rpc_get_multiple_accounts_failed",
            Self::AuthoritativeProtocolFeeUnresolved => "authoritative_protocol_fee_unresolved",
            Self::AuthoritativeCreatorVaultUnresolved => "authoritative_creator_vault_unresolved",
            Self::AuthoritativeFeeConfigAmbiguous => "authoritative_fee_config_ambiguous",
            Self::AuthoritativeEmbeddedQuoteTokenAccountsAmbiguous => {
                "authoritative_embedded_quote_token_accounts_ambiguous"
            }
            Self::AuthoritativeEmbeddedBaseTokenAccountsAmbiguous => {
                "authoritative_embedded_base_token_accounts_ambiguous"
            }
        };
        f.write_str(s)
    }
}

/// Outcome of parsing a PumpSwap pool market account via RPC (cold path).
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum PumpAmmMarketParseOutcome {
    Ok(PumpAmmPoolStatic),
    LocalFail(PumpAmmLocalParseFailReason),
}

/// How a single field in the 14-account PumpSwap static set was obtained (Scope 44 diagnostics).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PumpAmmFieldResolution {
    /// Fixed constant or canonical PDA (same for all pools / program).
    Deterministic,
    /// Parsed from known market-account byte offsets (layout).
    MarketLayout,
    /// Picked from RPC-resolved candidates (scan / balance ordering / heuristics).
    Heuristic,
    /// Copied from a parsed on-chain swap instruction (tx history).
    TxHistoryObservation,
    /// Read from JetStream / Geyser-supplied `pool_accounts` without re-parse.
    CacheObservation,
}

#[derive(Debug, Clone)]
pub struct PumpAmmFieldDiagnostic {
    pub resolution: PumpAmmFieldResolution,
    /// Short tag for grep (e.g. `global_config_offset_57`, `embedded_second_quote_ta`).
    pub tag: &'static str,
}

/// Provenance for the v14 pool account set and critical fields (cold-path / SIM forensics).
#[derive(Debug, Clone)]
pub struct PumpAmmPoolAccountsDiagnostic {
    /// High-level source: `livepoolcache_ready`, `rpc_market_parse`, `tx_history_local`, …
    pub source: &'static str,
    pub base_mint: Pubkey,
    pub pool_market: Pubkey,
    pub force_refresh: bool,
    /// When set, a successful swap tx that supplied this static set (compare ix accounts to v14).
    pub reference_swap_signature: Option<String>,
    pub protocol_fee_recipient: PumpAmmFieldDiagnostic,
    pub protocol_fee_recipient_ta: PumpAmmFieldDiagnostic,
    pub coin_creator_vault_ata: PumpAmmFieldDiagnostic,
    pub coin_creator_vault_authority: PumpAmmFieldDiagnostic,
    pub global_volume_accumulator: PumpAmmFieldDiagnostic,
    pub fee_config: PumpAmmFieldDiagnostic,
    pub fee_program: PumpAmmFieldDiagnostic,
}

impl PumpAmmPoolAccountsDiagnostic {
    /// Comma-separated v14 pubkeys for copy/paste compare against a mainnet explorer TX.
    pub fn format_v14_csv(accounts: &[Pubkey]) -> String {
        accounts
            .iter()
            .map(|p| p.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// Internal: classify market-parse resolution for Scope 44 diagnostics.
#[derive(Debug, Clone, Copy)]
enum PumpAmmFeeParseKind {
    EmbeddedSecondQuoteTa,
    GlobalConfigOffset57DerivedWsolAta,
    GlobalConfigScanEmbeddedQuoteTa,
    AuthoritySearchQuoteMint,
    AuthoritySearchBaseMint,
}

#[derive(Debug, Clone, Copy)]
enum PumpAmmCreatorParseKind {
    EmbeddedSecondBaseTa,
    MarketOffset211CreatorVaultPda,
    AuthoritySearchBaseMint,
    AuthoritySearchQuoteMint,
    LegacyPdaSeedProbe,
}

#[derive(Debug, Clone, Copy)]
enum PumpAmmGvaParseKind {
    EmbeddedAmmOwnedLargestData,
    PdaSeedProbe,
    SingletonGlobalVolumeAccumulator,
    /// Singleton PDA verified via `getAccount` (owner = AMM program) — cold-path authoritative parse.
    SingletonPdaRpcVerified,
}

#[derive(Debug, Clone, Copy)]
enum PumpAmmFeeConfigParseKind {
    EmbeddedFeeProgramOwnedFromMarket,
    FeeProgramPdaProbe,
    ConstantMainnetFeeConfig,
    /// Exactly one fee-program-owned account among market-derived candidates (RPC owner-verified).
    UniqueVerifiedFeeProgramAccount,
}

#[derive(Debug, Clone)]
pub struct PumpAmmPoolStatic {
    pool_market: Pubkey,
    global_config: Pubkey,
    base_mint: Pubkey,
    quote_mint: Pubkey,
    pool_base_vault: Pubkey,
    pool_quote_vault: Pubkey,
    protocol_fee_recipient: Pubkey,
    protocol_fee_recipient_ta: Pubkey,
    event_authority: Pubkey,
    coin_creator_vault_ata: Pubkey,
    coin_creator_vault_authority: Pubkey,
    global_volume_accumulator: Pubkey,
    fee_config: Pubkey,
    fee_program: Pubkey,
    /// Observed on-chain: this pool's `sell` instruction includes three trailing accounts after the 21 base metas.
    pub sell_requires_cashback_remaining: bool,
    /// Third trailing `sell` meta (ix #23, writable in observed reference); **not** user-derivable.
    pub sell_cashback_third_meta: Option<Pubkey>,
    /// Observed extended SELL ix account #21 (readonly); Scope 61. None = use legacy derived metas if third only.
    pub sell_extended_tail_0: Option<Pubkey>,
    /// Observed extended SELL ix account #22 (readonly); Scope 61.
    pub sell_extended_tail_1: Option<Pubkey>,
    /// Observed cashback `sell` fee-recipient pair (ix #24/#25) when layout has 26 accounts.
    pub sell_extended_fee_tail_0: Option<Pubkey>,
    pub sell_extended_fee_tail_1: Option<Pubkey>,
    /// 27-account extended `sell`: readonly pre-fee metas at ix #19/#20 before global fee pair.
    pub sell_requires_pre_fee_metas: bool,
    /// Second pre-fee meta (ix #20 on 27-account layout); first is usually `global_volume_accumulator` / v14[11].
    pub sell_pre_fee_meta_1: Option<Pubkey>,
    /// Set when this struct was built inside `pumpfun_amm` (parse / tx-history / cache reconstruct).
    pub last_parse_diagnostics: Option<PumpAmmPoolAccountsDiagnostic>,
}

/// Resolved v14 `pool_accounts` plus cold-path provenance (Scope 44).
#[derive(Debug, Clone)]
pub struct PoolAccountsV14WithDiagnostic {
    pub accounts: Vec<Pubkey>,
    pub diagnostic: PumpAmmPoolAccountsDiagnostic,
    /// Propagate to JetStream / SLAVE: pool needs extended `sell` (24 metas).
    pub sell_requires_cashback_remaining: bool,
    pub sell_cashback_third_meta: Option<Pubkey>,
    pub sell_extended_tail_0: Option<Pubkey>,
    pub sell_extended_tail_1: Option<Pubkey>,
    pub sell_extended_fee_tail_0: Option<Pubkey>,
    pub sell_extended_fee_tail_1: Option<Pubkey>,
    pub sell_requires_pre_fee_metas: bool,
    pub sell_pre_fee_meta_1: Option<Pubkey>,
    /// `true` only when the cold-path result is authoritative enough for SELL planning.
    pub sell_layout_ready: bool,
    /// Scope 49: structured outcome when `force_refresh` could not resolve SELL layout (empty if ready).
    pub force_refresh_sell_layout_diag: Option<PumpAmmForceRefreshSellLayoutDiag>,
}

/// Whether the local `getSignaturesForAddress` probe succeeded and what it returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PumpAmmLocalHistoryProbe {
    /// RPC succeeded and returned zero signatures (confirmed empty index page).
    Empty,
    /// RPC succeeded and returned at least one signature.
    NonEmpty,
    /// Observation or probe failed, or state is unknown — do **not** treat as empty history.
    Unknown,
}

impl PumpAmmLocalHistoryProbe {
    pub fn as_log_str(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::NonEmpty => "non_empty",
            Self::Unknown => "unknown",
        }
    }
}

/// Cold-path diagnostics for `force_refresh` SELL-layout resolution (market-data logs / supervisor).
#[derive(Debug, Clone)]
pub struct PumpAmmForceRefreshSellLayoutDiag {
    /// `true` only when the local signature listing RPC **succeeded** and returned zero entries.
    pub local_history_empty: bool,
    /// Local `observe_*` failed (e.g. `getSignaturesForAddress` error) before a layout was found.
    pub local_observation_failed: bool,
    /// Tri-state local index: empty vs non-empty vs unknown/error (for supervisor; prefer over bool alone).
    pub local_history_probe: PumpAmmLocalHistoryProbe,
    pub external_attempted: bool,
    pub external_sig_limit: Option<usize>,
    pub external_max_tx_fetches: Option<usize>,
    pub external_max_get_transaction_calls: Option<u32>,
    pub termination_reason: PumpAmmSellLayoutTerminationReason,
    pub last_external: Option<PumpAmmSellLayoutExternalAttemptSummary>,
}

/// Stable reason codes for force_refresh SELL-layout resolution (logs / ControlResponse forensics).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PumpAmmSellLayoutTerminationReason {
    LayoutFound,
    LocalLayoutResolved,
    ExternalSkippedLocalSigsPresent,
    ExternalSkippedNoFallbackRpc,
    LocalHistoryEmptyExternalTimeout,
    LocalHistoryEmptyExternalRpcError,
    LocalHistoryEmptyExternalHttp429,
    LocalHistoryEmptyExternalRateLimited,
    LocalHistoryEmptyExternalTimeoutBudgetExhausted,
    LocalHistoryEmptyExternalRequestBudgetExhausted,
    LocalHistoryEmptyExternalNoSellCandidates,
    /// Scope 50: bounded external scan saw at least one PumpSwap program ix, but none with `global:sell` discriminator (e.g. only `buy_exact_quote_in` CPIs).
    LocalHistoryEmptyExternalPumpAmmSeenButNoSellDiscriminator,
    /// Scope 50: `sell` discriminator seen but `accounts.len()` is not the supported 21/24 meta layout.
    LocalHistoryEmptyExternalPumpAmmSellAccountShapeUnsupported,
    /// Scope 50: `sell` ix matches supported length but `pump_amm_sell_layout_observation_from_parsed_swap_ix` could not derive layout.
    LocalHistoryEmptyExternalPumpAmmSellLayoutNotDerivable,
    /// Scope 50: decoded `sell` ix for another pool/base mint — not authoritative for this refresh target.
    LocalHistoryEmptyExternalPumpAmmSellPoolOrMintMismatch,
    LocalHistoryEmptyExternalEmptySignatures,
    LocalObservationError,
}

/// One bounded external observation attempt: aggregate counters + last failure/success signal.
#[derive(Debug, Clone)]
pub struct PumpAmmSellLayoutExternalAttemptSummary {
    pub rpc_method_last: &'static str,
    pub elapsed_total_ms: u128,
    pub get_signatures_calls: u32,
    /// `true` only after a successful `getSignaturesForAddress` response (even if zero sigs).
    pub get_signatures_succeeded: bool,
    pub get_transaction_calls: u32,
    pub signatures_limit_last: Option<usize>,
    pub signatures_returned_last: usize,
    pub transactions_fetched: usize,
    pub pump_amm_instructions_seen: usize,
    /// Ix where program is PumpSwap AMM and the Anchor discriminator is `global:sell` (any account length).
    pub pump_amm_sell_discriminator_seen: usize,
    pub sell_candidates_seen: usize,
    pub provider_status_last: PumpAmmSellLayoutProviderStatus,
    pub termination_reason: PumpAmmSellLayoutTerminationReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PumpAmmSellLayoutProviderStatus {
    Ok,
    Empty,
    Timeout,
    Http429,
    Http5xx,
    RpcError,
    RateLimited,
}

impl PumpAmmSellLayoutProviderStatus {
    fn from_error_class(c: ErrorClass) -> Self {
        match c {
            ErrorClass::RateLimited => Self::RateLimited,
            ErrorClass::Timeout => Self::Timeout,
            ErrorClass::Http(429) => Self::Http429,
            ErrorClass::Http(code) if (500..=599).contains(&code) => Self::Http5xx,
            ErrorClass::Http(_) => Self::RpcError,
            ErrorClass::Other => Self::RpcError,
        }
    }

    pub fn as_log_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Empty => "empty",
            Self::Timeout => "timeout",
            Self::Http429 => "http_429",
            Self::Http5xx => "http_5xx",
            Self::RpcError => "rpc_error",
            Self::RateLimited => "rate_limited",
        }
    }
}

impl PumpAmmSellLayoutTerminationReason {
    pub fn as_log_str(self) -> &'static str {
        match self {
            Self::LayoutFound => "layout_found",
            Self::LocalLayoutResolved => "local_layout_resolved",
            Self::ExternalSkippedLocalSigsPresent => "external_skipped_local_sigs_present",
            Self::ExternalSkippedNoFallbackRpc => "external_skipped_no_fallback_rpc",
            Self::LocalHistoryEmptyExternalTimeout => "local_history_empty_external_timeout",
            Self::LocalHistoryEmptyExternalRpcError => "local_history_empty_external_rpc_error",
            Self::LocalHistoryEmptyExternalHttp429 => "local_history_empty_external_http_429",
            Self::LocalHistoryEmptyExternalRateLimited => {
                "local_history_empty_external_rate_limited"
            }
            Self::LocalHistoryEmptyExternalTimeoutBudgetExhausted => {
                "local_history_empty_external_timeout_budget_exhausted"
            }
            Self::LocalHistoryEmptyExternalRequestBudgetExhausted => {
                "local_history_empty_external_request_budget_exhausted"
            }
            Self::LocalHistoryEmptyExternalNoSellCandidates => {
                "local_history_empty_external_no_sell_candidates"
            }
            Self::LocalHistoryEmptyExternalPumpAmmSeenButNoSellDiscriminator => {
                "local_history_empty_external_pump_amm_seen_but_no_sell_discriminator"
            }
            Self::LocalHistoryEmptyExternalPumpAmmSellAccountShapeUnsupported => {
                "local_history_empty_external_pump_amm_sell_account_shape_unsupported"
            }
            Self::LocalHistoryEmptyExternalPumpAmmSellLayoutNotDerivable => {
                "local_history_empty_external_pump_amm_sell_layout_not_derivable"
            }
            Self::LocalHistoryEmptyExternalPumpAmmSellPoolOrMintMismatch => {
                "local_history_empty_external_pump_amm_sell_pool_or_mint_mismatch"
            }
            Self::LocalHistoryEmptyExternalEmptySignatures => {
                "local_history_empty_external_empty_signatures"
            }
            Self::LocalObservationError => "local_observation_error",
        }
    }
}

/// Bounded external SELL-layout history scan (Free-tier friendly): small sig page + hard TX cap.
const FORCE_REFRESH_EXTERNAL_SIG_LIMIT: usize = 40;
const FORCE_REFRESH_EXTERNAL_MAX_TX_ATTEMPTS: usize = 40;
const FORCE_REFRESH_EXTERNAL_MAX_GET_TRANSACTION_CALLS: u32 = 40;
const BOUNDED_EXTERNAL_SELL_LAYOUT_TIMEOUT: Duration = Duration::from_secs(12);

#[derive(Debug, Clone, Copy)]
struct SellLayoutObserveLimits {
    signatures_limit: usize,
    max_tx_fetch_attempts: usize,
    max_get_transaction_calls: u32,
}

struct SellLayoutObserveOutcome {
    result: Result<PumpAmmSellReferenceObservation>,
    summary: PumpAmmSellLayoutExternalAttemptSummary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
enum PumpAmmAuthoritativeSellLayout {
    Unknown,
    Base,
    /// Extended `sell`: trailing metas #21/#22/#23 (26-account) or shifted #23–#26 (27-account);
    /// optional pre-fee pair at #19/#20 for 27-account layouts.
    Extended {
        pre_fee_0: Option<Pubkey>,
        pre_fee_1: Option<Pubkey>,
        tail_0: Pubkey,
        tail_1: Pubkey,
        tail_2: Pubkey,
        fee_tail_0: Option<Pubkey>,
        fee_tail_1: Option<Pubkey>,
    },
}

/// Successful same-pool SELL reference from TX-history during `force_refresh` (Scope 60).
/// Carries authoritative protocol fee metas from observed `sell` ix #9/#10 for v14 `[6]`/`[7]`.
#[derive(Debug, Clone)]
struct PumpAmmSellReferenceObservation {
    layout: PumpAmmAuthoritativeSellLayout,
    protocol_fee_recipient: Pubkey,
    protocol_fee_recipient_ta: Pubkey,
    reference_swap_signature: Option<String>,
}

impl PumpAmmSellReferenceObservation {
    fn unknown() -> Self {
        Self {
            layout: PumpAmmAuthoritativeSellLayout::Unknown,
            protocol_fee_recipient: Pubkey::default(),
            protocol_fee_recipient_ta: Pubkey::default(),
            reference_swap_signature: None,
        }
    }
}

fn local_history_probe_from_sell_layout_summary(
    s: &PumpAmmSellLayoutExternalAttemptSummary,
) -> PumpAmmLocalHistoryProbe {
    if s.get_signatures_succeeded && s.signatures_returned_last == 0 {
        PumpAmmLocalHistoryProbe::Empty
    } else if s.get_signatures_succeeded && s.signatures_returned_last > 0 {
        PumpAmmLocalHistoryProbe::NonEmpty
    } else {
        PumpAmmLocalHistoryProbe::Unknown
    }
}

fn force_refresh_no_fallback_termination_reason(
    local_observation_failed: bool,
) -> PumpAmmSellLayoutTerminationReason {
    if local_observation_failed {
        PumpAmmSellLayoutTerminationReason::LocalObservationError
    } else {
        PumpAmmSellLayoutTerminationReason::ExternalSkippedNoFallbackRpc
    }
}

/// Top-level `termination_reason` after an external SELL-layout attempt (must not be replaced by local observation failure).
fn force_refresh_external_termination_reason_after_attempt(
    layout: PumpAmmAuthoritativeSellLayout,
    timed_out: bool,
    summary: Option<&PumpAmmSellLayoutExternalAttemptSummary>,
) -> PumpAmmSellLayoutTerminationReason {
    if layout != PumpAmmAuthoritativeSellLayout::Unknown {
        PumpAmmSellLayoutTerminationReason::LayoutFound
    } else if timed_out {
        PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalTimeoutBudgetExhausted
    } else if let Some(s) = summary {
        s.termination_reason
    } else {
        PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalRpcError
    }
}

/// After a successful `getTransaction` in a bounded SELL-layout scan, default back to "still searching"
/// unless we already recorded a **decode-level** signal from a prior signature in the same scan.
fn sell_layout_scan_termination_after_successful_tx_fetch(
    prev: PumpAmmSellLayoutTerminationReason,
) -> PumpAmmSellLayoutTerminationReason {
    match prev {
        PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalPumpAmmSellAccountShapeUnsupported
        | PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalPumpAmmSellLayoutNotDerivable
        | PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalPumpAmmSellPoolOrMintMismatch => {
            prev
        }
        _ => PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalNoSellCandidates,
    }
}

/// Rank authoritative SELL layouts for TX-history merge (higher = stronger evidence).
///
/// P184e: a newer **26-account** `sell` must not terminate discovery before an older **27-account**
/// reference is considered; strength ordering prefers 27 > 26 > 24 > 21 (Base).
#[must_use]
fn authoritative_sell_layout_strength(layout: &PumpAmmAuthoritativeSellLayout) -> u8 {
    match layout {
        PumpAmmAuthoritativeSellLayout::Unknown => 0,
        PumpAmmAuthoritativeSellLayout::Base => 1,
        PumpAmmAuthoritativeSellLayout::Extended {
            pre_fee_0,
            pre_fee_1,
            fee_tail_0,
            fee_tail_1,
            ..
        } => {
            let has_pre_fee = pre_fee_0.filter(|p| *p != Pubkey::default()).is_some()
                && pre_fee_1.filter(|p| *p != Pubkey::default()).is_some();
            if has_pre_fee {
                4
            } else if fee_tail_0.filter(|p| *p != Pubkey::default()).is_some()
                && fee_tail_1.filter(|p| *p != Pubkey::default()).is_some()
            {
                3
            } else {
                2
            }
        }
    }
}

/// True when the scan can stop early — 27-account extended `sell` with both pre-fee metas.
#[must_use]
fn authoritative_sell_layout_is_terminal_for_scan(layout: &PumpAmmAuthoritativeSellLayout) -> bool {
    authoritative_sell_layout_strength(layout) >= 4
}

/// Merge SELL-layout observations while scanning **reverse-chronological** signatures (newest first).
///
/// Prefer the **strongest** layout (27 > 26 > 24 > Base). On equal strength, keep `best` (newer
/// evidence in newest-first scan). Fee recipient fields follow the winning layout (Scope 60).
fn merge_pump_amm_authoritative_sell_reference_observation(
    best: PumpAmmSellReferenceObservation,
    candidate: PumpAmmSellReferenceObservation,
) -> PumpAmmSellReferenceObservation {
    if matches!(best.layout, PumpAmmAuthoritativeSellLayout::Unknown) {
        return candidate;
    }
    if matches!(candidate.layout, PumpAmmAuthoritativeSellLayout::Unknown) {
        return best;
    }
    let best_strength = authoritative_sell_layout_strength(&best.layout);
    let cand_strength = authoritative_sell_layout_strength(&candidate.layout);
    if cand_strength > best_strength {
        candidate
    } else {
        best
    }
}

/// Same fold as `observe_authoritative_sell_layout_from_tx_history_with_rpc` when it sees
/// matching pool/mint `sell` layouts in **newest-first** order (one observation per outer step).
///
/// Used by unit tests to lock the scan conflict without RPC; production calls this from the scan loop.
fn fold_force_refresh_sell_reference_observations_newest_first(
    mut best: PumpAmmSellReferenceObservation,
    observations_newest_first: impl IntoIterator<Item = PumpAmmSellReferenceObservation>,
) -> PumpAmmSellReferenceObservation {
    for obs in observations_newest_first {
        best = merge_pump_amm_authoritative_sell_reference_observation(best, obs);
    }
    best
}

/// Cold-path reference `sell` txs known to use the 27-account extended layout (P184e).
fn pump_amm_known_v2_sell_reference_signatures(pool_market: &Pubkey) -> &'static [&'static str] {
    const STUCK_POOL: &str = "GrgDaBg4TGBQCDZk9HHw8JT24RnoDHtQnvgguKxGKStb";
    const STUCK_POOL_V2_REF: &str =
        "3XPKr7ynZzRSwvwiVvWpDr58pFCUVUqwySBiUwPMqwUTDGETFWaZXpYWm84DnAuSet4rQRmcwUwsfZ8Vg8gJqeae";
    if pool_market.to_string() == STUCK_POOL {
        &[STUCK_POOL_V2_REF]
    } else {
        &[]
    }
}

fn provider_status_from_anyhow_rpc(err: &anyhow::Error) -> PumpAmmSellLayoutProviderStatus {
    for cause in err.chain() {
        if let Some(ce) = cause.downcast_ref::<ClientError>() {
            return PumpAmmSellLayoutProviderStatus::from_error_class(
                SolanaRpc::classify_client_error(ce),
            );
        }
    }
    let s = format!("{err:#}").to_lowercase();
    if s.contains("timeout") || s.contains("timed out") || s.contains("deadline") {
        return PumpAmmSellLayoutProviderStatus::Timeout;
    }
    if s.contains("too many requests") || s.contains("rate limit") || s.contains("throttl") {
        return PumpAmmSellLayoutProviderStatus::RateLimited;
    }
    if s.contains("429") {
        return PumpAmmSellLayoutProviderStatus::Http429;
    }
    PumpAmmSellLayoutProviderStatus::RpcError
}

impl PumpAmmPoolStatic {
    /// V1 ordering for JetStream / `build_swap_ix_from_pool_accounts` (14 accounts).
    pub fn as_pool_accounts_v14(&self) -> Vec<Pubkey> {
        vec![
            self.pool_market,
            self.global_config,
            self.base_mint,
            self.quote_mint,
            self.pool_base_vault,
            self.pool_quote_vault,
            self.protocol_fee_recipient,
            self.protocol_fee_recipient_ta,
            self.event_authority,
            self.coin_creator_vault_ata,
            self.coin_creator_vault_authority,
            self.global_volume_accumulator,
            self.fee_config,
            self.fee_program,
        ]
    }
}

#[derive(Debug, Clone)]
struct PumpAmmUserAccounts {
    user_base_ta: Pubkey,
    user_quote_ta: Pubkey,
    user_volume_accumulator: Pubkey,
}

#[derive(Debug, Clone)]
struct TokenAccountMeta {
    address: Pubkey,
    mint: Pubkey,
    token_owner: Pubkey,
    balance: u64,
}

#[derive(Debug, Clone)]
struct ProgramOwnedAccountMeta {
    address: Pubkey,
    data_len: usize,
}

#[derive(Clone)]
pub struct PumpFunAmmDex {
    rpc: Arc<SolanaRpc>,
    user_authority: Option<Pubkey>,

    // Prevent concurrent pool discovery storms (e.g. parallel exits) from hammering RPC.
    discovery_lock: Arc<Mutex<()>>,

    // Cache by base mint (WSOL quote only for now)
    pools_by_base: DashMap<Pubkey, PumpAmmPoolStatic>,
    // Index by pool_market address (for load_pool_by_address)
    pools_by_market: DashMap<Pubkey, Pubkey>, // pool_market -> base_mint
    user_accounts: DashMap<(Pubkey, Pubkey), PumpAmmUserAccounts>, // (pool_market, user)
    // Extra cached data (e.g., token_program:<mint> → program_id)
    cached_data: DashMap<String, String>,

    /// Optional reference to the Geyser-fed LivePoolCache.
    /// When present, quote_exact_in() reads reserves from cache instead of RPC,
    /// and discover_pool_static() checks for cached pool_accounts first.
    /// This is the primary mechanism for eliminating RPC calls from the hot path.
    live_pool_cache: Option<Arc<LivePoolCache>>,

    /// When LivePoolCache is set and cache miss: if false, return None (Hot Path, no RPC).
    /// If true, fall back to RPC discovery (Cold Path, e.g. Liquidation). P3 #12.
    allow_rpc_on_miss: bool,

    /// Optional RPC with full tx index (e.g. Helius). **Only** used for bounded `getSignaturesForAddress`
    /// + `getTransaction` after local validator parse/history failed — set by `market-data` Cold Path only.
    bounded_tx_fallback_rpc: Option<Arc<SolanaRpc>>,
}

impl PumpFunAmmDex {
    pub fn new(rpc: Arc<SolanaRpc>) -> Self {
        Self {
            rpc,
            user_authority: None,
            discovery_lock: Arc::new(Mutex::new(())),
            pools_by_base: DashMap::new(),
            pools_by_market: DashMap::new(),
            user_accounts: DashMap::new(),
            cached_data: DashMap::new(),
            live_pool_cache: None,
            allow_rpc_on_miss: true, // No cache: always RPC (Cold Path only)
            bounded_tx_fallback_rpc: None,
        }
    }

    /// Create a new PumpFunAmmDex with a LivePoolCache reference for Geyser-first quoting.
    /// When the cache is provided, quote_exact_in() reads reserves from cache instead of RPC.
    /// `allow_rpc_on_miss`: false = Hot Path (Cache miss → None), true = Cold Path (Cache miss → RPC fallback). P3 #12.
    pub fn new_with_cache(
        rpc: Arc<SolanaRpc>,
        live_pool_cache: Arc<LivePoolCache>,
        allow_rpc_on_miss: bool,
    ) -> Self {
        let mut dex = Self::new(rpc);
        dex.live_pool_cache = Some(live_pool_cache);
        dex.allow_rpc_on_miss = allow_rpc_on_miss;
        dex
    }

    /// Cold path only (`market-data`): bounded TX-history fallback when local validator lacks index.
    pub fn set_bounded_tx_fallback_rpc(&mut self, rpc: Option<Arc<SolanaRpc>>) {
        self.bounded_tx_fallback_rpc = rpc;
    }

    /// Bounded external `getProgramAccounts` for PumpSwap markets (Helius) when local validator
    /// returns error or zero markets for the base_mint + WSOL memcmp filters. Cold path only.
    async fn try_bounded_external_pool_markets_via_program_accounts(
        &self,
        base_mint: Pubkey,
        local_failure_stage: &'static str,
    ) -> Result<Option<Vec<Pubkey>>> {
        const BOUNDED_EXTERNAL_GPA_TIMEOUT: Duration = Duration::from_secs(15);
        let Some(ref h_rpc) = self.bounded_tx_fallback_rpc else {
            info!(
                base_mint = %base_mint,
                local_failure_stage,
                "pump_amm: helius_unconfigured — bounded external getProgramAccounts (pool markets) skipped"
            );
            return Ok(None);
        };

        info!(
            base_mint = %base_mint,
            local_failure_stage,
            timeout_secs = BOUNDED_EXTERNAL_GPA_TIMEOUT.as_secs(),
            "pump_amm: bounded external pool-market discovery starting (Helius getProgramAccounts)"
        );

        match tokio::time::timeout(
            BOUNDED_EXTERNAL_GPA_TIMEOUT,
            Self::discover_pool_markets_via_program_accounts_with_rpc(h_rpc.as_ref(), base_mint),
        )
        .await
        {
            Ok(Ok(v)) if !v.is_empty() => {
                info!(
                    base_mint = %base_mint,
                    market_count = v.len(),
                    local_failure_stage,
                    "pump_amm: bounded external getProgramAccounts SUCCESS (pool market addresses)"
                );
                Ok(Some(v))
            }
            Ok(Ok(_)) => {
                warn!(
                    base_mint = %base_mint,
                    local_failure_stage,
                    "pump_amm: bounded external getProgramAccounts returned 0 markets (helius_miss)"
                );
                Ok(None)
            }
            Ok(Err(e)) => {
                warn!(
                    base_mint = %base_mint,
                    local_failure_stage,
                    error = %e,
                    "pump_amm: bounded external getProgramAccounts error (helius_failed)"
                );
                Ok(None)
            }
            Err(_) => {
                warn!(
                    base_mint = %base_mint,
                    local_failure_stage,
                    timeout_secs = BOUNDED_EXTERNAL_GPA_TIMEOUT.as_secs(),
                    "pump_amm: bounded external getProgramAccounts timed out (helius_failed)"
                );
                Ok(None)
            }
        }
    }

    /// After local market parse fails or local tx-history is empty: one bounded `getSignaturesForAddress`
    /// + `getTransaction` scan via optional full-index RPC (Helius). Never used without empty local sig list.
    async fn try_bounded_external_tx_history_pool(
        &self,
        pool_market: Pubkey,
        base_mint: Pubkey,
        local_parse_fail_reason: Option<PumpAmmLocalParseFailReason>,
        log_ctx: &'static str,
        force_refresh: bool,
    ) -> Result<Option<PumpAmmPoolStatic>> {
        const BOUNDED_EXTERNAL_TX_TIMEOUT: Duration = Duration::from_secs(12);
        let Some(ref h_rpc) = self.bounded_tx_fallback_rpc else {
            info!(
                pool = %pool_market,
                log_ctx,
                "pump_amm: helius_unconfigured — bounded external TX-history skipped"
            );
            return Ok(None);
        };

        let local_sigs_empty = match self
            .rpc
            .get_signatures_for_address(&pool_market, Some(1))
            .await
        {
            Ok(v) => v.is_empty(),
            Err(e) => {
                warn!(
                    pool = %pool_market,
                    error = %e,
                    log_ctx,
                    "pump_amm: local getSignaturesForAddress failed; treating as empty local tx index"
                );
                true
            }
        };
        if !local_sigs_empty {
            debug!(
                pool = %pool_market,
                log_ctx,
                "pump_amm: local validator has tx signatures — skipping bounded external TX-history"
            );
            return Ok(None);
        }

        info!(
            pool = %pool_market,
            base_mint = %base_mint,
            local_parse_fail_reason = ?local_parse_fail_reason.map(|r| r.to_string()),
            log_ctx,
            "pump_amm: bounded external TX-history fallback starting (Helius / full index)"
        );

        match tokio::time::timeout(
            BOUNDED_EXTERNAL_TX_TIMEOUT,
            self.discover_pool_static_via_tx_history_with_rpc(
                Arc::clone(h_rpc),
                pool_market,
                base_mint,
                force_refresh,
            ),
        )
        .await
        {
            Ok(Ok(Some(pool))) => {
                info!(
                    pool = %pool_market,
                    log_ctx,
                    "pump_amm: bounded external TX-history fallback SUCCESS"
                );
                Ok(Some(pool))
            }
            Ok(Ok(None)) => {
                warn!(
                    pool = %pool_market,
                    log_ctx,
                    "pump_amm: bounded external TX-history returned no pool"
                );
                Ok(None)
            }
            Ok(Err(e)) => {
                warn!(
                    pool = %pool_market,
                    error = %e,
                    log_ctx,
                    "pump_amm: bounded external TX-history error (helius_failed)"
                );
                Ok(None)
            }
            Err(_) => {
                warn!(
                    pool = %pool_market,
                    timeout_secs = BOUNDED_EXTERNAL_TX_TIMEOUT.as_secs(),
                    log_ctx,
                    "pump_amm: bounded external TX-history timed out (helius_failed)"
                );
                Ok(None)
            }
        }
    }

    pub fn set_user_authority(&mut self, user: Pubkey) {
        self.user_authority = Some(user);
    }

    /// Cold path: cache a fully resolved `PumpAmmPoolStatic` after external discovery (e.g. Helius TX-history).
    pub fn insert_pool_static_cache(&self, pool: PumpAmmPoolStatic) {
        let base_mint = pool.base_mint;
        let pool_market = pool.pool_market;
        self.pools_by_base.insert(base_mint, pool);
        self.pools_by_market.insert(pool_market, base_mint);
    }

    /// Fetch transaction via SolanaRpc and return as Value for legacy JSON parsers.
    async fn fetch_tx_as_value(&self, sig: &str) -> Result<Value> {
        Self::fetch_tx_as_value_with_rpc(self.rpc.as_ref(), sig).await
    }

    async fn fetch_tx_as_value_with_rpc(rpc: &SolanaRpc, sig: &str) -> Result<Value> {
        let sig_parsed = sig.parse::<Signature>().context("invalid signature")?;
        let cfg = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::JsonParsed),
            max_supported_transaction_version: Some(0),
            commitment: None,
        };
        let tx = rpc
            .get_transaction_with_config_retry(&sig_parsed, cfg)
            .await
            .context("getTransaction failed")?;
        let tx_val = serde_json::to_value(&tx).context("serialize transaction")?;
        Ok(json!({"result": tx_val}))
    }

    async fn rpc_account_owner_executable_for(
        rpc: &SolanaRpc,
        address: Pubkey,
    ) -> Result<Option<(Pubkey, bool)>> {
        let acc = match rpc.get_account_opt_retry(&address).await {
            Ok(Some(a)) => a,
            Ok(None) => return Ok(None),
            Err(e) => return Err(anyhow!("get_account failed: {e}")),
        };
        Ok(Some((acc.owner, acc.executable)))
    }

    /// Return the deterministic v1 pool-accounts list for a Pump.fun AMM pool.
    ///
    /// Ordering matches `MarketEventKind::DexPoolAccounts` (PumpSwap v1) and
    /// `PumpFunAmmDex::build_swap_ix_from_pool_accounts`.
    ///
    /// Returns 14 accounts:
    /// [0] pool_market, [1] global_config, [2] base_mint, [3] quote_mint,
    /// [4] pool_base_vault, [5] pool_quote_vault, [6] protocol_fee_recipient,
    /// [7] protocol_fee_recipient_ta, [8] event_authority, [9] coin_creator_vault_ata,
    /// [10] coin_creator_vault_authority, [11] global_volume_accumulator,
    /// [12] fee_config, [13] fee_program
    /// Optional pool_address hint for fast-path discovery (single getAccount vs slow getProgramAccounts).
    /// Used by I-24d EnsurePumpAmmPoolAccounts when execution-engine knows pool from cache/position.
    ///
    /// `force_refresh`: Cold-path recovery — skip LivePoolCache `pool_accounts` and in-memory
    /// `pools_by_base`, then re-parse the market account via RPC (fixes stale creator-vault / 14er set).
    ///
    /// Stable API: returns only the v14 `Vec<Pubkey>` (Eval / downstream compatibility). For Scope 44
    /// provenance, use [`Self::pool_accounts_v1_for_base_mint_with_hint_diagnostic`].
    pub async fn pool_accounts_v1_for_base_mint_with_hint(
        &self,
        base_mint: Pubkey,
        pool_address_hint: Option<Pubkey>,
        force_refresh: bool,
    ) -> Result<Option<Vec<Pubkey>>> {
        Ok(self
            .pool_accounts_v1_for_base_mint_with_hint_diagnostic(
                base_mint,
                pool_address_hint,
                force_refresh,
            )
            .await?
            .map(|w| w.accounts))
    }

    /// Same resolution as [`Self::pool_accounts_v1_for_base_mint_with_hint`], plus Scope 44 diagnostic
    /// metadata (cold path / market-data only — not required for generic callers).
    pub async fn pool_accounts_v1_for_base_mint_with_hint_diagnostic(
        &self,
        base_mint: Pubkey,
        pool_address_hint: Option<Pubkey>,
        force_refresh: bool,
    ) -> Result<Option<PoolAccountsV14WithDiagnostic>> {
        if force_refresh {
            if let Some((_, pool)) = self.pools_by_base.remove(&base_mint) {
                self.pools_by_market.remove(&pool.pool_market);
            }
            warn!(
                base_mint = %base_mint,
                "pump_amm: force_refresh — skipping LivePoolCache pool_accounts; authoritative RPC parse (Cold Path recovery)"
            );
        }

        // GEYSER-FIRST: Check LivePoolCache for pre-cached pool_accounts before RPC discovery.
        // These come from DexPoolAccounts events (parsed from verified on-chain swap txs)
        // and are more reliable than the heuristic-based RPC discovery.
        if !force_refresh {
            if let Some(ref cache) = self.live_pool_cache {
                if let Some(accounts) =
                    cache.get_ready_pump_amm_pool_accounts_by_base_mint(&base_mint)
                {
                    if accounts.len() >= 14 {
                        debug!(
                            base_mint = %base_mint,
                            accounts_len = accounts.len(),
                            "pump_amm: pool_accounts from LivePoolCache (Ready, ZERO RPC)"
                        );
                        let pool_market = accounts[0];
                        let (sell_requires, sell_third, sell_t0, sell_t1) =
                            cache.pump_amm_sell_extended_layout(&pool_market);
                        let (sell_fee_t0, sell_fee_t1) =
                            cache.pump_amm_sell_fee_tail_layout(&pool_market);
                        return Ok(Some(PoolAccountsV14WithDiagnostic {
                            accounts,
                            diagnostic: Self::pump_amm_livepoolcache_diagnostic(
                                "livepoolcache_ready",
                                force_refresh,
                                pool_market,
                                base_mint,
                            ),
                            sell_requires_cashback_remaining: sell_requires,
                            sell_cashback_third_meta: sell_third,
                            sell_extended_tail_0: sell_t0,
                            sell_extended_tail_1: sell_t1,
                            sell_extended_fee_tail_0: sell_fee_t0,
                            sell_extended_fee_tail_1: sell_fee_t1,
                            sell_requires_pre_fee_metas: cache
                                .pump_amm_sell_requires_pre_fee_metas(&pool_market),
                            sell_pre_fee_meta_1: cache.pump_amm_sell_pre_fee_meta_1(&pool_market),
                            sell_layout_ready: true,
                            force_refresh_sell_layout_diag: None,
                        }));
                    }
                }
                // Cache miss: Hot Path (allow_rpc_on_miss=false) → None. Cold Path (true) → RPC fallback. P3 #12.
                if !self.allow_rpc_on_miss {
                    debug!(base_mint = %base_mint, "pump_amm: pool_accounts cache miss, returning None (no RPC)");
                    return Ok(None);
                }
            }
        } else if !self.allow_rpc_on_miss {
            // force_refresh requires RPC; refuse when this dex instance is Hot-Path-only.
            debug!(
                base_mint = %base_mint,
                "pump_amm: force_refresh but allow_rpc_on_miss=false — cannot RPC refresh"
            );
            return Ok(None);
        }

        // Resolve pool market for fast path: explicit hint, else (recovery only) pool address from cache.
        let effective_pool_hint = pool_address_hint.or_else(|| {
            if force_refresh {
                self.live_pool_cache
                    .as_ref()
                    .and_then(|c| c.get_pump_amm_pool_address_by_base_mint(&base_mint))
            } else {
                None
            }
        });

        // I-24d FAST PATH: When pool_address_hint provided, try single getAccount first.
        // Avoids slow getProgramAccounts scan that routinely exceeds cold-path discovery timeout.
        if let Some(pool_market) = effective_pool_hint {
            info!(
                base_mint = %base_mint,
                pool = %pool_market,
                "pump_amm: pool_address hint provided, trying direct getAccount (fast path)"
            );
            match self
                .try_market_parse_outcome_for_pool(
                    pool_market,
                    base_mint,
                    Some(("pool_address_hint_getaccount", force_refresh)),
                )
                .await?
            {
                PumpAmmMarketParseOutcome::Ok(pool) => {
                    self.pools_by_base.insert(base_mint, pool.clone());
                    self.pools_by_market.insert(pool.pool_market, base_mint);
                    info!(
                        base_mint = %base_mint,
                        pool_market = %pool.pool_market,
                        "pump_amm: PumpAmmPoolStatic from pool_address hint (fast path)"
                    );
                    let diagnostic = pool.last_parse_diagnostics.clone().unwrap_or_else(|| {
                        Self::pump_amm_livepoolcache_diagnostic(
                            "pool_address_hint_fast_path_missing_diag",
                            force_refresh,
                            pool.pool_market,
                            base_mint,
                        )
                    });
                    return Ok(Some(
                        self.wrap_pool_accounts_v14_with_diagnostic(
                            base_mint,
                            pool,
                            force_refresh,
                            diagnostic,
                        )
                        .await?,
                    ));
                }
                PumpAmmMarketParseOutcome::LocalFail(reason) => {
                    warn!(
                        base_mint = %base_mint,
                        pool = %pool_market,
                        local_parse_fail_reason = %reason,
                        "pump_amm: pool_address hint local market parse failed (structured reason)"
                    );
                    if let Some(pool) = self
                        .try_bounded_external_tx_history_pool(
                            pool_market,
                            base_mint,
                            Some(reason),
                            "pool_address_hint",
                            force_refresh,
                        )
                        .await?
                    {
                        self.insert_pool_static_cache(pool.clone());
                        let diagnostic = pool.last_parse_diagnostics.clone().unwrap_or_else(|| {
                            Self::pump_amm_tx_history_diagnostic(
                                "bounded_external_tx_history_pool_address_hint",
                                force_refresh,
                                pool.pool_market,
                                base_mint,
                                None,
                            )
                        });
                        return Ok(Some(
                            self.wrap_pool_accounts_v14_with_diagnostic(
                                base_mint,
                                pool,
                                force_refresh,
                                diagnostic,
                            )
                            .await?,
                        ));
                    }
                    return Err(anyhow!(
                        "pump_amm: pool_address hint parse failed local_parse={} (base_mint={}, pool={}); refusing unbounded getProgramAccounts (I-24d)",
                        reason,
                        base_mint,
                        pool_market
                    ));
                }
            }
        }

        // RPC FALLBACK (Cold Path only): No LivePoolCache or allow_rpc_on_miss — discover pool via RPC heuristics.
        let pool = match self.discover_pool_static(base_mint, force_refresh).await? {
            Some(p) => p,
            None => return Ok(None),
        };

        // CRITICAL: global_volume_accumulator is required for BUY (BuyExactQuoteIn).
        // The PumpSwap program validates it exists and is initialized.
        // Without it: "AccountNotInitialized" error (Custom(3012)).
        let diagnostic = pool.last_parse_diagnostics.clone().unwrap_or_else(|| {
            Self::pump_amm_livepoolcache_diagnostic(
                "discover_pool_static_missing_diag",
                force_refresh,
                pool.pool_market,
                base_mint,
            )
        });
        Ok(Some(
            self.wrap_pool_accounts_v14_with_diagnostic(base_mint, pool, force_refresh, diagnostic)
                .await?,
        ))
    }

    /// Convenience wrapper: pool_accounts_v1_for_base_mint without hint.
    pub async fn pool_accounts_v1_for_base_mint(
        &self,
        base_mint: Pubkey,
    ) -> Result<Option<Vec<Pubkey>>> {
        self.pool_accounts_v1_for_base_mint_with_hint(base_mint, None, false)
            .await
    }

    async fn wrap_pool_accounts_v14_with_diagnostic(
        &self,
        base_mint: Pubkey,
        mut pool: PumpAmmPoolStatic,
        force_refresh: bool,
        diagnostic: PumpAmmPoolAccountsDiagnostic,
    ) -> Result<PoolAccountsV14WithDiagnostic> {
        let (sell_layout_ready, force_refresh_sell_layout_diag) = if force_refresh {
            let (sell_obs, diag) = self
                .resolve_authoritative_sell_layout_for_force_refresh(&pool, base_mint)
                .await?;
            Self::apply_sell_reference_protocol_fee_recipients_for_force_refresh(
                &mut pool, &sell_obs,
            );
            match sell_obs.layout {
                PumpAmmAuthoritativeSellLayout::Unknown => (false, diag),
                PumpAmmAuthoritativeSellLayout::Base => {
                    pool.sell_requires_cashback_remaining = false;
                    pool.sell_cashback_third_meta = None;
                    pool.sell_extended_tail_0 = None;
                    pool.sell_extended_tail_1 = None;
                    pool.sell_extended_fee_tail_0 = None;
                    pool.sell_extended_fee_tail_1 = None;
                    pool.sell_requires_pre_fee_metas = false;
                    pool.sell_pre_fee_meta_1 = None;
                    (true, None)
                }
                PumpAmmAuthoritativeSellLayout::Extended {
                    pre_fee_0,
                    pre_fee_1,
                    tail_0,
                    tail_1,
                    tail_2,
                    fee_tail_0,
                    fee_tail_1,
                } => {
                    pool.sell_requires_cashback_remaining = true;
                    pool.sell_requires_pre_fee_metas =
                        pre_fee_0.filter(|p| *p != Pubkey::default()).is_some()
                            && pre_fee_1.filter(|p| *p != Pubkey::default()).is_some();
                    if let Some(p0) = pre_fee_0.filter(|p| *p != Pubkey::default()) {
                        pool.global_volume_accumulator = p0;
                    }
                    pool.sell_pre_fee_meta_1 = pre_fee_1.filter(|p| *p != Pubkey::default());
                    pool.sell_extended_tail_0 = Some(tail_0);
                    pool.sell_extended_tail_1 = Some(tail_1);
                    pool.sell_cashback_third_meta = Some(tail_2);
                    pool.sell_extended_fee_tail_0 = fee_tail_0.filter(|p| *p != Pubkey::default());
                    pool.sell_extended_fee_tail_1 = fee_tail_1.filter(|p| *p != Pubkey::default());
                    (true, None)
                }
            }
        } else {
            (true, None)
        };

        Ok(PoolAccountsV14WithDiagnostic {
            accounts: pool.as_pool_accounts_v14(),
            diagnostic,
            sell_requires_cashback_remaining: pool.sell_requires_cashback_remaining,
            sell_cashback_third_meta: pool.sell_cashback_third_meta,
            sell_extended_tail_0: pool.sell_extended_tail_0,
            sell_extended_tail_1: pool.sell_extended_tail_1,
            sell_extended_fee_tail_0: pool.sell_extended_fee_tail_0,
            sell_extended_fee_tail_1: pool.sell_extended_fee_tail_1,
            sell_requires_pre_fee_metas: pool.sell_requires_pre_fee_metas,
            sell_pre_fee_meta_1: pool.sell_pre_fee_meta_1,
            sell_layout_ready,
            force_refresh_sell_layout_diag,
        })
    }

    /// Scope 60: same-pool successful `sell` reference ix carries authoritative protocol fee metas (#9/#10).
    /// Apply only when observation came from TX decode (signature present), not from stale cache-only hints.
    fn apply_sell_reference_protocol_fee_recipients_for_force_refresh(
        pool: &mut PumpAmmPoolStatic,
        obs: &PumpAmmSellReferenceObservation,
    ) {
        if obs.layout == PumpAmmAuthoritativeSellLayout::Unknown {
            return;
        }
        let Some(ref sig) = obs.reference_swap_signature else {
            return;
        };
        if sig.is_empty() {
            return;
        }
        pool.protocol_fee_recipient = obs.protocol_fee_recipient;
        pool.protocol_fee_recipient_ta = obs.protocol_fee_recipient_ta;
    }

    /// Cold-path only: when TX-history scan found a weaker extended layout, try curated 27-account refs.
    async fn upgrade_sell_layout_observation_with_known_v2_reference(
        &self,
        tx_rpc: Arc<SolanaRpc>,
        pool_market: Pubkey,
        base_mint: Pubkey,
        mut obs: PumpAmmSellReferenceObservation,
    ) -> PumpAmmSellReferenceObservation {
        if authoritative_sell_layout_is_terminal_for_scan(&obs.layout) {
            return obs;
        }
        let baseline_strength = authoritative_sell_layout_strength(&obs.layout);
        for sig in pump_amm_known_v2_sell_reference_signatures(&pool_market) {
            if let Some(ref_obs) = self
                .try_observe_sell_layout_from_reference_signature(
                    tx_rpc.as_ref(),
                    pool_market,
                    base_mint,
                    sig,
                )
                .await
            {
                obs = merge_pump_amm_authoritative_sell_reference_observation(obs, ref_obs);
                if authoritative_sell_layout_strength(&obs.layout) > baseline_strength {
                    info!(
                        pool = %pool_market,
                        base_mint = %base_mint,
                        reference_swap_signature = %obs.reference_swap_signature.as_deref().unwrap_or(""),
                        sell_ix_account_count = PUMPFUN_AMM_SELL_EXTENDED_V2_TOTAL_ACCOUNTS,
                        layout = ?obs.layout,
                        "pump_amm: upgraded force_refresh SELL layout from known 27-account reference tx"
                    );
                    return obs;
                }
            }
        }
        obs
    }

    /// Fetch and decode a single known-good 27-account `sell` reference (cold path / discovery only).
    async fn try_observe_sell_layout_from_reference_signature(
        &self,
        tx_rpc: &SolanaRpc,
        pool_market: Pubkey,
        base_mint: Pubkey,
        signature: &str,
    ) -> Option<PumpAmmSellReferenceObservation> {
        let tx_v = Self::fetch_tx_as_value_with_rpc(tx_rpc, signature)
            .await
            .ok()?;
        let msg = tx_v
            .get("result")
            .and_then(|r| r.get("transaction"))
            .and_then(|t| t.get("message"))?;
        let meta = tx_v
            .get("result")
            .and_then(|r| r.get("meta"))
            .unwrap_or(&serde_json::Value::Null);
        let mut account_keys = Self::parse_account_keys(msg).ok()?;
        Self::extend_with_loaded_addresses(&mut account_keys, meta);
        let sell_layout_sell_disc = anchor_disc("sell");
        for ix in Self::collect_all_instructions(msg, meta) {
            let program_id = Self::program_id_str_from_instruction_json(ix, &account_keys)?;
            if program_id != PUMPFUN_AMM_PROGRAM_ID {
                continue;
            }
            let ix_data = Self::pump_amm_ix_data_from_json(ix)?;
            let disc8: [u8; 8] = ix_data.get(..8).and_then(|s| s.try_into().ok())?;
            if disc8 != sell_layout_sell_disc {
                continue;
            }
            let acc_strings = Self::pump_amm_ix_account_strings_from_json(ix, &account_keys)?;
            if !pump_amm_sell_ix_account_len_supported(acc_strings.len()) {
                continue;
            }
            let (observed_pool, observed_base, cand_obs) =
                Self::pump_amm_sell_reference_observation_from_parsed_swap_ix(
                    &acc_strings,
                    &ix_data,
                    Some(signature.to_string()),
                )?;
            if observed_pool == pool_market && observed_base == base_mint {
                return Some(cand_obs);
            }
        }
        None
    }

    async fn resolve_authoritative_sell_layout_for_force_refresh(
        &self,
        pool: &PumpAmmPoolStatic,
        base_mint: Pubkey,
    ) -> Result<(
        PumpAmmSellReferenceObservation,
        Option<PumpAmmForceRefreshSellLayoutDiag>,
    )> {
        let local_limits = SellLayoutObserveLimits {
            signatures_limit: 200,
            max_tx_fetch_attempts: 200,
            max_get_transaction_calls: 200,
        };
        let local_outcome = self
            .observe_authoritative_sell_layout_from_tx_history_with_rpc(
                Arc::clone(&self.rpc),
                pool.pool_market,
                base_mint,
                "local_force_refresh_sell_layout",
                local_limits,
                false,
            )
            .await;

        let local_observation_failed = local_outcome.result.is_err();
        let local_probe_after_observation =
            local_history_probe_from_sell_layout_summary(&local_outcome.summary);

        match local_outcome.result {
            Ok(obs) if obs.layout != PumpAmmAuthoritativeSellLayout::Unknown => {
                let merged = self
                    .upgrade_sell_layout_observation_with_known_v2_reference(
                        Arc::clone(&self.rpc),
                        pool.pool_market,
                        base_mint,
                        obs,
                    )
                    .await;
                return Ok((merged, None));
            }
            Ok(_) => {}
            Err(e) => {
                warn!(
                    pool = %pool.pool_market,
                    base_mint = %base_mint,
                    error = %e,
                    termination_reason = %PumpAmmSellLayoutTerminationReason::LocalObservationError.as_log_str(),
                    "pump_amm: local force_refresh SELL-layout observation failed"
                );
            }
        }

        let Some(ref h_rpc) = self.bounded_tx_fallback_rpc else {
            let local_history_empty =
                local_probe_after_observation == PumpAmmLocalHistoryProbe::Empty;
            let termination_reason =
                force_refresh_no_fallback_termination_reason(local_observation_failed);
            let diag = PumpAmmForceRefreshSellLayoutDiag {
                local_history_empty,
                local_observation_failed,
                local_history_probe: local_probe_after_observation,
                external_attempted: false,
                external_sig_limit: None,
                external_max_tx_fetches: None,
                external_max_get_transaction_calls: None,
                termination_reason,
                last_external: None,
            };
            return Ok((PumpAmmSellReferenceObservation::unknown(), Some(diag)));
        };

        let local_one_sig_probe = match self
            .rpc
            .get_signatures_for_address(&pool.pool_market, Some(1))
            .await
        {
            Ok(v) if v.is_empty() => PumpAmmLocalHistoryProbe::Empty,
            Ok(_) => PumpAmmLocalHistoryProbe::NonEmpty,
            Err(e) => {
                warn!(
                    pool = %pool.pool_market,
                    base_mint = %base_mint,
                    error = %e,
                    "pump_amm: local tx index check failed during force_refresh; trying bounded external SELL-layout observation"
                );
                PumpAmmLocalHistoryProbe::Unknown
            }
        };
        if local_one_sig_probe == PumpAmmLocalHistoryProbe::NonEmpty {
            let diag = PumpAmmForceRefreshSellLayoutDiag {
                local_history_empty: false,
                local_observation_failed,
                local_history_probe: local_one_sig_probe,
                external_attempted: false,
                external_sig_limit: None,
                external_max_tx_fetches: None,
                external_max_get_transaction_calls: None,
                termination_reason:
                    PumpAmmSellLayoutTerminationReason::ExternalSkippedLocalSigsPresent,
                last_external: None,
            };
            return Ok((PumpAmmSellReferenceObservation::unknown(), Some(diag)));
        }

        let external_limits = SellLayoutObserveLimits {
            signatures_limit: FORCE_REFRESH_EXTERNAL_SIG_LIMIT,
            max_tx_fetch_attempts: FORCE_REFRESH_EXTERNAL_MAX_TX_ATTEMPTS,
            max_get_transaction_calls: FORCE_REFRESH_EXTERNAL_MAX_GET_TRANSACTION_CALLS,
        };

        let external_fut = self.observe_authoritative_sell_layout_from_tx_history_with_rpc(
            Arc::clone(h_rpc),
            pool.pool_market,
            base_mint,
            "bounded_external_force_refresh_sell_layout",
            external_limits,
            true,
        );

        let timed = tokio::time::timeout(BOUNDED_EXTERNAL_SELL_LAYOUT_TIMEOUT, external_fut).await;

        let (sell_obs, summary_opt, timed_out) = match timed {
            Ok(obs) => {
                let out = match obs.result {
                    Ok(v) if v.layout != PumpAmmAuthoritativeSellLayout::Unknown => {
                        self.upgrade_sell_layout_observation_with_known_v2_reference(
                            Arc::clone(h_rpc),
                            pool.pool_market,
                            base_mint,
                            v,
                        )
                        .await
                    }
                    Ok(_) => PumpAmmSellReferenceObservation::unknown(),
                    Err(_) => PumpAmmSellReferenceObservation::unknown(),
                };
                (out, Some(obs.summary), false)
            }
            Err(_) => {
                warn!(
                    pool = %pool.pool_market,
                    base_mint = %base_mint,
                    timeout_secs = BOUNDED_EXTERNAL_SELL_LAYOUT_TIMEOUT.as_secs(),
                    termination_reason = %PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalTimeoutBudgetExhausted.as_log_str(),
                    "pump_amm: bounded external SELL-layout observation timed out (outer budget)"
                );
                (PumpAmmSellReferenceObservation::unknown(), None, true)
            }
        };

        let termination_reason = force_refresh_external_termination_reason_after_attempt(
            sell_obs.layout,
            timed_out,
            summary_opt.as_ref(),
        );

        let local_history_empty = local_one_sig_probe == PumpAmmLocalHistoryProbe::Empty;
        let local_history_probe = if local_one_sig_probe != PumpAmmLocalHistoryProbe::Unknown {
            local_one_sig_probe
        } else {
            local_probe_after_observation
        };

        let diag = PumpAmmForceRefreshSellLayoutDiag {
            local_history_empty,
            local_observation_failed,
            local_history_probe,
            external_attempted: true,
            external_sig_limit: Some(FORCE_REFRESH_EXTERNAL_SIG_LIMIT),
            external_max_tx_fetches: Some(FORCE_REFRESH_EXTERNAL_MAX_TX_ATTEMPTS),
            external_max_get_transaction_calls: Some(
                FORCE_REFRESH_EXTERNAL_MAX_GET_TRANSACTION_CALLS,
            ),
            termination_reason,
            last_external: summary_opt,
        };

        Ok((sell_obs, Some(diag)))
    }

    async fn observe_authoritative_sell_layout_from_tx_history_with_rpc(
        &self,
        tx_rpc: Arc<SolanaRpc>,
        pool_market: Pubkey,
        base_mint: Pubkey,
        log_ctx: &'static str,
        limits: SellLayoutObserveLimits,
        structured_telemetry: bool,
    ) -> SellLayoutObserveOutcome {
        let t0 = std::time::Instant::now();
        let mut summary = PumpAmmSellLayoutExternalAttemptSummary {
            rpc_method_last: "getSignaturesForAddress",
            elapsed_total_ms: 0,
            get_signatures_calls: 0,
            get_signatures_succeeded: false,
            get_transaction_calls: 0,
            signatures_limit_last: Some(limits.signatures_limit),
            signatures_returned_last: 0,
            transactions_fetched: 0,
            pump_amm_instructions_seen: 0,
            pump_amm_sell_discriminator_seen: 0,
            sell_candidates_seen: 0,
            provider_status_last: PumpAmmSellLayoutProviderStatus::Ok,
            termination_reason:
                PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalNoSellCandidates,
        };

        let sigs = {
            let stage_start = std::time::Instant::now();
            summary.get_signatures_calls += 1;
            match tx_rpc
                .get_signatures_for_address(&pool_market, Some(limits.signatures_limit))
                .await
            {
                Ok(v) => {
                    summary.get_signatures_succeeded = true;
                    summary.signatures_returned_last = v.len();
                    summary.provider_status_last = PumpAmmSellLayoutProviderStatus::Ok;
                    if structured_telemetry {
                        info!(
                            pool = %pool_market,
                            base_mint = %base_mint,
                            log_ctx,
                            stage = "getSignaturesForAddress",
                            elapsed_ms = stage_start.elapsed().as_millis() as u64,
                            request_limit = limits.signatures_limit,
                            signatures_returned = v.len(),
                            provider_status = %summary.provider_status_last.as_log_str(),
                            "pump_amm: force_refresh SELL-layout external RPC stage"
                        );
                    }
                    v
                }
                Err(e) => {
                    summary.provider_status_last =
                        PumpAmmSellLayoutProviderStatus::from_error_class(
                            SolanaRpc::classify_client_error(&e),
                        );
                    let tr = match summary.provider_status_last {
                        PumpAmmSellLayoutProviderStatus::Http429 => {
                            PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalHttp429
                        }
                        PumpAmmSellLayoutProviderStatus::RateLimited => {
                            PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalRateLimited
                        }
                        PumpAmmSellLayoutProviderStatus::Timeout => {
                            PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalTimeout
                        }
                        _ => PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalRpcError,
                    };
                    summary.termination_reason = tr;
                    summary.elapsed_total_ms = t0.elapsed().as_millis();
                    if structured_telemetry {
                        warn!(
                            pool = %pool_market,
                            base_mint = %base_mint,
                            log_ctx,
                            stage = "getSignaturesForAddress",
                            elapsed_ms = stage_start.elapsed().as_millis() as u64,
                            request_limit = limits.signatures_limit,
                            signatures_returned = 0usize,
                            provider_status = %summary.provider_status_last.as_log_str(),
                            termination_reason = %tr.as_log_str(),
                            error = %e,
                            "pump_amm: force_refresh SELL-layout external RPC stage failed"
                        );
                    } else {
                        warn!(
                            pool = %pool_market,
                            base_mint = %base_mint,
                            log_ctx,
                            error = %e,
                            "pump_amm: getSignaturesForAddress failed for authoritative SELL-layout observation"
                        );
                    }
                    return SellLayoutObserveOutcome {
                        result: Err(anyhow!("getSignaturesForAddress failed: {e}")),
                        summary,
                    };
                }
            }
        };

        if sigs.is_empty() {
            summary.provider_status_last = PumpAmmSellLayoutProviderStatus::Empty;
            summary.termination_reason =
                PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalEmptySignatures;
            if structured_telemetry {
                info!(
                    pool = %pool_market,
                    base_mint = %base_mint,
                    log_ctx,
                    stage = "getSignaturesForAddress",
                    elapsed_ms = t0.elapsed().as_millis() as u64,
                    request_limit = limits.signatures_limit,
                    signatures_returned = 0usize,
                    provider_status = %summary.provider_status_last.as_log_str(),
                    termination_reason = %summary.termination_reason.as_log_str(),
                    "pump_amm: no signatures available for authoritative SELL-layout observation"
                );
            } else {
                info!(
                    pool = %pool_market,
                    base_mint = %base_mint,
                    log_ctx,
                    "pump_amm: no signatures available for authoritative SELL-layout observation"
                );
            }
            summary.elapsed_total_ms = t0.elapsed().as_millis();
            return SellLayoutObserveOutcome {
                result: Ok(PumpAmmSellReferenceObservation::unknown()),
                summary,
            };
        }

        let sell_layout_sell_disc = anchor_disc("sell");
        let sell_layout_buy_disc = anchor_disc("buy_exact_quote_in");

        let mut best_obs = PumpAmmSellReferenceObservation::unknown();
        let mut tx_attempts = 0usize;
        for s in &sigs {
            if tx_attempts >= limits.max_tx_fetch_attempts {
                summary.termination_reason =
                    PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalRequestBudgetExhausted;
                break;
            }
            if s.err.is_some() {
                continue;
            }
            tx_attempts += 1;

            if summary.get_transaction_calls >= limits.max_get_transaction_calls {
                summary.termination_reason =
                    PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalRequestBudgetExhausted;
                break;
            }

            let sig = s.signature.to_string();
            let gt_start = std::time::Instant::now();
            summary.rpc_method_last = "getTransaction";
            summary.get_transaction_calls += 1;

            let tx_v = match Self::fetch_tx_as_value_with_rpc(tx_rpc.as_ref(), &sig).await {
                Ok(v) => {
                    summary.provider_status_last = PumpAmmSellLayoutProviderStatus::Ok;
                    summary.termination_reason =
                        sell_layout_scan_termination_after_successful_tx_fetch(
                            summary.termination_reason,
                        );
                    if structured_telemetry
                        && (summary.get_transaction_calls == 1
                            || summary.get_transaction_calls % 5 == 0)
                    {
                        info!(
                            pool = %pool_market,
                            base_mint = %base_mint,
                            log_ctx,
                            stage = "getTransaction",
                            elapsed_ms = gt_start.elapsed().as_millis() as u64,
                            transactions_fetched = summary.transactions_fetched,
                            get_transaction_calls = summary.get_transaction_calls,
                            provider_status = %summary.provider_status_last.as_log_str(),
                            "pump_amm: force_refresh SELL-layout external RPC stage"
                        );
                    }
                    v
                }
                Err(e) => {
                    summary.provider_status_last = provider_status_from_anyhow_rpc(&e);
                    summary.termination_reason = match summary.provider_status_last {
                        PumpAmmSellLayoutProviderStatus::Http429 => {
                            PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalHttp429
                        }
                        PumpAmmSellLayoutProviderStatus::RateLimited => {
                            PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalRateLimited
                        }
                        PumpAmmSellLayoutProviderStatus::Timeout => {
                            PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalTimeout
                        }
                        _ => PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalRpcError,
                    };
                    if structured_telemetry {
                        warn!(
                            pool = %pool_market,
                            base_mint = %base_mint,
                            log_ctx,
                            stage = "getTransaction",
                            elapsed_ms = gt_start.elapsed().as_millis() as u64,
                            signature = %sig,
                            get_transaction_calls = summary.get_transaction_calls,
                            provider_status = %summary.provider_status_last.as_log_str(),
                            termination_reason = %summary.termination_reason.as_log_str(),
                            error = %e,
                            "pump_amm: force_refresh SELL-layout getTransaction failed; skipping signature"
                        );
                    } else {
                        warn!(
                            pool = %pool_market,
                            base_mint = %base_mint,
                            signature = %sig,
                            log_ctx,
                            error = %e,
                            "pump_amm: getTransaction failed during authoritative SELL-layout scan; skipping signature"
                        );
                    }
                    continue;
                }
            };

            let msg = match tx_v
                .get("result")
                .and_then(|r| r.get("transaction"))
                .and_then(|t| t.get("message"))
            {
                Some(v) => v,
                None => continue,
            };
            let meta = tx_v
                .get("result")
                .and_then(|r| r.get("meta"))
                .unwrap_or(&Value::Null);

            let mut account_keys = match Self::parse_account_keys(msg) {
                Ok(v) => v,
                Err(_) => continue,
            };
            Self::extend_with_loaded_addresses(&mut account_keys, meta);

            summary.transactions_fetched += 1;
            let mut ix_count_stage: u32 = 0;
            for ix in Self::collect_all_instructions(msg, meta) {
                ix_count_stage += 1;
                let Some(program_id) =
                    Self::program_id_str_from_instruction_json(ix, &account_keys)
                else {
                    continue;
                };
                if program_id != PUMPFUN_AMM_PROGRAM_ID {
                    continue;
                }
                summary.pump_amm_instructions_seen += 1;
                let Some(ix_data) = Self::pump_amm_ix_data_from_json(ix) else {
                    continue;
                };
                let disc8: Option<[u8; 8]> = ix_data.get(..8).and_then(|s| s.try_into().ok());
                let Some(disc8) = disc8 else {
                    continue;
                };
                if disc8 == sell_layout_sell_disc {
                    summary.pump_amm_sell_discriminator_seen += 1;
                } else if disc8 != sell_layout_buy_disc {
                    continue;
                }
                let Some(acc_strings) =
                    Self::pump_amm_ix_account_strings_from_json(ix, &account_keys)
                else {
                    continue;
                };
                if disc8 == sell_layout_sell_disc
                    && !pump_amm_sell_ix_account_len_supported(acc_strings.len())
                {
                    summary.termination_reason =
                        PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalPumpAmmSellAccountShapeUnsupported;
                    continue;
                }
                let Some((observed_pool, observed_base, cand_obs)) =
                    Self::pump_amm_sell_reference_observation_from_parsed_swap_ix(
                        &acc_strings,
                        &ix_data,
                        Some(sig.clone()),
                    )
                else {
                    if disc8 == sell_layout_sell_disc {
                        summary.termination_reason =
                            PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalPumpAmmSellLayoutNotDerivable;
                    }
                    continue;
                };
                summary.sell_candidates_seen += 1;
                if structured_telemetry && summary.sell_candidates_seen <= 3 {
                    info!(
                        pool = %pool_market,
                        base_mint = %base_mint,
                        log_ctx,
                        stage = "decode_instruction",
                        elapsed_ms = t0.elapsed().as_millis() as u64,
                        signature = %sig,
                        sell_candidates_seen = summary.sell_candidates_seen,
                        "pump_amm: force_refresh SELL-layout candidate instruction decoded"
                    );
                }
                if observed_pool != pool_market || observed_base != base_mint {
                    if disc8 == sell_layout_sell_disc {
                        summary.termination_reason =
                            PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalPumpAmmSellPoolOrMintMismatch;
                    }
                    continue;
                }
                best_obs = fold_force_refresh_sell_reference_observations_newest_first(
                    best_obs,
                    std::iter::once(cand_obs),
                );
                if authoritative_sell_layout_is_terminal_for_scan(&best_obs.layout) {
                    summary.termination_reason = PumpAmmSellLayoutTerminationReason::LayoutFound;
                    summary.elapsed_total_ms = t0.elapsed().as_millis();
                    let sell_ix_account_count = acc_strings.len() as u8;
                    let (tail0_log, tail1_log, tail2_log, pre0_log, pre1_log) =
                        match best_obs.layout {
                            PumpAmmAuthoritativeSellLayout::Extended {
                                pre_fee_0,
                                pre_fee_1,
                                tail_0,
                                tail_1,
                                tail_2,
                                fee_tail_0: _,
                                fee_tail_1: _,
                            } => (
                                Some(tail_0),
                                Some(tail_1),
                                Some(tail_2),
                                pre_fee_0,
                                pre_fee_1,
                            ),
                            _ => (None, None, None, None, None),
                        };
                    let (tail0_ix, tail1_ix, tail2_ix) = if usize::from(sell_ix_account_count)
                        == PUMPFUN_AMM_SELL_EXTENDED_V2_TOTAL_ACCOUNTS
                    {
                        (
                            PUMPFUN_AMM_SELL_EXT_TAIL_0_IX_V2,
                            PUMPFUN_AMM_SELL_EXT_TAIL_1_IX_V2,
                            PUMPFUN_AMM_SELL_EXT_THIRD_META_IX_V2,
                        )
                    } else {
                        (
                            PUMPFUN_AMM_SELL_EXT_TAIL_0_IX,
                            PUMPFUN_AMM_SELL_EXT_TAIL_1_IX,
                            PUMPFUN_AMM_SELL_EXT_THIRD_META_IX,
                        )
                    };
                    info!(
                        pool = %pool_market,
                        base_mint = %base_mint,
                        log_ctx,
                        signature = %sig,
                        layout = ?best_obs.layout,
                        protocol_fee_recipient_source = "sell_reference_ix",
                        reference_swap_signature = %best_obs.reference_swap_signature.as_deref().unwrap_or(""),
                        sell_reference_protocol_fee_recipient = %best_obs.protocol_fee_recipient,
                        sell_reference_protocol_fee_recipient_ta = %best_obs.protocol_fee_recipient_ta,
                        final_v14_protocol_fee_slot_6 = %best_obs.protocol_fee_recipient,
                        final_v14_protocol_fee_slot_7 = %best_obs.protocol_fee_recipient_ta,
                        final_sell_ix_meta_9 = %best_obs.protocol_fee_recipient,
                        final_sell_ix_meta_10 = %best_obs.protocol_fee_recipient_ta,
                        sell_ix_account_count,
                        sell_extended_tail_source = "sell_reference_ix",
                        sell_extended_tail_0 = ?tail0_log,
                        sell_extended_tail_1 = ?tail1_log,
                        sell_extended_tail_2 = ?tail2_log,
                        sell_ix_meta_19 = ?pre0_log,
                        sell_ix_meta_20 = ?pre1_log,
                        sell_ix_tail_0_ix = tail0_ix,
                        sell_ix_meta_at_tail_0 = ?tail0_log,
                        sell_ix_tail_1_ix = tail1_ix,
                        sell_ix_meta_at_tail_1 = ?tail1_log,
                        sell_ix_tail_2_ix = tail2_ix,
                        sell_ix_meta_at_tail_2 = ?tail2_log,
                        transactions_fetched = summary.transactions_fetched,
                        pump_amm_instructions_seen = summary.pump_amm_instructions_seen,
                        sell_candidates_seen = summary.sell_candidates_seen,
                        termination_reason = %summary.termination_reason.as_log_str(),
                        "pump_amm: authoritative SELL-layout observed from successful swap tx (extended wins over newer base-only evidence)"
                    );
                    return SellLayoutObserveOutcome {
                        result: Ok(best_obs),
                        summary,
                    };
                }
            }
            if structured_telemetry
                && summary.transactions_fetched > 0
                && summary.transactions_fetched % 5 == 0
            {
                info!(
                    pool = %pool_market,
                    base_mint = %base_mint,
                    log_ctx,
                    stage = "scan_transactions",
                    elapsed_ms = t0.elapsed().as_millis() as u64,
                    transactions_fetched = summary.transactions_fetched,
                    instructions_decoded_in_last_tx = ix_count_stage,
                    pump_amm_instructions_seen = summary.pump_amm_instructions_seen,
                    sell_candidates_seen = summary.sell_candidates_seen,
                    "pump_amm: force_refresh SELL-layout scan progress"
                );
            }
        }

        if best_obs.layout != PumpAmmAuthoritativeSellLayout::Unknown {
            summary.termination_reason = PumpAmmSellLayoutTerminationReason::LayoutFound;
            summary.elapsed_total_ms = t0.elapsed().as_millis();
            let (tail0_log, tail1_log, tail2_log, pre0_log, pre1_log) = match best_obs.layout {
                PumpAmmAuthoritativeSellLayout::Extended {
                    pre_fee_0,
                    pre_fee_1,
                    tail_0,
                    tail_1,
                    tail_2,
                    fee_tail_0: _,
                    fee_tail_1: _,
                } => (
                    Some(tail_0),
                    Some(tail_1),
                    Some(tail_2),
                    pre_fee_0,
                    pre_fee_1,
                ),
                _ => (None, None, None, None, None),
            };
            let sell_ix_account_count = match best_obs.layout {
                PumpAmmAuthoritativeSellLayout::Extended {
                    pre_fee_0,
                    pre_fee_1,
                    ..
                } if pre_fee_0.filter(|p| *p != Pubkey::default()).is_some()
                    && pre_fee_1.filter(|p| *p != Pubkey::default()).is_some() =>
                {
                    PUMPFUN_AMM_SELL_EXTENDED_V2_TOTAL_ACCOUNTS as u8
                }
                PumpAmmAuthoritativeSellLayout::Extended {
                    fee_tail_0,
                    fee_tail_1,
                    ..
                } if fee_tail_0.filter(|p| *p != Pubkey::default()).is_some()
                    && fee_tail_1.filter(|p| *p != Pubkey::default()).is_some() =>
                {
                    PUMPFUN_AMM_SELL_CASHBACK_TOTAL_ACCOUNTS as u8
                }
                PumpAmmAuthoritativeSellLayout::Extended { .. } => {
                    PUMPFUN_AMM_SELL_EXTENDED_TOTAL_ACCOUNTS as u8
                }
                PumpAmmAuthoritativeSellLayout::Base => 21,
                PumpAmmAuthoritativeSellLayout::Unknown => 0,
            };
            let (tail0_ix, tail1_ix, tail2_ix) = if usize::from(sell_ix_account_count)
                == PUMPFUN_AMM_SELL_EXTENDED_V2_TOTAL_ACCOUNTS
            {
                (
                    PUMPFUN_AMM_SELL_EXT_TAIL_0_IX_V2,
                    PUMPFUN_AMM_SELL_EXT_TAIL_1_IX_V2,
                    PUMPFUN_AMM_SELL_EXT_THIRD_META_IX_V2,
                )
            } else {
                (
                    PUMPFUN_AMM_SELL_EXT_TAIL_0_IX,
                    PUMPFUN_AMM_SELL_EXT_TAIL_1_IX,
                    PUMPFUN_AMM_SELL_EXT_THIRD_META_IX,
                )
            };
            info!(
                pool = %pool_market,
                base_mint = %base_mint,
                log_ctx,
                layout = ?best_obs.layout,
                protocol_fee_recipient_source = "sell_reference_ix",
                reference_swap_signature = %best_obs.reference_swap_signature.as_deref().unwrap_or(""),
                sell_reference_protocol_fee_recipient = %best_obs.protocol_fee_recipient,
                sell_reference_protocol_fee_recipient_ta = %best_obs.protocol_fee_recipient_ta,
                final_v14_protocol_fee_slot_6 = %best_obs.protocol_fee_recipient,
                final_v14_protocol_fee_slot_7 = %best_obs.protocol_fee_recipient_ta,
                final_sell_ix_meta_9 = %best_obs.protocol_fee_recipient,
                final_sell_ix_meta_10 = %best_obs.protocol_fee_recipient_ta,
                sell_ix_account_count,
                sell_extended_tail_source = "sell_reference_ix",
                sell_extended_tail_0 = ?tail0_log,
                sell_extended_tail_1 = ?tail1_log,
                sell_extended_tail_2 = ?tail2_log,
                sell_ix_meta_19 = ?pre0_log,
                sell_ix_meta_20 = ?pre1_log,
                sell_ix_tail_0_ix = tail0_ix,
                sell_ix_meta_at_tail_0 = ?tail0_log,
                sell_ix_tail_1_ix = tail1_ix,
                sell_ix_meta_at_tail_1 = ?tail1_log,
                sell_ix_tail_2_ix = tail2_ix,
                sell_ix_meta_at_tail_2 = ?tail2_log,
                transactions_fetched = summary.transactions_fetched,
                pump_amm_instructions_seen = summary.pump_amm_instructions_seen,
                sell_candidates_seen = summary.sell_candidates_seen,
                termination_reason = %summary.termination_reason.as_log_str(),
                "pump_amm: authoritative SELL-layout observed after full bounded scan (base-only or no extended in window)"
            );
            return SellLayoutObserveOutcome {
                result: Ok(best_obs),
                summary,
            };
        }

        summary.elapsed_total_ms = t0.elapsed().as_millis();
        if summary.termination_reason
            == PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalNoSellCandidates
        {
            if summary.pump_amm_instructions_seen > 0
                && summary.pump_amm_sell_discriminator_seen == 0
            {
                summary.termination_reason =
                    PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalPumpAmmSeenButNoSellDiscriminator;
            }
            info!(
                pool = %pool_market,
                base_mint = %base_mint,
                log_ctx,
                transactions_fetched = summary.transactions_fetched,
                pump_amm_instructions_seen = summary.pump_amm_instructions_seen,
                pump_amm_sell_discriminator_seen = summary.pump_amm_sell_discriminator_seen,
                sell_candidates_seen = summary.sell_candidates_seen,
                termination_reason = %summary.termination_reason.as_log_str(),
                "pump_amm: no successful SELL tx observed for authoritative SELL-layout"
            );
        }

        SellLayoutObserveOutcome {
            result: Ok(PumpAmmSellReferenceObservation::unknown()),
            summary,
        }
    }

    fn derive_user_volume_accumulator(
        program_id: Pubkey,
        pool_market: Pubkey,
        user: Pubkey,
    ) -> Pubkey {
        // BUY layout uses pool-scoped seeds (pool_market, user) — see on-chain `buy_exact_quote_in`.
        let seeds: [&[u8]; 3] = [
            b"user_volume_accumulator",
            pool_market.as_ref(),
            user.as_ref(),
        ];
        Pubkey::find_program_address(&seeds, &program_id).0
    }

    /// PumpSwap extended `sell`: `user_volume_accumulator` PDA uses **only** the user pubkey (same seed
    /// pattern as PumpFun cashback). Verified against mainnet sig
    /// `2CCmRDScAErjuBLnVJbGEyV3jsWbuNZpniZ5iTLSwZoE84nmyf285hqJXjRStMHJUaJ9Ex7EvL9fgwAVM83qGd3o`.
    fn derive_pump_amm_user_volume_accumulator_for_sell_cashback(
        program_id: Pubkey,
        user: Pubkey,
    ) -> Pubkey {
        Pubkey::find_program_address(&[b"user_volume_accumulator", user.as_ref()], &program_id).0
    }

    /// First two trailing `sell` metas (#21 user volume WSOL ATA, #22 user volume accumulator) when
    /// cashback/extended path is active — **always** derived from `(user, quote_mint, quote_tp)`.
    /// The **third** meta (#23 pool-v2 / context) is pool-specific and must come from Geyser / TX-history.
    pub fn pump_amm_sell_cashback_first_two_metas(
        user: Pubkey,
        quote_mint: Pubkey,
        quote_token_program: Pubkey,
    ) -> (Pubkey, Pubkey) {
        let program_id =
            Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID).expect("PUMPFUN_AMM_PROGRAM_ID is valid");
        let user_vol =
            Self::derive_pump_amm_user_volume_accumulator_for_sell_cashback(program_id, user);
        let user_vol_wsol_ata =
            Self::derive_ata_with_program(user_vol, quote_mint, quote_token_program);
        (user_vol_wsol_ata, user_vol)
    }

    /// Insert global FeeConfig + FeeProgram after coin-creator vault authority; optional 27-account pre-fee pair first.
    fn push_pump_amm_sell_global_fee_metas(
        metas: &mut Vec<AccountMeta>,
        sell_requires_pre_fee_metas: bool,
        pre_fee_meta_0: Pubkey,
        sell_pre_fee_meta_1: Option<Pubkey>,
        sell_fee_config: Pubkey,
        sell_fee_program: Pubkey,
    ) -> Result<()> {
        if sell_requires_pre_fee_metas {
            if pre_fee_meta_0 == Pubkey::default() {
                return Err(anyhow!(
                    "pump_amm SELL: 27-account layout requires global_volume_accumulator pre_fee_meta_0"
                ));
            }
            let pre_fee_1 = sell_pre_fee_meta_1
                .filter(|p| *p != Pubkey::default())
                .ok_or_else(|| {
                    anyhow!(
                        "pump_amm SELL: 27-account layout requires sell_pre_fee_meta_1 (authoritative observation required)"
                    )
                })?;
            metas.push(AccountMeta::new_readonly(pre_fee_meta_0, false));
            metas.push(AccountMeta::new_readonly(pre_fee_1, false));
        }
        metas.push(AccountMeta::new_readonly(sell_fee_config, false));
        metas.push(AccountMeta::new_readonly(sell_fee_program, false));
        Ok(())
    }

    /// Append extended/cashback SELL trailing metas for the **derived intent-user path**:
    /// #21/#22 always from [`Self::pump_amm_sell_cashback_first_two_metas`] (writable),
    /// #23 pool `third_meta` readonly (derived-user layout; writable #23 → on-chain PrivilegeEscalation),
    /// optional #24/#25 fee pair readonly from observation. Cached reference-trader volume tails are ignored.
    #[allow(clippy::too_many_arguments)]
    fn push_pump_amm_sell_extended_trailing_metas(
        metas: &mut Vec<AccountMeta>,
        user: Pubkey,
        quote_mint: Pubkey,
        quote_token_program: Pubkey,
        third: Pubkey,
        sell_requires_pre_fee_metas: bool,
        sell_extended_fee_tail_0: Option<Pubkey>,
        sell_extended_fee_tail_1: Option<Pubkey>,
        cached_observed_volume_tail_0: Option<Pubkey>,
        cached_observed_volume_tail_1: Option<Pubkey>,
    ) -> Result<()> {
        let (user_vol_wsol_ata, user_vol) =
            Self::pump_amm_sell_cashback_first_two_metas(user, quote_mint, quote_token_program);
        if let (Some(c0), Some(c1)) = (
            cached_observed_volume_tail_0.filter(|p| *p != Pubkey::default()),
            cached_observed_volume_tail_1.filter(|p| *p != Pubkey::default()),
        ) {
            if c0 != user_vol_wsol_ata || c1 != user_vol {
                warn!(
                    intent_user = %user,
                    cached_tail_21 = %c0,
                    cached_tail_22 = %c1,
                    derived_tail_21 = %user_vol_wsol_ata,
                    derived_tail_22 = %user_vol,
                    sell_extended_tail_source = "derived_for_intent_user",
                    cached_tail_mismatch = true,
                    "pump_amm SELL: ignoring cached volume tail #21/#22 from reference trader; using intent-user derivation"
                );
            }
        }
        metas.push(AccountMeta::new(user_vol_wsol_ata, false));
        metas.push(AccountMeta::new(user_vol, false));
        metas.push(AccountMeta::new_readonly(third, false));
        if sell_requires_pre_fee_metas {
            let f0 = sell_extended_fee_tail_0
                .filter(|p| *p != Pubkey::default())
                .ok_or_else(|| {
                    anyhow!(
                        "pump_amm SELL: 27-account layout requires sell_extended_fee_tail_0 (authoritative observation required)"
                    )
                })?;
            metas.push(AccountMeta::new_readonly(f0, false));
        } else if let (Some(f0), Some(f1)) = (
            sell_extended_fee_tail_0.filter(|p| *p != Pubkey::default()),
            sell_extended_fee_tail_1.filter(|p| *p != Pubkey::default()),
        ) {
            metas.push(AccountMeta::new_readonly(f0, false));
            metas.push(AccountMeta::new_readonly(f1, false));
        }
        Ok(())
    }

    async fn rpc_get_account_owner_and_executable(
        &self,
        address: Pubkey,
    ) -> Result<Option<(Pubkey, bool)>> {
        Self::rpc_account_owner_executable_for(self.rpc.as_ref(), address).await
    }

    async fn rpc_get_account_owner_executable_and_data(
        &self,
        address: Pubkey,
    ) -> Result<Option<(Pubkey, bool, Vec<u8>)>> {
        let acc = match self.rpc.get_account_opt_retry(&address).await {
            Ok(Some(a)) => a,
            Ok(None) => return Ok(None),
            Err(e) => return Err(anyhow!("get_account failed: {e}")),
        };
        Ok(Some((acc.owner, acc.executable, acc.data)))
    }

    fn parse_spl_token_account_mint_and_owner(data: &[u8]) -> Option<(Pubkey, Pubkey)> {
        // SPL token account layout: mint @ 0..32, owner @ 32..64
        if data.len() < 64 {
            return None;
        }
        let mint = Pubkey::new_from_array(data.get(0..32)?.try_into().ok()?);
        let owner = Pubkey::new_from_array(data.get(32..64)?.try_into().ok()?);
        Some((mint, owner))
    }

    fn parse_spl_token_account_amount(data: &[u8]) -> Option<u64> {
        // SPL token account layout: amount @ 64..72 (little-endian u64).
        let amt_bytes: [u8; 8] = data.get(64..72)?.try_into().ok()?;
        Some(u64::from_le_bytes(amt_bytes))
    }

    fn derive_ata_with_program(owner: Pubkey, mint: Pubkey, token_program: Pubkey) -> Pubkey {
        let owner_spl = SplProgramPubkey::new_from_array(owner.to_bytes());
        let mint_spl = SplProgramPubkey::new_from_array(mint.to_bytes());
        let token_program_spl = SplProgramPubkey::new_from_array(token_program.to_bytes());
        let ata_spl = spl_associated_token_account::get_associated_token_address_with_program_id(
            &owner_spl,
            &mint_spl,
            &token_program_spl,
        );
        Pubkey::new_from_array(ata_spl.to_bytes())
    }

    async fn derive_existing_pda(
        &self,
        program_id: Pubkey,
        seed_sets: &[Vec<Vec<u8>>],
    ) -> Result<Option<Pubkey>> {
        for seed_set in seed_sets {
            let seed_slices: Vec<&[u8]> = seed_set.iter().map(|s| s.as_slice()).collect();
            let candidate = Pubkey::find_program_address(&seed_slices, &program_id).0;
            let Some((owner, executable)) =
                self.rpc_get_account_owner_and_executable(candidate).await?
            else {
                continue;
            };
            if !executable && owner == program_id {
                return Ok(Some(candidate));
            }
        }
        Ok(None)
    }

    /// Cold path: full market parse outcome with a stable local-fail reason code (for `market-data` logs).
    pub async fn try_market_parse_outcome_for_pool(
        &self,
        pool_market: Pubkey,
        expected_base_mint: Pubkey,
        diagnostic_ctx: Option<(&'static str, bool)>,
    ) -> Result<PumpAmmMarketParseOutcome> {
        self.try_parse_pool_static_from_market_account_inner(
            pool_market,
            expected_base_mint,
            None,
            diagnostic_ctx,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    fn pump_amm_market_parse_diagnostics(
        source: &'static str,
        force_refresh: bool,
        pool_market: Pubkey,
        base_mint: Pubkey,
        fee: PumpAmmFeeParseKind,
        creator: PumpAmmCreatorParseKind,
        gva: PumpAmmGvaParseKind,
        fee_cfg: PumpAmmFeeConfigParseKind,
        authoritative_market_parse: bool,
    ) -> PumpAmmPoolAccountsDiagnostic {
        let (pfr_res, pfr_tag) = match fee {
            PumpAmmFeeParseKind::EmbeddedSecondQuoteTa => {
                if authoritative_market_parse {
                    (
                        PumpAmmFieldResolution::MarketLayout,
                        "market_derived_non_pool_quote_token_account",
                    )
                } else {
                    (
                        PumpAmmFieldResolution::Heuristic,
                        "embedded_second_quote_ta",
                    )
                }
            }
            PumpAmmFeeParseKind::GlobalConfigOffset57DerivedWsolAta => (
                PumpAmmFieldResolution::Deterministic,
                "global_config_offset_57_derived_wsol_ata",
            ),
            PumpAmmFeeParseKind::GlobalConfigScanEmbeddedQuoteTa => (
                PumpAmmFieldResolution::Heuristic,
                "global_config_scan_embedded_quote_ta_min_balance",
            ),
            PumpAmmFeeParseKind::AuthoritySearchQuoteMint => (
                PumpAmmFieldResolution::Heuristic,
                "authority_search_quote_mint",
            ),
            PumpAmmFeeParseKind::AuthoritySearchBaseMint => (
                PumpAmmFieldResolution::Heuristic,
                "authority_search_base_mint",
            ),
        };
        let (pfr_ta_res, pfr_ta_tag) = match fee {
            PumpAmmFeeParseKind::EmbeddedSecondQuoteTa => {
                if authoritative_market_parse {
                    (
                        PumpAmmFieldResolution::MarketLayout,
                        "market_derived_non_pool_quote_token_account_address",
                    )
                } else {
                    (
                        PumpAmmFieldResolution::Heuristic,
                        "embedded_second_quote_ta_address",
                    )
                }
            }
            PumpAmmFeeParseKind::GlobalConfigOffset57DerivedWsolAta => (
                PumpAmmFieldResolution::Deterministic,
                "derived_ata_from_global_config_pfr",
            ),
            PumpAmmFeeParseKind::GlobalConfigScanEmbeddedQuoteTa => (
                PumpAmmFieldResolution::Heuristic,
                "global_config_scan_embedded_quote_ta",
            ),
            PumpAmmFeeParseKind::AuthoritySearchQuoteMint
            | PumpAmmFeeParseKind::AuthoritySearchBaseMint => (
                PumpAmmFieldResolution::Heuristic,
                "authority_search_existing_or_derived_ata",
            ),
        };
        let (cc_ata_res, cc_ata_tag) = match creator {
            PumpAmmCreatorParseKind::EmbeddedSecondBaseTa => {
                if authoritative_market_parse {
                    (
                        PumpAmmFieldResolution::MarketLayout,
                        "market_derived_non_pool_base_token_account",
                    )
                } else {
                    (PumpAmmFieldResolution::Heuristic, "embedded_second_base_ta")
                }
            }
            PumpAmmCreatorParseKind::MarketOffset211CreatorVaultPda => (
                PumpAmmFieldResolution::MarketLayout,
                "market_offset_211_creator_vault_wsol_ata",
            ),
            PumpAmmCreatorParseKind::AuthoritySearchBaseMint
            | PumpAmmCreatorParseKind::AuthoritySearchQuoteMint => (
                PumpAmmFieldResolution::Heuristic,
                "authority_search_creator_vault_ta",
            ),
            PumpAmmCreatorParseKind::LegacyPdaSeedProbe => (
                PumpAmmFieldResolution::Heuristic,
                "legacy_pda_probe_derived_wsol_ata",
            ),
        };
        let (cc_auth_res, cc_auth_tag) = match creator {
            PumpAmmCreatorParseKind::EmbeddedSecondBaseTa => {
                if authoritative_market_parse {
                    (
                        PumpAmmFieldResolution::MarketLayout,
                        "market_derived_non_pool_base_ta_owner",
                    )
                } else {
                    (
                        PumpAmmFieldResolution::Heuristic,
                        "embedded_second_base_ta_owner",
                    )
                }
            }
            PumpAmmCreatorParseKind::MarketOffset211CreatorVaultPda => (
                PumpAmmFieldResolution::MarketLayout,
                "creator_vault_pda_from_market_seed",
            ),
            PumpAmmCreatorParseKind::AuthoritySearchBaseMint
            | PumpAmmCreatorParseKind::AuthoritySearchQuoteMint => {
                (PumpAmmFieldResolution::Heuristic, "authority_search_owner")
            }
            PumpAmmCreatorParseKind::LegacyPdaSeedProbe => (
                PumpAmmFieldResolution::Heuristic,
                "legacy_pda_probe_authority",
            ),
        };
        let (gva_res, gva_tag) = match gva {
            PumpAmmGvaParseKind::EmbeddedAmmOwnedLargestData => (
                PumpAmmFieldResolution::Heuristic,
                "amm_program_owned_max_datalen_candidate",
            ),
            PumpAmmGvaParseKind::PdaSeedProbe => (
                PumpAmmFieldResolution::Heuristic,
                "pda_seed_probe_global_volume_accumulator",
            ),
            PumpAmmGvaParseKind::SingletonGlobalVolumeAccumulator => (
                PumpAmmFieldResolution::Deterministic,
                "singleton_pda_global_volume_accumulator",
            ),
            PumpAmmGvaParseKind::SingletonPdaRpcVerified => (
                PumpAmmFieldResolution::Deterministic,
                "singleton_pda_global_volume_accumulator_rpc_verified",
            ),
        };
        let (fc_res, fc_tag) = match fee_cfg {
            PumpAmmFeeConfigParseKind::EmbeddedFeeProgramOwnedFromMarket => (
                PumpAmmFieldResolution::Heuristic,
                "fee_program_owned_from_market_candidate",
            ),
            PumpAmmFeeConfigParseKind::FeeProgramPdaProbe => {
                (PumpAmmFieldResolution::Heuristic, "fee_program_pda_probe")
            }
            PumpAmmFeeConfigParseKind::ConstantMainnetFeeConfig => (
                PumpAmmFieldResolution::Deterministic,
                "constant_PUMPFUN_AMM_FEE_CONFIG",
            ),
            PumpAmmFeeConfigParseKind::UniqueVerifiedFeeProgramAccount => (
                PumpAmmFieldResolution::MarketLayout,
                "unique_fee_program_owned_account_market_candidates",
            ),
        };
        PumpAmmPoolAccountsDiagnostic {
            source,
            base_mint,
            pool_market,
            force_refresh,
            reference_swap_signature: None,
            protocol_fee_recipient: PumpAmmFieldDiagnostic {
                resolution: pfr_res,
                tag: pfr_tag,
            },
            protocol_fee_recipient_ta: PumpAmmFieldDiagnostic {
                resolution: pfr_ta_res,
                tag: pfr_ta_tag,
            },
            coin_creator_vault_ata: PumpAmmFieldDiagnostic {
                resolution: cc_ata_res,
                tag: cc_ata_tag,
            },
            coin_creator_vault_authority: PumpAmmFieldDiagnostic {
                resolution: cc_auth_res,
                tag: cc_auth_tag,
            },
            global_volume_accumulator: PumpAmmFieldDiagnostic {
                resolution: gva_res,
                tag: gva_tag,
            },
            fee_config: PumpAmmFieldDiagnostic {
                resolution: fc_res,
                tag: fc_tag,
            },
            fee_program: PumpAmmFieldDiagnostic {
                resolution: PumpAmmFieldResolution::Deterministic,
                tag: "constant_PUMPFUN_AMM_FEE_PROGRAM",
            },
        }
    }

    fn pump_amm_tx_history_diagnostic(
        source: &'static str,
        force_refresh: bool,
        pool_market: Pubkey,
        base_mint: Pubkey,
        reference_sig: Option<String>,
    ) -> PumpAmmPoolAccountsDiagnostic {
        let obs = PumpAmmFieldResolution::TxHistoryObservation;
        PumpAmmPoolAccountsDiagnostic {
            source,
            base_mint,
            pool_market,
            force_refresh,
            reference_swap_signature: reference_sig,
            protocol_fee_recipient: PumpAmmFieldDiagnostic {
                resolution: obs,
                tag: "swap_ix_index_9",
            },
            protocol_fee_recipient_ta: PumpAmmFieldDiagnostic {
                resolution: obs,
                tag: "swap_ix_index_10",
            },
            coin_creator_vault_ata: PumpAmmFieldDiagnostic {
                resolution: obs,
                tag: "swap_ix_index_17",
            },
            coin_creator_vault_authority: PumpAmmFieldDiagnostic {
                resolution: obs,
                tag: "swap_ix_index_18",
            },
            global_volume_accumulator: PumpAmmFieldDiagnostic {
                resolution: obs,
                tag: "swap_ix_index_16",
            },
            fee_config: PumpAmmFieldDiagnostic {
                resolution: obs,
                tag: "swap_ix_index_19",
            },
            fee_program: PumpAmmFieldDiagnostic {
                resolution: obs,
                tag: "swap_ix_index_20",
            },
        }
    }

    /// Local paginated tx scanner uses **different** account indices for fee_config/fee_program than
    /// `discover_pool_static_via_tx_history_market_only_inner` (see inline comments at each site).
    fn pump_amm_tx_history_diagnostic_local_paginated_scan(
        source: &'static str,
        force_refresh: bool,
        pool_market: Pubkey,
        base_mint: Pubkey,
        reference_sig: Option<String>,
    ) -> PumpAmmPoolAccountsDiagnostic {
        let obs = PumpAmmFieldResolution::TxHistoryObservation;
        PumpAmmPoolAccountsDiagnostic {
            source,
            base_mint,
            pool_market,
            force_refresh,
            reference_swap_signature: reference_sig,
            protocol_fee_recipient: PumpAmmFieldDiagnostic {
                resolution: obs,
                tag: "swap_ix_index_9",
            },
            protocol_fee_recipient_ta: PumpAmmFieldDiagnostic {
                resolution: obs,
                tag: "swap_ix_index_10",
            },
            coin_creator_vault_ata: PumpAmmFieldDiagnostic {
                resolution: obs,
                tag: "swap_ix_index_17",
            },
            coin_creator_vault_authority: PumpAmmFieldDiagnostic {
                resolution: obs,
                tag: "swap_ix_index_18",
            },
            global_volume_accumulator: PumpAmmFieldDiagnostic {
                resolution: obs,
                tag: "swap_ix_index_19",
            },
            fee_config: PumpAmmFieldDiagnostic {
                resolution: obs,
                tag: "swap_ix_index_21_amm_owned_guard",
            },
            fee_program: PumpAmmFieldDiagnostic {
                resolution: obs,
                tag: "swap_ix_index_22",
            },
        }
    }

    fn pump_amm_livepoolcache_diagnostic(
        source: &'static str,
        force_refresh: bool,
        pool_market: Pubkey,
        base_mint: Pubkey,
    ) -> PumpAmmPoolAccountsDiagnostic {
        let cache = PumpAmmFieldResolution::CacheObservation;
        PumpAmmPoolAccountsDiagnostic {
            source,
            base_mint,
            pool_market,
            force_refresh,
            reference_swap_signature: None,
            protocol_fee_recipient: PumpAmmFieldDiagnostic {
                resolution: cache,
                tag: "jetstream_v14_slot6",
            },
            protocol_fee_recipient_ta: PumpAmmFieldDiagnostic {
                resolution: cache,
                tag: "jetstream_v14_slot7",
            },
            coin_creator_vault_ata: PumpAmmFieldDiagnostic {
                resolution: cache,
                tag: "jetstream_v14_slot9",
            },
            coin_creator_vault_authority: PumpAmmFieldDiagnostic {
                resolution: cache,
                tag: "jetstream_v14_slot10",
            },
            global_volume_accumulator: PumpAmmFieldDiagnostic {
                resolution: cache,
                tag: "jetstream_v14_slot11",
            },
            fee_config: PumpAmmFieldDiagnostic {
                resolution: cache,
                tag: "jetstream_v14_slot12",
            },
            fee_program: PumpAmmFieldDiagnostic {
                resolution: cache,
                tag: "jetstream_v14_slot13",
            },
        }
    }

    /// Parse PumpSwap AMM pool structure from on-chain market account.
    /// If `prefetched_data` is Some, use it instead of re-fetching via RPC.
    /// This avoids transient RPC inconsistencies when the account was already fetched.
    ///
    /// COLD PATH ONLY. Uses RPC (getAccountInfo). Never call from hot path.
    async fn try_parse_pool_static_from_market_account_inner(
        &self,
        pool_market: Pubkey,
        expected_base_mint: Pubkey,
        prefetched_data: Option<(Pubkey, bool, Vec<u8>)>,
        diagnostic_ctx: Option<(&'static str, bool)>,
    ) -> Result<PumpAmmMarketParseOutcome> {
        let (diag_source, diag_force_refresh) =
            diagnostic_ctx.unwrap_or(("rpc_market_parse", false));
        // Cold-path `force_refresh` (EnsurePumpAmm): publish only v14 sets with zero heuristic fields.
        let authoritative_market_parse = diag_force_refresh;
        let pump_amm_program = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID)?;
        let expected_quote_mint = Pubkey::from_str(WSOL_MINT)?;

        let (owner, executable, data) = if let Some(pf) = prefetched_data {
            pf
        } else {
            let Some(r) = self
                .rpc_get_account_owner_executable_and_data(pool_market)
                .await?
            else {
                return Ok(PumpAmmMarketParseOutcome::LocalFail(
                    PumpAmmLocalParseFailReason::PoolMarketNotFound,
                ));
            };
            r
        };
        if executable || owner != pump_amm_program {
            return Ok(PumpAmmMarketParseOutcome::LocalFail(
                PumpAmmLocalParseFailReason::PoolMarketOwnerMismatch,
            ));
        }

        if data.len() < PUMPFUN_AMM_MARKET_MIN_DATA_LEN {
            return Ok(PumpAmmMarketParseOutcome::LocalFail(
                PumpAmmLocalParseFailReason::PoolMarketDataTooShort,
            ));
        }

        // Canonical global config (same for every pool). Do **not** slice from market bytes:
        // misaligned/wrong offsets yield pubkeys with no on-chain account → Anchor 3012 on swap.
        let global_config = Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG)?;
        let base_mint = Pubkey::new_from_array(
            data[PUMPFUN_AMM_MARKET_BASE_MINT_OFFSET as usize
                ..(PUMPFUN_AMM_MARKET_BASE_MINT_OFFSET as usize + 32)]
                .try_into()
                .map_err(|_| anyhow!("market base_mint slice"))?,
        );
        let quote_mint = Pubkey::new_from_array(
            data[PUMPFUN_AMM_MARKET_QUOTE_MINT_OFFSET as usize
                ..(PUMPFUN_AMM_MARKET_QUOTE_MINT_OFFSET as usize + 32)]
                .try_into()
                .map_err(|_| anyhow!("market quote_mint slice"))?,
        );

        // This fallback is only used for WSOL pairs and assumes the program's swap semantics
        // (buy uses quote-in, sell uses base-in).
        if base_mint != expected_base_mint || quote_mint != expected_quote_mint {
            return Ok(PumpAmmMarketParseOutcome::LocalFail(
                PumpAmmLocalParseFailReason::BaseOrQuoteMintMismatch,
            ));
        }

        // Parse the remaining 32-byte fields after quote_mint; these typically include the
        // pool vaults + fee/creator accounts.
        let mut rest_pubkeys: Vec<Pubkey> = Vec::new();
        let mut off = (PUMPFUN_AMM_MARKET_QUOTE_MINT_OFFSET as usize) + 32;
        while off + 32 <= data.len() {
            let pk = Pubkey::new_from_array(
                data[off..off + 32]
                    .try_into()
                    .map_err(|_| anyhow!("market rest pubkey slice"))?,
            );
            rest_pubkeys.push(pk);
            off += 32;
        }

        // The observed market layout is not 32-byte aligned (base_mint starts at offset 43), so
        // vault/accounts can appear at non-32-byte boundaries. Scan raw bytes for candidate
        // Pubkeys and resolve them in batches.
        let _embedded_pubkeys: HashSet<Pubkey> = rest_pubkeys.iter().copied().collect();
        let mut scanned_pubkeys: Vec<Pubkey> = Vec::new();
        if data.len() >= 32 {
            for i in 0..=(data.len() - 32) {
                let pk = Pubkey::new_from_array(
                    data[i..i + 32]
                        .try_into()
                        .map_err(|_| anyhow!("market scan pubkey slice"))?,
                );
                scanned_pubkeys.push(pk);
            }
        }
        scanned_pubkeys.sort();
        scanned_pubkeys.dedup();

        let mut all_candidates = rest_pubkeys.clone();
        all_candidates.extend(scanned_pubkeys);
        all_candidates.sort();
        all_candidates.dedup();

        let token_program = Pubkey::new_from_array(spl_token::id().to_bytes());
        let token_2022_program = Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")?;
        let associated_token_program =
            Pubkey::new_from_array(spl_associated_token_account::id().to_bytes());
        let system_program = Pubkey::from_str("11111111111111111111111111111111")?;
        let fee_program = Pubkey::from_str(PUMPFUN_AMM_FEE_PROGRAM_ID)?;
        let mut token_accounts: Vec<TokenAccountMeta> = Vec::new();
        let mut non_token_pubkeys: Vec<Pubkey> = Vec::new();
        let mut program_owned_accounts: Vec<ProgramOwnedAccountMeta> = Vec::new();
        let mut fee_program_owned_accounts: Vec<ProgramOwnedAccountMeta> = Vec::new();

        const MULTI_ACCOUNTS_CHUNK: usize = 100;
        for chunk in all_candidates.chunks(MULTI_ACCOUNTS_CHUNK) {
            let accounts = match self.rpc.rpc.get_multiple_accounts(chunk).await {
                Ok(a) => a,
                Err(e) => {
                    warn!(
                        pool = %pool_market,
                        error = %e,
                        "pump_amm market parse: get_multiple_accounts failed (market candidate chunk)"
                    );
                    return Ok(PumpAmmMarketParseOutcome::LocalFail(
                        PumpAmmLocalParseFailReason::RpcGetMultipleAccountsFailed,
                    ));
                }
            };

            for (pk, acc_opt) in chunk.iter().copied().zip(accounts.into_iter()) {
                let Some(acc) = acc_opt else {
                    continue;
                };

                let acc_owner = acc.owner;
                let acc_data = acc.data;

                if acc_owner == token_program || acc_owner == token_2022_program {
                    if let Some((mint, token_owner)) =
                        Self::parse_spl_token_account_mint_and_owner(&acc_data)
                    {
                        let balance = Self::parse_spl_token_account_amount(&acc_data).unwrap_or(0);
                        token_accounts.push(TokenAccountMeta {
                            address: pk,
                            mint,
                            token_owner,
                            balance,
                        });
                        continue;
                    }
                }

                if acc_owner == pump_amm_program {
                    program_owned_accounts.push(ProgramOwnedAccountMeta {
                        address: pk,
                        data_len: acc_data.len(),
                    });
                    continue;
                }

                // fee_config must be owned by the Fee Program, not the AMM program
                if acc_owner == fee_program {
                    fee_program_owned_accounts.push(ProgramOwnedAccountMeta {
                        address: pk,
                        data_len: acc_data.len(),
                    });
                    continue;
                }

                if !acc.executable && pk != Pubkey::default() {
                    non_token_pubkeys.push(pk);
                }
            }
        }

        // Helper: try to find an authority whose token account for `mint` exists.
        // Prefer ATA; if none exists, fall back to an indexed getProgramAccounts lookup
        // (covers non-ATA fee recipient accounts).
        let find_authority_with_existing_token_account = |candidates: Vec<Pubkey>, mint: Pubkey| async move {
            for cand in candidates {
                // 1) Fast path: ATA exists.
                for tp in [token_program, token_2022_program] {
                    let ata = Self::derive_ata_with_program(cand, mint, tp);
                    let Some((ata_owner, ata_exec, ata_data)) =
                        self.rpc_get_account_owner_executable_and_data(ata).await?
                    else {
                        continue;
                    };
                    if ata_exec || (ata_owner != token_program && ata_owner != token_2022_program) {
                        continue;
                    }
                    let Some((ata_mint, ata_token_owner)) =
                        Self::parse_spl_token_account_mint_and_owner(&ata_data)
                    else {
                        continue;
                    };
                    if ata_mint == mint && ata_token_owner == cand {
                        return Ok::<Option<(Pubkey, Pubkey)>, anyhow::Error>(Some((cand, ata)));
                    }
                }

                // FIX-32: ATA not found on-chain — derive and use anyway.
                // PumpSwap creates it via CreateIdempotent during swap.
                // No getTokenAccountsByOwner fallback (incompatible with restricted validator secondary indexes).
                if mint == expected_quote_mint {
                    let derived_ata = Self::derive_ata_with_program(cand, mint, token_program);
                    warn!(
                        candidate = %cand,
                        mint = %mint,
                        derived_ata = %derived_ata,
                        "pump_amm: ATA not on-chain, using derived address (PumpSwap will create via CreateIdempotent)"
                    );
                    return Ok::<Option<(Pubkey, Pubkey)>, anyhow::Error>(Some((
                        cand,
                        derived_ata,
                    )));
                }
            }
            Ok::<Option<(Pubkey, Pubkey)>, anyhow::Error>(None)
        };

        let mut base_token_accounts: Vec<TokenAccountMeta> = token_accounts
            .iter()
            .filter(|t| t.mint == base_mint)
            .cloned()
            .collect();
        let mut quote_token_accounts: Vec<TokenAccountMeta> = token_accounts
            .iter()
            .filter(|t| t.mint == quote_mint)
            .cloned()
            .collect();

        base_token_accounts.sort_by_key(|t| std::cmp::Reverse(t.balance));
        quote_token_accounts.sort_by_key(|t| std::cmp::Reverse(t.balance));

        let Some(pool_base_vault) = base_token_accounts.first().map(|t| t.address) else {
            return Ok(PumpAmmMarketParseOutcome::LocalFail(
                PumpAmmLocalParseFailReason::NoBaseVaultTokenAccount,
            ));
        };
        let Some(pool_quote_vault) = quote_token_accounts.first().map(|t| t.address) else {
            return Ok(PumpAmmMarketParseOutcome::LocalFail(
                PumpAmmLocalParseFailReason::NoQuoteVaultTokenAccount,
            ));
        };

        // Build a list of plausible authorities for fee/creator recipients.
        // These can appear as plain Pubkeys in the market account even when the corresponding
        // token accounts are not embedded.
        let mut authority_candidates: Vec<Pubkey> = non_token_pubkeys
            .iter()
            .copied()
            .filter(|pk| {
                *pk != Pubkey::default()
                    && *pk != pool_market
                    && *pk != global_config
                    && *pk != base_mint
                    && *pk != quote_mint
                    && *pk != pool_base_vault
                    && *pk != pool_quote_vault
                    && *pk != pump_amm_program
                    && *pk != token_program
                    && *pk != token_2022_program
                    && *pk != associated_token_program
                    && *pk != system_program
            })
            .collect();
        authority_candidates.sort();
        authority_candidates.dedup();

        // Protocol fee recipient: embedded non-pool quote TA (market layout), else `global_config`
        // fixed offset + SPL ATA derivation, else (non-authoritative path only) global_config scan
        // / authority search. Authoritative cold-path parse refuses scan/heuristic fee resolution.
        let (protocol_fee_recipient, protocol_fee_recipient_ta, fee_parse_kind) =
            if authoritative_market_parse {
                let quote_non_pool: Vec<&TokenAccountMeta> = quote_token_accounts
                    .iter()
                    .filter(|t| t.address != pool_quote_vault)
                    .collect();
                match quote_non_pool.len() {
                    0 => {
                        let gc_account_opt = self
                            .rpc_get_account_owner_executable_and_data(global_config)
                            .await?;

                        let fixed_protocol_fee_from_global_config = if let Some((
                            gc_owner,
                            gc_exec,
                            gc_data,
                        )) = gc_account_opt.as_ref()
                        {
                            if !*gc_exec
                                && *gc_owner == pump_amm_program
                                && gc_data.len()
                                    >= PUMPFUN_AMM_GLOBAL_CONFIG_PROTOCOL_FEE_RECIPIENT_OFFSET + 32
                            {
                                let pfr = Pubkey::new_from_array(
                                    gc_data[PUMPFUN_AMM_GLOBAL_CONFIG_PROTOCOL_FEE_RECIPIENT_OFFSET
                                        ..PUMPFUN_AMM_GLOBAL_CONFIG_PROTOCOL_FEE_RECIPIENT_OFFSET
                                            + 32]
                                        .try_into()
                                        .map_err(|_| {
                                            anyhow!("global_config protocol_fee_recipient slice")
                                        })?,
                                );
                                if pfr != Pubkey::default() {
                                    let pfr_ta = Self::derive_ata_with_program(
                                        pfr,
                                        quote_mint,
                                        token_program,
                                    );
                                    info!(
                                        pool = %pool_market,
                                        protocol_fee_recipient = %pfr,
                                        protocol_fee_recipient_ta = %pfr_ta,
                                        "pump_amm: authoritative protocol fee from global_config offset \
                                         (no non-pool quote TA in market)"
                                    );
                                    Some((pfr, pfr_ta))
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        };

                        if let Some((pfr, ta)) = fixed_protocol_fee_from_global_config {
                            (
                                pfr,
                                ta,
                                PumpAmmFeeParseKind::GlobalConfigOffset57DerivedWsolAta,
                            )
                        } else {
                            warn!(
                                pool = %pool_market,
                                "pump_amm authoritative market parse FAIL: protocol_fee_recipient not \
                                 in global_config at fixed offset (no unique non-pool quote TA)"
                            );
                            return Ok(PumpAmmMarketParseOutcome::LocalFail(
                                PumpAmmLocalParseFailReason::AuthoritativeProtocolFeeUnresolved,
                            ));
                        }
                    }
                    1 => {
                        let t = quote_non_pool[0];
                        (
                            t.token_owner,
                            t.address,
                            PumpAmmFeeParseKind::EmbeddedSecondQuoteTa,
                        )
                    }
                    n => {
                        warn!(
                            pool = %pool_market,
                            non_pool_quote_ta_count = n,
                            "pump_amm authoritative market parse FAIL: multiple quote-mint token \
                             accounts besides pool vault (ambiguous embedded protocol fee TA)"
                        );
                        return Ok(PumpAmmMarketParseOutcome::LocalFail(
                            PumpAmmLocalParseFailReason::AuthoritativeEmbeddedQuoteTokenAccountsAmbiguous,
                        ));
                    }
                }
            } else if let Some(t) = quote_token_accounts
                .iter()
                .find(|t| t.address != pool_quote_vault)
            {
                (
                    t.token_owner,
                    t.address,
                    PumpAmmFeeParseKind::EmbeddedSecondQuoteTa,
                )
            } else {
                // Tx-history-free: `protocol_fee_recipient` is stored in the canonical `global_config`
                // account at a fixed offset (verified against mainnet swap ix accounts; Scope 41).
                // Pool markets often have only one WSOL vault (pool) — no embedded fee TA to discover.
                let gc_account_opt = self
                    .rpc_get_account_owner_executable_and_data(global_config)
                    .await?;

                let fixed_protocol_fee_from_global_config = if let Some((
                    gc_owner,
                    gc_exec,
                    gc_data,
                )) = gc_account_opt.as_ref()
                {
                    if !*gc_exec
                        && *gc_owner == pump_amm_program
                        && gc_data.len()
                            >= PUMPFUN_AMM_GLOBAL_CONFIG_PROTOCOL_FEE_RECIPIENT_OFFSET + 32
                    {
                        let pfr = Pubkey::new_from_array(
                            gc_data[PUMPFUN_AMM_GLOBAL_CONFIG_PROTOCOL_FEE_RECIPIENT_OFFSET
                                ..PUMPFUN_AMM_GLOBAL_CONFIG_PROTOCOL_FEE_RECIPIENT_OFFSET + 32]
                                .try_into()
                                .map_err(|_| {
                                    anyhow!("global_config protocol_fee_recipient slice")
                                })?,
                        );
                        if pfr != Pubkey::default() {
                            let pfr_ta =
                                Self::derive_ata_with_program(pfr, quote_mint, token_program);
                            info!(
                                pool = %pool_market,
                                protocol_fee_recipient = %pfr,
                                protocol_fee_recipient_ta = %pfr_ta,
                                "pump_amm: protocol fee accounts from global_config fixed offset \
                                 (no second quote TA in market; tx-history-free)"
                            );
                            Some((pfr, pfr_ta))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                if let Some((pfr, ta)) = fixed_protocol_fee_from_global_config {
                    (
                        pfr,
                        ta,
                        PumpAmmFeeParseKind::GlobalConfigOffset57DerivedWsolAta,
                    )
                } else {
                    let mut extra_authority_candidates: Vec<Pubkey> = Vec::new();
                    let mut embedded_fee_ta_from_global: Option<(Pubkey, Pubkey, u64)> = None;

                    if let Some((gc_owner, gc_exec, gc_data)) = gc_account_opt.as_ref() {
                        if !*gc_exec && *gc_owner == pump_amm_program && gc_data.len() >= 32 {
                            // Scan the global_config raw bytes for candidate pubkeys.
                            let mut gc_pubkeys: Vec<Pubkey> = Vec::new();
                            for i in 0..=(gc_data.len().saturating_sub(32)) {
                                let pk = Pubkey::new_from_array(
                                    gc_data[i..i + 32]
                                        .try_into()
                                        .map_err(|_| anyhow!("global_config scan pubkey slice"))?,
                                );
                                gc_pubkeys.push(pk);
                            }
                            gc_pubkeys.sort();
                            gc_pubkeys.dedup();

                            for chunk in gc_pubkeys.chunks(MULTI_ACCOUNTS_CHUNK) {
                                let accounts = match self.rpc.rpc.get_multiple_accounts(chunk).await
                                {
                                    Ok(a) => a,
                                    Err(e) => {
                                        warn!(
                                            pool = %pool_market,
                                            error = %e,
                                            "pump_amm market parse: get_multiple_accounts failed (global_config scan)"
                                        );
                                        return Ok(PumpAmmMarketParseOutcome::LocalFail(
                                            PumpAmmLocalParseFailReason::RpcGetMultipleAccountsFailed,
                                        ));
                                    }
                                };

                                for (pk, acc_opt) in chunk.iter().copied().zip(accounts.into_iter())
                                {
                                    let Some(acc) = acc_opt else {
                                        continue;
                                    };
                                    let acc_owner = acc.owner;
                                    let acc_data = acc.data;

                                    if acc_owner == token_program || acc_owner == token_2022_program
                                    {
                                        if let Some((mint, token_owner)) =
                                            Self::parse_spl_token_account_mint_and_owner(&acc_data)
                                        {
                                            if mint == quote_mint && pk != pool_quote_vault {
                                                let bal =
                                                    Self::parse_spl_token_account_amount(&acc_data)
                                                        .unwrap_or(0);
                                                match embedded_fee_ta_from_global {
                                                    None => {
                                                        embedded_fee_ta_from_global =
                                                            Some((token_owner, pk, bal));
                                                    }
                                                    Some((_prev_owner, _prev_ta, prev_bal)) => {
                                                        // Heuristic: prefer the smaller balance (fee TA
                                                        // tends to hold little vs pool vault).
                                                        if bal < prev_bal {
                                                            embedded_fee_ta_from_global =
                                                                Some((token_owner, pk, bal));
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        continue;
                                    }

                                    // Keep any existing, non-token pubkeys as additional authority candidates.
                                    if !acc.executable
                                        && pk != Pubkey::default()
                                        && pk != pool_market
                                        && pk != global_config
                                        && pk != base_mint
                                        && pk != quote_mint
                                        && pk != pool_base_vault
                                        && pk != pool_quote_vault
                                        && pk != pump_amm_program
                                        && pk != token_program
                                        && pk != token_2022_program
                                        && pk != associated_token_program
                                        && pk != system_program
                                    {
                                        extra_authority_candidates.push(pk);
                                    }
                                }
                            }
                        }
                    }

                    if let Some((owner, ta, _bal)) = embedded_fee_ta_from_global {
                        (
                            owner,
                            ta,
                            PumpAmmFeeParseKind::GlobalConfigScanEmbeddedQuoteTa,
                        )
                    } else {
                        // Retry ATA derivation with any extra authorities found in global_config.
                        let mut combined = authority_candidates.clone();
                        combined.extend(extra_authority_candidates);
                        combined.sort();
                        combined.dedup();

                        if let Some((auth, ta)) =
                            find_authority_with_existing_token_account(combined.clone(), quote_mint)
                                .await?
                        {
                            (auth, ta, PumpAmmFeeParseKind::AuthoritySearchQuoteMint)
                        } else if let Some((auth, ta)) =
                            // Some fee flows (notably `sell`) can accrue fees in the input mint.
                            // If the protocol fee recipient TA is not for WSOL, fall back to base mint.
                            find_authority_with_existing_token_account(
                                    combined.clone(),
                                    base_mint,
                                )
                                .await?
                        {
                            (auth, ta, PumpAmmFeeParseKind::AuthoritySearchBaseMint)
                        } else {
                            // Cannot infer protocol fee accounts from market/global_config — do not substitute
                            // a global mainnet recipient (pool-specific; see Bug #35).
                            warn!(
                                pool = %pool_market,
                                "pump_amm market parse FAIL: no protocol fee recipient token account \
                                 (global_config={global_config} tried_mints=[{quote_mint},{base_mint}] \
                                 authority_candidates_count={})",
                                combined.len(),
                            );
                            return Ok(PumpAmmMarketParseOutcome::LocalFail(
                                PumpAmmLocalParseFailReason::ProtocolFeeRecipientTokenAccountMissing,
                            ));
                        }
                    }
                }
            };

        // Creator vault: embedded non-pool base TA or market offset 211 + `creator_vault` PDA;
        // authoritative cold-path parse refuses authority search / multi-seed PDA probing.
        let (coin_creator_vault_authority, coin_creator_vault_ata, creator_parse_kind) =
            if authoritative_market_parse {
                let base_non_pool: Vec<&TokenAccountMeta> = base_token_accounts
                    .iter()
                    .filter(|t| t.address != pool_base_vault)
                    .collect();
                match base_non_pool.len() {
                    0 => {
                        if data.len() >= PUMPFUN_AMM_MARKET_CREATOR_SEED_OFFSET + 32 {
                            let creator_seed = Pubkey::new_from_array(
                                data[PUMPFUN_AMM_MARKET_CREATOR_SEED_OFFSET
                                    ..PUMPFUN_AMM_MARKET_CREATOR_SEED_OFFSET + 32]
                                    .try_into()
                                    .map_err(|_| anyhow!("market creator_seed slice"))?,
                            );
                            if creator_seed != Pubkey::default() {
                                let (auth, _) = Pubkey::find_program_address(
                                    &[b"creator_vault", creator_seed.as_ref()],
                                    &pump_amm_program,
                                );
                                let ata =
                                    Self::derive_ata_with_program(auth, quote_mint, token_program);
                                info!(
                                    pool = %pool_market,
                                    creator_seed = %creator_seed,
                                    coin_creator_vault_authority = %auth,
                                    coin_creator_vault_ata = %ata,
                                    "pump_amm: authoritative creator vault from market offset 211 + creator_vault PDA"
                                );
                                (
                                    auth,
                                    ata,
                                    PumpAmmCreatorParseKind::MarketOffset211CreatorVaultPda,
                                )
                            } else {
                                warn!(
                                    pool = %pool_market,
                                    "pump_amm authoritative market parse FAIL: creator_seed at offset 211 is default"
                                );
                                return Ok(PumpAmmMarketParseOutcome::LocalFail(
                                    PumpAmmLocalParseFailReason::AuthoritativeCreatorVaultUnresolved,
                                ));
                            }
                        } else {
                            warn!(
                                pool = %pool_market,
                                data_len = data.len(),
                                "pump_amm authoritative market parse FAIL: market data too short for creator_seed at offset 211"
                            );
                            return Ok(PumpAmmMarketParseOutcome::LocalFail(
                                PumpAmmLocalParseFailReason::AuthoritativeCreatorVaultUnresolved,
                            ));
                        }
                    }
                    1 => {
                        let t = base_non_pool[0];
                        (
                            t.token_owner,
                            t.address,
                            PumpAmmCreatorParseKind::EmbeddedSecondBaseTa,
                        )
                    }
                    n => {
                        warn!(
                            pool = %pool_market,
                            non_pool_base_ta_count = n,
                            "pump_amm authoritative market parse FAIL: multiple base-mint token \
                             accounts besides pool vault (ambiguous embedded creator vault TA)"
                        );
                        return Ok(PumpAmmMarketParseOutcome::LocalFail(
                            PumpAmmLocalParseFailReason::AuthoritativeEmbeddedBaseTokenAccountsAmbiguous,
                        ));
                    }
                }
            } else if let Some(t) = base_token_accounts
                .iter()
                .find(|t| t.address != pool_base_vault)
            {
                (
                    t.token_owner,
                    t.address,
                    PumpAmmCreatorParseKind::EmbeddedSecondBaseTa,
                )
            } else if data.len() >= PUMPFUN_AMM_MARKET_CREATOR_SEED_OFFSET + 32 {
                let creator_seed = Pubkey::new_from_array(
                    data[PUMPFUN_AMM_MARKET_CREATOR_SEED_OFFSET
                        ..PUMPFUN_AMM_MARKET_CREATOR_SEED_OFFSET + 32]
                        .try_into()
                        .map_err(|_| anyhow!("market creator_seed slice"))?,
                );
                if creator_seed != Pubkey::default() {
                    let (auth, _) = Pubkey::find_program_address(
                        &[b"creator_vault", creator_seed.as_ref()],
                        &pump_amm_program,
                    );
                    // On-chain swaps use the creator fee vault as a WSOL (quote) token account for this authority.
                    let ata = Self::derive_ata_with_program(auth, quote_mint, token_program);
                    info!(
                        pool = %pool_market,
                        creator_seed = %creator_seed,
                        coin_creator_vault_authority = %auth,
                        coin_creator_vault_ata = %ata,
                        "pump_amm: creator vault from market offset 211 + creator_vault PDA (quote-mint ATA)"
                    );
                    (
                        auth,
                        ata,
                        PumpAmmCreatorParseKind::MarketOffset211CreatorVaultPda,
                    )
                } else if let Some((auth, ta)) = find_authority_with_existing_token_account(
                    authority_candidates.clone(),
                    base_mint,
                )
                .await?
                {
                    (auth, ta, PumpAmmCreatorParseKind::AuthoritySearchBaseMint)
                } else if let Some((auth, ta)) = find_authority_with_existing_token_account(
                    authority_candidates.clone(),
                    quote_mint,
                )
                .await?
                {
                    (auth, ta, PumpAmmCreatorParseKind::AuthoritySearchQuoteMint)
                } else {
                    match self
                        .derive_existing_pda(
                            pump_amm_program,
                            &[
                                vec![
                                    b"creator_vault_authority".to_vec(),
                                    pool_market.to_bytes().to_vec(),
                                ],
                                vec![b"creator_vault".to_vec(), pool_market.to_bytes().to_vec()],
                                vec![b"creator".to_vec(), pool_market.to_bytes().to_vec()],
                                vec![b"vault_authority".to_vec(), pool_market.to_bytes().to_vec()],
                                vec![b"token_creator".to_vec(), pool_market.to_bytes().to_vec()],
                            ],
                        )
                        .await?
                    {
                        Some(derived_authority) => {
                            // Same as offset-211 path: creator fee vault is the quote-mint (WSOL) ATA for this authority.
                            let derived_ata = Self::derive_ata_with_program(
                                derived_authority,
                                expected_quote_mint,
                                token_program,
                            );
                            warn!(
                                pool = %pool_market,
                                base_mint = %base_mint,
                                derived_ata = %derived_ata,
                                authority_candidates_count = authority_candidates.len(),
                                "pump_amm: creator vault ATA not on-chain; using derived address (FIX-32 parity)"
                            );
                            (
                                derived_authority,
                                derived_ata,
                                PumpAmmCreatorParseKind::LegacyPdaSeedProbe,
                            )
                        }
                        None => {
                            warn!(
                                pool = %pool_market,
                                base_mint = %base_mint,
                                "pump_amm market parse FAIL: no creator vault token account \
                                 (no embedded ATA; offset 211 path unavailable; no ATA found; no valid PDA; \
                                 authority_candidates_count={})",
                                authority_candidates.len()
                            );
                            return Ok(PumpAmmMarketParseOutcome::LocalFail(
                                PumpAmmLocalParseFailReason::CreatorVaultMissing,
                            ));
                        }
                    }
                }
            } else if let Some((auth, ta)) =
                find_authority_with_existing_token_account(authority_candidates.clone(), base_mint)
                    .await?
            {
                (auth, ta, PumpAmmCreatorParseKind::AuthoritySearchBaseMint)
            } else if let Some((auth, ta)) =
                find_authority_with_existing_token_account(authority_candidates.clone(), quote_mint)
                    .await?
            {
                (auth, ta, PumpAmmCreatorParseKind::AuthoritySearchQuoteMint)
            } else {
                match self
                    .derive_existing_pda(
                        pump_amm_program,
                        &[
                            vec![
                                b"creator_vault_authority".to_vec(),
                                pool_market.to_bytes().to_vec(),
                            ],
                            vec![b"creator_vault".to_vec(), pool_market.to_bytes().to_vec()],
                            vec![b"creator".to_vec(), pool_market.to_bytes().to_vec()],
                            vec![b"vault_authority".to_vec(), pool_market.to_bytes().to_vec()],
                            vec![b"token_creator".to_vec(), pool_market.to_bytes().to_vec()],
                        ],
                    )
                    .await?
                {
                    Some(derived_authority) => {
                        let derived_ata = Self::derive_ata_with_program(
                            derived_authority,
                            expected_quote_mint,
                            token_program,
                        );
                        warn!(
                            pool = %pool_market,
                            base_mint = %base_mint,
                            derived_ata = %derived_ata,
                            authority_candidates_count = authority_candidates.len(),
                            "pump_amm: creator vault ATA not on-chain; using derived address (FIX-32 parity)"
                        );
                        (
                            derived_authority,
                            derived_ata,
                            PumpAmmCreatorParseKind::LegacyPdaSeedProbe,
                        )
                    }
                    None => {
                        warn!(
                            pool = %pool_market,
                            base_mint = %base_mint,
                            "pump_amm market parse FAIL: no creator vault token account \
                             (no embedded ATA; no ATA found; no valid PDA; \
                             authority_candidates_count={})",
                            authority_candidates.len()
                        );
                        return Ok(PumpAmmMarketParseOutcome::LocalFail(
                            PumpAmmLocalParseFailReason::CreatorVaultMissing,
                        ));
                    }
                }
            };

        // Swap ix account #15 must be the `__event_authority` PDA (ConstraintSeeds). Do not infer
        // from market bytes; same pattern as `global_config`.
        let event_authority = pump_amm_canonical_event_authority(&pump_amm_program);

        // Extract fee_config (Fee Program–owned) and global_volume_accumulator (AMM singleton PDA).
        // Authoritative cold-path parse: no "largest AMM-owned account" or multi-seed PDA probing.
        let (fee_config_raw, mut fee_config_parse_kind) = if authoritative_market_parse {
            match fee_program_owned_accounts.len() {
                0 => (
                    Pubkey::default(),
                    PumpAmmFeeConfigParseKind::ConstantMainnetFeeConfig,
                ),
                1 => (
                    fee_program_owned_accounts[0].address,
                    PumpAmmFeeConfigParseKind::UniqueVerifiedFeeProgramAccount,
                ),
                n => {
                    warn!(
                        pool = %pool_market,
                        fee_program_candidate_count = n,
                        "pump_amm authoritative market parse FAIL: multiple fee-program-owned \
                         pubkey candidates from market (ambiguous fee_config)"
                    );
                    return Ok(PumpAmmMarketParseOutcome::LocalFail(
                        PumpAmmLocalParseFailReason::AuthoritativeFeeConfigAmbiguous,
                    ));
                }
            }
        } else if !fee_program_owned_accounts.is_empty() {
            // Prefer the first fee-program owned account found in market data
            (
                fee_program_owned_accounts
                    .first()
                    .map(|m| m.address)
                    .unwrap_or_default(),
                PumpAmmFeeConfigParseKind::EmbeddedFeeProgramOwnedFromMarket,
            )
        } else {
            // Fallback: try deriving fee_config PDA from Fee Program
            (
                self.derive_existing_pda(
                    fee_program,
                    &[
                        vec![b"fee_config".to_vec(), global_config.to_bytes().to_vec()],
                        vec![b"fee_config".to_vec(), pool_market.to_bytes().to_vec()],
                        vec![b"fee_config".to_vec()],
                        vec![b"fees".to_vec(), global_config.to_bytes().to_vec()],
                        vec![b"fees".to_vec(), pool_market.to_bytes().to_vec()],
                    ],
                )
                .await?
                .unwrap_or_default(),
                PumpAmmFeeConfigParseKind::FeeProgramPdaProbe,
            )
        };

        let (mut global_volume_accumulator, mut gva_parse_kind) = if authoritative_market_parse {
            let singleton = pump_amm_singleton_global_volume_accumulator(&pump_amm_program);
            let Some((owner, executable)) =
                self.rpc_get_account_owner_and_executable(singleton).await?
            else {
                warn!(
                    pool = %pool_market,
                    singleton_gva = %singleton,
                    "pump_amm authoritative market parse FAIL: global_volume_accumulator PDA account missing"
                );
                return Ok(PumpAmmMarketParseOutcome::LocalFail(
                    PumpAmmLocalParseFailReason::GlobalVolumeAccumulatorMissing,
                ));
            };
            if executable || owner != pump_amm_program {
                warn!(
                    pool = %pool_market,
                    singleton_gva = %singleton,
                    owner = %owner,
                    executable = %executable,
                    "pump_amm authoritative market parse FAIL: global_volume_accumulator PDA owner mismatch"
                );
                return Ok(PumpAmmMarketParseOutcome::LocalFail(
                    PumpAmmLocalParseFailReason::GlobalVolumeAccumulatorMissing,
                ));
            }
            info!(
                pool = %pool_market,
                global_volume_accumulator = %singleton,
                "pump_amm: authoritative global_volume_accumulator = singleton PDA (RPC owner verified)"
            );
            (singleton, PumpAmmGvaParseKind::SingletonPdaRpcVerified)
        } else if !program_owned_accounts.is_empty() {
            // global_volume_accumulator is owned by the AMM program
            // Heuristic: prefer the larger account (volume accumulator tends to be bigger)
            let mut sorted = program_owned_accounts.clone();
            sorted.sort_by_key(|m| m.data_len);
            (
                sorted.last().map(|m| m.address).unwrap_or_default(),
                PumpAmmGvaParseKind::EmbeddedAmmOwnedLargestData,
            )
        } else {
            // Fallback: try deriving from AMM program (singleton first — matches on-chain PumpSwap).
            (
                self.derive_existing_pda(
                    pump_amm_program,
                    &[
                        vec![b"global_volume_accumulator".to_vec()],
                        vec![
                            b"global_volume_accumulator".to_vec(),
                            global_config.to_bytes().to_vec(),
                        ],
                        vec![
                            b"global_volume_accumulator".to_vec(),
                            pool_market.to_bytes().to_vec(),
                        ],
                        vec![
                            b"volume_accumulator".to_vec(),
                            global_config.to_bytes().to_vec(),
                        ],
                        vec![
                            b"volume_accumulator".to_vec(),
                            pool_market.to_bytes().to_vec(),
                        ],
                    ],
                )
                .await?
                .unwrap_or_default(),
                PumpAmmGvaParseKind::PdaSeedProbe,
            )
        };

        // Non-authoritative: known-pool fast path when embedded candidates miss the singleton PDA.
        if !authoritative_market_parse && global_volume_accumulator == Pubkey::default() {
            let singleton = pump_amm_singleton_global_volume_accumulator(&pump_amm_program);
            info!(
                pool = %pool_market,
                global_volume_accumulator = %singleton,
                "pump_amm: using singleton global_volume_accumulator PDA (heuristic empty)"
            );
            global_volume_accumulator = singleton;
            gva_parse_kind = PumpAmmGvaParseKind::SingletonGlobalVolumeAccumulator;
        }

        // Same global fee_config pubkey for all PumpSwap pools (see build_swap_ix_from_pool_accounts).
        let fee_config = if fee_config_raw == Pubkey::default() {
            fee_config_parse_kind = PumpAmmFeeConfigParseKind::ConstantMainnetFeeConfig;
            Pubkey::from_str(PUMPFUN_AMM_FEE_CONFIG)?
        } else {
            fee_config_raw
        };

        if fee_config == Pubkey::default() {
            warn!(
                pool = %pool_market,
                "pump_amm market parse FAIL: fee_config is default"
            );
            return Ok(PumpAmmMarketParseOutcome::LocalFail(
                PumpAmmLocalParseFailReason::FeeConfigMissing,
            ));
        }
        if global_volume_accumulator == Pubkey::default() {
            warn!(
                pool = %pool_market,
                "pump_amm market parse FAIL: global_volume_accumulator is default"
            );
            return Ok(PumpAmmMarketParseOutcome::LocalFail(
                PumpAmmLocalParseFailReason::GlobalVolumeAccumulatorMissing,
            ));
        }

        if protocol_fee_recipient == Pubkey::default()
            || protocol_fee_recipient_ta == Pubkey::default()
        {
            warn!(
                pool = %pool_market,
                "pump_amm market parse FAIL: protocol_fee_recipient unresolved after market + \
                 global_config paths (no Fee-Program PDA guessing — Bug #35)"
            );
            return Ok(PumpAmmMarketParseOutcome::LocalFail(
                PumpAmmLocalParseFailReason::ProtocolFeeRecipientUnresolved,
            ));
        }

        let diag = Self::pump_amm_market_parse_diagnostics(
            diag_source,
            diag_force_refresh,
            pool_market,
            base_mint,
            fee_parse_kind,
            creator_parse_kind,
            gva_parse_kind,
            fee_config_parse_kind,
            authoritative_market_parse,
        );

        Ok(PumpAmmMarketParseOutcome::Ok(PumpAmmPoolStatic {
            pool_market,
            global_config,
            base_mint,
            quote_mint,
            pool_base_vault,
            pool_quote_vault,
            protocol_fee_recipient,
            protocol_fee_recipient_ta,
            event_authority,
            coin_creator_vault_ata,
            coin_creator_vault_authority,
            global_volume_accumulator,
            fee_config,
            fee_program,
            sell_requires_cashback_remaining: false,
            sell_cashback_third_meta: None,
            sell_extended_tail_0: None,
            sell_extended_tail_1: None,
            sell_extended_fee_tail_0: None,
            sell_extended_fee_tail_1: None,
            sell_requires_pre_fee_metas: false,
            sell_pre_fee_meta_1: None,
            last_parse_diagnostics: Some(diag),
        }))
    }

    /// Discover PumpSwap pool market addresses by base_mint via getProgramAccounts RPC.
    ///
    /// COLD PATH ONLY. Uses RPC (getProgramAccounts). Never call from hot path.
    async fn discover_pool_markets_via_program_accounts_with_rpc(
        rpc: &SolanaRpc,
        base_mint: Pubkey,
    ) -> Result<Vec<Pubkey>> {
        use solana_commitment_config::CommitmentConfig;

        let program_id = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID)?;

        let filters = vec![
            RpcFilterType::Memcmp(Memcmp::new(
                PUMPFUN_AMM_MARKET_BASE_MINT_OFFSET as usize,
                MemcmpEncodedBytes::Base58(base_mint.to_string()),
            )),
            RpcFilterType::Memcmp(Memcmp::new(
                PUMPFUN_AMM_MARKET_QUOTE_MINT_OFFSET as usize,
                MemcmpEncodedBytes::Base58(WSOL_MINT.to_string()),
            )),
        ];

        let config = RpcProgramAccountsConfig {
            filters: Some(filters),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                commitment: Some(CommitmentConfig::confirmed()),
                ..Default::default()
            },
            ..Default::default()
        };

        let accounts = rpc
            .get_program_accounts_with_config_retry(&program_id, config)
            .await
            .map_err(|e| anyhow!("getProgramAccounts failed: {e}"))?;

        let mut out: Vec<Pubkey> = accounts.into_iter().map(|(pk, _)| pk).collect();
        out.sort();
        out.dedup();
        Ok(out)
    }

    /// Discover PumpSwap pool market addresses by base_mint via local validator RPC.
    async fn discover_pool_markets_via_program_accounts(
        &self,
        base_mint: Pubkey,
    ) -> Result<Vec<Pubkey>> {
        Self::discover_pool_markets_via_program_accounts_with_rpc(self.rpc.as_ref(), base_mint)
            .await
    }

    /// Bounded TX-history scan for one pool market (Cold Path). Uses `tx_rpc` for signatures + txs
    /// so `market-data` can pass Helius when the local validator has no history index.
    pub async fn discover_pool_static_via_tx_history_with_rpc(
        &self,
        tx_rpc: Arc<SolanaRpc>,
        pool_market: Pubkey,
        base_mint: Pubkey,
        force_refresh: bool,
    ) -> Result<Option<PumpAmmPoolStatic>> {
        self.discover_pool_static_via_tx_history_market_only_inner(
            tx_rpc,
            pool_market,
            base_mint,
            force_refresh,
        )
        .await
    }

    async fn discover_pool_static_via_tx_history_market_only(
        &self,
        pool_market: Pubkey,
        base_mint: Pubkey,
        force_refresh: bool,
    ) -> Result<Option<PumpAmmPoolStatic>> {
        self.discover_pool_static_via_tx_history_market_only_inner(
            Arc::clone(&self.rpc),
            pool_market,
            base_mint,
            force_refresh,
        )
        .await
    }

    async fn discover_pool_static_via_tx_history_market_only_inner(
        &self,
        tx_rpc: Arc<SolanaRpc>,
        pool_market: Pubkey,
        base_mint: Pubkey,
        force_refresh: bool,
    ) -> Result<Option<PumpAmmPoolStatic>> {
        // Minimal, bounded tx-history fallback.
        // Some Pump AMM market accounts do not embed the fee/creator token accounts we need to
        // build a full swap ix. In that case, scan only the market's txs to find a successful
        // swap and extract the canonical account set from the on-chain transaction.

        info!(
            "pump_amm TX-history: starting getSignaturesForAddress for market={} base_mint={} limit=200",
            pool_market, base_mint
        );

        let sigs = tx_rpc
            .get_signatures_for_address(&pool_market, Some(200))
            .await
            .map_err(|e| anyhow!("getSignaturesForAddress failed: {e}"))?;

        info!(
            "pump_amm TX-history: found {} signatures for market={} base_mint={}, starting transaction scan...",
            sigs.len(), pool_market, base_mint
        );

        if sigs.is_empty() {
            info!(
                "pump_amm TX-history: no signatures found for market={}, returning None",
                pool_market
            );
            return Ok(None);
        }

        const MAX_TX_FETCHES: usize = 200;
        let mut fetched = 0usize;
        let mut scanned_tx_count = 0usize;
        const DEBUG_REF_TX: &str = "3nj499thZ6JrdrC2WGGGRKoSC5Ydrat9gxP3XEnW5JK5ZWnXPzHE2QuAX8y7gvfsjRaLxCy3qkn6BYc1sxtfYiiY";

        for s in &sigs {
            if fetched >= MAX_TX_FETCHES {
                break;
            }
            if s.err.is_some() {
                continue;
            }
            let sig = s.signature.to_string();

            if sig == DEBUG_REF_TX {
                info!(
                    "pump_amm TX-history: FOUND reference TX in signature list! sig={}",
                    sig
                );
            }

            fetched += 1;

            if fetched % 20 == 0 {
                info!(
                    "pump_amm TX-history: scanned {}/{} transactions for market={}...",
                    fetched,
                    sigs.len(),
                    pool_market
                );
            }

            let tx_v = Self::fetch_tx_as_value_with_rpc(tx_rpc.as_ref(), &sig).await?;

            let msg = match tx_v
                .get("result")
                .and_then(|r| r.get("transaction"))
                .and_then(|t| t.get("message"))
            {
                Some(v) => v,
                None => continue,
            };
            let meta = tx_v
                .get("result")
                .and_then(|r| r.get("meta"))
                .unwrap_or(&Value::Null);

            let mut account_keys = match Self::parse_account_keys(msg) {
                Ok(v) => v,
                Err(_) => continue,
            };
            Self::extend_with_loaded_addresses(&mut account_keys, meta);

            scanned_tx_count += 1;

            let is_ref_tx = sig == DEBUG_REF_TX;
            if is_ref_tx {
                info!(
                    "pump_amm TX-history: processing reference TX sig={} account_keys_count={}",
                    sig,
                    account_keys.len()
                );
            }

            for ix in Self::collect_all_instructions(msg, meta) {
                let Some(program_id) =
                    Self::program_id_str_from_instruction_json(ix, &account_keys)
                else {
                    continue;
                };
                if program_id != PUMPFUN_AMM_PROGRAM_ID {
                    if is_ref_tx {
                        info!(
                            "pump_amm TX-history: reference TX ix program_id={} (not PumpSwap AMM, skipping)",
                            program_id
                        );
                    }
                    continue;
                }

                if is_ref_tx {
                    info!("pump_amm TX-history: reference TX has PumpSwap AMM instruction!");
                }

                let Some(acc_strings) =
                    Self::pump_amm_ix_account_strings_from_json(ix, &account_keys)
                else {
                    continue;
                };
                let Some(ix_data) = Self::pump_amm_ix_data_from_json(ix) else {
                    if is_ref_tx {
                        info!("pump_amm TX-history: reference TX missing ix data (need base58/binary for discriminator)");
                    }
                    continue;
                };

                let Some(pool) = Self::pump_amm_pool_static_from_parsed_swap_ix(
                    &acc_strings,
                    &ix_data,
                    |s_opt| {
                        Self::pump_amm_tx_history_diagnostic(
                            "tx_history_market_scan",
                            force_refresh,
                            pool_market,
                            base_mint,
                            s_opt.or_else(|| Some(sig.clone())),
                        )
                    },
                ) else {
                    if is_ref_tx {
                        info!(
                            "pump_amm TX-history: reference TX not buy/sell or bad account count sig={} n_accounts={}",
                            sig,
                            acc_strings.len()
                        );
                    }
                    continue;
                };

                if pool.pool_market != pool_market {
                    if scanned_tx_count == 1 || is_ref_tx {
                        info!(
                            "pump_amm TX-history: market mismatch sig={} expected={} actual={}",
                            sig, pool_market, pool.pool_market
                        );
                    }
                    continue;
                }
                if pool.base_mint != base_mint {
                    if scanned_tx_count == 1 || is_ref_tx {
                        info!(
                            "pump_amm TX-history: base_mint mismatch sig={} expected={} actual={}",
                            sig, base_mint, pool.base_mint
                        );
                    }
                    continue;
                }

                if is_ref_tx {
                    info!(
                        "pump_amm TX-history: reference TX parsed OK (sell_cashback_remaining={})",
                        pool.sell_requires_cashback_remaining
                    );
                }

                // Fee guardrails: BUY uses pool-specific `fee_config` (AMM-owned); SELL uses global fee accounts.
                let expected_fee_program = Pubkey::from_str(PUMPFUN_AMM_FEE_PROGRAM_ID)?;
                if pool.fee_program != expected_fee_program {
                    if is_ref_tx {
                        info!(
                            "pump_amm TX-history: reference TX fee_program mismatch expected={} actual={}",
                            expected_fee_program, pool.fee_program
                        );
                    }
                    continue;
                }

                if is_ref_tx {
                    info!("pump_amm TX-history: reference TX fee_program OK, checking fee_config owner...");
                }

                let Some((fee_owner, fee_executable)) =
                    Self::rpc_account_owner_executable_for(tx_rpc.as_ref(), pool.fee_config)
                        .await?
                else {
                    if is_ref_tx {
                        info!("pump_amm TX-history: reference TX fee_config account not found");
                    }
                    continue;
                };

                let pump_amm_program = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID)?;
                let fee_ok = if acc_strings.len() == 23 {
                    !fee_executable && fee_owner == pump_amm_program
                } else {
                    !fee_executable && fee_owner == expected_fee_program
                };

                if is_ref_tx {
                    info!(
                        "pump_amm TX-history: reference TX fee_config owner={} executable={} buy_layout={} fee_ok={}",
                        fee_owner,
                        fee_executable,
                        acc_strings.len() == 23,
                        fee_ok
                    );
                }

                if !fee_ok {
                    if is_ref_tx {
                        info!("pump_amm TX-history: reference TX fee_config owner check FAILED");
                    }
                    continue;
                }

                if is_ref_tx {
                    info!("pump_amm TX-history: reference TX SUCCESS! Returning pool...");
                }

                return Ok(Some(pool));
            }
        }

        info!(
            "pump_amm TX-history: scanned {} transactions, no matching swap found for market={} base_mint={}",
            scanned_tx_count, pool_market, base_mint
        );
        Ok(None)
    }

    /// COLD PATH ONLY — RPC fallback when LivePoolCache misses. Never called in Hot Path. P3 #12.
    ///
    /// `force_refresh`: skip in-memory and LivePoolCache `pool_accounts` reconstruction so RPC
    /// market parse / heuristics run (EnsurePumpAmm recovery).
    async fn discover_pool_static(
        &self,
        base_mint: Pubkey,
        force_refresh: bool,
    ) -> Result<Option<PumpAmmPoolStatic>> {
        if !force_refresh {
            if let Some(v) = self.pools_by_base.get(&base_mint) {
                return Ok(Some(v.clone()));
            }
        }

        // FIX-23: GEYSER-FIRST — Construct PumpAmmPoolStatic from LivePoolCache before any RPC.
        // The cache contains all 14 pool_accounts from DexPoolAccounts events (parsed from
        // verified on-chain swap txs). This eliminates getProgramAccounts + getMultipleAccounts
        // RPC calls (~500-3000ms) in the hot path.
        if !force_refresh {
            if let Some(ref cache) = self.live_pool_cache {
                if let Some(accounts) =
                    cache.get_ready_pump_amm_pool_accounts_by_base_mint(&base_mint)
                {
                    if accounts.len() >= 14 {
                        let pump_amm_program = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID)?;
                        let global_config = Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG)?;
                        if accounts[1] != global_config {
                            warn!(
                                cached = %accounts[1],
                                expected = %global_config,
                                "pump_amm: cache pool_accounts[1] != canonical global_config; using canonical"
                            );
                        }
                        let event_authority = pump_amm_canonical_event_authority(&pump_amm_program);
                        if accounts[8] != event_authority {
                            warn!(
                                cached = %accounts[8],
                                expected = %event_authority,
                                "pump_amm: cache pool_accounts[8] != canonical event_authority; using canonical"
                            );
                        }
                        let (sell_requires, sell_third, sell_t0, sell_t1) =
                            cache.pump_amm_sell_extended_layout(&accounts[0]);
                        let pool = PumpAmmPoolStatic {
                            pool_market: accounts[0],
                            global_config,
                            base_mint: accounts[2],
                            quote_mint: accounts[3],
                            pool_base_vault: accounts[4],
                            pool_quote_vault: accounts[5],
                            protocol_fee_recipient: accounts[6],
                            protocol_fee_recipient_ta: accounts[7],
                            event_authority,
                            coin_creator_vault_ata: accounts[9],
                            coin_creator_vault_authority: accounts[10],
                            global_volume_accumulator: accounts[11],
                            fee_config: accounts[12],
                            fee_program: accounts[13],
                            sell_requires_cashback_remaining: sell_requires,
                            sell_cashback_third_meta: sell_third,
                            sell_extended_tail_0: sell_t0,
                            sell_extended_tail_1: sell_t1,
                            sell_extended_fee_tail_0: cache
                                .pump_amm_sell_fee_tail_layout(&accounts[0])
                                .0,
                            sell_extended_fee_tail_1: cache
                                .pump_amm_sell_fee_tail_layout(&accounts[0])
                                .1,
                            sell_requires_pre_fee_metas: cache
                                .pump_amm_sell_requires_pre_fee_metas(&accounts[0]),
                            sell_pre_fee_meta_1: cache.pump_amm_sell_pre_fee_meta_1(&accounts[0]),
                            last_parse_diagnostics: Some(Self::pump_amm_livepoolcache_diagnostic(
                                "discover_pool_static_livepoolcache_reconstruct",
                                force_refresh,
                                accounts[0],
                                base_mint,
                            )),
                        };
                        // Cache internally for build_swap_ix() (sync path)
                        self.pools_by_base.insert(base_mint, pool.clone());
                        self.pools_by_market.insert(pool.pool_market, base_mint);
                        info!(
                            base_mint = %base_mint,
                            pool_market = %pool.pool_market,
                            "pump_amm: PumpAmmPoolStatic from LivePoolCache (ZERO RPC discovery)"
                        );
                        return Ok(Some(pool));
                    }
                }
            }
        }

        // FIX-31: FAST PATH — If the LivePoolCache has the pool address (even without
        // pool_accounts), use a single getAccount call to parse the pool account data.
        // This avoids the slow getProgramAccounts scan that routinely times out (>10s).
        if let Some(ref cache) = self.live_pool_cache {
            if let Some(pool_address) = cache.get_pump_amm_pool_address_by_base_mint(&base_mint) {
                info!(
                    base_mint = %base_mint,
                    pool = %pool_address,
                    "pump_amm: LivePoolCache has pool address but no pool_accounts, trying direct getAccount"
                );
                match self
                    .try_market_parse_outcome_for_pool(
                        pool_address,
                        base_mint,
                        Some(("livepoolcache_pool_address_getaccount", force_refresh)),
                    )
                    .await
                {
                    Ok(PumpAmmMarketParseOutcome::Ok(pool)) => {
                        self.pools_by_base.insert(base_mint, pool.clone());
                        self.pools_by_market.insert(pool.pool_market, base_mint);
                        info!(
                            base_mint = %base_mint,
                            pool_market = %pool.pool_market,
                            "pump_amm: PumpAmmPoolStatic from direct getAccount (fast path)"
                        );
                        return Ok(Some(pool));
                    }
                    Ok(PumpAmmMarketParseOutcome::LocalFail(reason)) => {
                        warn!(
                            base_mint = %base_mint,
                            pool = %pool_address,
                            local_parse_fail_reason = %reason,
                            "pump_amm: cached pool address local market parse failed"
                        );
                        if let Some(pool) = self
                            .try_bounded_external_tx_history_pool(
                                pool_address,
                                base_mint,
                                Some(reason),
                                "livepoolcache_pool_address",
                                force_refresh,
                            )
                            .await?
                        {
                            self.insert_pool_static_cache(pool.clone());
                            return Ok(Some(pool));
                        }
                        return Err(anyhow!(
                            "pump_amm: LivePoolCache pool address present but market parse failed local_parse={} (base_mint={}, pool={}); refusing unbounded RPC discovery",
                            reason,
                            base_mint,
                            pool_address
                        ));
                    }
                    Err(e) => {
                        warn!(
                            base_mint = %base_mint,
                            pool = %pool_address,
                            error = %e,
                            "pump_amm: cached pool address parse RPC error; refusing getProgramAccounts (no unbounded scan)"
                        );
                        return Err(e.context(format!(
                            "pump_amm: cached pool address parse error (base_mint={}, pool={}); refusing unbounded RPC discovery",
                            base_mint, pool_address
                        )));
                    }
                }
            }
        }

        // Avoid concurrent discovery attempts for the same base mint.
        // This significantly reduces RPC rate-limits when `parallel_exits` is enabled.
        let _guard = self.discovery_lock.lock().await;
        if let Some(v) = self.pools_by_base.get(&base_mint) {
            return Ok(Some(v.clone()));
        }

        // RPC FALLBACK: LivePoolCache miss (new pool not yet indexed by Geyser, or cold start
        // before PoolCacheUpdate events arrive). This is the cold path — acceptable per architecture.
        warn!(
            base_mint = %base_mint,
            "pump_amm: LivePoolCache miss for pool discovery, falling back to RPC"
        );

        let mut discovery_err: Option<anyhow::Error> = None;
        let mut markets = match self
            .discover_pool_markets_via_program_accounts(base_mint)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                discovery_err = Some(e);
                Vec::new()
            }
        };

        let local_gpa_error = discovery_err.is_some();
        let local_gpa_zero_markets = markets.is_empty();
        if local_gpa_error || local_gpa_zero_markets {
            if local_gpa_error {
                if let Some(ref e) = discovery_err {
                    warn!(
                        base_mint = %base_mint,
                        error = %e,
                        "pump_amm: local getProgramAccounts failed or unusable; attempting bounded external pool-market discovery"
                    );
                }
            } else {
                info!(
                    base_mint = %base_mint,
                    "pump_amm: local getProgramAccounts returned 0 markets; attempting bounded external pool-market discovery"
                );
            }

            let stage = if local_gpa_error {
                "local_getProgramAccounts_error"
            } else {
                "local_getProgramAccounts_zero_markets"
            };

            if let Some(external) = self
                .try_bounded_external_pool_markets_via_program_accounts(base_mint, stage)
                .await?
            {
                for pk in external {
                    if !markets.contains(&pk) {
                        markets.push(pk);
                    }
                }
                markets.sort();
                markets.dedup();
                if local_gpa_error && !markets.is_empty() {
                    info!(
                        base_mint = %base_mint,
                        market_count = markets.len(),
                        "pump_amm: external pool-market discovery recovered after local getProgramAccounts error"
                    );
                    discovery_err = None;
                }
            }
        }

        // Fast path: if we can locate the pool market via program-accounts lookup, attempt to
        // build the full static account set by parsing on-chain state + deriving PDAs. This avoids
        // relying on tx-history (some pools can exist with no successful swaps yet).
        let mut market_parse_err: Option<anyhow::Error> = None;
        let first_market = markets.first().copied();
        let mut first_market_local_parse_fail: Option<PumpAmmLocalParseFailReason> = None;
        for m in &markets {
            match self
                .try_market_parse_outcome_for_pool(
                    *m,
                    base_mint,
                    Some(("getprogramaccounts_market_parse", force_refresh)),
                )
                .await
            {
                Ok(PumpAmmMarketParseOutcome::Ok(pool)) => {
                    self.pools_by_base.insert(base_mint, pool.clone());
                    self.pools_by_market.insert(pool.pool_market, base_mint);
                    return Ok(Some(pool));
                }
                Ok(PumpAmmMarketParseOutcome::LocalFail(reason)) => {
                    if first_market == Some(*m) {
                        first_market_local_parse_fail = Some(reason);
                    }
                    warn!(
                        market = %m,
                        base_mint = %base_mint,
                        local_parse_fail_reason = %reason,
                        "pump_amm: local market parse failed (getProgramAccounts path)"
                    );
                }
                Err(e) => {
                    market_parse_err = Some(anyhow!("{e:#}").context(format!(
                        "pump_amm market parse failed market={m} base_mint={base_mint}"
                    )));
                }
            }
        }

        // If we found market accounts but couldn't parse a usable static account set, do a narrow
        // tx-history fallback by scanning only the market address. This is far cheaper than the
        // legacy scan across multiple addresses/pages.
        if let Some(m) = markets.first().copied() {
            info!(
                "pump_amm attempting TX-history fallback for market {} base_mint {}",
                m, base_mint
            );

            match self
                .discover_pool_static_via_tx_history_market_only(m, base_mint, force_refresh)
                .await
            {
                Ok(Some(pool)) => {
                    info!(
                        "pump_amm TX-history fallback SUCCESS for market {} base_mint {}",
                        m, base_mint
                    );
                    self.pools_by_base.insert(base_mint, pool.clone());
                    self.pools_by_market.insert(pool.pool_market, base_mint);
                    return Ok(Some(pool));
                }
                Ok(None) => {
                    warn!(
                        "pump_amm TX-history fallback returned None for market {} base_mint {} (tx_history_unavailable on local RPC)",
                        m, base_mint
                    );
                }
                Err(e) => {
                    warn!(
                        "pump_amm TX-history fallback ERROR for market {} base_mint {}: {:#}",
                        m, base_mint, e
                    );
                }
            }

            if let Some(pool) = self
                .try_bounded_external_tx_history_pool(
                    m,
                    base_mint,
                    first_market_local_parse_fail,
                    "discover_pool_static_after_local_tx_history",
                    force_refresh,
                )
                .await?
            {
                self.insert_pool_static_cache(pool.clone());
                info!(
                    market = %m,
                    base_mint = %base_mint,
                    "pump_amm: bounded external TX-history SUCCESS after local tx-history miss"
                );
                return Ok(Some(pool));
            }

            if let Some(e) = market_parse_err {
                return Err(e);
            }

            return Err(anyhow!(
                "pump_amm market(s) found but could not build pool static base_mint={base_mint} markets={markets:?}"
            ));
        }

        // TX-based discovery: prefer scanning the pool market(s) (stable) over the mint address.
        // On pruned RPC, mint-address history can be missing; program-accounts lookup + market
        // history tends to be more reliable.
        let mut scan_addresses: Vec<String> = Vec::new();
        for m in &markets {
            scan_addresses.push(m.to_string());
        }
        scan_addresses.push(base_mint.to_string());
        scan_addresses.sort();
        scan_addresses.dedup();

        for addr in scan_addresses {
            // We paginate because the newest signatures can be dominated by our own failed
            // liquidation attempts (which we intentionally skip), and the first successful
            // PumpSwap trades can be older than the initial page on busy pools.
            // IMPORTANT: tx-history calls are expensive; cap requests to avoid rate-limits.
            const SIG_PAGE_SIZE: usize = 200;
            const SIG_MAX_PAGES: usize = 100; // up to ~20k signatures
            const SIG_TX_PER_PAGE: usize = 40; // cap getTransaction calls per page
            let addr_pk = match Pubkey::from_str(&addr) {
                Ok(p) => p,
                Err(_) => continue,
            };
            let mut before: Option<Signature> = None;

            for _page in 0..SIG_MAX_PAGES {
                let sigs = match self
                    .rpc
                    .get_signatures_for_address_with_config(
                        &addr_pk,
                        Some(SIG_PAGE_SIZE),
                        before.as_ref(),
                        None,
                    )
                    .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        discovery_err = Some(anyhow!("{e}"));
                        break;
                    }
                };
                if sigs.is_empty() {
                    break;
                }

                // Update pagination cursor
                before = sigs.last().and_then(|s| s.signature.parse().ok());

                let page_len = sigs.len();
                let take_n = SIG_TX_PER_PAGE.min(page_len);
                let step = if take_n <= 1 {
                    1
                } else {
                    (page_len - 1) / (take_n - 1)
                };

                for i in 0..take_n {
                    let idx = (i * step).min(page_len.saturating_sub(1));
                    let s = &sigs[idx];
                    if s.err.is_some() {
                        continue;
                    }
                    let sig = s.signature.to_string();

                    let tx_v = match self.fetch_tx_as_value(&sig).await {
                        Ok(v) => v,
                        Err(e) => {
                            discovery_err = Some(anyhow!("{e}"));
                            break;
                        }
                    };

                    let msg = match tx_v
                        .get("result")
                        .and_then(|r| r.get("transaction"))
                        .and_then(|t| t.get("message"))
                    {
                        Some(v) => v,
                        None => continue,
                    };

                    let meta = tx_v
                        .get("result")
                        .and_then(|r| r.get("meta"))
                        .unwrap_or(&Value::Null);

                    let mut account_keys = match Self::parse_account_keys(msg) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    Self::extend_with_loaded_addresses(&mut account_keys, meta);

                    for ix in Self::collect_all_instructions(msg, meta) {
                        let Some(program_id) =
                            Self::program_id_str_from_instruction_json(ix, &account_keys)
                        else {
                            continue;
                        };
                        if program_id != PUMPFUN_AMM_PROGRAM_ID {
                            continue;
                        }

                        let Some(acc_strings) =
                            Self::pump_amm_ix_account_strings_from_json(ix, &account_keys)
                        else {
                            continue;
                        };
                        let Some(ix_data) = Self::pump_amm_ix_data_from_json(ix) else {
                            continue;
                        };
                        let Some(mut pool) = Self::pump_amm_pool_static_from_parsed_swap_ix(
                            &acc_strings,
                            &ix_data,
                            |_| {
                                Self::pump_amm_tx_history_diagnostic_local_paginated_scan(
                                    "tx_history_paginated_address_scan",
                                    force_refresh,
                                    Pubkey::default(),
                                    base_mint,
                                    Some(sig.clone()),
                                )
                            },
                        ) else {
                            continue;
                        };

                        if pool.base_mint != base_mint {
                            continue;
                        }
                        if pool.quote_mint != Pubkey::from_str(WSOL_MINT).unwrap_or_default() {
                            continue;
                        }

                        let pump_amm_program = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID)?;
                        let expected_fee_program = Pubkey::from_str(PUMPFUN_AMM_FEE_PROGRAM_ID)?;
                        if pool.fee_program != expected_fee_program {
                            continue;
                        }
                        let Some((fee_owner, fee_executable)) = self
                            .rpc_get_account_owner_and_executable(pool.fee_config)
                            .await?
                        else {
                            continue;
                        };
                        let fee_ok = if acc_strings.len() == 23 {
                            !fee_executable && fee_owner == pump_amm_program
                        } else {
                            !fee_executable && fee_owner == expected_fee_program
                        };
                        if !fee_ok {
                            continue;
                        }

                        pool.last_parse_diagnostics =
                            Some(Self::pump_amm_tx_history_diagnostic_local_paginated_scan(
                                "tx_history_paginated_address_scan",
                                force_refresh,
                                pool.pool_market,
                                base_mint,
                                Some(sig.clone()),
                            ));

                        self.pools_by_base.insert(base_mint, pool.clone());
                        self.pools_by_market.insert(pool.pool_market, base_mint);
                        return Ok(Some(pool));
                    }
                }

                if discovery_err.is_some() {
                    break;
                }

                // Small delay between pages to reduce rate-limit pressure.
                sleep(Duration::from_millis(200)).await;

                if discovery_err.is_some() {
                    break;
                }
            }

            if discovery_err.is_some() {
                break;
            }
        }

        if let Some(e) = discovery_err {
            return Err(anyhow!(e).context("pump_amm pool discovery failed"));
        }

        Ok(None)
    }

    async fn discover_user_accounts(
        &self,
        pool_market: Pubkey,
        base_mint: Pubkey,
        user: Pubkey,
    ) -> Result<Option<PumpAmmUserAccounts>> {
        if let Some(v) = self.user_accounts.get(&(pool_market, user)) {
            return Ok(Some(v.clone()));
        }

        // Scan transactions of the user for a Pump.fun AMM ix on this pool.
        // Scan deeper because recent txs may be unrelated (or failed).
        let sigs = self
            .rpc
            .get_signatures_for_address(&user, Some(500))
            .await
            .map_err(|e| anyhow!("getSignaturesForAddress failed: {e}"))?;

        for s in &sigs {
            if s.err.is_some() {
                continue;
            }
            let sig = s.signature.to_string();

            let tx_v = match self.fetch_tx_as_value(&sig).await {
                Ok(v) => v,
                Err(_) => continue,
            };

            let msg = match tx_v
                .get("result")
                .and_then(|r| r.get("transaction"))
                .and_then(|t| t.get("message"))
            {
                Some(v) => v,
                None => continue,
            };

            let meta = tx_v
                .get("result")
                .and_then(|r| r.get("meta"))
                .unwrap_or(&Value::Null);

            let mut account_keys = match Self::parse_account_keys(msg) {
                Ok(v) => v,
                Err(_) => continue,
            };
            Self::extend_with_loaded_addresses(&mut account_keys, meta);

            for ix in Self::collect_all_instructions(msg, meta) {
                let Some(program_id) =
                    Self::program_id_str_from_instruction_json(ix, &account_keys)
                else {
                    continue;
                };
                if program_id != PUMPFUN_AMM_PROGRAM_ID {
                    continue;
                }

                let Some(acc_strings) =
                    Self::pump_amm_ix_account_strings_from_json(ix, &account_keys)
                else {
                    continue;
                };
                if !pump_amm_sell_ix_account_len_supported(acc_strings.len()) {
                    continue;
                }

                let pool_ix = Pubkey::from_str(acc_strings[0].as_str())?;
                if pool_ix != pool_market {
                    continue;
                }
                let base_ix = Pubkey::from_str(acc_strings[3].as_str())?;
                if base_ix != base_mint {
                    continue;
                }

                let program_id = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID)?;
                let ua = PumpAmmUserAccounts {
                    user_base_ta: Pubkey::from_str(acc_strings[5].as_str())?,
                    user_quote_ta: Pubkey::from_str(acc_strings[6].as_str())?,
                    // SELL layouts place fee/pre-fee metas at #19/#20, not user_volume (BUY #19).
                    user_volume_accumulator: Self::derive_user_volume_accumulator(
                        program_id,
                        pool_market,
                        user,
                    ),
                };
                self.user_accounts.insert((pool_market, user), ua.clone());
                return Ok(Some(ua));
            }
        }

        // No prior tx found for this user/pool. Fall back to deterministic ATAs and a derived
        // user-volume PDA. This matches the on-chain account layout we observed, and allows new
        // wallets to trade without requiring historical lookups.
        let program_id = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID)?;
        let ua = PumpAmmUserAccounts {
            user_base_ta: Self::derive_ata(user, base_mint),
            user_quote_ta: Self::derive_ata(user, Pubkey::from_str(WSOL_MINT)?),
            user_volume_accumulator: Self::derive_user_volume_accumulator(
                program_id,
                pool_market,
                user,
            ),
        };
        self.user_accounts.insert((pool_market, user), ua.clone());
        Ok(Some(ua))
    }

    fn parse_account_keys(msg: &Value) -> Result<Vec<String>> {
        let keys = msg
            .get("accountKeys")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("missing message.accountKeys"))?;
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            if let Some(s) = k.as_str() {
                out.push(s.to_string());
            } else if let Some(obj) = k.as_object() {
                let s = obj
                    .get("pubkey")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow!("accountKeys element missing pubkey"))?;
                out.push(s.to_string());
            } else {
                return Err(anyhow!("unexpected accountKeys element: {k}"));
            }
        }
        Ok(out)
    }

    fn extend_with_loaded_addresses(out: &mut Vec<String>, meta: &Value) {
        let Some(loaded) = meta.get("loadedAddresses") else {
            return;
        };

        if let Some(w) = loaded.get("writable").and_then(|v| v.as_array()) {
            for k in w.iter().filter_map(|v| v.as_str()) {
                out.push(k.to_string());
            }
        }

        if let Some(r) = loaded.get("readonly").and_then(|v| v.as_array()) {
            for k in r.iter().filter_map(|v| v.as_str()) {
                out.push(k.to_string());
            }
        }
    }

    fn collect_all_instructions<'a>(msg: &'a Value, meta: &'a Value) -> Vec<&'a Value> {
        let mut out: Vec<&'a Value> = Vec::new();

        if let Some(ixs) = msg.get("instructions").and_then(|v| v.as_array()) {
            out.extend(ixs.iter());
        }

        if let Some(inner) = meta.get("innerInstructions").and_then(|v| v.as_array()) {
            for entry in inner {
                if let Some(ixs) = entry.get("instructions").and_then(|v| v.as_array()) {
                    out.extend(ixs.iter());
                }
            }
        }

        out
    }

    /// `getTransaction` with `encoding: jsonParsed` uses string `programId` and omits `programIdIndex`
    /// on many inner instructions. Resolve to the same base58 string as `message.accountKeys` entries.
    fn program_id_str_from_instruction_json<'a>(
        ix: &'a Value,
        message_account_keys: &'a [String],
    ) -> Option<&'a str> {
        let ix = Self::pump_amm_instruction_json_body(ix);
        if let Some(s) = ix.get("programId").and_then(|v| v.as_str()) {
            return Some(s);
        }
        let idx = ix.get("programIdIndex").and_then(|v| v.as_u64())? as usize;
        message_account_keys.get(idx).map(|s| s.as_str())
    }

    /// Unwrap validator `jsonParsed` instruction envelopes (`Compiled` / `Parsed`).
    fn pump_amm_instruction_json_body(ix: &Value) -> &Value {
        if let Some(inner) = ix.get("Compiled") {
            return inner;
        }
        if let Some(inner) = ix.get("Parsed") {
            return inner;
        }
        ix
    }

    fn pump_amm_ix_data_from_json(ix: &Value) -> Option<Vec<u8>> {
        let ix = Self::pump_amm_instruction_json_body(ix);
        let d = ix.get("data")?;
        if let Some(s) = d.as_str() {
            return bs58::decode(s).into_vec().ok();
        }
        if let Some(arr) = d.as_array() {
            let mut out = Vec::with_capacity(arr.len());
            for v in arr {
                let b = v.as_u64()? as u8;
                out.push(b);
            }
            return Some(out);
        }
        None
    }

    /// Decode `instruction.accounts` from JSON-RPC / Helius transaction shapes.
    ///
    /// Standard Solana encoding uses a **u64 index array** into `message.accountKeys` (plus loaded
    /// addresses). Some providers instead return **base58 pubkey strings** for the same field.
    /// PumpSwap observers must accept both or SELL-layout discovery falsely returns `Unknown`.
    fn pump_amm_ix_account_strings_from_json(
        ix: &Value,
        message_account_keys: &[String],
    ) -> Option<Vec<String>> {
        let ix = Self::pump_amm_instruction_json_body(ix);
        let arr = ix.get("accounts")?.as_array()?;
        if arr.is_empty() {
            return None;
        }
        if arr[0].as_str().is_some() {
            let mut out = Vec::with_capacity(arr.len());
            for v in arr {
                out.push(v.as_str()?.to_string());
            }
            return Some(out);
        }
        let mut out = Vec::with_capacity(arr.len());
        for v in arr {
            let idx = v.as_u64()? as usize;
            out.push(message_account_keys.get(idx)?.clone());
        }
        Some(out)
    }

    /// Parse `buy_exact_quote_in` (23 accounts) or `sell` (21 or 24 accounts) into static pool fields.
    /// `acc_accounts` must be the resolved account list (indices expanded into base58 strings, or
    /// direct pubkey strings from RPC).
    fn pump_amm_pool_static_from_parsed_swap_ix(
        acc_accounts: &[String],
        ix_data: &[u8],
        diag_factory: impl FnOnce(Option<String>) -> PumpAmmPoolAccountsDiagnostic,
    ) -> Option<PumpAmmPoolStatic> {
        if ix_data.len() < 8 {
            return None;
        }
        let disc: [u8; 8] = ix_data[0..8].try_into().ok()?;
        let buy_disc = anchor_disc("buy_exact_quote_in");
        let sell_disc = anchor_disc("sell");

        let pump_amm_program = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID).ok()?;
        let singleton_gva = pump_amm_singleton_global_volume_accumulator(&pump_amm_program);

        let parse_pk = |i: usize| -> Option<Pubkey> {
            let s = acc_accounts.get(i)?;
            Pubkey::from_str(s).ok()
        };

        if disc == buy_disc {
            if acc_accounts.len() != 23 {
                return None;
            }
            return Some(PumpAmmPoolStatic {
                pool_market: parse_pk(0)?,
                global_config: parse_pk(2)?,
                base_mint: parse_pk(3)?,
                quote_mint: parse_pk(4)?,
                pool_base_vault: parse_pk(7)?,
                pool_quote_vault: parse_pk(8)?,
                protocol_fee_recipient: parse_pk(9)?,
                protocol_fee_recipient_ta: parse_pk(10)?,
                event_authority: parse_pk(15)?,
                coin_creator_vault_ata: parse_pk(17)?,
                coin_creator_vault_authority: parse_pk(18)?,
                global_volume_accumulator: parse_pk(16)?,
                fee_config: parse_pk(20)?,
                fee_program: parse_pk(21)?,
                sell_requires_cashback_remaining: false,
                sell_cashback_third_meta: None,
                sell_extended_tail_0: None,
                sell_extended_tail_1: None,
                sell_extended_fee_tail_0: None,
                sell_extended_fee_tail_1: None,
                sell_requires_pre_fee_metas: false,
                sell_pre_fee_meta_1: None,
                last_parse_diagnostics: Some(diag_factory(None)),
            });
        }

        if disc == sell_disc {
            if !pump_amm_sell_ix_account_len_supported(acc_accounts.len()) {
                return None;
            }
            let pool_market = parse_pk(0)?;
            let base_mint = parse_pk(3)?;
            let ext = pump_amm_sell_extended_fields_from_ix_accounts(
                &acc_accounts
                    .iter()
                    .filter_map(|s| Pubkey::from_str(s).ok())
                    .collect::<Vec<_>>(),
            )?;
            let filter_pk = |p: Option<Pubkey>| p.filter(|pk| *pk != Pubkey::default());
            let (third, tail_0, tail_1, fee_t0, fee_t1) = if ext.requires_extended {
                (
                    filter_pk(ext.third_meta),
                    filter_pk(ext.tail_0),
                    filter_pk(ext.tail_1),
                    filter_pk(ext.fee_tail_0),
                    filter_pk(ext.fee_tail_1),
                )
            } else {
                (None, None, None, None, None)
            };
            let (fee_config_ix, fee_program_ix) =
                pump_amm_sell_ix_uses_global_fee_at(acc_accounts.len())?;
            let gva = if ext.requires_pre_fee_metas {
                filter_pk(ext.pre_fee_meta_0).unwrap_or(singleton_gva)
            } else {
                singleton_gva
            };
            return Some(PumpAmmPoolStatic {
                pool_market,
                global_config: parse_pk(2)?,
                base_mint,
                quote_mint: parse_pk(4)?,
                pool_base_vault: parse_pk(7)?,
                pool_quote_vault: parse_pk(8)?,
                protocol_fee_recipient: parse_pk(9)?,
                protocol_fee_recipient_ta: parse_pk(10)?,
                event_authority: parse_pk(15)?,
                coin_creator_vault_ata: parse_pk(17)?,
                coin_creator_vault_authority: parse_pk(18)?,
                global_volume_accumulator: gva,
                fee_config: parse_pk(fee_config_ix)?,
                fee_program: parse_pk(fee_program_ix)?,
                sell_requires_cashback_remaining: ext.requires_extended,
                sell_cashback_third_meta: third,
                sell_extended_tail_0: tail_0,
                sell_extended_tail_1: tail_1,
                sell_extended_fee_tail_0: fee_t0,
                sell_extended_fee_tail_1: fee_t1,
                sell_requires_pre_fee_metas: ext.requires_pre_fee_metas,
                sell_pre_fee_meta_1: filter_pk(ext.pre_fee_meta_1),
                last_parse_diagnostics: Some(diag_factory(None)),
            });
        }

        None
    }

    /// Parse a successful pool-matching `sell` ix into layout + authoritative protocol fee metas (#9/#10).
    fn pump_amm_sell_reference_observation_from_parsed_swap_ix(
        acc_accounts: &[String],
        ix_data: &[u8],
        reference_swap_signature: Option<String>,
    ) -> Option<(Pubkey, Pubkey, PumpAmmSellReferenceObservation)> {
        let pool = Self::pump_amm_pool_static_from_parsed_swap_ix(acc_accounts, ix_data, |_| {
            Self::pump_amm_livepoolcache_diagnostic(
                "sell_layout_observation",
                true,
                Pubkey::default(),
                Pubkey::default(),
            )
        })?;
        let layout = if !pool.sell_requires_cashback_remaining {
            PumpAmmAuthoritativeSellLayout::Base
        } else {
            PumpAmmAuthoritativeSellLayout::Extended {
                pre_fee_0: if pool.sell_requires_pre_fee_metas {
                    Some(pool.global_volume_accumulator)
                } else {
                    None
                },
                pre_fee_1: pool.sell_pre_fee_meta_1,
                tail_0: pool
                    .sell_extended_tail_0
                    .filter(|p| *p != Pubkey::default())?,
                tail_1: pool
                    .sell_extended_tail_1
                    .filter(|p| *p != Pubkey::default())?,
                tail_2: pool
                    .sell_cashback_third_meta
                    .filter(|p| *p != Pubkey::default())?,
                fee_tail_0: pool.sell_extended_fee_tail_0,
                fee_tail_1: pool.sell_extended_fee_tail_1,
            }
        };
        let obs = PumpAmmSellReferenceObservation {
            layout,
            protocol_fee_recipient: pool.protocol_fee_recipient,
            protocol_fee_recipient_ta: pool.protocol_fee_recipient_ta,
            reference_swap_signature,
        };
        Some((pool.pool_market, pool.base_mint, obs))
    }

    #[cfg(test)]
    fn pump_amm_sell_layout_observation_from_parsed_swap_ix(
        acc_accounts: &[String],
        ix_data: &[u8],
    ) -> Option<(Pubkey, Pubkey, PumpAmmAuthoritativeSellLayout)> {
        Self::pump_amm_sell_reference_observation_from_parsed_swap_ix(acc_accounts, ix_data, None)
            .map(|(pm, bm, o)| (pm, bm, o.layout))
    }

    async fn get_vault_amount(&self, ta: Pubkey) -> Result<u64> {
        let acc = self
            .rpc
            .get_account_opt_retry(&ta)
            .await
            .map_err(|e| anyhow!("get_account failed: {e}"))?
            .ok_or_else(|| anyhow!("token account {ta} not found"))?;
        Self::parse_spl_token_account_amount(&acc.data)
            .ok_or_else(|| anyhow!("invalid token account data for {ta}"))
    }

    /// Cold Path only: authoritative SPL token balances for PumpSwap pool vaults.
    ///
    /// `pool_accounts` must follow the canonical layout where `[4]` is `pool_base_vault` and
    /// `[5]` is `pool_quote_vault` (same as `PumpAmmState::pool_base_token_account` / quote).
    /// Used after successful I-24d discovery so JetStream/SLAVE caches receive non-degenerate
    /// reserves without local healing in execution-engine.
    pub async fn fetch_pump_amm_vault_reserves(
        &self,
        pool_accounts: &[Pubkey],
    ) -> Result<(u64, u64)> {
        if pool_accounts.len() < 6 {
            return Err(anyhow!(
                "pool_accounts len {} < 6 (cannot read vault pubkeys)",
                pool_accounts.len()
            ));
        }
        let base_vault = pool_accounts[4];
        let quote_vault = pool_accounts[5];
        let (base_res, quote_res) = tokio::try_join!(
            self.get_vault_amount(base_vault),
            self.get_vault_amount(quote_vault),
        )?;
        Ok((base_res, quote_res))
    }

    fn quote_cp(
        &self,
        amount_in: u64,
        in_reserve: u128,
        out_reserve: u128,
        fee_bps: u32,
    ) -> (u64, u32) {
        if in_reserve == 0 || out_reserve == 0 {
            return (0, 0);
        }
        let amt_in_post_fee = (amount_in as u128)
            .saturating_mul((10_000u32.saturating_sub(fee_bps)) as u128)
            / 10_000u128;
        if amt_in_post_fee == 0 {
            return (0, 0);
        }
        let out = (amt_in_post_fee.saturating_mul(out_reserve))
            / (in_reserve.saturating_add(amt_in_post_fee));
        let out_u64 = out.min(u64::MAX as u128) as u64;

        // Rough impact approximation: in / in_reserve
        let impact = ((amount_in as u128) * 10_000u128 / in_reserve).min(10_000u128) as u32;
        (out_u64, impact)
    }

    fn build_ix_data(disc: [u8; 8], a: u64, b: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + 16);
        out.extend_from_slice(&disc);
        out.extend_from_slice(&a.to_le_bytes());
        out.extend_from_slice(&b.to_le_bytes());
        out
    }

    fn derive_ata(owner: Pubkey, mint: Pubkey) -> Pubkey {
        // Default to SPL Token if no specific program provided
        let spl_token_program = Pubkey::new_from_array(spl_token::id().to_bytes());
        Self::derive_ata_with_program(owner, mint, spl_token_program)
    }
}

#[async_trait]
impl Dex for PumpFunAmmDex {
    async fn refresh_pools(&self) -> Result<()> {
        Ok(())
    }

    /// Load a single pool by its market address (pool_address) via getAccount RPC.
    ///
    /// COLD PATH ONLY. For PumpFunAmm, the primary path uses LivePoolCache (Geyser);
    /// this RPC-based load is for Multi-Hop / fallback when cache misses.
    /// NOT getProgramAccounts - no scanning.
    async fn load_pool_by_address(&self, pool_address: &Pubkey) -> Result<()> {
        // Check if already cached via market index
        if self.pools_by_market.contains_key(pool_address) {
            debug!("pump_amm pool {} already in cache", pool_address);
            return Ok(());
        }

        debug!(
            "Loading pump_amm pool {} via single getAccount",
            pool_address
        );

        // Fetch the pool market account to get base_mint
        let pump_amm_program = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID)?;
        let expected_quote_mint = Pubkey::from_str(WSOL_MINT)?;

        let account = self
            .rpc
            .get_account_retry(pool_address)
            .await
            .map_err(|e| anyhow!("Failed to fetch pump_amm pool {}: {}", pool_address, e))?;

        if account.owner != pump_amm_program {
            return Err(anyhow!(
                "pump_amm pool {} has wrong owner: {}",
                pool_address,
                account.owner
            ));
        }

        // Parse base_mint from market account data
        if account.data.len() < PUMPFUN_AMM_MARKET_MIN_DATA_LEN {
            return Err(anyhow!(
                "pump_amm pool {} data too short: {} < {}",
                pool_address,
                account.data.len(),
                PUMPFUN_AMM_MARKET_MIN_DATA_LEN
            ));
        }

        let base_mint = Pubkey::new_from_array(
            account.data[PUMPFUN_AMM_MARKET_BASE_MINT_OFFSET as usize
                ..(PUMPFUN_AMM_MARKET_BASE_MINT_OFFSET as usize + 32)]
                .try_into()
                .map_err(|_| anyhow!("pump_amm market base_mint slice"))?,
        );
        let quote_mint = Pubkey::new_from_array(
            account.data[PUMPFUN_AMM_MARKET_QUOTE_MINT_OFFSET as usize
                ..(PUMPFUN_AMM_MARKET_QUOTE_MINT_OFFSET as usize + 32)]
                .try_into()
                .map_err(|_| anyhow!("pump_amm market quote_mint slice"))?,
        );

        // PumpSwap AMM only supports WSOL quote
        if quote_mint != expected_quote_mint {
            return Err(anyhow!(
                "pump_amm pool {} has unexpected quote_mint: {} (expected WSOL)",
                pool_address,
                quote_mint
            ));
        }

        // Use existing method to parse full pool structure.
        // Pass pre-fetched data to avoid redundant RPC call that can fail
        // due to transient RPC endpoint inconsistencies.
        let prefetched = (account.owner, account.executable, account.data);
        match self
            .try_parse_pool_static_from_market_account_inner(
                *pool_address,
                base_mint,
                Some(prefetched),
                Some(("rpc_market_parse_single_pool_account", false)),
            )
            .await
        {
            Ok(PumpAmmMarketParseOutcome::Ok(pool)) => {
                self.pools_by_base.insert(base_mint, pool.clone());
                self.pools_by_market.insert(*pool_address, base_mint);
                debug!(
                    "Loaded pump_amm pool {}: base_mint={} via single RPC call",
                    pool_address, base_mint
                );
                Ok(())
            }
            Ok(PumpAmmMarketParseOutcome::LocalFail(reason)) => {
                // Market-account heuristics failed (common for Token-2022 pools or
                // pools with non-standard account layouts).
                // Fallback: parse instruction accounts from a real on-chain swap tx.
                warn!(
                    pool = %pool_address,
                    base_mint = %base_mint,
                    local_parse_fail_reason = %reason,
                    "pump_amm load_pool_by_address: market-account parse failed, trying TX-history fallback"
                );
                match self
                    .discover_pool_static_via_tx_history_market_only(*pool_address, base_mint, false)
                    .await
                {
                    Ok(Some(pool)) => {
                        info!(
                            pool = %pool_address,
                            base_mint = %base_mint,
                            "pump_amm load_pool_by_address: TX-history fallback SUCCESS"
                        );
                        self.pools_by_base.insert(base_mint, pool.clone());
                        self.pools_by_market.insert(*pool_address, base_mint);
                        Ok(())
                    }
                    Ok(None) => Err(anyhow!(
                        "pump_amm pool {} could not be parsed (local_parse={}, TX-history also None)",
                        pool_address,
                        reason
                    )),
                    Err(e) => Err(anyhow!(
                        "pump_amm pool {} local_parse={}, TX-history fallback failed: {}",
                        pool_address,
                        reason,
                        e
                    )),
                }
            }
            Err(e) => {
                // Market-account parse produced an error. Also try TX-history.
                warn!(
                    pool = %pool_address,
                    base_mint = %base_mint,
                    error = %e,
                    "pump_amm load_pool_by_address: market-account parse error, trying TX-history fallback"
                );
                match self
                    .discover_pool_static_via_tx_history_market_only(
                        *pool_address,
                        base_mint,
                        false,
                    )
                    .await
                {
                    Ok(Some(pool)) => {
                        info!(
                            pool = %pool_address,
                            base_mint = %base_mint,
                            "pump_amm load_pool_by_address: TX-history fallback SUCCESS (after market-account error)"
                        );
                        self.pools_by_base.insert(base_mint, pool.clone());
                        self.pools_by_market.insert(*pool_address, base_mint);
                        Ok(())
                    }
                    Ok(None) => Err(anyhow!(
                        "pump_amm pool {} parse failed: {} (TX-history also None)",
                        pool_address,
                        e
                    )),
                    Err(e2) => Err(anyhow!(
                        "pump_amm pool {} parse failed: {} (TX-history also failed: {})",
                        pool_address,
                        e,
                        e2
                    )),
                }
            }
        }
    }

    /// Set pool data directly from accounts list (NO RPC!)
    ///
    /// Accounts format (v1 from DexPoolAccounts, 14 elements):
    /// [0] pool_market
    /// [1] global_config
    /// [2] base_mint
    /// [3] quote_mint
    /// [4] pool_base_vault
    /// [5] pool_quote_vault
    /// [6] protocol_fee_recipient (from Geyser/RPC/intent; must not be overwritten with a global constant)
    /// [7] protocol_fee_recipient_ta (observed fee TA for this pool)
    /// [8] event_authority
    /// [9] coin_creator_vault_ata
    /// [10] coin_creator_vault_authority
    /// [11] global_volume_accumulator
    /// [12] fee_config
    /// [13] fee_program
    fn set_pool_from_accounts(&self, pool_address: &str, accounts: &[String]) -> Result<()> {
        // Accept both 14-element (v1 with volume accumulators) and 12-element (v2 without)
        if accounts.len() < 12 {
            return Err(anyhow!(
                "pump_amm set_pool_from_accounts requires at least 12 accounts, got {}",
                accounts.len()
            ));
        }

        let parse_pubkey = |s: &str, name: &str| -> Result<Pubkey> {
            Pubkey::from_str(s).map_err(|e| anyhow!("Invalid {} pubkey '{}': {}", name, s, e))
        };

        let pool_market = parse_pubkey(&accounts[0], "pool_market")?;
        let global_config = Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG)?;
        if accounts.len() > 1 {
            if let Ok(parsed_gc) = parse_pubkey(&accounts[1], "global_config") {
                if parsed_gc != global_config {
                    warn!(
                        intent = %parsed_gc,
                        expected = %global_config,
                        "pump_amm set_pool_from_accounts: accounts[1] != canonical global_config; using canonical"
                    );
                }
            }
        }
        let base_mint = parse_pubkey(&accounts[2], "base_mint")?;
        let quote_mint = parse_pubkey(&accounts[3], "quote_mint")?;
        let pool_base_vault = parse_pubkey(&accounts[4], "pool_base_vault")?;
        let pool_quote_vault = parse_pubkey(&accounts[5], "pool_quote_vault")?;
        let parsed_protocol_fee_recipient = parse_pubkey(&accounts[6], "protocol_fee_recipient")?;
        let parsed_protocol_fee_recipient_ta =
            parse_pubkey(&accounts[7], "protocol_fee_recipient_ta")?;
        // PumpSwap only uses WSOL quote; fee recipient ATA uses SPL Token program for quote mint.
        let quote_token_program_for_fee = Pubkey::new_from_array(spl_token::id().to_bytes());
        let (protocol_fee_recipient, protocol_fee_recipient_ta) =
            pump_amm_resolve_protocol_fee_accounts(
                parsed_protocol_fee_recipient,
                parsed_protocol_fee_recipient_ta,
                quote_mint,
                quote_token_program_for_fee,
            )?;
        let pump_amm_program = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID)?;
        let parsed_event_authority = parse_pubkey(&accounts[8], "event_authority")?;
        let event_authority = pump_amm_canonical_event_authority(&pump_amm_program);
        if parsed_event_authority != event_authority {
            warn!(
                intent = %parsed_event_authority,
                expected = %event_authority,
                "pump_amm set_pool_from_accounts: accounts[8] != canonical event_authority; using canonical"
            );
        }
        let coin_creator_vault_ata = parse_pubkey(&accounts[9], "coin_creator_vault_ata")?;
        let coin_creator_vault_authority =
            parse_pubkey(&accounts[10], "coin_creator_vault_authority")?;

        // CRITICAL: v1 format (14 accounts) vs v2 format (12 accounts) have different layouts!
        // v1: [0..10]=common, [11]=global_volume_accumulator, [12]=fee_config, [13]=fee_program
        // v2: [0..10]=common, [11]=fee_config (no volume accumulators)
        let (global_volume_accumulator, fee_config, fee_program) = if accounts.len() >= 14 {
            // v1 format: 14 accounts with volume accumulators
            let gva = parse_pubkey(&accounts[11], "global_volume_accumulator")?;
            let fc = parse_pubkey(&accounts[12], "fee_config")?;
            let fp = parse_pubkey(&accounts[13], "fee_program")?;
            info!(
                accounts_len = accounts.len(),
                format = "v1",
                fee_config = %fc,
                fee_program = %fp,
                "pump_amm set_pool_from_accounts: parsed v1 format"
            );
            (gva, fc, fp)
        } else {
            // v2 format: 12 accounts without volume accumulators
            let fc = parse_pubkey(&accounts[11], "fee_config")?;
            let fp = Pubkey::from_str(PUMPFUN_AMM_FEE_PROGRAM_ID)?;
            info!(
                accounts_len = accounts.len(),
                format = "v2",
                fee_config = %fc,
                fee_program = %fp,
                "pump_amm set_pool_from_accounts: parsed v2 format"
            );
            (Pubkey::default(), fc, fp)
        };

        // Validate pool_address matches accounts[0]
        let expected_pool = parse_pubkey(pool_address, "pool_address")?;
        if expected_pool != pool_market {
            return Err(anyhow!(
                "pool_address {} does not match accounts[0] {}",
                pool_address,
                pool_market
            ));
        }

        let pool = PumpAmmPoolStatic {
            pool_market,
            global_config,
            base_mint,
            quote_mint,
            pool_base_vault,
            pool_quote_vault,
            protocol_fee_recipient,
            protocol_fee_recipient_ta,
            event_authority,
            coin_creator_vault_ata,
            coin_creator_vault_authority,
            global_volume_accumulator,
            fee_config,
            fee_program,
            sell_requires_cashback_remaining: false,
            sell_cashback_third_meta: None,
            sell_extended_tail_0: None,
            sell_extended_tail_1: None,
            sell_extended_fee_tail_0: None,
            sell_extended_fee_tail_1: None,
            sell_requires_pre_fee_metas: false,
            sell_pre_fee_meta_1: None,
            last_parse_diagnostics: None,
        };

        debug!(
            pool_market = %pool_market,
            base_mint = %base_mint,
            "pump_amm pool set from intent accounts (NO RPC)"
        );

        self.pools_by_base.insert(base_mint, pool);
        self.pools_by_market.insert(pool_market, base_mint);

        Ok(())
    }

    async fn quote_exact_in(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
    ) -> Result<Option<Quote>> {
        // WSOL pairs only.
        let (base_mint_str, is_buy) = if input_mint == WSOL_MINT {
            (output_mint, true)
        } else if output_mint == WSOL_MINT {
            (input_mint, false)
        } else {
            return Ok(None);
        };

        let base_mint = Pubkey::from_str(base_mint_str).context("invalid base mint")?;

        // GEYSER-FIRST: Try LivePoolCache for reserves before any RPC call.
        // The cache is populated by market-data via Geyser account subscriptions
        // and propagated to execution-engine via PoolCacheUpdate JetStream events.
        if let Some(ref cache) = self.live_pool_cache {
            if let Some((base_r, quote_r, pool_market)) =
                cache.get_pump_amm_reserves_by_base_mint(&base_mint)
            {
                let base_reserve = base_r as u128;
                let quote_reserve = quote_r as u128;

                let (in_reserve, out_reserve) = if is_buy {
                    (quote_reserve, base_reserve)
                } else {
                    (base_reserve, quote_reserve)
                };

                let (amount_out, price_impact_bps) =
                    self.quote_cp(amount_in, in_reserve, out_reserve, DEFAULT_TOTAL_FEE_BPS);
                if amount_out == 0 {
                    if self.allow_rpc_on_miss {
                        warn!(
                            base_mint = %base_mint_str,
                            base_reserve = base_r,
                            quote_reserve = quote_r,
                            "pump_amm: cache reserves degenerate (one side=0), Cold Path falling through to RPC"
                        );
                    } else {
                        return Ok(None);
                    }
                } else {
                    debug!(
                        base_mint = %base_mint_str,
                        pool = %pool_market,
                        base_reserve = base_r,
                        quote_reserve = quote_r,
                        amount_out,
                        "pump_amm: quote from LivePoolCache (ZERO RPC)"
                    );

                    return Ok(Some(Quote {
                        amount_out,
                        price_impact_bps,
                        route: vec![pool_market.to_string()],
                        fee_bps: DEFAULT_TOTAL_FEE_BPS,
                        in_reserve,
                        out_reserve,
                        input_mint: input_mint.to_string(),
                        output_mint: output_mint.to_string(),
                        tick_spacing: None,
                    }));
                }
            }
            // Cache miss: Hot Path (allow_rpc_on_miss=false) → None. Cold Path (true) → RPC fallback. P3 #12.
            if !self.allow_rpc_on_miss {
                debug!(base_mint = %base_mint_str, "pump_amm: quote cache miss, returning None (no RPC)");
                return Ok(None);
            }
        }

        // RPC FALLBACK (Cold Path only): No LivePoolCache or allow_rpc_on_miss — discover pool and fetch vault reserves via RPC.
        warn!(
            base_mint = %base_mint_str,
            "pump_amm: no LivePoolCache, using RPC fallback for quote"
        );

        let pool = match self.discover_pool_static(base_mint, false).await? {
            Some(p) => p,
            None => return Ok(None),
        };

        // Read pool vault reserves from the (fast) local RPC.
        let base_reserve = self.get_vault_amount(pool.pool_base_vault).await? as u128;
        let quote_reserve = self.get_vault_amount(pool.pool_quote_vault).await? as u128;

        let (in_reserve, out_reserve) = if is_buy {
            (quote_reserve, base_reserve)
        } else {
            (base_reserve, quote_reserve)
        };

        let (amount_out, price_impact_bps) =
            self.quote_cp(amount_in, in_reserve, out_reserve, DEFAULT_TOTAL_FEE_BPS);
        if amount_out == 0 {
            return Ok(None);
        }

        Ok(Some(Quote {
            amount_out,
            price_impact_bps,
            route: vec![pool.pool_market.to_string()],
            fee_bps: DEFAULT_TOTAL_FEE_BPS,
            in_reserve,
            out_reserve,
            input_mint: input_mint.to_string(),
            output_mint: output_mint.to_string(),
            tick_spacing: None,
        }))
    }

    fn build_swap_ix(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
        min_out: u64,
    ) -> Result<Vec<Instruction>> {
        let user = self
            .user_authority
            .ok_or_else(|| anyhow!("pump_amm user_authority not set"))?;

        let program_id = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID)?;

        let (base_mint_str, is_buy) = if input_mint == WSOL_MINT {
            (output_mint, true)
        } else if output_mint == WSOL_MINT {
            (input_mint, false)
        } else {
            return Err(anyhow!("pump_amm only supports WSOL pairs"));
        };

        let base_mint = Pubkey::from_str(base_mint_str)?;

        // Blocking in sync fn: we rely on caches being primed by quote path.
        // If not present, we fail fast with a clear error.
        let pool = self
            .pools_by_base
            .get(&base_mint)
            .map(|v| v.clone())
            .ok_or_else(|| {
                anyhow!("pump_amm pool not discovered/cached for base_mint={base_mint}")
            })?;

        // Prefer discovered user accounts if available; fallback to ATAs for token accounts.
        let user_acc = self
            .user_accounts
            .get(&(pool.pool_market, user))
            .map(|v| v.clone());
        // Get token program from cache for correct ATA derivation (Token-2022 support)
        let base_token_program = self
            .cached_data
            .get(&format!("token_program:{}", pool.base_mint))
            .and_then(|v| Pubkey::from_str(&v).ok())
            .unwrap_or_else(|| Pubkey::new_from_array(spl_token::id().to_bytes()));
        let quote_token_program = Pubkey::new_from_array(spl_token::id().to_bytes()); // WSOL always uses SPL Token

        let user_base_ta = user_acc
            .as_ref()
            .map(|u| u.user_base_ta)
            .unwrap_or_else(|| {
                Self::derive_ata_with_program(user, pool.base_mint, base_token_program)
            });
        let user_quote_ta = user_acc
            .as_ref()
            .map(|u| u.user_quote_ta)
            .unwrap_or_else(|| {
                Self::derive_ata_with_program(user, pool.quote_mint, quote_token_program)
            });
        let user_volume = user_acc
            .as_ref()
            .map(|u| u.user_volume_accumulator)
            .unwrap_or_else(|| {
                Self::derive_user_volume_accumulator(program_id, pool.pool_market, user)
            });

        let disc = if is_buy {
            anchor_disc("buy_exact_quote_in")
        } else {
            anchor_disc("sell")
        };
        let data = Self::build_ix_data(disc, amount_in, min_out);

        // Account ordering differs between BUY (23 accounts) and SELL (21 accounts).
        // Reference: observed on-chain Pump.fun AMM swap transactions.
        // BUY includes global_volume_accumulator (#16) and user_volume (#19), SELL does not.
        let global_config = Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG)?;
        if pool.global_config != global_config {
            warn!(
                pool = %pool.global_config,
                expected = %global_config,
                "pump_amm build_swap_ix: cached pool.global_config != canonical; using canonical"
            );
        }
        let event_authority = pump_amm_canonical_event_authority(&program_id);
        if pool.event_authority != event_authority {
            warn!(
                pool = %pool.event_authority,
                expected = %event_authority,
                "pump_amm build_swap_ix: cached pool.event_authority != canonical; using canonical"
            );
        }
        // SELL meta #19/#20: global Fee Program accounts (mainnet-constant). Cached `pool.fee_config`
        // may differ from v14/market parse — using it as SELL meta #19 caused Anchor Custom(3002)
        // (wrong account type vs program expectation); Scope 59 restores global constant semantics.
        let sell_fee_config = Pubkey::from_str(PUMPFUN_AMM_FEE_CONFIG)?;
        let sell_fee_program = Pubkey::from_str(PUMPFUN_AMM_FEE_PROGRAM_ID)?;
        let (protocol_fee_recipient, protocol_fee_recipient_ta) =
            pump_amm_resolve_protocol_fee_accounts(
                pool.protocol_fee_recipient,
                pool.protocol_fee_recipient_ta,
                pool.quote_mint,
                quote_token_program,
            )?;
        let mut metas = vec![
            AccountMeta::new(pool.pool_market, false),         // 0
            AccountMeta::new(user, true),                      // 1
            AccountMeta::new_readonly(global_config, false),   // 2
            AccountMeta::new_readonly(pool.base_mint, false),  // 3
            AccountMeta::new_readonly(pool.quote_mint, false), // 4
            AccountMeta::new(user_base_ta, false),             // 5
            AccountMeta::new(user_quote_ta, false),            // 6
            AccountMeta::new(pool.pool_base_vault, false),     // 7
            AccountMeta::new(pool.pool_quote_vault, false),    // 8
            AccountMeta::new_readonly(protocol_fee_recipient, false), // 9
            AccountMeta::new(protocol_fee_recipient_ta, false), // 10
            AccountMeta::new_readonly(base_token_program, false), // 11
            AccountMeta::new_readonly(Pubkey::new_from_array(spl_token::id().to_bytes()), false), // 12
            AccountMeta::new_readonly(
                Pubkey::new_from_array(solana_system_program::id().to_bytes()),
                false,
            ), // 13
            AccountMeta::new_readonly(
                Pubkey::new_from_array(spl_associated_token_account::id().to_bytes()),
                false,
            ), // 14
            AccountMeta::new_readonly(event_authority, false), // 15
        ];

        if is_buy {
            // BUY: accounts 16-22 (23 total)
            metas.push(AccountMeta::new(pool.global_volume_accumulator, false)); // 16
            metas.push(AccountMeta::new(pool.coin_creator_vault_ata, false)); // 17
            metas.push(AccountMeta::new_readonly(
                pool.coin_creator_vault_authority,
                false,
            )); // 18
            metas.push(AccountMeta::new(user_volume, false)); // 19
            metas.push(AccountMeta::new_readonly(pool.fee_config, false)); // 20
            metas.push(AccountMeta::new_readonly(pool.fee_program, false)); // 21
            metas.push(AccountMeta::new_readonly(program_id, false)); // 22
        } else {
            // SELL: 21 base metas; extended path adds three trailing accounts when observed on-chain.
            metas.push(AccountMeta::new_readonly(program_id, false)); // 16
            metas.push(AccountMeta::new(pool.coin_creator_vault_ata, false)); // 17
            metas.push(AccountMeta::new_readonly(
                pool.coin_creator_vault_authority,
                false,
            )); // 18
                // `pools_by_base` can be stale vs monotonic LivePoolCache (Geyser); prefer DashMap for extended SELL.
            let mut sell_requires_cashback_remaining = pool.sell_requires_cashback_remaining;
            let mut sell_cashback_third_meta = pool.sell_cashback_third_meta;
            let mut cached_observed_volume_tail_0 = pool.sell_extended_tail_0;
            let mut cached_observed_volume_tail_1 = pool.sell_extended_tail_1;
            let mut sell_extended_fee_tail_0 = pool.sell_extended_fee_tail_0;
            let mut sell_extended_fee_tail_1 = pool.sell_extended_fee_tail_1;
            let mut sell_requires_pre_fee_metas = pool.sell_requires_pre_fee_metas;
            let mut sell_pre_fee_meta_1 = pool.sell_pre_fee_meta_1;
            if let Some(ref cache) = self.live_pool_cache {
                let (dash_flag, dash_third, dash_t0, dash_t1) =
                    cache.pump_amm_sell_extended_layout(&pool.pool_market);
                let (dash_fee_t0, dash_fee_t1) =
                    cache.pump_amm_sell_fee_tail_layout(&pool.pool_market);
                if dash_flag {
                    sell_requires_cashback_remaining = true;
                    sell_cashback_third_meta = dash_third.or(sell_cashback_third_meta);
                    cached_observed_volume_tail_0 = dash_t0.or(cached_observed_volume_tail_0);
                    cached_observed_volume_tail_1 = dash_t1.or(cached_observed_volume_tail_1);
                    sell_extended_fee_tail_0 = dash_fee_t0.or(sell_extended_fee_tail_0);
                    sell_extended_fee_tail_1 = dash_fee_t1.or(sell_extended_fee_tail_1);
                }
                if cache.pump_amm_sell_requires_pre_fee_metas(&pool.pool_market) {
                    sell_requires_pre_fee_metas = true;
                }
                sell_pre_fee_meta_1 = cache
                    .pump_amm_sell_pre_fee_meta_1(&pool.pool_market)
                    .or(sell_pre_fee_meta_1);
            }
            Self::push_pump_amm_sell_global_fee_metas(
                &mut metas,
                sell_requires_pre_fee_metas,
                pool.global_volume_accumulator,
                sell_pre_fee_meta_1,
                sell_fee_config,
                sell_fee_program,
            )?;
            if sell_requires_cashback_remaining {
                let Some(third) = sell_cashback_third_meta.filter(|p| *p != Pubkey::default())
                else {
                    return Err(anyhow!(
                        "pump_amm SELL: extended layout required but sell_cashback_third_meta missing (authoritative observation required)"
                    ));
                };
                Self::push_pump_amm_sell_extended_trailing_metas(
                    &mut metas,
                    user,
                    pool.quote_mint,
                    quote_token_program,
                    third,
                    sell_requires_pre_fee_metas,
                    sell_extended_fee_tail_0,
                    sell_extended_fee_tail_1,
                    cached_observed_volume_tail_0,
                    cached_observed_volume_tail_1,
                )?;
            }
        }

        Ok(vec![Instruction {
            program_id,
            accounts: metas,
            data,
        }])
    }

    fn list_pairs(&self) -> Vec<(String, String)> {
        Vec::new()
    }

    fn cache_extra_data(&self, key: &str, value: &str) {
        self.cached_data.insert(key.to_string(), value.to_string());
    }
}

impl PumpFunAmmDex {
    /// Build a Pump.fun AMM (PumpSwap) swap instruction purely from static pool accounts.
    ///
    /// This is the intent-driven path: execution-engine can plan/simulate without any
    /// on-chain discovery (no tx-history scans, no Helius).
    ///
    /// `pool_accounts` must be the v1 ordered list produced by market-data
    /// (MarketEventKind::DexPoolAccounts):
    /// [0] pool_market
    /// [1] global_config
    /// [2] base_mint
    /// [3] quote_mint
    /// [4] pool_base_vault
    /// [5] pool_quote_vault
    /// [6] protocol_fee_recipient (observed for this pool; used as ix meta #9)
    /// [7] protocol_fee_recipient_ta (observed fee TA; ix meta #10)
    /// [8] event_authority (cache slot; builders use canonical `__event_authority` PDA for ix metas)
    /// [9] coin_creator_vault_ata
    /// [10] coin_creator_vault_authority
    /// [11] global_volume_accumulator
    /// [12] fee_config
    /// [13] fee_program
    ///
    /// `base_token_program` - Optional token program override for the base token.
    /// Use `Some(TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb)` for Token-2022 tokens.
    /// Defaults to SPL Token if None.
    ///
    /// Stable public API (Eval / external callers): extended SELL without observed ix tail #21/#22
    /// uses the legacy derived first-two metas + `sell_cashback_third_meta`. For the Scope 61
    /// observed-tail path, use [`Self::build_swap_ix_from_pool_accounts_with_extended_tail`].
    #[allow(clippy::too_many_arguments)]
    pub fn build_swap_ix_from_pool_accounts(
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
        min_out: u64,
        user: Pubkey,
        pool_accounts: &[Pubkey],
        base_token_program: Option<Pubkey>,
        sell_requires_cashback_remaining: bool,
        sell_cashback_third_meta: Option<Pubkey>,
    ) -> Result<Vec<Instruction>> {
        Self::build_swap_ix_from_pool_accounts_with_extended_tail(
            input_mint,
            output_mint,
            amount_in,
            min_out,
            user,
            pool_accounts,
            base_token_program,
            sell_requires_cashback_remaining,
            sell_cashback_third_meta,
            false,
            None,
            None,
            None,
            None,
            None,
        )
    }

    /// Same as [`Self::build_swap_ix_from_pool_accounts`] plus optional observed extended SELL
    /// fee tail (#24/#25). `sell_extended_tail_0/1` are ignored for build (user volume metas
    /// #21/#22 are always derived for `user`). Cold-path / tx_builder only.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build_swap_ix_from_pool_accounts_with_extended_tail(
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
        min_out: u64,
        user: Pubkey,
        pool_accounts: &[Pubkey],
        base_token_program: Option<Pubkey>,
        sell_requires_cashback_remaining: bool,
        sell_cashback_third_meta: Option<Pubkey>,
        sell_requires_pre_fee_metas: bool,
        sell_pre_fee_meta_1: Option<Pubkey>,
        sell_extended_tail_0: Option<Pubkey>,
        sell_extended_tail_1: Option<Pubkey>,
        sell_extended_fee_tail_0: Option<Pubkey>,
        sell_extended_fee_tail_1: Option<Pubkey>,
    ) -> Result<Vec<Instruction>> {
        // Require 14 accounts (v1 format with global_volume_accumulator)
        if pool_accounts.len() < 14 {
            return Err(anyhow!(
                "pump_amm expected at least 14 pool_accounts (v1 format), got {}",
                pool_accounts.len()
            ));
        }

        let program_id = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID)?;
        let expected_fee_program = Pubkey::from_str(PUMPFUN_AMM_FEE_PROGRAM_ID)?;

        let pool_market = pool_accounts[0];
        let global_config = Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG)?;
        if pool_accounts.len() > 1 && pool_accounts[1] != global_config {
            warn!(
                from_pool_accounts = %pool_accounts[1],
                expected = %global_config,
                "pump_amm build_swap_ix_from_pool_accounts: pool_accounts[1] != canonical global_config; using canonical"
            );
        }
        let base_mint = pool_accounts[2];
        let quote_mint = pool_accounts[3];
        let pool_base_vault = pool_accounts[4];
        let pool_quote_vault = pool_accounts[5];
        let event_authority = pump_amm_canonical_event_authority(&program_id);
        if pool_accounts.len() > 8 && pool_accounts[8] != event_authority {
            warn!(
                from_pool_accounts = %pool_accounts[8],
                expected = %event_authority,
                "pump_amm build_swap_ix_from_pool_accounts: pool_accounts[8] != canonical event_authority; using canonical"
            );
        }
        let coin_creator_vault_ata = pool_accounts[9];
        let coin_creator_vault_authority = pool_accounts[10];
        let global_volume_accumulator = pool_accounts[11]; // REQUIRED for BUY!

        // BUY uses pool row fee fields from v14 ([12]/[13]); SELL meta #19/#20 must use global
        // Fee Program accounts (same as successful mainnet reference txs). v14[12] is not always
        // the SELL fee_config account type — using it caused Custom(3002) (Scope 58 regression).
        let parsed_fee_config = pool_accounts[12];
        let parsed_fee_program = pool_accounts[13];
        let buy_fee_config = if parsed_fee_config == Pubkey::default() {
            warn!(
                "pump_amm build_swap_ix_from_pool_accounts: pool_accounts[12] fee_config missing — falling back to PUMPFUN_AMM_FEE_CONFIG constant"
            );
            Pubkey::from_str(PUMPFUN_AMM_FEE_CONFIG)?
        } else {
            parsed_fee_config
        };
        let buy_fee_program = if parsed_fee_program == Pubkey::default() {
            warn!(
                "pump_amm build_swap_ix_from_pool_accounts: pool_accounts[13] fee_program missing — falling back to expected fee program id"
            );
            expected_fee_program
        } else if parsed_fee_program != expected_fee_program {
            warn!(
                from_pool_accounts = %parsed_fee_program,
                expected = %expected_fee_program,
                "pump_amm build_swap_ix_from_pool_accounts: pool_accounts[13] fee_program != expected; using pool_accounts value (authoritative for BUY)"
            );
            parsed_fee_program
        } else {
            parsed_fee_program
        };

        let sell_fee_config = Pubkey::from_str(PUMPFUN_AMM_FEE_CONFIG)?;
        let sell_fee_program = expected_fee_program;

        let (expected_base, is_buy) = if input_mint == WSOL_MINT {
            (output_mint, true)
        } else if output_mint == WSOL_MINT {
            (input_mint, false)
        } else {
            return Err(anyhow!("pump_amm only supports WSOL pairs"));
        };

        let expected_base = Pubkey::from_str(expected_base)?;
        if expected_base != base_mint {
            return Err(anyhow!(
                "pump_amm base_mint mismatch: intent expects {expected_base}, pool_accounts has {base_mint}"
            ));
        }

        // Resolve token programs: use override for base token (Token-2022 support),
        // WSOL always uses SPL Token.
        let base_tp = base_token_program
            .unwrap_or_else(|| Pubkey::new_from_array(spl_token::id().to_bytes()));
        let quote_tp = Pubkey::new_from_array(spl_token::id().to_bytes()); // WSOL always SPL Token

        let parsed_pfr = pool_accounts.get(6).copied().unwrap_or_default();
        let parsed_pfr_ta = pool_accounts.get(7).copied().unwrap_or_default();
        let (protocol_fee_recipient, protocol_fee_recipient_ta) =
            pump_amm_resolve_protocol_fee_accounts(
                parsed_pfr,
                parsed_pfr_ta,
                quote_mint,
                quote_tp,
            )?;

        // User token accounts are deterministic ATAs with correct token program.
        let user_base_ta = Self::derive_ata_with_program(user, base_mint, base_tp);
        let user_quote_ta = Self::derive_ata_with_program(user, quote_mint, quote_tp);

        // User volume accumulator is a PDA; needed for BUY transactions.
        let user_vol = Self::derive_user_volume_accumulator(program_id, pool_market, user);

        let disc = if is_buy {
            anchor_disc("buy_exact_quote_in")
        } else {
            anchor_disc("sell")
        };
        let data = Self::build_ix_data(disc, amount_in, min_out);

        // Account ordering differs between BUY (23 accounts) and SELL (21 accounts).
        // BUY requires global_volume_accumulator and user_volume_accumulator.
        // See dex_parser.rs for reference account ordering from on-chain transactions.
        let metas = if is_buy {
            // BUY: 23 accounts
            vec![
                AccountMeta::new(pool_market, false),                     // 0
                AccountMeta::new(user, true),                             // 1
                AccountMeta::new_readonly(global_config, false),          // 2
                AccountMeta::new_readonly(base_mint, false),              // 3
                AccountMeta::new_readonly(quote_mint, false),             // 4
                AccountMeta::new(user_base_ta, false),                    // 5
                AccountMeta::new(user_quote_ta, false),                   // 6
                AccountMeta::new(pool_base_vault, false),                 // 7
                AccountMeta::new(pool_quote_vault, false),                // 8
                AccountMeta::new_readonly(protocol_fee_recipient, false), // 9
                AccountMeta::new(protocol_fee_recipient_ta, false),       // 10
                AccountMeta::new_readonly(base_tp, false), // 11 - base token program (Token-2022 aware)
                AccountMeta::new_readonly(quote_tp, false), // 12 - quote token program (always SPL)
                AccountMeta::new_readonly(
                    Pubkey::new_from_array(solana_system_program::id().to_bytes()),
                    false,
                ), // 13
                AccountMeta::new_readonly(
                    Pubkey::new_from_array(spl_associated_token_account::id().to_bytes()),
                    false,
                ), // 14
                AccountMeta::new_readonly(event_authority, false), // 15
                AccountMeta::new(global_volume_accumulator, false), // 16 - REQUIRED for BUY!
                AccountMeta::new(coin_creator_vault_ata, false), // 17
                AccountMeta::new_readonly(coin_creator_vault_authority, false), // 18
                AccountMeta::new(user_vol, false),         // 19 - user volume accumulator
                AccountMeta::new_readonly(buy_fee_config, false), // 20
                AccountMeta::new_readonly(buy_fee_program, false), // 21
                AccountMeta::new_readonly(program_id, false), // 22
            ]
        } else {
            // SELL: 21 base metas; some pools require three trailing accounts (mainnet 24-account sells).
            let mut metas = vec![
                AccountMeta::new(pool_market, false),                     // 0
                AccountMeta::new(user, true),                             // 1
                AccountMeta::new_readonly(global_config, false),          // 2
                AccountMeta::new_readonly(base_mint, false),              // 3
                AccountMeta::new_readonly(quote_mint, false),             // 4
                AccountMeta::new(user_base_ta, false),                    // 5
                AccountMeta::new(user_quote_ta, false),                   // 6
                AccountMeta::new(pool_base_vault, false),                 // 7
                AccountMeta::new(pool_quote_vault, false),                // 8
                AccountMeta::new_readonly(protocol_fee_recipient, false), // 9
                AccountMeta::new(protocol_fee_recipient_ta, false),       // 10
                AccountMeta::new_readonly(base_tp, false), // 11 - base token program (Token-2022 aware)
                AccountMeta::new_readonly(quote_tp, false), // 12 - quote token program (always SPL)
                AccountMeta::new_readonly(
                    Pubkey::new_from_array(solana_system_program::id().to_bytes()),
                    false,
                ), // 13
                AccountMeta::new_readonly(
                    Pubkey::new_from_array(spl_associated_token_account::id().to_bytes()),
                    false,
                ), // 14
                AccountMeta::new_readonly(event_authority, false), // 15
                AccountMeta::new_readonly(program_id, false), // 16
                AccountMeta::new(coin_creator_vault_ata, false), // 17
                AccountMeta::new_readonly(coin_creator_vault_authority, false), // 18
            ];
            Self::push_pump_amm_sell_global_fee_metas(
                &mut metas,
                sell_requires_pre_fee_metas,
                global_volume_accumulator,
                sell_pre_fee_meta_1,
                sell_fee_config,
                sell_fee_program,
            )?;
            if sell_requires_cashback_remaining {
                let Some(third) = sell_cashback_third_meta.filter(|p| *p != Pubkey::default())
                else {
                    return Err(anyhow!(
                        "pump_amm SELL: extended layout required but sell_cashback_third_meta missing (MASTER/Geyser observation required)"
                    ));
                };
                Self::push_pump_amm_sell_extended_trailing_metas(
                    &mut metas,
                    user,
                    quote_mint,
                    quote_tp,
                    third,
                    sell_requires_pre_fee_metas,
                    sell_extended_fee_tail_0,
                    sell_extended_fee_tail_1,
                    sell_extended_tail_0,
                    sell_extended_tail_1,
                )?;
            }
            metas
        };

        Ok(vec![Instruction {
            program_id,
            accounts: metas,
            data,
        }])
    }

    /// Prime discovery caches for a base mint (static pool) and a user (user-specific accounts).
    pub async fn ensure_discovered_for_user(&self, base_mint: Pubkey, user: Pubkey) -> Result<()> {
        let pool = self
            .discover_pool_static(base_mint, false)
            .await?
            .ok_or_else(|| anyhow!("pump_amm: no pool found for base_mint={base_mint}"))?;

        let _ua = self
            .discover_user_accounts(pool.pool_market, base_mint, user)
            .await?
            .ok_or_else(|| {
                anyhow!("pump_amm: no user accounts found for user={user} base_mint={base_mint}")
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::live_pool_cache::{CachedPoolState, LivePoolCache, PumpAmmState};
    use crate::ipc::DexPoolReadiness;
    use crate::solana::dex::Dex;
    use crate::solana::rpc::SolanaRpc;
    use base64::Engine;
    use serde_json::json;
    use std::str::FromStr;
    use std::sync::Arc;

    /// `global_config` prefix from mainnet (643 bytes); only bytes [0..96) needed for offset-57 check.
    /// Cross-check: finalized swap `3QBRgEPpPcXLEPSD8NXQFALrUwrMkdhSWHg3SpC4HR13uQS78gUeYK36bcKcbjhQqnTA83xL1KiRXvzJJRM4hCPD`
    /// for pool `5rNMGrJ3V2vUY3GAuxiVKZmKCn6c5N6n7Ld5EWvgceVX` — ix inner accounts[9]/[10].
    #[test]
    fn pump_amm_global_config_protocol_fee_recipient_offset_matches_mainnet_swap() {
        let gc_b64 = "lQicyqD8sNnTu4yrNBzgUoRX8sOBfTJ4RBlj3NVf7Vi6JMmZ3awCqhQAAAAAAAAABQAAAAAAAAAASsL40N1cvJfjKJwZfLUGKlTz2Va5zm5RFfllZ6pcs+ZgjMwd/Olh";
        let gc_data = base64::engine::general_purpose::STANDARD
            .decode(gc_b64)
            .expect("decode global_config fixture");
        assert!(
            gc_data.len() >= PUMPFUN_AMM_GLOBAL_CONFIG_PROTOCOL_FEE_RECIPIENT_OFFSET + 32,
            "fixture too short"
        );
        let pfr = Pubkey::new_from_array(
            gc_data[PUMPFUN_AMM_GLOBAL_CONFIG_PROTOCOL_FEE_RECIPIENT_OFFSET
                ..PUMPFUN_AMM_GLOBAL_CONFIG_PROTOCOL_FEE_RECIPIENT_OFFSET + 32]
                .try_into()
                .unwrap(),
        );
        let expected_pfr =
            Pubkey::from_str("62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV").unwrap();
        assert_eq!(pfr, expected_pfr);

        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
        let token_program = Pubkey::new_from_array(spl_token::id().to_bytes());
        let expected_ta = Pubkey::from_str("94qWNrtmfn42h3ZjUZwWvK1MEo9uVmmrBPd2hpNjYDjb").unwrap();
        let derived_ta = PumpFunAmmDex::derive_ata_with_program(pfr, wsol, token_program);
        assert_eq!(
            derived_ta, expected_ta,
            "WSOL ATA for protocol_fee_recipient must match swap ix account #10"
        );
    }

    /// Scope 49: failed local `getSignaturesForAddress` must not be classified as "empty history".
    #[test]
    fn scope49_local_history_probe_unknown_when_get_signatures_failed() {
        let s = PumpAmmSellLayoutExternalAttemptSummary {
            rpc_method_last: "getSignaturesForAddress",
            elapsed_total_ms: 0,
            get_signatures_calls: 1,
            get_signatures_succeeded: false,
            get_transaction_calls: 0,
            signatures_limit_last: Some(200),
            signatures_returned_last: 0,
            transactions_fetched: 0,
            pump_amm_instructions_seen: 0,
            pump_amm_sell_discriminator_seen: 0,
            sell_candidates_seen: 0,
            provider_status_last: PumpAmmSellLayoutProviderStatus::RpcError,
            termination_reason:
                PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalRpcError,
        };
        assert_eq!(
            local_history_probe_from_sell_layout_summary(&s),
            PumpAmmLocalHistoryProbe::Unknown
        );
    }

    #[test]
    fn scope49_local_history_probe_empty_when_rpc_ok_zero_sigs() {
        let s = PumpAmmSellLayoutExternalAttemptSummary {
            rpc_method_last: "getSignaturesForAddress",
            elapsed_total_ms: 1,
            get_signatures_calls: 1,
            get_signatures_succeeded: true,
            get_transaction_calls: 0,
            signatures_limit_last: Some(200),
            signatures_returned_last: 0,
            transactions_fetched: 0,
            pump_amm_instructions_seen: 0,
            pump_amm_sell_discriminator_seen: 0,
            sell_candidates_seen: 0,
            provider_status_last: PumpAmmSellLayoutProviderStatus::Ok,
            termination_reason:
                PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalNoSellCandidates,
        };
        assert_eq!(
            local_history_probe_from_sell_layout_summary(&s),
            PumpAmmLocalHistoryProbe::Empty
        );
    }

    /// Scope 49: no-fallback diag must surface `LocalObservationError` in structured `termination_reason`.
    #[test]
    fn scope49_no_fallback_termination_prefers_local_observation_error() {
        assert_eq!(
            force_refresh_no_fallback_termination_reason(true),
            PumpAmmSellLayoutTerminationReason::LocalObservationError
        );
        assert_eq!(
            force_refresh_no_fallback_termination_reason(false),
            PumpAmmSellLayoutTerminationReason::ExternalSkippedNoFallbackRpc
        );
    }

    /// Local observation may fail, but after an external attempt the top-level reason stays external (e.g. outer timeout).
    #[test]
    fn scope49_external_termination_not_hidden_by_local_observation_failed() {
        let tr = force_refresh_external_termination_reason_after_attempt(
            PumpAmmAuthoritativeSellLayout::Unknown,
            true,
            None,
        );
        assert_eq!(
            tr,
            PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalTimeoutBudgetExhausted
        );
        // Independent signal for supervisor: local_observation_failed would still be true on the diag.
        let tr_with_summary = force_refresh_external_termination_reason_after_attempt(
            PumpAmmAuthoritativeSellLayout::Unknown,
            false,
            Some(&PumpAmmSellLayoutExternalAttemptSummary {
                rpc_method_last: "getTransaction",
                elapsed_total_ms: 100,
                get_signatures_calls: 1,
                get_signatures_succeeded: true,
                get_transaction_calls: 3,
                signatures_limit_last: Some(40),
                signatures_returned_last: 5,
                transactions_fetched: 2,
                pump_amm_instructions_seen: 1,
                pump_amm_sell_discriminator_seen: 0,
                sell_candidates_seen: 0,
                provider_status_last: PumpAmmSellLayoutProviderStatus::Ok,
                termination_reason:
                    PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalHttp429,
            }),
        );
        assert_eq!(
            tr_with_summary,
            PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalHttp429
        );
    }

    /// Scope 50: `jsonParsed` inner CPI instructions often omit `programIdIndex` but carry `programId`.
    #[test]
    fn scope50_program_id_str_from_instruction_accepts_json_parsed_shape() {
        let keys = vec!["11111111111111111111111111111111".to_string()];
        let ix: Value = json!({
            "programId": PUMPFUN_AMM_PROGRAM_ID,
            "accounts": [],
            "data": "xx",
            "stackHeight": 2
        });
        assert_eq!(
            PumpFunAmmDex::program_id_str_from_instruction_json(&ix, &keys),
            Some(PUMPFUN_AMM_PROGRAM_ID)
        );

        let ix_wrapped: Value = json!({
            "Parsed": {
                "programId": PUMPFUN_AMM_PROGRAM_ID,
            }
        });
        assert_eq!(
            PumpFunAmmDex::program_id_str_from_instruction_json(&ix_wrapped, &keys),
            Some(PUMPFUN_AMM_PROGRAM_ID)
        );

        let ix_compiled: Value = json!({
            "Compiled": {
                "programId": PUMPFUN_AMM_PROGRAM_ID,
                "accounts": ["11111111111111111111111111111111"],
                "data": "33e685a4017f83ad00000000000000000000000000000000000000000000000000"
            }
        });
        assert_eq!(
            PumpFunAmmDex::pump_amm_ix_account_strings_from_json(&ix_compiled, &keys)
                .map(|v| v.len()),
            Some(1)
        );

        let ix_idx: Value = json!({
            "programIdIndex": 0u64,
            "accounts": [],
            "data": "xx"
        });
        assert_eq!(
            PumpFunAmmDex::program_id_str_from_instruction_json(&ix_idx, &keys),
            Some("11111111111111111111111111111111")
        );
    }

    #[test]
    fn scope50_sell_layout_scan_preserves_decode_termination_across_later_successful_tx_fetch() {
        assert_eq!(
            sell_layout_scan_termination_after_successful_tx_fetch(
                PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalPumpAmmSellAccountShapeUnsupported,
            ),
            PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalPumpAmmSellAccountShapeUnsupported
        );
        assert_eq!(
            sell_layout_scan_termination_after_successful_tx_fetch(
                PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalNoSellCandidates,
            ),
            PumpAmmSellLayoutTerminationReason::LocalHistoryEmptyExternalNoSellCandidates
        );
    }

    /// Scope 54: newer 21-account SELL in history must not erase extended (24-account) evidence.
    /// Scope 60: fee recipient fields follow the winning reference row.
    #[test]
    fn scope54_merge_sell_layout_extended_wins_over_base() {
        let t0 = Pubkey::new_unique();
        let t1 = Pubkey::new_unique();
        let t2 = Pubkey::new_unique();
        let pfr_base = Pubkey::new_unique();
        let pfr_ta_base = Pubkey::new_unique();
        let pfr_ext = Pubkey::new_unique();
        let pfr_ta_ext = Pubkey::new_unique();
        let base_obs = PumpAmmSellReferenceObservation {
            layout: PumpAmmAuthoritativeSellLayout::Base,
            protocol_fee_recipient: pfr_base,
            protocol_fee_recipient_ta: pfr_ta_base,
            reference_swap_signature: Some("sig_base".to_string()),
        };
        let ext_obs = PumpAmmSellReferenceObservation {
            layout: PumpAmmAuthoritativeSellLayout::Extended {
                pre_fee_0: None,
                pre_fee_1: None,
                tail_0: t0,
                tail_1: t1,
                tail_2: t2,
                fee_tail_0: None,
                fee_tail_1: None,
            },
            protocol_fee_recipient: pfr_ext,
            protocol_fee_recipient_ta: pfr_ta_ext,
            reference_swap_signature: Some("sig_ext".to_string()),
        };
        assert_eq!(
            merge_pump_amm_authoritative_sell_reference_observation(
                base_obs.clone(),
                ext_obs.clone()
            )
            .layout,
            PumpAmmAuthoritativeSellLayout::Extended {
                pre_fee_0: None,
                pre_fee_1: None,
                tail_0: t0,
                tail_1: t1,
                tail_2: t2,
                fee_tail_0: None,
                fee_tail_1: None,
            }
        );
        let merged =
            merge_pump_amm_authoritative_sell_reference_observation(base_obs, ext_obs.clone());
        assert_eq!(merged.protocol_fee_recipient, pfr_ext);
        assert_eq!(merged.protocol_fee_recipient_ta, pfr_ta_ext);
        assert_eq!(merged.reference_swap_signature.as_deref(), Some("sig_ext"));

        let base_obs2 = PumpAmmSellReferenceObservation {
            layout: PumpAmmAuthoritativeSellLayout::Base,
            protocol_fee_recipient: pfr_base,
            protocol_fee_recipient_ta: pfr_ta_base,
            reference_swap_signature: Some("sig_base".to_string()),
        };
        let merged2 =
            merge_pump_amm_authoritative_sell_reference_observation(ext_obs.clone(), base_obs2);
        assert_eq!(
            merged2.layout,
            PumpAmmAuthoritativeSellLayout::Extended {
                pre_fee_0: None,
                pre_fee_1: None,
                tail_0: t0,
                tail_1: t1,
                tail_2: t2,
                fee_tail_0: None,
                fee_tail_1: None,
            }
        );
        assert_eq!(merged2.protocol_fee_recipient, pfr_ext);
    }

    #[test]
    fn scope54_merge_sell_layout_base_accumulates_from_unknown() {
        let pfr = Pubkey::new_unique();
        let base_obs = PumpAmmSellReferenceObservation {
            layout: PumpAmmAuthoritativeSellLayout::Base,
            protocol_fee_recipient: pfr,
            protocol_fee_recipient_ta: Pubkey::new_unique(),
            reference_swap_signature: Some("sig_b".to_string()),
        };
        assert_eq!(
            merge_pump_amm_authoritative_sell_reference_observation(
                PumpAmmSellReferenceObservation::unknown(),
                base_obs.clone(),
            )
            .layout,
            PumpAmmAuthoritativeSellLayout::Base
        );
        assert_eq!(
            merge_pump_amm_authoritative_sell_reference_observation(
                base_obs.clone(),
                PumpAmmSellReferenceObservation::unknown(),
            )
            .layout,
            PumpAmmAuthoritativeSellLayout::Base
        );
    }

    /// Scope 54 PR follow-up: same fold as the force_refresh TX-history scan (newest sig first).
    ///
    /// Simulates two successful pool-matching `sell` decodes: a **newer** 21-account Base tx, then an
    /// **older** 24-account Extended tx in the signature list — the scan must not stop at Base.
    #[test]
    fn scope54_force_refresh_sell_layout_fold_newest_base_then_older_extended() {
        let t0 = Pubkey::new_unique();
        let t1 = Pubkey::new_unique();
        let t2 = Pubkey::new_unique();
        let pfr_ext = Pubkey::new_unique();
        let pfr_ta_ext = Pubkey::new_unique();
        let obs = [
            PumpAmmSellReferenceObservation {
                layout: PumpAmmAuthoritativeSellLayout::Base,
                protocol_fee_recipient: Pubkey::new_unique(),
                protocol_fee_recipient_ta: Pubkey::new_unique(),
                reference_swap_signature: Some("sig_newer_base".to_string()),
            },
            PumpAmmSellReferenceObservation {
                layout: PumpAmmAuthoritativeSellLayout::Extended {
                    pre_fee_0: None,
                    pre_fee_1: None,
                    tail_0: t0,
                    tail_1: t1,
                    tail_2: t2,
                    fee_tail_0: None,
                    fee_tail_1: None,
                },
                protocol_fee_recipient: pfr_ext,
                protocol_fee_recipient_ta: pfr_ta_ext,
                reference_swap_signature: Some("sig_older_ext".to_string()),
            },
        ];
        let out = fold_force_refresh_sell_reference_observations_newest_first(
            PumpAmmSellReferenceObservation::unknown(),
            obs,
        );
        assert_eq!(
            out.layout,
            PumpAmmAuthoritativeSellLayout::Extended {
                pre_fee_0: None,
                pre_fee_1: None,
                tail_0: t0,
                tail_1: t1,
                tail_2: t2,
                fee_tail_0: None,
                fee_tail_1: None,
            }
        );
        assert_eq!(out.protocol_fee_recipient, pfr_ext);
        assert_eq!(
            out.reference_swap_signature.as_deref(),
            Some("sig_older_ext")
        );
    }

    /// Cold-path authoritative market parse must not label any v14 field `Heuristic` in Scope 44 logs.
    #[test]
    fn pump_amm_authoritative_market_parse_diagnostic_has_no_heuristic_fields() {
        let pool = Pubkey::new_unique();
        let base = Pubkey::new_unique();
        let d = PumpFunAmmDex::pump_amm_market_parse_diagnostics(
            "test_authoritative",
            true,
            pool,
            base,
            PumpAmmFeeParseKind::GlobalConfigOffset57DerivedWsolAta,
            PumpAmmCreatorParseKind::MarketOffset211CreatorVaultPda,
            PumpAmmGvaParseKind::SingletonPdaRpcVerified,
            PumpAmmFeeConfigParseKind::UniqueVerifiedFeeProgramAccount,
            true,
        );
        for (name, fd) in [
            ("pfr", &d.protocol_fee_recipient),
            ("pfr_ta", &d.protocol_fee_recipient_ta),
            ("cc_ata", &d.coin_creator_vault_ata),
            ("cc_auth", &d.coin_creator_vault_authority),
            ("gva", &d.global_volume_accumulator),
            ("fee_cfg", &d.fee_config),
            ("fee_prog", &d.fee_program),
        ] {
            assert_ne!(
                fd.resolution,
                PumpAmmFieldResolution::Heuristic,
                "field {name} must not be Heuristic for authoritative successful parse"
            );
        }
    }

    fn make_pump_amm_cache_with_reserves(
        pool_market: Pubkey,
        base_mint: Pubkey,
        base_reserve: u64,
        quote_reserve: u64,
    ) -> Arc<LivePoolCache> {
        let cache = LivePoolCache::new();
        cache.upsert(
            pool_market,
            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint,
                quote_mint: Pubkey::from_str(WSOL_MINT).unwrap(),
                pool_base_token_account: Pubkey::new_unique(),
                pool_quote_token_account: Pubkey::new_unique(),
                base_reserve: Some(base_reserve),
                quote_reserve: Some(quote_reserve),
                pool_accounts: vec![],
                creator: None,
            }),
            100,
        );
        Arc::new(cache)
    }

    fn make_pump_amm_cache_with_pool_accounts(
        pool_market: Pubkey,
        base_mint: Pubkey,
        pool_accounts: Vec<Pubkey>,
    ) -> Arc<LivePoolCache> {
        let cache = LivePoolCache::new();
        cache.upsert(
            pool_market,
            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint,
                quote_mint: Pubkey::from_str(WSOL_MINT).unwrap(),
                pool_base_token_account: Pubkey::new_unique(),
                pool_quote_token_account: Pubkey::new_unique(),
                base_reserve: Some(1),
                quote_reserve: Some(1),
                pool_accounts,
                creator: None,
            }),
            100,
        );
        cache.merge_pump_amm_pool_accounts_readiness(pool_market, DexPoolReadiness::Ready);
        Arc::new(cache)
    }

    fn make_empty_cache() -> Arc<LivePoolCache> {
        Arc::new(LivePoolCache::new())
    }

    #[tokio::test]
    async fn test_quote_exact_in_cache_hit_no_rpc() {
        let base_mint = Pubkey::new_unique();
        let pool_market = Pubkey::new_unique();
        let cache = make_pump_amm_cache_with_reserves(
            pool_market,
            base_mint,
            1_000_000_000_000,
            50_000_000_000,
        );
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let dex = PumpFunAmmDex::new_with_cache(rpc, cache, false);

        let base_mint_str = base_mint.to_string();
        let result = dex
            .quote_exact_in(WSOL_MINT, &base_mint_str, 1_000_000_000)
            .await;

        let quote = result.expect("quote should succeed");
        assert!(quote.is_some(), "expected Some(Quote) on cache hit");
        let quote = quote.unwrap();
        assert!(quote.amount_out > 0);
        assert!(quote.route.contains(&pool_market.to_string()));
        assert_eq!(quote.fee_bps, 125);
    }

    #[tokio::test]
    async fn fetch_pump_amm_vault_reserves_rejects_short_accounts() {
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let dex = PumpFunAmmDex::new_with_cache(rpc, make_empty_cache(), false);
        let short = vec![Pubkey::new_unique(); 5];
        let err = dex
            .fetch_pump_amm_vault_reserves(&short)
            .await
            .expect_err("expected err");
        assert!(
            err.to_string().contains("pool_accounts len"),
            "unexpected err: {err}"
        );
    }

    #[tokio::test]
    async fn test_quote_exact_in_cache_miss_returns_none() {
        let base_mint = Pubkey::new_unique();
        let cache = make_empty_cache();
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let dex = PumpFunAmmDex::new_with_cache(rpc, cache, false);

        let base_mint_str = base_mint.to_string();
        let result = dex
            .quote_exact_in(WSOL_MINT, &base_mint_str, 1_000_000)
            .await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_pool_accounts_v1_for_base_mint_cache_hit() {
        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
        let base_mint = Pubkey::new_unique();
        let pool_market = Pubkey::new_unique();
        let pool_accounts: Vec<Pubkey> = vec![
            pool_market,
            Pubkey::new_unique(),
            base_mint,
            wsol,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        assert_eq!(pool_accounts.len(), 14);

        let cache =
            make_pump_amm_cache_with_pool_accounts(pool_market, base_mint, pool_accounts.clone());
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let dex = PumpFunAmmDex::new_with_cache(rpc, cache, false);

        let result = dex.pool_accounts_v1_for_base_mint(base_mint).await;

        assert!(result.is_ok());
        let accounts = result.unwrap();
        assert!(accounts.is_some());
        let accounts = accounts.unwrap();
        assert_eq!(accounts.len(), 14);
        assert_eq!(accounts, pool_accounts);
    }

    #[tokio::test]
    async fn test_pool_accounts_v1_for_base_mint_cache_miss() {
        let base_mint = Pubkey::new_unique();
        let cache = make_empty_cache();
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let dex = PumpFunAmmDex::new_with_cache(rpc, cache, false);

        let result = dex.pool_accounts_v1_for_base_mint(base_mint).await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    /// force_refresh requires RPC; Hot-Path dex (`allow_rpc_on_miss=false`) must not return stale cache.
    #[tokio::test]
    async fn test_pool_accounts_force_refresh_refuses_without_rpc_permission() {
        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
        let base_mint = Pubkey::new_unique();
        let pool_market = Pubkey::new_unique();
        let pool_accounts: Vec<Pubkey> = vec![
            pool_market,
            Pubkey::new_unique(),
            base_mint,
            wsol,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        let cache =
            make_pump_amm_cache_with_pool_accounts(pool_market, base_mint, pool_accounts.clone());
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let dex = PumpFunAmmDex::new_with_cache(rpc, cache, false);

        let result = dex
            .pool_accounts_v1_for_base_mint_with_hint(base_mint, Some(pool_market), true)
            .await;

        assert!(result.is_ok());
        assert!(
            result.unwrap().is_none(),
            "force_refresh with allow_rpc_on_miss=false must not use cache or RPC"
        );
    }

    /// I-24d: A bad `pool_address_hint` must not fall through to `getProgramAccounts` (~20s+).
    #[tokio::test]
    async fn test_pool_address_hint_parse_fail_errors_without_global_scan() {
        let base_mint = Pubkey::new_unique();
        // Missing account → try_parse returns Ok(None) after one cheap RPC attempt.
        let bad_hint = Pubkey::new_unique();
        let cache = make_empty_cache();
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let dex = PumpFunAmmDex::new_with_cache(rpc, cache, true);

        let fut = dex.pool_accounts_v1_for_base_mint_with_hint(base_mint, Some(bad_hint), false);
        let completed = tokio::time::timeout(std::time::Duration::from_secs(3), fut)
            .await
            .expect("must not block on getProgramAccounts global scan");

        assert!(
            completed.is_err(),
            "expected Err for failed hint parse; got {completed:?}"
        );
        let msg = format!("{:#}", completed.unwrap_err());
        assert!(
            msg.contains("I-24d") || msg.contains("pool_address hint"),
            "unexpected error message: {msg}"
        );
    }

    #[test]
    fn test_build_swap_ix_from_pool_accounts() {
        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
        let base_mint = Pubkey::new_unique();
        let base_mint_str = base_mint.to_string();
        let user = Pubkey::new_unique();

        let pool_accounts: Vec<Pubkey> = vec![
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            base_mint,
            wsol,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        assert_eq!(pool_accounts.len(), 14);

        let result = PumpFunAmmDex::build_swap_ix_from_pool_accounts(
            WSOL_MINT,
            &base_mint_str,
            1_000_000_000,
            100_000,
            user,
            &pool_accounts,
            None,
            false,
            None,
        );

        assert!(result.is_ok());
        let ixs = result.unwrap();
        assert!(!ixs.is_empty());
        assert_eq!(
            ixs[0].program_id,
            Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID).unwrap()
        );
        assert!(!ixs[0].data.is_empty());
    }

    /// SELL path: instruction account #2 must be the canonical global_config (not pool_accounts[1]).
    #[test]
    fn test_pumpswap_sell_global_config_meta_is_canonical() {
        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
        let base_mint = Pubkey::new_unique();
        let user = Pubkey::new_unique();
        let canonical_gc = Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG).unwrap();

        let pool_accounts: Vec<Pubkey> = vec![
            Pubkey::new_unique(), // pool
            Pubkey::new_unique(), // wrong on purpose — must not appear at ix[2]
            base_mint,
            wsol,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        assert_eq!(pool_accounts.len(), 14);
        assert_ne!(pool_accounts[1], canonical_gc);

        let ixs = PumpFunAmmDex::build_swap_ix_from_pool_accounts(
            &base_mint.to_string(),
            WSOL_MINT,
            1_000_000,
            1,
            user,
            &pool_accounts,
            None,
            false,
            None,
        )
        .expect("SELL build");

        assert_eq!(ixs[0].accounts.len(), 21);
        assert_eq!(ixs[0].accounts[2].pubkey, canonical_gc);
    }

    /// SELL path: instruction account #15 must be the canonical `__event_authority` PDA (not pool_accounts[8]).
    #[test]
    fn test_pumpswap_sell_event_authority_meta_is_canonical() {
        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
        let base_mint = Pubkey::new_unique();
        let user = Pubkey::new_unique();
        let program_id = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID).unwrap();
        let canonical_ea = pump_amm_canonical_event_authority(&program_id);

        let pool_accounts: Vec<Pubkey> = vec![
            Pubkey::new_unique(),
            Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG).unwrap(),
            base_mint,
            wsol,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(), // wrong on purpose — must not appear at ix[15]
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        assert_eq!(pool_accounts.len(), 14);
        assert_ne!(pool_accounts[8], canonical_ea);

        let ixs = PumpFunAmmDex::build_swap_ix_from_pool_accounts(
            &base_mint.to_string(),
            WSOL_MINT,
            1_000_000,
            1,
            user,
            &pool_accounts,
            None,
            false,
            None,
        )
        .expect("SELL build");

        assert_eq!(ixs[0].accounts.len(), 21);
        assert_eq!(ixs[0].accounts[15].pubkey, canonical_ea);
    }

    /// Known-pool parse: singleton `global_volume_accumulator` PDA must match mainnet fixture (see `tests/pump_amm_market_parse_offsets.rs`).
    #[test]
    fn test_pump_amm_singleton_global_volume_accumulator_fixture() {
        let program_id = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID).unwrap();
        let gva = pump_amm_singleton_global_volume_accumulator(&program_id);
        let expected = Pubkey::from_str("C2aFPdENg4A2HQsmrd5rTw5TaYBX5Ku887cWjbFKtZpw").unwrap();
        assert_eq!(gva, expected);
    }

    #[test]
    fn test_pump_amm_sell_layout_observation_parses_standard_sell() {
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let mut account_keys: Vec<Pubkey> = (0..21).map(|_| Pubkey::new_unique()).collect();
        account_keys[0] = pool_market;
        account_keys[2] = Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG).unwrap();
        account_keys[3] = base_mint;
        account_keys[4] = Pubkey::from_str(WSOL_MINT).unwrap();
        account_keys[20] = Pubkey::from_str(PUMPFUN_AMM_FEE_PROGRAM_ID).unwrap();
        let acc_strings: Vec<String> = account_keys.iter().map(ToString::to_string).collect();
        let ix_data = pump_amm_sell_ix_discriminator().to_vec();

        let observed = PumpFunAmmDex::pump_amm_sell_layout_observation_from_parsed_swap_ix(
            &acc_strings,
            &ix_data,
        )
        .expect("standard sell observation");

        assert_eq!(observed.0, pool_market);
        assert_eq!(observed.1, base_mint);
        assert_eq!(observed.2, PumpAmmAuthoritativeSellLayout::Base);
    }

    #[test]
    fn test_pump_amm_sell_layout_observation_parses_26_account_cashback_sell() {
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let third_meta = Pubkey::new_unique();
        let tail0 = Pubkey::new_unique();
        let tail1 = Pubkey::new_unique();
        let fee_tail0 = Pubkey::new_unique();
        let fee_tail1 = Pubkey::new_unique();
        let mut account_keys: Vec<Pubkey> = (0..26).map(|_| Pubkey::new_unique()).collect();
        account_keys[0] = pool_market;
        account_keys[2] = Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG).unwrap();
        account_keys[3] = base_mint;
        account_keys[4] = Pubkey::from_str(WSOL_MINT).unwrap();
        account_keys[20] = Pubkey::from_str(PUMPFUN_AMM_FEE_PROGRAM_ID).unwrap();
        account_keys[21] = tail0;
        account_keys[22] = tail1;
        account_keys[23] = third_meta;
        account_keys[24] = fee_tail0;
        account_keys[25] = fee_tail1;
        let acc_strings: Vec<String> = account_keys.iter().map(ToString::to_string).collect();
        let ix_data = pump_amm_sell_ix_discriminator().to_vec();

        let observed = PumpFunAmmDex::pump_amm_sell_layout_observation_from_parsed_swap_ix(
            &acc_strings,
            &ix_data,
        )
        .expect("26-account sell observation");

        assert_eq!(
            observed.2,
            PumpAmmAuthoritativeSellLayout::Extended {
                pre_fee_0: None,
                pre_fee_1: None,
                tail_0: tail0,
                tail_1: tail1,
                tail_2: third_meta,
                fee_tail_0: Some(fee_tail0),
                fee_tail_1: Some(fee_tail1),
            }
        );
    }

    /// P184d: 27-account extended `sell` — pre-fee readonly metas at #19/#20, global fee pair at #21/#22.
    #[test]
    fn p184d_sell_layout_observation_parses_27_account_pre_fee_extended_sell() {
        let pool_market = Pubkey::from_str("GrgDaBg4TGBQCDZk9HHw8JT24RnoDHtQnvgguKxGKStb").unwrap();
        let base_mint = Pubkey::new_unique();
        let pre_fee_0 = Pubkey::new_unique();
        let pre_fee_1 = Pubkey::new_unique();
        let third_meta = Pubkey::new_unique();
        let tail0 = Pubkey::new_unique();
        let tail1 = Pubkey::new_unique();
        let fee_tail0 = Pubkey::new_unique();
        let fee_config = Pubkey::from_str(PUMPFUN_AMM_FEE_CONFIG).unwrap();
        let fee_program = Pubkey::from_str(PUMPFUN_AMM_FEE_PROGRAM_ID).unwrap();
        let mut account_keys: Vec<Pubkey> = (0..27).map(|_| Pubkey::new_unique()).collect();
        account_keys[0] = pool_market;
        account_keys[2] = Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG).unwrap();
        account_keys[3] = base_mint;
        account_keys[4] = Pubkey::from_str(WSOL_MINT).unwrap();
        account_keys[PUMPFUN_AMM_SELL_PRE_FEE_0_IX_V2] = pre_fee_0;
        account_keys[PUMPFUN_AMM_SELL_PRE_FEE_1_IX_V2] = pre_fee_1;
        account_keys[PUMPFUN_AMM_SELL_FEE_CONFIG_IX_V2] = fee_config;
        account_keys[PUMPFUN_AMM_SELL_FEE_PROGRAM_IX_V2] = fee_program;
        account_keys[PUMPFUN_AMM_SELL_EXT_TAIL_0_IX_V2] = tail0;
        account_keys[PUMPFUN_AMM_SELL_EXT_TAIL_1_IX_V2] = tail1;
        account_keys[PUMPFUN_AMM_SELL_EXT_THIRD_META_IX_V2] = third_meta;
        account_keys[PUMPFUN_AMM_SELL_FEE_TAIL_0_IX_V2] = fee_tail0;
        let acc_strings: Vec<String> = account_keys.iter().map(ToString::to_string).collect();
        let ix_data = pump_amm_sell_ix_discriminator().to_vec();

        let observed = PumpFunAmmDex::pump_amm_sell_layout_observation_from_parsed_swap_ix(
            &acc_strings,
            &ix_data,
        )
        .expect("27-account sell observation");

        assert_eq!(observed.0, pool_market);
        assert_eq!(observed.1, base_mint);
        assert_eq!(
            observed.2,
            PumpAmmAuthoritativeSellLayout::Extended {
                pre_fee_0: Some(pre_fee_0),
                pre_fee_1: Some(pre_fee_1),
                tail_0: tail0,
                tail_1: tail1,
                tail_2: third_meta,
                fee_tail_0: Some(fee_tail0),
                fee_tail_1: None,
            }
        );
        let ext = pump_amm_sell_extended_fields_from_ix_accounts(&account_keys).expect("fields");
        assert!(ext.requires_pre_fee_metas);
        assert_eq!(ext.pre_fee_meta_0, Some(pre_fee_0));
        assert_eq!(ext.pre_fee_meta_1, Some(pre_fee_1));
    }

    #[test]
    fn test_pump_amm_sell_layout_observation_parses_extended_sell() {
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let third_meta = Pubkey::new_unique();
        let tail0 = Pubkey::new_unique();
        let tail1 = Pubkey::new_unique();
        let mut account_keys: Vec<Pubkey> = (0..24).map(|_| Pubkey::new_unique()).collect();
        account_keys[0] = pool_market;
        account_keys[2] = Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG).unwrap();
        account_keys[3] = base_mint;
        account_keys[4] = Pubkey::from_str(WSOL_MINT).unwrap();
        account_keys[20] = Pubkey::from_str(PUMPFUN_AMM_FEE_PROGRAM_ID).unwrap();
        account_keys[21] = tail0;
        account_keys[22] = tail1;
        account_keys[23] = third_meta;
        let acc_strings: Vec<String> = account_keys.iter().map(ToString::to_string).collect();
        let ix_data = pump_amm_sell_ix_discriminator().to_vec();

        let observed = PumpFunAmmDex::pump_amm_sell_layout_observation_from_parsed_swap_ix(
            &acc_strings,
            &ix_data,
        )
        .expect("extended sell observation");

        assert_eq!(observed.0, pool_market);
        assert_eq!(observed.1, base_mint);
        assert_eq!(
            observed.2,
            PumpAmmAuthoritativeSellLayout::Extended {
                pre_fee_0: None,
                pre_fee_1: None,
                tail_0: tail0,
                tail_1: tail1,
                tail_2: third_meta,
                fee_tail_0: None,
                fee_tail_1: None,
            }
        );
    }

    #[test]
    fn test_pump_amm_sell_extended_layout_ready_v2_requires_fee_tail_0() {
        let third = Pubkey::new_unique();
        let pre1 = Pubkey::new_unique();
        let fee0 = Pubkey::new_unique();
        assert!(
            !pump_amm_sell_extended_layout_ready(PumpAmmSellExtendedReadinessParams {
                sell_requires_extended: true,
                third_meta: Some(third),
                fee_tail_0: None,
                fee_tail_1: None,
                sell_requires_fee_tail: false,
                sell_requires_pre_fee_metas: true,
                sell_pre_fee_meta_1: Some(pre1),
            }),
            "27-account layout must not be ready without fee_tail_0"
        );
        assert!(pump_amm_sell_extended_layout_ready(
            PumpAmmSellExtendedReadinessParams {
                sell_requires_extended: true,
                third_meta: Some(third),
                fee_tail_0: Some(fee0),
                fee_tail_1: None,
                sell_requires_fee_tail: false,
                sell_requires_pre_fee_metas: true,
                sell_pre_fee_meta_1: Some(pre1),
            }
        ));
    }

    #[test]
    fn test_push_pump_amm_sell_extended_trailing_metas_v2_errors_without_fee_tail_0() {
        let mut metas = Vec::new();
        let err = PumpFunAmmDex::push_pump_amm_sell_extended_trailing_metas(
            &mut metas,
            Pubkey::new_unique(),
            Pubkey::from_str(WSOL_MINT).unwrap(),
            Pubkey::new_from_array(spl_token::id().to_bytes()),
            Pubkey::new_unique(),
            true,
            None,
            None,
            None,
            None,
        );
        assert!(
            err.is_err(),
            "V2 extended SELL must fail when fee_tail_0 is missing"
        );
    }

    /// Scope 60: force_refresh reference SELL ix #9/#10 must overwrite wrong market-derived v14 `[6]`/`[7]`.
    #[test]
    fn scope60_force_refresh_apply_sell_reference_protocol_fee_recipients_to_pool() {
        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let wrong_pfr = Pubkey::from_str("62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV").unwrap();
        let wrong_pfr_ta =
            Pubkey::from_str("94qWNrtmfn42h3ZjUZwWvK1MEo9uVmmrBPd2hpNjYDjb").unwrap();
        let auth_pfr = Pubkey::from_str("AVmoTthdrX6tKt4nDjco2D775W2YK3sDhxPcMmzUAmTY").unwrap();
        let auth_pfr_ta = Pubkey::from_str("FGptqdxjahafaCzpZ1T6EDtCzYMv7Dyn5MgBLyB3VUFW").unwrap();
        let third_meta = Pubkey::from_str("AktftA98kSWAxn6kVSoqBXBELUArjKu2H9WmKB48ULFY").unwrap();

        let mut pool = PumpAmmPoolStatic {
            pool_market,
            global_config: Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG).unwrap(),
            base_mint,
            quote_mint: wsol,
            pool_base_vault: Pubkey::new_unique(),
            pool_quote_vault: Pubkey::new_unique(),
            protocol_fee_recipient: wrong_pfr,
            protocol_fee_recipient_ta: wrong_pfr_ta,
            event_authority: Pubkey::new_unique(),
            coin_creator_vault_ata: Pubkey::new_unique(),
            coin_creator_vault_authority: Pubkey::new_unique(),
            global_volume_accumulator: Pubkey::new_unique(),
            fee_config: Pubkey::new_unique(),
            fee_program: Pubkey::from_str(PUMPFUN_AMM_FEE_PROGRAM_ID).unwrap(),
            sell_requires_cashback_remaining: false,
            sell_cashback_third_meta: None,
            sell_extended_tail_0: None,
            sell_extended_tail_1: None,
            sell_extended_fee_tail_0: None,
            sell_extended_fee_tail_1: None,
            sell_requires_pre_fee_metas: false,
            sell_pre_fee_meta_1: None,
            last_parse_diagnostics: None,
        };

        let mut account_keys: Vec<Pubkey> = (0..24).map(|_| Pubkey::new_unique()).collect();
        account_keys[0] = pool_market;
        account_keys[2] = Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG).unwrap();
        account_keys[3] = base_mint;
        account_keys[4] = wsol;
        account_keys[9] = auth_pfr;
        account_keys[10] = auth_pfr_ta;
        account_keys[19] = Pubkey::from_str(PUMPFUN_AMM_FEE_CONFIG).unwrap();
        account_keys[20] = Pubkey::from_str(PUMPFUN_AMM_FEE_PROGRAM_ID).unwrap();
        let ref_t0 = Pubkey::from_str("CXfrfpXNoQ8Qj4Zf6MxTqoh3datTxgKFsQnt7MFv257z").unwrap();
        let ref_t1 = Pubkey::from_str("5YxQFdt3Tr9zJLvkFccqXVUwhdTWJQc1fFg2YPbxvxeD").unwrap();
        account_keys[21] = ref_t0;
        account_keys[22] = ref_t1;
        account_keys[23] = third_meta;
        let acc_strings: Vec<String> = account_keys.iter().map(ToString::to_string).collect();
        let ix_data = pump_amm_sell_ix_discriminator().to_vec();

        let (_, _, sell_obs) =
            PumpFunAmmDex::pump_amm_sell_reference_observation_from_parsed_swap_ix(
                &acc_strings,
                &ix_data,
                Some(
                    "3TsEarZgfUg5BzfVzE7a7mdCyqTgsqXzmf3iCE9GnzzTFYP3SdzEWtGMzE9WXzAoWQgwaVnW3fVocqzZQP4j217A"
                        .to_string(),
                ),
            )
            .expect("reference observation");

        assert_eq!(
            sell_obs.layout,
            PumpAmmAuthoritativeSellLayout::Extended {
                pre_fee_0: None,
                pre_fee_1: None,
                tail_0: ref_t0,
                tail_1: ref_t1,
                tail_2: third_meta,
                fee_tail_0: None,
                fee_tail_1: None,
            }
        );

        PumpFunAmmDex::apply_sell_reference_protocol_fee_recipients_for_force_refresh(
            &mut pool, &sell_obs,
        );
        pool.sell_requires_cashback_remaining = true;
        pool.sell_cashback_third_meta = Some(third_meta);

        let v14 = pool.as_pool_accounts_v14();
        assert_eq!(v14[6], auth_pfr);
        assert_eq!(v14[7], auth_pfr_ta);
        assert_ne!(v14[6], wrong_pfr);
    }

    /// Scope 60: builder keeps global SELL fee_config/fee_program (Scope 59) when v14 `[6]`/`[7]` match reference.
    #[test]
    fn scope60_extended_sell_preserves_reference_fee_metas_and_global_fee_slots() {
        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
        let base_mint = Pubkey::new_unique();
        let user = Pubkey::new_unique();
        let auth_pfr = Pubkey::from_str("AVmoTthdrX6tKt4nDjco2D775W2YK3sDhxPcMmzUAmTY").unwrap();
        let auth_pfr_ta = Pubkey::from_str("FGptqdxjahafaCzpZ1T6EDtCzYMv7Dyn5MgBLyB3VUFW").unwrap();
        let third_meta = Pubkey::from_str("AktftA98kSWAxn6kVSoqBXBELUArjKu2H9WmKB48ULFY").unwrap();
        let ref_tail_0 = Pubkey::from_str("CXfrfpXNoQ8Qj4Zf6MxTqoh3datTxgKFsQnt7MFv257z").unwrap();
        let ref_tail_1 = Pubkey::from_str("5YxQFdt3Tr9zJLvkFccqXVUwhdTWJQc1fFg2YPbxvxeD").unwrap();
        let fee_program = Pubkey::from_str(PUMPFUN_AMM_FEE_PROGRAM_ID).unwrap();

        let pool_accounts: Vec<Pubkey> = vec![
            Pubkey::new_unique(),
            Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG).unwrap(),
            base_mint,
            wsol,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            auth_pfr,
            auth_pfr_ta,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            fee_program,
        ];
        assert_eq!(pool_accounts.len(), 14);

        let ixs = PumpFunAmmDex::build_swap_ix_from_pool_accounts_with_extended_tail(
            &base_mint.to_string(),
            WSOL_MINT,
            1_000_000,
            1,
            user,
            &pool_accounts,
            None,
            true,
            Some(third_meta),
            false,
            None,
            Some(ref_tail_0),
            Some(ref_tail_1),
            None,
            None,
        )
        .expect("extended SELL");

        assert_eq!(ixs[0].accounts.len(), 24);
        assert_eq!(ixs[0].accounts[9].pubkey, auth_pfr);
        assert_eq!(ixs[0].accounts[10].pubkey, auth_pfr_ta);
        assert_eq!(
            ixs[0].accounts[19].pubkey,
            Pubkey::from_str(PUMPFUN_AMM_FEE_CONFIG).unwrap()
        );
        assert_eq!(ixs[0].accounts[20].pubkey, fee_program);
        let quote_tp = Pubkey::new_from_array(spl_token::id().to_bytes());
        let (derived_21, derived_22) =
            PumpFunAmmDex::pump_amm_sell_cashback_first_two_metas(user, wsol, quote_tp);
        assert_eq!(ixs[0].accounts[21].pubkey, derived_21);
        assert_eq!(ixs[0].accounts[22].pubkey, derived_22);
        assert_ne!(derived_21, ref_tail_0);
        assert_ne!(derived_22, ref_tail_1);
        assert_eq!(ixs[0].accounts[23].pubkey, third_meta);
        assert!(ixs[0].accounts[21].is_writable);
        assert!(ixs[0].accounts[22].is_writable);
        assert!(
            !ixs[0].accounts[23].is_writable,
            "derived-user extended SELL: #23 third_meta must be readonly"
        );
    }

    /// Scope 60: without a TX-backed observation, apply must not rewrite protocol fee recipients.
    #[test]
    fn scope60_unknown_sell_observation_does_not_override_pool_fee_recipients() {
        let wrong_pfr = Pubkey::new_unique();
        let wrong_ta = Pubkey::new_unique();
        let mut pool = PumpAmmPoolStatic {
            pool_market: Pubkey::new_unique(),
            global_config: Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG).unwrap(),
            base_mint: Pubkey::new_unique(),
            quote_mint: Pubkey::from_str(WSOL_MINT).unwrap(),
            pool_base_vault: Pubkey::new_unique(),
            pool_quote_vault: Pubkey::new_unique(),
            protocol_fee_recipient: wrong_pfr,
            protocol_fee_recipient_ta: wrong_ta,
            event_authority: Pubkey::new_unique(),
            coin_creator_vault_ata: Pubkey::new_unique(),
            coin_creator_vault_authority: Pubkey::new_unique(),
            global_volume_accumulator: Pubkey::new_unique(),
            fee_config: Pubkey::new_unique(),
            fee_program: Pubkey::from_str(PUMPFUN_AMM_FEE_PROGRAM_ID).unwrap(),
            sell_requires_cashback_remaining: false,
            sell_cashback_third_meta: None,
            sell_extended_tail_0: None,
            sell_extended_tail_1: None,
            sell_extended_fee_tail_0: None,
            sell_extended_fee_tail_1: None,
            sell_requires_pre_fee_metas: false,
            sell_pre_fee_meta_1: None,
            last_parse_diagnostics: None,
        };
        PumpFunAmmDex::apply_sell_reference_protocol_fee_recipients_for_force_refresh(
            &mut pool,
            &PumpAmmSellReferenceObservation::unknown(),
        );
        assert_eq!(pool.protocol_fee_recipient, wrong_pfr);
        assert_eq!(pool.protocol_fee_recipient_ta, wrong_ta);
    }

    /// Helius / some RPC encodings return `instruction.accounts` as base58 strings instead of indices.
    #[test]
    fn test_pump_amm_ix_account_strings_accepts_pubkey_array() {
        let message_keys: Vec<String> = Vec::new();
        let ix = json!({
            "accounts": [
                "B8bvg3KzXzGAq51QjirhPTw5ChhiZWn2kNwvQd3YZFN8",
                "So11111111111111111111111111111111111111112"
            ]
        });
        let out = PumpFunAmmDex::pump_amm_ix_account_strings_from_json(&ix, &message_keys)
            .expect("pubkey-string accounts");
        assert_eq!(out.len(), 2);
        assert_eq!(out[0], "B8bvg3KzXzGAq51QjirhPTw5ChhiZWn2kNwvQd3YZFN8");
    }

    #[test]
    fn test_pump_amm_ix_account_strings_resolves_index_array() {
        let message_keys: Vec<String> = vec![
            "Key0xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".to_string(),
            "Key1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".to_string(),
            "Key2xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".to_string(),
        ];
        let ix = json!({ "accounts": [0u64, 2u64] });
        let out = PumpFunAmmDex::pump_amm_ix_account_strings_from_json(&ix, &message_keys)
            .expect("index accounts");
        assert_eq!(out, vec![message_keys[0].clone(), message_keys[2].clone()]);
    }

    /// Mainnet 24-account `sell` (sig `2CCmRDScAErjuBLnVJbGEyV3jsWbuNZpniZ5iTLSwZoE84nmyf285hqJXjRStMHJUaJ9Ex7EvL9fgwAVM83qGd3o`): first two trailing metas are user-derivable; third is observed-only.
    #[test]
    fn test_pumpswap_sell_extended_first_two_metas_match_mainnet_reference() {
        let user = Pubkey::from_str("AazGgNrpzFAE5S5WENfP4YxaZ2oiVXjuAJFRCvF56E5e").unwrap();
        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
        let quote_tp = Pubkey::new_from_array(spl_token::id().to_bytes());
        let expected_user_vol_pda =
            Pubkey::from_str("A5jW7JYYX3Mxf9qC1bmuNoCFW9togSr5EoKzhJaXtSJG").unwrap();
        let expected_user_vol_wsol_ata =
            Pubkey::from_str("8vNm4swQAQgcUDZxYHQyghMyH76SFuS6iX8nvv1Z3nJt").unwrap();
        let (wsol_ata, uv_pda) =
            PumpFunAmmDex::pump_amm_sell_cashback_first_two_metas(user, wsol, quote_tp);
        assert_eq!(wsol_ata, expected_user_vol_wsol_ata);
        assert_eq!(uv_pda, expected_user_vol_pda);
    }

    /// Creator-vault PDA fallback must derive the quote-mint (WSOL) ATA — same as offset-211 path (not `base_mint`).
    #[test]
    fn test_pump_amm_creator_vault_authority_derives_quote_mint_ata() {
        // Mainnet pool 5rNMGrJ3… — `creator_vault` authority from successful swaps (see tests/pump_amm_market_parse_offsets.rs).
        let auth = Pubkey::from_str("6tkGUcYBJJ2c1pdtMQayUpEFEpyP3QqY8Lf6pvvpF5Fq").unwrap();
        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
        let token_program = Pubkey::new_from_array(spl_token::id().to_bytes());
        let ata_wsol = PumpFunAmmDex::derive_ata_with_program(auth, wsol, token_program);
        let ata_wrong_mint =
            PumpFunAmmDex::derive_ata_with_program(auth, Pubkey::new_unique(), token_program);
        assert_ne!(
            ata_wsol, ata_wrong_mint,
            "creator fee vault must be the WSOL ATA for this authority, not an ATA for another mint"
        );
    }

    /// SELL path: ix[9]/[10] must match pool_accounts[6]/[7] (non-global protocol fee recipients).
    #[test]
    fn test_pumpswap_sell_protocol_fee_metas_preserve_pool_accounts() {
        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
        let base_mint = Pubkey::new_unique();
        let user = Pubkey::new_unique();
        let quote_tp = Pubkey::new_from_array(spl_token::id().to_bytes());
        let other_recipient = Pubkey::new_unique();
        // Mainnet ref sell 2XvVpoa5... uses a pool-specific recipient; model that here.
        let observed_pfr = Pubkey::new_unique();
        assert_ne!(observed_pfr, other_recipient);
        let observed_pfr_ta = PumpFunAmmDex::derive_ata_with_program(observed_pfr, wsol, quote_tp);

        let pool_accounts: Vec<Pubkey> = vec![
            Pubkey::new_unique(),
            Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG).unwrap(),
            base_mint,
            wsol,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            observed_pfr,
            observed_pfr_ta,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        assert_eq!(pool_accounts.len(), 14);

        let ixs = PumpFunAmmDex::build_swap_ix_from_pool_accounts(
            &base_mint.to_string(),
            WSOL_MINT,
            1_000_000,
            1,
            user,
            &pool_accounts,
            None,
            false,
            None,
        )
        .expect("SELL build");

        assert_eq!(ixs[0].accounts.len(), 21);
        assert_eq!(ixs[0].accounts[9].pubkey, observed_pfr);
        assert_eq!(ixs[0].accounts[10].pubkey, observed_pfr_ta);
    }

    /// Extended SELL (24 accounts): meta #19 must be global `PUMPFUN_AMM_FEE_CONFIG` even when v14[12]
    /// is a different pubkey (SELL expects that account type; v12 alone caused Scope 58 Custom 3002).
    #[test]
    fn pumpswap_extended_sell_uses_global_fee_config() {
        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
        let base_mint = Pubkey::new_unique();
        let user = Pubkey::new_unique();
        let quote_tp = Pubkey::new_from_array(spl_token::id().to_bytes());
        let third_meta = Pubkey::new_unique();
        let row_fee_config =
            Pubkey::from_str("DcsvhShq8ZaUyU7NtjskVRfKHW7DrSjdu7mkgpzoHyxB").unwrap();
        assert_ne!(
            row_fee_config,
            Pubkey::from_str(PUMPFUN_AMM_FEE_CONFIG).unwrap()
        );
        let fee_program = Pubkey::from_str(PUMPFUN_AMM_FEE_PROGRAM_ID).unwrap();
        let observed_pfr = Pubkey::new_unique();
        let observed_pfr_ta = PumpFunAmmDex::derive_ata_with_program(observed_pfr, wsol, quote_tp);

        let pool_accounts: Vec<Pubkey> = vec![
            Pubkey::new_unique(),
            Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG).unwrap(),
            base_mint,
            wsol,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            observed_pfr,
            observed_pfr_ta,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            row_fee_config,
            fee_program,
        ];
        assert_eq!(pool_accounts.len(), 14);

        let ixs = PumpFunAmmDex::build_swap_ix_from_pool_accounts(
            &base_mint.to_string(),
            WSOL_MINT,
            1_000_000,
            1,
            user,
            &pool_accounts,
            None,
            true,
            Some(third_meta),
        )
        .expect("extended SELL build");

        assert_eq!(ixs[0].accounts.len(), 24);
        assert_eq!(
            ixs[0].accounts[19].pubkey,
            Pubkey::from_str(PUMPFUN_AMM_FEE_CONFIG).unwrap(),
            "SELL ix meta #19 must use global FeeConfig constant, not v14[12]"
        );
        assert_eq!(ixs[0].accounts[20].pubkey, fee_program);
        assert_eq!(ixs[0].accounts[23].pubkey, third_meta);
    }

    /// Regression guard (Scope 58): using `pool_accounts[12]` as SELL `fee_config` meta caused
    /// Anchor `Custom(3002)` (wrong account type vs program). SELL must keep global `5PH…` + `pfee…`.
    #[test]
    fn pumpswap_sell_fee_config_scope58_custom3002_regression_guard() {
        pumpswap_extended_sell_uses_global_fee_config();
    }

    /// P184b: foreign reference volume tails in cache must not override intent-user #21/#22.
    #[test]
    fn pumpswap_extended_sell_ignores_foreign_cached_volume_tails_at_build() {
        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
        let our_user = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let third_meta = Pubkey::new_unique();
        let foreign_tail_0 = Pubkey::new_unique();
        let foreign_tail_1 = Pubkey::new_unique();
        let quote_tp = Pubkey::new_from_array(spl_token::id().to_bytes());
        let pool_accounts: Vec<Pubkey> = vec![
            Pubkey::new_unique(),
            Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG).unwrap(),
            base_mint,
            wsol,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::from_str(PUMPFUN_AMM_FEE_PROGRAM_ID).unwrap(),
        ];
        let ixs = PumpFunAmmDex::build_swap_ix_from_pool_accounts_with_extended_tail(
            &base_mint.to_string(),
            WSOL_MINT,
            1,
            1,
            our_user,
            &pool_accounts,
            None,
            true,
            Some(third_meta),
            false,
            None,
            Some(foreign_tail_0),
            Some(foreign_tail_1),
            None,
            None,
        )
        .expect("extended SELL");
        let (expected_21, expected_22) =
            PumpFunAmmDex::pump_amm_sell_cashback_first_two_metas(our_user, wsol, quote_tp);
        assert_eq!(ixs[0].accounts[21].pubkey, expected_21);
        assert_eq!(ixs[0].accounts[22].pubkey, expected_22);
        assert_ne!(expected_21, foreign_tail_0);
        assert_ne!(expected_22, foreign_tail_1);
        assert!(
            !ixs[0].accounts[23].is_writable,
            "derived-user extended SELL: #23 third_meta must be readonly"
        );
    }

    /// SELL path: Token-2022 base mint — ix[11] must be Token-2022 program; user base ATA (ix[5]) must match derivation.
    /// Wrong SPL program → wrong ATA → Custom(6023) NotEnoughTokensToSell on-chain.
    #[test]
    fn test_pumpswap_sell_token2022_base_program_and_user_ata() {
        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
        let base_mint = Pubkey::new_unique();
        let user = Pubkey::new_unique();
        let token_2022 = Pubkey::new_from_array(spl_token_2022::id().to_bytes());

        let pool_accounts: Vec<Pubkey> = vec![
            Pubkey::new_unique(),
            Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG).unwrap(),
            base_mint,
            wsol,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        assert_eq!(pool_accounts.len(), 14);

        let ixs = PumpFunAmmDex::build_swap_ix_from_pool_accounts(
            &base_mint.to_string(),
            WSOL_MINT,
            1_000_000,
            1,
            user,
            &pool_accounts,
            Some(token_2022),
            false,
            None,
        )
        .expect("SELL build Token-2022");

        assert_eq!(ixs[0].accounts[11].pubkey, token_2022);
        let owner_spl = SplProgramPubkey::new_from_array(user.to_bytes());
        let mint_spl = SplProgramPubkey::new_from_array(base_mint.to_bytes());
        let token_program_spl = SplProgramPubkey::new_from_array(token_2022.to_bytes());
        let expected_user_base =
            spl_associated_token_account::get_associated_token_address_with_program_id(
                &owner_spl,
                &mint_spl,
                &token_program_spl,
            );
        let expected_user_base = Pubkey::new_from_array(expected_user_base.to_bytes());
        assert_eq!(ixs[0].accounts[5].pubkey, expected_user_base);
    }

    /// Cached/sync `build_swap_ix`: ix #9/#10 follow `PumpAmmPoolStatic.protocol_fee_*` from cache.
    #[test]
    fn test_build_swap_ix_uses_cached_protocol_fee_accounts() {
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let mut dex = PumpFunAmmDex::new(rpc);
        let user = Pubkey::new_unique();
        dex.set_user_authority(user);

        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
        let base_mint = Pubkey::new_unique();
        let program_id = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID).unwrap();
        let quote_tp = Pubkey::new_from_array(spl_token::id().to_bytes());
        let cached_pfr = Pubkey::new_unique();
        let cached_pfr_ta = PumpFunAmmDex::derive_ata_with_program(cached_pfr, wsol, quote_tp);

        let pool = PumpAmmPoolStatic {
            pool_market: Pubkey::new_unique(),
            global_config: Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG).unwrap(),
            base_mint,
            quote_mint: wsol,
            pool_base_vault: Pubkey::new_unique(),
            pool_quote_vault: Pubkey::new_unique(),
            protocol_fee_recipient: cached_pfr,
            protocol_fee_recipient_ta: cached_pfr_ta,
            event_authority: pump_amm_canonical_event_authority(&program_id),
            coin_creator_vault_ata: Pubkey::new_unique(),
            coin_creator_vault_authority: Pubkey::new_unique(),
            global_volume_accumulator: Pubkey::new_unique(),
            fee_config: Pubkey::from_str(PUMPFUN_AMM_FEE_CONFIG).unwrap(),
            fee_program: Pubkey::from_str(PUMPFUN_AMM_FEE_PROGRAM_ID).unwrap(),
            sell_requires_cashback_remaining: false,
            sell_cashback_third_meta: None,
            sell_extended_tail_0: None,
            sell_extended_tail_1: None,
            sell_extended_fee_tail_0: None,
            sell_extended_fee_tail_1: None,
            sell_requires_pre_fee_metas: false,
            sell_pre_fee_meta_1: None,
            last_parse_diagnostics: None,
        };
        dex.pools_by_base.insert(base_mint, pool);

        let ixs = dex
            .build_swap_ix(&base_mint.to_string(), WSOL_MINT, 1_000_000, 1)
            .expect("SELL build_swap_ix");

        assert_eq!(ixs[0].accounts.len(), 21);
        assert_eq!(ixs[0].accounts[9].pubkey, cached_pfr);
        assert_eq!(ixs[0].accounts[10].pubkey, cached_pfr_ta);
    }

    /// Both fee pubkeys default → error (no global recipient substitution).
    /// Recipient set, TA missing → derive fee ATA from observed recipient.
    #[test]
    fn test_build_swap_ix_from_pool_accounts_errors_when_both_fee_pubkeys_missing() {
        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
        let base_mint = Pubkey::new_unique();
        let user = Pubkey::new_unique();
        let quote_tp = Pubkey::new_from_array(spl_token::id().to_bytes());

        let pool_accounts: Vec<Pubkey> = vec![
            Pubkey::new_unique(),
            Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG).unwrap(),
            base_mint,
            wsol,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::default(),
            Pubkey::default(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        assert_eq!(pool_accounts.len(), 14);

        let err = PumpFunAmmDex::build_swap_ix_from_pool_accounts(
            &base_mint.to_string(),
            WSOL_MINT,
            1_000_000,
            1,
            user,
            &pool_accounts,
            None,
            false,
            None,
        )
        .expect_err("must not invent protocol fee accounts");
        assert!(
            err.to_string().contains("protocol_fee_recipient"),
            "unexpected err: {err}"
        );

        // Recipient set but TA missing → derive ATA from recipient only.
        let mut pool_accounts = pool_accounts;
        let observed = Pubkey::new_unique();
        pool_accounts[6] = observed;
        pool_accounts[7] = Pubkey::default();
        let ixs = PumpFunAmmDex::build_swap_ix_from_pool_accounts(
            &base_mint.to_string(),
            WSOL_MINT,
            1_000_000,
            1,
            user,
            &pool_accounts,
            None,
            false,
            None,
        )
        .expect("SELL build partial fee");
        assert_eq!(ixs[0].accounts[9].pubkey, observed);
        assert_eq!(
            ixs[0].accounts[10].pubkey,
            PumpFunAmmDex::derive_ata_with_program(observed, wsol, quote_tp)
        );
    }
}
