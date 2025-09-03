
use anyhow::{anyhow, Result};
use std::sync::Arc;

use solana_sdk::{
    hash::Hash,
    pubkey::Pubkey as SdkPubkey,
    signature::{read_keypair_file, Keypair, Signature},
    signer::Signer,
    transaction::Transaction,
};

// system transfer comes from solana-system-program in Agave 2.x
// System instruction builders (Solana 3.x system program crate)
// Build system transfer manually (system_instruction module removed in 3.x public API); use legacy helper via solana_program re-export in spl_token if available, else construct inline
use spl_token::solana_program::instruction::Instruction as ProgInstruction;
// System program id constant
const SYSTEM_PROGRAM_ID: SdkPubkey = SdkPubkey::new_from_array([111,111,111,111,111,111,111,111,111,111,111,111,111,111,111,111,111,111,111,111,111,111,111,111,111,111,111,111,111,111,111,111]); // "111111..." base58 system program
// (ProgInstruction already imported above)
use solana_sdk::instruction::{Instruction as SdkInstruction, AccountMeta as SdkAccountMeta};

// Our RPC wrapper
use crate::solana::rpc::SolanaRpc;

// --- SPL Program IDs & helpers ---
use spl_token::id as spl_token_program_id;
use spl_token_2022::id as spl_token_2022_program_id;

// Associated token account (program-facing helpers)
use spl_associated_token_account::{get_associated_token_address_with_program_id, instruction::create_associated_token_account_idempotent};

// Token instruction builders
use spl_token::instruction as spl_ix;
use spl_token_2022::instruction as spl22_ix;

// Use the spl_token re-export of solana_program::Pubkey so the function signatures align
use spl_token::solana_program::pubkey::Pubkey as SplProgPubkey;
#[inline]
fn sdk_to_spl(pk: &SdkPubkey) -> SplProgPubkey { SplProgPubkey::new_from_array(pk.to_bytes()) }
#[inline]
fn spl_to_sdk(pk: &SplProgPubkey) -> SdkPubkey { SdkPubkey::new_from_array(pk.to_bytes()) }
fn prog_ix_to_sdk(ix: ProgInstruction) -> SdkInstruction {
    SdkInstruction {
        program_id: SdkPubkey::new_from_array(ix.program_id.to_bytes()),
        accounts: ix.accounts.into_iter().map(|a| SdkAccountMeta {
            pubkey: SdkPubkey::new_from_array(a.pubkey.to_bytes()),
            is_signer: a.is_signer,
            is_writable: a.is_writable,
        }).collect(),
        data: ix.data,
    }
}

#[derive(Clone)]
pub struct Treasury {
    pub keypair: Arc<Keypair>,
}

impl Treasury {
    pub fn load(path: &str) -> Result<Self> {
        let expanded = shellexpand::tilde(path).to_string();
        let kp = read_keypair_file(&expanded)
            .map_err(|e| anyhow!("Failed to read keypair {expanded}: {e}"))?;
        Ok(Self { keypair: Arc::new(kp) })
    }

    pub fn pubkey(&self) -> SdkPubkey {
        self.keypair.pubkey()
    }

    /// Read SOL balance (lamports)
    pub async fn sol_balance(&self, rpc: &SolanaRpc) -> Result<u64> {
        Ok(rpc.rpc.get_balance(&self.pubkey()).await?)
    }

    /// Determine token program for a given mint (spl-token vs token-2022); returns **SDK** Pubkey
    pub async fn token_program_for_mint(&self, rpc: &SolanaRpc, mint: &SdkPubkey) -> Result<SdkPubkey> {
        let acct = rpc.rpc.get_account(mint).await?;
        let owner_sdk: SdkPubkey = acct.owner;

        // Convert program IDs to SDK for comparison
        let spl_token_sdk = SdkPubkey::new_from_array(spl_token_program_id().to_bytes());
        let spl_token22_sdk = SdkPubkey::new_from_array(spl_token_2022_program_id().to_bytes());

        if owner_sdk == spl_token_sdk {
            Ok(spl_token_sdk)
        } else if owner_sdk == spl_token22_sdk {
            Ok(spl_token22_sdk)
        } else {
            Err(anyhow!("Mint owner is neither spl-token nor spl-token-2022: {}", owner_sdk))
        }
    }

    /// Compute ATA address (returns (ATA, token_program) as **SDK** Pubkeys)
    pub async fn ata_address(&self, rpc: &SolanaRpc, owner: &SdkPubkey, mint: &SdkPubkey) -> Result<(SdkPubkey, SdkPubkey)> {
        let token_prog = self.token_program_for_mint(rpc, mint).await?;
        // Derive using program pubkeys, then convert back
        let ata_prog = get_associated_token_address_with_program_id(
            &sdk_to_spl(owner),
            &sdk_to_spl(mint),
            &sdk_to_spl(&token_prog),
        );
        Ok((spl_to_sdk(&ata_prog), token_prog))
    }

    /// Ensure ATA exists (idempotent). Returns ATA **SDK** Pubkey.
    pub async fn ensure_ata(&self, rpc: &SolanaRpc, owner: &SdkPubkey, mint: &SdkPubkey) -> Result<SdkPubkey> {
        let (ata, token_prog) = self.ata_address(rpc, owner, mint).await?;

        // Already present?
        if rpc.rpc.get_account(&ata).await.is_ok() {
            return Ok(ata);
        }

        // Build program instruction then treat it as SDK Instruction (layouts identical)
        let ix_prog = create_associated_token_account_idempotent(
            &sdk_to_spl(&self.pubkey()),
            &sdk_to_spl(owner),
            &sdk_to_spl(mint),
            &sdk_to_spl(&token_prog),
        );
        let ix = prog_ix_to_sdk(ix_prog);

        let bh: Hash = rpc.rpc.get_latest_blockhash().await?;
        let tx = Transaction::new_signed_with_payer(
            &[ix],
            Some(&self.pubkey()),
            &[self.keypair.as_ref()],
            bh,
        );
        let _sig = rpc.rpc.send_and_confirm_transaction(&tx).await?;
        Ok(ata)
    }

    /// Transfer native SOL
    pub async fn transfer_sol(&self, rpc: &SolanaRpc, to: &SdkPubkey, lamports: u64) -> Result<Signature> {
        // Manually craft system transfer instruction
        let ix = SdkInstruction {
            program_id: SYSTEM_PROGRAM_ID,
            accounts: vec![
                SdkAccountMeta { pubkey: self.pubkey(), is_signer: true, is_writable: true },
                SdkAccountMeta { pubkey: *to, is_signer: false, is_writable: true },
            ],
            data: {
                // system transfer: instruction enum index 2 (per historical definition) + u64 lamports little-endian
                let mut d = Vec::with_capacity(1 + 8);
                d.push(2u8);
                d.extend_from_slice(&lamports.to_le_bytes());
                d
            },
        };
        let bh = rpc.rpc.get_latest_blockhash().await?;
        let tx = Transaction::new_signed_with_payer(
            &[ix],
            Some(&self.pubkey()),
            &[self.keypair.as_ref()],
            bh,
        );
        let sig = rpc.rpc.send_and_confirm_transaction(&tx).await?;
        Ok(sig)
    }

    /// Transfer SPL-Token (classic or 2022). `amount` in base units.
    pub async fn transfer_spl(&self, rpc: &SolanaRpc, mint: &SdkPubkey, to_owner: &SdkPubkey, amount: u64) -> Result<Signature> {
        // Ensure ATAs and determine token program
        let (from_ata, prog_from_sdk) = self.ata_address(rpc, &self.pubkey(), mint).await?;
        let (to_ata,   prog_to_sdk)   = self.ata_address(rpc, to_owner, mint).await?;
        if prog_from_sdk != prog_to_sdk {
            return Err(anyhow!("source and destination token program mismatch"));
        }
        if rpc.rpc.get_account(&to_ata).await.is_err() {
            self.ensure_ata(rpc, to_owner, mint).await?;
        }

        // Optional: decimals
        let decimals = self.try_mint_decimals(rpc, mint).await.ok();

        // Convert to program pubkeys for the token instruction builders
    let from_ata_p = sdk_to_spl(&from_ata);
    let to_ata_p   = sdk_to_spl(&to_ata);
    let mint_p     = sdk_to_spl(mint);
    let owner_p    = sdk_to_spl(&self.pubkey());

        // Figure out which token program we're on
        let spl_token_sdk = SdkPubkey::new_from_array(spl_token_program_id().to_bytes());
        let is_classic = prog_from_sdk == spl_token_sdk;

    let ix_prog = if is_classic {
            if let Some(d) = decimals {
                spl_ix::transfer_checked(&spl_token_program_id(), &from_ata_p, &mint_p, &to_ata_p, &owner_p, &[], amount, d)?
            } else {
                spl_ix::transfer(&spl_token_program_id(), &from_ata_p, &to_ata_p, &owner_p, &[], amount)?
            }
        } else {
            if let Some(d) = decimals {
                spl22_ix::transfer_checked(&spl_token_2022_program_id(), &from_ata_p, &mint_p, &to_ata_p, &owner_p, &[], amount, d)?
            } else {
                // transfer (unchecked) is deprecated in 2022; prefer checked when possible
                spl22_ix::transfer_checked(&spl_token_2022_program_id(), &from_ata_p, &mint_p, &to_ata_p, &owner_p, &[], amount, decimals.unwrap_or(0))?
            }
        };
    let ix = prog_ix_to_sdk(ix_prog);
        let bh = rpc.rpc.get_latest_blockhash().await?;
        let tx = Transaction::new_signed_with_payer(
            &[ix],
            Some(&self.pubkey()),
            &[self.keypair.as_ref()],
            bh,
        );
        let sig = rpc.rpc.send_and_confirm_transaction(&tx).await?;
        Ok(sig)
    }

    /// Wrap SOL → WSOL (classic token program)
    pub async fn wrap_sol(&self, rpc: &SolanaRpc, lamports: u64) -> Result<(SdkPubkey, Signature)> {
        let wsol_mint_sdk = SdkPubkey::new_from_array(spl_token::native_mint::id().to_bytes());
        let owner = self.pubkey();
        let ata = self.ensure_ata(rpc, &owner, &wsol_mint_sdk).await?;

        // 1) send SOL to ATA
        let ix_transfer = SdkInstruction {
            program_id: SYSTEM_PROGRAM_ID,
            accounts: vec![
                SdkAccountMeta { pubkey: owner, is_signer: true, is_writable: true },
                SdkAccountMeta { pubkey: ata, is_signer: false, is_writable: true },
            ],
            data: {
                let mut d = Vec::with_capacity(1 + 8);
                d.push(2u8);
                d.extend_from_slice(&lamports.to_le_bytes());
                d
            },
        };
        // 2) sync native (needs program pubkey for ATA)
    let ata_prog = sdk_to_spl(&ata);
    let ix_sync = prog_ix_to_sdk(spl_ix::sync_native(&spl_token_program_id(), &ata_prog)?);

        let bh = rpc.rpc.get_latest_blockhash().await?;
        let tx = Transaction::new_signed_with_payer(
            &[ix_transfer, ix_sync],
            Some(&owner),
            &[self.keypair.as_ref()],
            bh,
        );
        let sig = rpc.rpc.send_and_confirm_transaction(&tx).await?;
        Ok((ata, sig))
    }

    /// Unwrap WSOL → SOL (close ATA to recipient or self)
    pub async fn unwrap_wsol(&self, rpc: &SolanaRpc, recipient: Option<SdkPubkey>) -> Result<Signature> {
        let wsol_mint_sdk = SdkPubkey::new_from_array(spl_token::native_mint::id().to_bytes());
        let owner = self.pubkey();
        let (ata, prog_sdk) = self.ata_address(rpc, &owner, &wsol_mint_sdk).await?;

        let spl_token_sdk = SdkPubkey::new_from_array(spl_token_program_id().to_bytes());
        if prog_sdk != spl_token_sdk {
            return Err(anyhow!("WSOL must use classic spl-token program"));
        }

        let dest = recipient.unwrap_or(owner);
    let ata_p = sdk_to_spl(&ata);
    let dest_p = sdk_to_spl(&dest);
    let owner_p = sdk_to_spl(&owner);

    let ix = prog_ix_to_sdk(spl_ix::close_account(&spl_token_program_id(), &ata_p, &dest_p, &owner_p, &[])?);
        let bh = rpc.rpc.get_latest_blockhash().await?;
        let tx = Transaction::new_signed_with_payer(
            &[ix],
            Some(&owner),
            &[self.keypair.as_ref()],
            bh,
        );
        let sig = rpc.rpc.send_and_confirm_transaction(&tx).await?;
        Ok(sig)
    }

    async fn try_mint_decimals(&self, rpc: &SolanaRpc, mint: &SdkPubkey) -> Result<u8> {
        // Prefer RPC supply (avoids SPL struct unpack differences)
        if let Ok(supply) = rpc.rpc.get_token_supply(mint).await {
            return Ok(supply.decimals as u8);
        }
        // Fallback: raw account read (decimals at offset 44 in mint layout)
        let acct = rpc.rpc.get_account(mint).await?;
        if acct.data.len() > 44 {
            Ok(acct.data[44])
        } else {
            Err(anyhow!("mint account data too short to read decimals"))
        }
    }
}
