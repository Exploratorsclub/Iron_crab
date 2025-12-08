use anyhow::{anyhow, Context, Result};
use base64::Engine as _; // for base64 decode on Engine instances
use std::path::{Path, PathBuf};
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
// (ProgInstruction already imported above)
use solana_sdk::instruction::{AccountMeta as SdkAccountMeta, Instruction as SdkInstruction};
// Access system program id from crate to avoid hardcoding
#[inline]
fn system_program_id() -> SdkPubkey {
    SdkPubkey::new_from_array(solana_system_program::id().to_bytes())
}

// Our RPC wrapper
use crate::solana::rpc::SolanaRpc;

// --- SPL Program IDs & helpers ---
use spl_token::id as spl_token_program_id;
use spl_token_2022::id as spl_token_2022_program_id;

// Associated token account (program-facing helpers)
use spl_associated_token_account::{
    get_associated_token_address_with_program_id,
    instruction::create_associated_token_account_idempotent,
};

// Token instruction builders
use spl_token::instruction as spl_ix;
use spl_token_2022::instruction as spl22_ix;

// Use the spl_token re-export of solana_program::Pubkey so the function signatures align
use spl_token::solana_program::pubkey::Pubkey as SplProgPubkey;
#[inline]
fn sdk_to_spl(pk: &SdkPubkey) -> SplProgPubkey {
    SplProgPubkey::new_from_array(pk.to_bytes())
}
#[inline]
fn spl_to_sdk(pk: &SplProgPubkey) -> SdkPubkey {
    SdkPubkey::new_from_array(pk.to_bytes())
}
fn prog_ix_to_sdk(ix: ProgInstruction) -> SdkInstruction {
    SdkInstruction {
        program_id: SdkPubkey::new_from_array(ix.program_id.to_bytes()),
        accounts: ix
            .accounts
            .into_iter()
            .map(|a| SdkAccountMeta {
                pubkey: SdkPubkey::new_from_array(a.pubkey.to_bytes()),
                is_signer: a.is_signer,
                is_writable: a.is_writable,
            })
            .collect(),
        data: ix.data,
    }
}

#[derive(Clone)]
pub struct Treasury {
    signer: Arc<dyn Signer + Send + Sync>,
}

impl Treasury {
    /// Construct from any signer (e.g., remote signer or KMS-backed signer)
    pub fn from_signer(signer: Arc<dyn Signer + Send + Sync>) -> Self {
        Self { signer }
    }
    /// Load from an on-disk keypair file (JSON array), with basic path hardening.
    pub fn load(path: &str) -> Result<Self> {
        Self::load_secure(path, false)
    }

    /// Secure loader with optional strict mode. When strict=true, the path must reside under allowed dirs.
    /// Allowed dirs: env IRONCRAB_KEYPAIR_ALLOWED_DIRS (split by ';' or ','), otherwise defaults to
    ///   - %USERPROFILE%/.config/solana
    ///   - %APPDATA%/Solana (Windows)
    ///   - ./secrets (workspace-local)
    pub fn load_secure(path: &str, strict: bool) -> Result<Self> {
        let expanded = shellexpand::tilde(path).to_string();
        Self::validate_keypair_path(&expanded, strict)?;
        let kp = read_keypair_file(&expanded)
            .map_err(|e| anyhow!("Failed to read keypair {expanded}: {e}"))?;
        Ok(Self {
            signer: Arc::new(kp),
        })
    }

    /// Load from environment variables in priority order:
    /// - IRONCRAB_KEYPAIR_JSON: JSON array string of 32 or 64 bytes
    /// - IRONCRAB_KEYPAIR_B64: base64 of 32 or 64 bytes
    /// - IRONCRAB_KEYPAIR_PATH: file path (validated, strict if IRONCRAB_KEYPAIR_STRICT=1)
    pub fn load_from_env() -> Result<Self> {
        if let Ok(js) = std::env::var("IRONCRAB_KEYPAIR_JSON") {
            let bytes: Vec<u8> =
                serde_json::from_str(&js).context("parse IRONCRAB_KEYPAIR_JSON")?;
            let secret: [u8; 32] = match bytes.len() {
                32 => <[u8; 32]>::try_from(bytes.as_slice()).unwrap(),
                64 => <[u8; 32]>::try_from(&bytes[..32]).unwrap(),
                n => {
                    return Err(anyhow!(
                        "IRONCRAB_KEYPAIR_JSON must be 32 or 64 bytes, got {n}"
                    ))
                }
            };
            let kp = Keypair::new_from_array(secret);
            return Ok(Self {
                signer: Arc::new(kp),
            });
        }
        if let Ok(b64) = std::env::var("IRONCRAB_KEYPAIR_B64") {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(b64)
                .context("decode IRONCRAB_KEYPAIR_B64")?;
            let secret: [u8; 32] = match bytes.len() {
                32 => <[u8; 32]>::try_from(bytes.as_slice()).unwrap(),
                64 => <[u8; 32]>::try_from(&bytes[..32]).unwrap(),
                n => {
                    return Err(anyhow!(
                        "IRONCRAB_KEYPAIR_B64 must decode to 32 or 64 bytes, got {n}"
                    ))
                }
            };
            let kp = Keypair::new_from_array(secret);
            return Ok(Self {
                signer: Arc::new(kp),
            });
        }
        if let Ok(bs58) = std::env::var("IRONCRAB_KEYPAIR_BASE58") {
            // Expects secret key in base58 (64 bytes) per solana-keypair API
            let kp = Keypair::from_base58_string(&bs58);
            return Ok(Self {
                signer: Arc::new(kp),
            });
        }
        if let Ok(p) = std::env::var("IRONCRAB_KEYPAIR_PATH") {
            let strict = std::env::var("IRONCRAB_KEYPAIR_STRICT")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);
            return Self::load_secure(&p, strict);
        }
        Err(anyhow!("No keypair env provided; set IRONCRAB_KEYPAIR_JSON, IRONCRAB_KEYPAIR_B64, or IRONCRAB_KEYPAIR_PATH"))
    }

    fn validate_keypair_path(path: &str, strict: bool) -> Result<()> {
        // Reject UNC/network paths when strict
        #[cfg(windows)]
        if strict && path.starts_with("\\\\") {
            return Err(anyhow!("UNC paths are disallowed in strict mode"));
        }
        let canon = std::fs::canonicalize(Path::new(path))
            .with_context(|| format!("canonicalize {path}"))?;
        if !strict {
            return Ok(());
        }
        // Build allowed directories list
        let allowed_env = std::env::var("IRONCRAB_KEYPAIR_ALLOWED_DIRS").unwrap_or_default();
        let mut allowed: Vec<PathBuf> = allowed_env
            .split([';', ','].as_ref())
            .filter(|s| !s.trim().is_empty())
            .map(|s| shellexpand::tilde(s).to_string())
            .map(PathBuf::from)
            .collect();
        if allowed.is_empty() {
            if let Some(home) = dirs::home_dir() {
                allowed.push(home.join(".config").join("solana"));
            }
            if let Some(appdata) = std::env::var_os("APPDATA") {
                allowed.push(PathBuf::from(appdata).join("Solana"));
            }
            let mut ws = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
            ws.push("secrets");
            allowed.push(ws);
        }
        let mut ok = false;
        for base in allowed {
            if let Ok(base_c) = std::fs::canonicalize(&base) {
                if canon.starts_with(&base_c) {
                    ok = true;
                    break;
                }
            }
        }
        if !ok {
            return Err(anyhow!("keypair path not under allowed directories (enable by setting IRONCRAB_KEYPAIR_ALLOWED_DIRS or disable strict)"));
        }
        Ok(())
    }

    pub fn pubkey(&self) -> SdkPubkey {
        self.signer.pubkey()
    }

    /// Expose a reference to the inner signer for transaction signing.
    pub fn signer_ref(&self) -> &(dyn Signer + Send + Sync) {
        self.signer.as_ref()
    }

    /// Read SOL balance (lamports)
    pub async fn sol_balance(&self, rpc: &SolanaRpc) -> Result<u64> {
        Ok(rpc.rpc.get_balance(&self.pubkey()).await?)
    }

    /// Determine token program for a given mint (spl-token vs token-2022); returns **SDK** Pubkey
    pub async fn token_program_for_mint(
        &self,
        rpc: &SolanaRpc,
        mint: &SdkPubkey,
    ) -> Result<SdkPubkey> {
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
            Err(anyhow!(
                "Mint owner is neither spl-token nor spl-token-2022: {}",
                owner_sdk
            ))
        }
    }

    /// Compute ATA address (returns (ATA, token_program) as **SDK** Pubkeys)
    pub async fn ata_address(
        &self,
        rpc: &SolanaRpc,
        owner: &SdkPubkey,
        mint: &SdkPubkey,
    ) -> Result<(SdkPubkey, SdkPubkey)> {
        let token_prog = self.token_program_for_mint(rpc, mint).await?;
        // Derive using program pubkeys, then convert back
        let ata_prog = get_associated_token_address_with_program_id(
            &sdk_to_spl(owner),
            &sdk_to_spl(mint),
            &sdk_to_spl(&token_prog),
        );
        Ok((spl_to_sdk(&ata_prog), token_prog))
    }

    /// Build ATA creation instruction (idempotent - safe to call even if ATA exists).
    /// Returns (ata_address, optional_create_instruction).
    /// If ATA already exists, instruction is None.
    pub async fn build_ata_ix(
        &self,
        rpc: &SolanaRpc,
        owner: &SdkPubkey,
        mint: &SdkPubkey,
    ) -> Result<(SdkPubkey, Option<solana_sdk::instruction::Instruction>)> {
        let (ata, token_prog) = self.ata_address(rpc, owner, mint).await?;

        // Already present?
        if rpc.rpc.get_account(&ata).await.is_ok() {
            return Ok((ata, None)); // No instruction needed
        }

        // Build create instruction
        let ix_prog = create_associated_token_account_idempotent(
            &sdk_to_spl(&self.pubkey()),
            &sdk_to_spl(owner),
            &sdk_to_spl(mint),
            &sdk_to_spl(&token_prog),
        );
        let ix = prog_ix_to_sdk(ix_prog);
        Ok((ata, Some(ix)))
    }

    /// Build ATA creation instruction for Pump.fun (skips RPC checks for speed).
    /// Assumes standard SPL Token Program and always returns the idempotent create instruction.
    pub fn build_ata_ix_pumpfun(
        &self,
        owner: &SdkPubkey,
        mint: &SdkPubkey,
    ) -> (SdkPubkey, solana_sdk::instruction::Instruction) {
        let token_prog = spl_token::id();
        
        // Derive ATA address
        let ata_prog = get_associated_token_address_with_program_id(
            &sdk_to_spl(owner),
            &sdk_to_spl(mint),
            &token_prog,
        );
        let ata = spl_to_sdk(&ata_prog);

        // Build create instruction (idempotent)
        let ix_prog = create_associated_token_account_idempotent(
            &sdk_to_spl(&self.pubkey()),
            &sdk_to_spl(owner),
            &sdk_to_spl(mint),
            &token_prog,
        );
        let ix = prog_ix_to_sdk(ix_prog);
        
        (ata, ix)
    }

    /// Ensure ATA exists (idempotent). Returns ATA **SDK** Pubkey.
    /// DEPRECATED: Use build_ata_ix to include ATA creation in swap TX instead of separate TX.
    pub async fn ensure_ata(
        &self,
        rpc: &SolanaRpc,
        owner: &SdkPubkey,
        mint: &SdkPubkey,
    ) -> Result<SdkPubkey> {
        let (ata, maybe_ix) = self.build_ata_ix(rpc, owner, mint).await?;
        
        // If instruction exists, send separate TX
        if let Some(ix) = maybe_ix {
            let bh: Hash = rpc.rpc.get_latest_blockhash().await?;
            let mut tx = Transaction::new_with_payer(&[ix], Some(&self.pubkey()));
            tx.try_sign(&[self.signer.as_ref()], bh)?;
            let _sig = rpc.rpc.send_and_confirm_transaction(&tx).await?;
        }
        
        Ok(ata)
    }

    /// Transfer native SOL
    pub async fn transfer_sol(
        &self,
        rpc: &SolanaRpc,
        to: &SdkPubkey,
        lamports: u64,
    ) -> Result<Signature> {
        // Manually craft system transfer instruction
        let ix = SdkInstruction {
            program_id: system_program_id(),
            accounts: vec![
                SdkAccountMeta {
                    pubkey: self.pubkey(),
                    is_signer: true,
                    is_writable: true,
                },
                SdkAccountMeta {
                    pubkey: *to,
                    is_signer: false,
                    is_writable: true,
                },
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
        let mut tx = Transaction::new_with_payer(&[ix], Some(&self.pubkey()));
        tx.try_sign(&[self.signer.as_ref()], bh)?;
        let sig = rpc.rpc.send_and_confirm_transaction(&tx).await?;
        Ok(sig)
    }

    /// Transfer SPL-Token (classic or 2022). `amount` in base units.
    pub async fn transfer_spl(
        &self,
        rpc: &SolanaRpc,
        mint: &SdkPubkey,
        to_owner: &SdkPubkey,
        amount: u64,
    ) -> Result<Signature> {
        // Ensure ATAs and determine token program
        let (from_ata, prog_from_sdk) = self.ata_address(rpc, &self.pubkey(), mint).await?;
        let (to_ata, prog_to_sdk) = self.ata_address(rpc, to_owner, mint).await?;
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
        let to_ata_p = sdk_to_spl(&to_ata);
        let mint_p = sdk_to_spl(mint);
        let owner_p = sdk_to_spl(&self.pubkey());

        // Figure out which token program we're on
        let spl_token_sdk = SdkPubkey::new_from_array(spl_token_program_id().to_bytes());
        let is_classic = prog_from_sdk == spl_token_sdk;

        let ix_prog = if is_classic {
            if let Some(d) = decimals {
                spl_ix::transfer_checked(
                    &spl_token_program_id(),
                    &from_ata_p,
                    &mint_p,
                    &to_ata_p,
                    &owner_p,
                    &[],
                    amount,
                    d,
                )?
            } else {
                spl_ix::transfer(
                    &spl_token_program_id(),
                    &from_ata_p,
                    &to_ata_p,
                    &owner_p,
                    &[],
                    amount,
                )?
            }
        } else if let Some(d) = decimals {
            spl22_ix::transfer_checked(
                &spl_token_2022_program_id(),
                &from_ata_p,
                &mint_p,
                &to_ata_p,
                &owner_p,
                &[],
                amount,
                d,
            )?
        } else {
            // transfer (unchecked) is deprecated in 2022; prefer checked when possible
            spl22_ix::transfer_checked(
                &spl_token_2022_program_id(),
                &from_ata_p,
                &mint_p,
                &to_ata_p,
                &owner_p,
                &[],
                amount,
                decimals.unwrap_or(0),
            )?
        };
        let ix = prog_ix_to_sdk(ix_prog);
        let bh = rpc.rpc.get_latest_blockhash().await?;
        let mut tx = Transaction::new_with_payer(&[ix], Some(&self.pubkey()));
        tx.try_sign(&[self.signer.as_ref()], bh)?;
        let sig = rpc.rpc.send_and_confirm_transaction(&tx).await?;
        Ok(sig)
    }

    /// Wrap SOL → WSOL (classic token program)
    /// Build WSOL wrap instructions (without sending TX).
    /// Returns (wsol_ata, vec![optional_create_ata_ix, transfer_ix, sync_ix]).
    /// Use this to include wrapping atomically in swap TX.
    pub async fn build_wrap_sol_ixs(
        &self,
        rpc: &SolanaRpc,
        lamports: u64,
    ) -> Result<(SdkPubkey, Vec<solana_sdk::instruction::Instruction>)> {
        let wsol_mint_sdk = SdkPubkey::new_from_array(spl_token::native_mint::id().to_bytes());
        let owner = self.pubkey();
        
        // Build ATA creation if needed
        let (ata, maybe_ata_ix) = self.build_ata_ix(rpc, &owner, &wsol_mint_sdk).await?;
        
        let mut ixs = Vec::new();
        
        // Add ATA creation if needed
        if let Some(ix) = maybe_ata_ix {
            ixs.push(ix);
        }
        
        // Transfer SOL to WSOL ATA
        let ix_transfer = SdkInstruction {
            program_id: system_program_id(),
            accounts: vec![
                SdkAccountMeta {
                    pubkey: owner,
                    is_signer: true,
                    is_writable: true,
                },
                SdkAccountMeta {
                    pubkey: ata,
                    is_signer: false,
                    is_writable: true,
                },
            ],
            data: {
                let mut d = Vec::with_capacity(4 + 8);
                d.extend_from_slice(&2u32.to_le_bytes()); // Transfer discriminator
                d.extend_from_slice(&lamports.to_le_bytes());
                d
            },
        };
        ixs.push(ix_transfer);
        
        // Sync native
        let ata_prog = sdk_to_spl(&ata);
        let ix_sync = prog_ix_to_sdk(spl_ix::sync_native(&spl_token_program_id(), &ata_prog)?);
        ixs.push(ix_sync);
        
        Ok((ata, ixs))
    }

    /// Wrap SOL into WSOL ATA (separate TX).
    /// DEPRECATED: Use build_wrap_sol_ixs to include wrapping in swap TX instead.
    pub async fn wrap_sol(&self, rpc: &SolanaRpc, lamports: u64) -> Result<(SdkPubkey, Signature)> {
        let (ata, ixs) = self.build_wrap_sol_ixs(rpc, lamports).await?;
        
        let bh = rpc.rpc.get_latest_blockhash().await?;
        let mut tx = Transaction::new_with_payer(&ixs, Some(&self.pubkey()));
        tx.try_sign(&[self.signer.as_ref()], bh)?;
        let sig = rpc.rpc.send_and_confirm_transaction(&tx).await?;
        Ok((ata, sig))
    }

    /// Unwrap WSOL → SOL (close ATA to recipient or self)
    pub async fn unwrap_wsol(
        &self,
        rpc: &SolanaRpc,
        recipient: Option<SdkPubkey>,
    ) -> Result<Signature> {
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

        let ix = prog_ix_to_sdk(spl_ix::close_account(
            &spl_token_program_id(),
            &ata_p,
            &dest_p,
            &owner_p,
            &[],
        )?);
        let bh = rpc.rpc.get_latest_blockhash().await?;
        let mut tx = Transaction::new_with_payer(&[ix], Some(&owner));
        tx.try_sign(&[self.signer.as_ref()], bh)?;
        let sig = rpc.rpc.send_and_confirm_transaction(&tx).await?;
        Ok(sig)
    }

    async fn try_mint_decimals(&self, rpc: &SolanaRpc, mint: &SdkPubkey) -> Result<u8> {
        // Delegate to centralized helper with metrics and consistent behavior
        crate::solana::token_utils::try_token_decimals(rpc, mint).await
    }
}
