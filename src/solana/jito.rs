// Jito MEV-Protection Client
//
// Supports bundle submission to Jito Block Engines for:
// - MEV protection (atomic execution)
// - Parallel transaction execution
// - Priority fee optimization
//
// Block Engine URLs:
// - Amsterdam: https://amsterdam.mainnet.block-engine.jito.wtf
// - Frankfurt: https://frankfurt.mainnet.block-engine.jito.wtf
// - New York: https://ny.mainnet.block-engine.jito.wtf
// - Tokyo: https://tokyo.mainnet.block-engine.jito.wtf
// - Salt Lake City: https://slc.mainnet.block-engine.jito.wtf

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    transaction::{Transaction, VersionedTransaction},
};
use std::str::FromStr;

/// System program ID
const SYSTEM_PROGRAM_ID: Pubkey = solana_sdk::pubkey!("11111111111111111111111111111111");

/// Build system transfer instruction manually (Solana 3.x compatible)
fn build_system_transfer(from: &Pubkey, to: &Pubkey, lamports: u64) -> Instruction {
    Instruction {
        program_id: SYSTEM_PROGRAM_ID,
        accounts: vec![
            AccountMeta {
                pubkey: *from,
                is_signer: true,
                is_writable: true,
            },
            AccountMeta {
                pubkey: *to,
                is_signer: false,
                is_writable: true,
            },
        ],
        data: {
            // System transfer: instruction index 2 + u64 lamports (little-endian)
            let mut d = Vec::with_capacity(12);
            d.extend_from_slice(&2u32.to_le_bytes());
            d.extend_from_slice(&lamports.to_le_bytes());
            d
        },
    }
}
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::metrics::{JITO_RATE_LIMIT_RETRIES_TOTAL, JITO_SUBMIT_THROTTLED_TOTAL};

/// Default minimum gap between Jito bundle submissions (ms). Jito limit: 1 req/s.
pub const JITO_SUBMIT_MIN_GAP_MS_DEFAULT: u64 = 1100;

/// Backoff before retrying a rate-limited Jito submit (ms).
const JITO_RATE_LIMIT_RETRY_BACKOFF_MS: u64 = 1200;

/// Process-wide throttle for Jito `sendBundle` calls to stay under the 1 req/s rate limit.
#[derive(Debug)]
pub struct JitoSubmitThrottle {
    min_gap: Duration,
    last_submit: Mutex<Option<Instant>>,
}

impl JitoSubmitThrottle {
    pub fn new(min_gap_ms: u64) -> Self {
        let gap_ms = min_gap_ms.max(1);
        Self {
            min_gap: Duration::from_millis(gap_ms),
            last_submit: Mutex::new(None),
        }
    }

    /// Block until at least `min_gap` has elapsed since the previous submit slot was taken.
    pub async fn acquire_submit_slot(&self) {
        loop {
            let mut guard = self.last_submit.lock().await;
            let now = Instant::now();
            if let Some(prev) = *guard {
                let elapsed = now.duration_since(prev);
                if elapsed < self.min_gap {
                    let wait = self.min_gap - elapsed;
                    drop(guard);
                    JITO_SUBMIT_THROTTLED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(wait).await;
                    continue;
                }
            }
            *guard = Some(now);
            return;
        }
    }
}

impl Default for JitoSubmitThrottle {
    fn default() -> Self {
        Self::new(JITO_SUBMIT_MIN_GAP_MS_DEFAULT)
    }
}

/// Returns true when a Jito RPC error indicates per-second rate limiting (-32097).
pub fn is_jito_rate_limit_error(err: &anyhow::Error) -> bool {
    let msg = err.to_string();
    msg.contains("-32097") || msg.contains("Rate limit")
}

/// Jito tip accounts - one of these must receive the tip
const JITO_TIP_ACCOUNTS: &[&str] = &[
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4bVmqZzUQFNRNqiQ1qCEdLb",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
];

/// Jito Block Engine URLs
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JitoRegion {
    Amsterdam,
    Frankfurt,
    NewYork,
    Tokyo,
    SaltLakeCity,
}

impl JitoRegion {
    pub fn url(&self) -> &'static str {
        match self {
            JitoRegion::Amsterdam => "https://amsterdam.mainnet.block-engine.jito.wtf",
            JitoRegion::Frankfurt => "https://frankfurt.mainnet.block-engine.jito.wtf",
            JitoRegion::NewYork => "https://ny.mainnet.block-engine.jito.wtf",
            JitoRegion::Tokyo => "https://tokyo.mainnet.block-engine.jito.wtf",
            JitoRegion::SaltLakeCity => "https://slc.mainnet.block-engine.jito.wtf",
        }
    }

    pub fn all() -> Vec<JitoRegion> {
        vec![
            JitoRegion::Frankfurt,
            JitoRegion::Amsterdam,
            JitoRegion::NewYork,
            JitoRegion::Tokyo,
            JitoRegion::SaltLakeCity,
        ]
    }
}

impl FromStr for JitoRegion {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "amsterdam" => Ok(JitoRegion::Amsterdam),
            "frankfurt" => Ok(JitoRegion::Frankfurt),
            "ny" | "newyork" | "new_york" => Ok(JitoRegion::NewYork),
            "tokyo" => Ok(JitoRegion::Tokyo),
            "slc" | "saltlakecity" | "salt_lake_city" => Ok(JitoRegion::SaltLakeCity),
            _ => Err(anyhow!("Unknown Jito region: {}", s)),
        }
    }
}

/// Response from Jito bundle submission
#[derive(Debug, Deserialize)]
pub struct BundleResponse {
    pub jsonrpc: String,
    pub result: Option<String>,
    pub error: Option<JitoError>,
    #[serde(default)]
    pub id: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct JitoError {
    pub code: i64,
    pub message: String,
}

/// Response from bundle status check
#[derive(Debug, Deserialize)]
pub struct BundleStatusResponse {
    pub jsonrpc: String,
    pub result: Option<BundleStatusResult>,
    pub error: Option<JitoError>,
    #[serde(default)]
    pub id: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct BundleStatusResult {
    pub context: BundleContext,
    pub value: Vec<BundleStatusValue>,
}

#[derive(Debug, Deserialize)]
pub struct BundleContext {
    pub slot: u64,
}

#[derive(Debug, Deserialize)]
pub struct BundleStatusValue {
    pub bundle_id: String,
    pub transactions: Vec<String>,
    pub slot: u64,
    pub confirmation_status: String,
    pub err: Option<serde_json::Value>,
}

/// Bundle tip instruction builder
#[derive(Debug, Serialize)]
struct JsonRpcRequest<T> {
    jsonrpc: String,
    id: u64,
    method: String,
    params: T,
}

/// Jito Client for submitting bundles
pub struct JitoClient {
    http_client: reqwest::Client,
    regions: Vec<JitoRegion>,
    tip_lamports: u64,
}

impl JitoClient {
    /// Create a new Jito client
    ///
    /// # Arguments
    /// * `regions` - Block engine regions to use (will try in order)
    /// * `tip_lamports` - Tip amount in lamports (minimum 1000, recommended 10000+)
    pub fn new(regions: Vec<JitoRegion>, tip_lamports: u64) -> Self {
        Self {
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("Failed to create HTTP client"),
            regions: if regions.is_empty() {
                vec![JitoRegion::Frankfurt]
            } else {
                regions
            },
            tip_lamports: tip_lamports.max(1000), // Minimum tip
        }
    }

    /// Create with default settings (Frankfurt, 10k lamports tip)
    pub fn with_defaults() -> Self {
        Self::new(vec![JitoRegion::Frankfurt], 10_000)
    }

    /// Get a random tip account
    pub fn random_tip_account() -> Pubkey {
        let idx = rand::random::<usize>() % JITO_TIP_ACCOUNTS.len();
        Pubkey::from_str(JITO_TIP_ACCOUNTS[idx]).expect("Invalid tip account")
    }

    /// Build a tip instruction to pay Jito validators
    /// This should be the LAST instruction in the bundle
    pub fn build_tip_instruction(&self, payer: &Pubkey, tip_lamports: u64) -> Result<Instruction> {
        let tip_account = Self::random_tip_account();
        Ok(build_system_transfer(payer, &tip_account, tip_lamports))
    }

    /// Add tip instruction to existing transaction instructions
    /// Note: Caller should rebuild transaction with the returned instructions
    pub fn build_tip_ix_for_payer(
        &self,
        payer: &Pubkey,
    ) -> Result<solana_sdk::instruction::Instruction> {
        self.build_tip_instruction(payer, self.tip_lamports)
    }

    /// Submit a bundle of transactions to Jito
    ///
    /// # Arguments
    /// * `transactions` - Signed transactions to submit as a bundle
    ///
    /// # Returns
    /// Bundle ID if successful
    pub async fn send_bundle(&self, transactions: &[Transaction]) -> Result<String> {
        if transactions.is_empty() {
            return Err(anyhow!("Cannot submit empty bundle"));
        }

        // Serialize transactions to base58 (Jito requires base58, not base64!)
        let serialized: Vec<String> = transactions
            .iter()
            .map(|tx| {
                bs58::encode(bincode::serialize(tx).expect("Failed to serialize tx")).into_string()
            })
            .collect();

        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: 1,
            method: "sendBundle".to_string(),
            params: vec![serialized],
        };

        // Submit to ALL regions in PARALLEL for lowest latency
        // Jito bundles are idempotent (deduplicated by tx signature hash)
        // so parallel submission is safe and recommended
        self.submit_to_all_regions_parallel(&request, transactions.len(), "legacy")
            .await
    }

    /// Internal helper: submit request to all regions in parallel, return first success
    async fn submit_to_all_regions_parallel(
        &self,
        request: &JsonRpcRequest<Vec<Vec<String>>>,
        tx_count: usize,
        tx_type: &str,
    ) -> Result<String> {
        use futures::future::join_all;

        let futures: Vec<_> = self.regions.iter().map(|region| {
            let url = format!("{}/api/v1/bundles", region.url());
            let client = self.http_client.clone();
            let request_clone = serde_json::to_string(request).expect("serialize request");
            let region_clone = *region;

            async move {
                debug!(
                    "Submitting {} bundle to Jito {} ({} txs)",
                    tx_type,
                    region_clone.url(),
                    tx_count
                );

                let result = client
                    .post(&url)
                    .header("Content-Type", "application/json")
                    .body(request_clone)
                    .send()
                    .await;

                match result {
                    Ok(response) => match response.json::<BundleResponse>().await {
                        Ok(bundle_resp) => {
                            if let Some(bundle_id) = bundle_resp.result {
                                info!(
                                    bundle_id = %bundle_id,
                                    region = ?region_clone,
                                    tx_count,
                                    tx_type,
                                    "Jito bundle submitted successfully"
                                );
                                return Ok(bundle_id);
                            }
                            if let Some(err) = bundle_resp.error {
                                // Rate limit errors are common, only debug log them
                                if err.code == -32097 {
                                    debug!(
                                        code = err.code,
                                        message = %err.message,
                                        region = ?region_clone,
                                        "Jito region rate limited"
                                    );
                                } else {
                                    warn!(
                                        code = err.code,
                                        message = %err.message,
                                        region = ?region_clone,
                                        "Jito bundle error"
                                    );
                                }
                                Err(anyhow!("Jito error {}: {}", err.code, err.message))
                            } else {
                                Err(anyhow!("Jito returned neither result nor error"))
                            }
                        }
                        Err(e) => {
                            warn!(?e, region = ?region_clone, "Failed to parse Jito response");
                            Err(e.into())
                        }
                    },
                    Err(e) => {
                        warn!(?e, region = ?region_clone, "Failed to connect to Jito block engine");
                        Err(e.into())
                    }
                }
            }
        }).collect();

        // Wait for all requests to complete
        let results = join_all(futures).await;

        // Return first success, or collect all errors
        let mut errors = Vec::new();
        for result in results {
            match result {
                Ok(bundle_id) => return Ok(bundle_id),
                Err(e) => errors.push(e),
            }
        }

        // All failed - return combined error
        let error_summary: Vec<String> = errors.iter().map(|e| e.to_string()).collect();
        Err(anyhow!(
            "All Jito regions failed: {}",
            error_summary.join("; ")
        ))
    }

    /// Submit a bundle of versioned transactions to Jito
    ///
    /// Use this for transactions with Address Lookup Tables (ALTs).
    /// Jito supports versioned transactions since v2.
    ///
    /// # Arguments
    /// * `transactions` - Signed versioned transactions to submit as a bundle
    ///
    /// # Returns
    /// Bundle ID if successful
    pub async fn send_versioned_bundle(
        &self,
        transactions: &[VersionedTransaction],
    ) -> Result<String> {
        if transactions.is_empty() {
            return Err(anyhow!("Cannot submit empty bundle"));
        }

        // Serialize versioned transactions to base58
        // VersionedTransaction serializes differently from legacy Transaction
        let serialized: Vec<String> = transactions
            .iter()
            .map(|tx| {
                bs58::encode(bincode::serialize(tx).expect("Failed to serialize versioned tx"))
                    .into_string()
            })
            .collect();

        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: 1,
            method: "sendBundle".to_string(),
            params: vec![serialized],
        };

        // Submit to ALL regions in PARALLEL for lowest latency
        // Jito bundles are idempotent (deduplicated by tx signature hash)
        // so parallel submission is safe and recommended
        self.submit_to_all_regions_parallel(&request, transactions.len(), "versioned")
            .await
    }

    /// Submit a versioned bundle after global throttle spacing, with one retry on rate limit.
    pub async fn send_versioned_bundle_throttled(
        &self,
        throttle: &JitoSubmitThrottle,
        transactions: &[VersionedTransaction],
    ) -> Result<String> {
        throttle.acquire_submit_slot().await;
        match self.send_versioned_bundle(transactions).await {
            Ok(bundle_id) => Ok(bundle_id),
            Err(e) if is_jito_rate_limit_error(&e) => {
                JITO_RATE_LIMIT_RETRIES_TOTAL.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(JITO_RATE_LIMIT_RETRY_BACKOFF_MS)).await;
                throttle.acquire_submit_slot().await;
                self.send_versioned_bundle(transactions).await
            }
            Err(e) => Err(e),
        }
    }

    /// Check bundle status
    pub async fn get_bundle_status(&self, bundle_ids: &[String]) -> Result<Vec<BundleStatusValue>> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: 1,
            method: "getBundleStatuses".to_string(),
            params: vec![bundle_ids.to_vec()],
        };

        for region in &self.regions {
            let url = format!("{}/api/v1/bundles", region.url());

            match self
                .http_client
                .post(&url)
                .header("Content-Type", "application/json")
                .json(&request)
                .send()
                .await
            {
                Ok(response) => {
                    if let Ok(status_resp) = response.json::<BundleStatusResponse>().await {
                        if let Some(result) = status_resp.result {
                            return Ok(result.value);
                        }
                    }
                }
                Err(e) => {
                    debug!(?e, region = ?region, "Failed to check bundle status");
                }
            }
        }

        Ok(vec![])
    }

    /// Wait for bundle confirmation (with timeout)
    pub async fn wait_for_bundle(
        &self,
        bundle_id: &str,
        timeout_secs: u64,
    ) -> Result<BundleStatusValue> {
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(timeout_secs);

        while start.elapsed() < timeout {
            let statuses = self.get_bundle_status(&[bundle_id.to_string()]).await?;

            if let Some(status) = statuses.into_iter().find(|s| s.bundle_id == bundle_id) {
                if status.confirmation_status == "confirmed"
                    || status.confirmation_status == "finalized"
                {
                    return Ok(status);
                }
                if status.err.is_some() {
                    return Err(anyhow!("Bundle failed: {:?}", status.err));
                }
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        Err(anyhow!(
            "Bundle confirmation timeout after {}s",
            timeout_secs
        ))
    }
}

/// Builder for creating multi-transaction bundles
pub struct BundleBuilder {
    transactions: Vec<Transaction>,
    tip_lamports: u64,
    payer: Pubkey,
}

impl BundleBuilder {
    pub fn new(payer: Pubkey, tip_lamports: u64) -> Self {
        Self {
            transactions: Vec::new(),
            tip_lamports,
            payer,
        }
    }

    /// Add a transaction to the bundle
    pub fn add_transaction(mut self, tx: Transaction) -> Self {
        self.transactions.push(tx);
        self
    }

    /// Add multiple transactions to the bundle
    pub fn add_transactions(mut self, txs: Vec<Transaction>) -> Self {
        self.transactions.extend(txs);
        self
    }

    /// Build the bundle - note: tip should be added to last TX before calling this
    ///
    /// Returns transactions ready for signing
    pub fn build(self) -> Vec<Transaction> {
        self.transactions
    }

    /// Get tip instruction for the payer (add to last TX manually)
    pub fn get_tip_instruction(&self) -> Result<Instruction> {
        let tip_account = JitoClient::random_tip_account();
        Ok(build_system_transfer(
            &self.payer,
            &tip_account,
            self.tip_lamports,
        ))
    }

    /// Get current transaction count
    pub fn len(&self) -> usize {
        self.transactions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_is_jito_rate_limit_error() {
        let err = anyhow::anyhow!("Jito error -32097: Rate limit exceeded. Limit: 1 per second");
        assert!(is_jito_rate_limit_error(&err));
        let other = anyhow::anyhow!("Jito error -32600: Invalid request");
        assert!(!is_jito_rate_limit_error(&other));
    }

    #[tokio::test]
    async fn test_jito_submit_throttle_enforces_min_gap() {
        let throttle = Arc::new(JitoSubmitThrottle::new(200));
        let t0 = Instant::now();
        throttle.acquire_submit_slot().await;
        throttle.acquire_submit_slot().await;
        let elapsed = t0.elapsed();
        assert!(
            elapsed >= Duration::from_millis(180),
            "expected >=200ms gap between submits, got {:?}",
            elapsed
        );
    }

    #[test]
    fn test_jito_region_parsing() {
        assert_eq!(
            JitoRegion::from_str("frankfurt").unwrap(),
            JitoRegion::Frankfurt
        );
        assert_eq!(
            JitoRegion::from_str("amsterdam").unwrap(),
            JitoRegion::Amsterdam
        );
        assert_eq!(JitoRegion::from_str("ny").unwrap(), JitoRegion::NewYork);
        assert_eq!(JitoRegion::from_str("tokyo").unwrap(), JitoRegion::Tokyo);
    }

    #[test]
    fn test_tip_account_valid() {
        let tip_account = JitoClient::random_tip_account();
        assert!(JITO_TIP_ACCOUNTS
            .iter()
            .any(|&acc| Pubkey::from_str(acc).unwrap() == tip_account));
    }
}
