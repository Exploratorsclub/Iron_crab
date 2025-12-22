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
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use serde::{Deserialize, Serialize};
use solana_sdk::{
    pubkey::Pubkey,
    system_instruction,
    transaction::Transaction,
};
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, info, warn};

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
    pub id: u64,
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
    pub id: u64,
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
            regions: if regions.is_empty() { vec![JitoRegion::Frankfurt] } else { regions },
            tip_lamports: tip_lamports.max(1000), // Minimum tip
        }
    }
    
    /// Create with default settings (Frankfurt, 10k lamports tip)
    pub fn default() -> Self {
        Self::new(vec![JitoRegion::Frankfurt], 10_000)
    }
    
    /// Get a random tip account
    pub fn random_tip_account() -> Pubkey {
        let idx = rand::random::<usize>() % JITO_TIP_ACCOUNTS.len();
        Pubkey::from_str(JITO_TIP_ACCOUNTS[idx]).expect("Invalid tip account")
    }
    
    /// Build a tip instruction to pay Jito validators
    /// This should be the LAST instruction in the bundle
    pub fn build_tip_instruction(
        &self,
        payer: &Pubkey,
        tip_lamports: u64,
    ) -> Result<solana_sdk::instruction::Instruction> {
        let tip_account = Self::random_tip_account();
        Ok(system_instruction::transfer(payer, &tip_account, tip_lamports))
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
        
        // Serialize transactions to base64
        let serialized: Vec<String> = transactions
            .iter()
            .map(|tx| BASE64.encode(bincode::serialize(tx).expect("Failed to serialize tx")))
            .collect();
        
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: 1,
            method: "sendBundle".to_string(),
            params: vec![serialized],
        };
        
        // Try each region in order
        let mut last_error = None;
        for region in &self.regions {
            let url = format!("{}/api/v1/bundles", region.url());
            debug!("Submitting bundle to Jito {} ({} txs)", region.url(), transactions.len());
            
            match self.http_client
                .post(&url)
                .header("Content-Type", "application/json")
                .json(&request)
                .send()
                .await
            {
                Ok(response) => {
                    match response.json::<BundleResponse>().await {
                        Ok(bundle_resp) => {
                            if let Some(bundle_id) = bundle_resp.result {
                                info!(
                                    bundle_id = %bundle_id,
                                    region = ?region,
                                    tx_count = transactions.len(),
                                    "Jito bundle submitted successfully"
                                );
                                return Ok(bundle_id);
                            }
                            if let Some(err) = bundle_resp.error {
                                warn!(
                                    code = err.code,
                                    message = %err.message,
                                    region = ?region,
                                    "Jito bundle error"
                                );
                                last_error = Some(anyhow!("Jito error {}: {}", err.code, err.message));
                            }
                        }
                        Err(e) => {
                            warn!(?e, region = ?region, "Failed to parse Jito response");
                            last_error = Some(e.into());
                        }
                    }
                }
                Err(e) => {
                    warn!(?e, region = ?region, "Failed to connect to Jito block engine");
                    last_error = Some(e.into());
                }
            }
        }
        
        Err(last_error.unwrap_or_else(|| anyhow!("All Jito regions failed")))
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
            
            match self.http_client
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
                if status.confirmation_status == "confirmed" || 
                   status.confirmation_status == "finalized" {
                    return Ok(status);
                }
                if status.err.is_some() {
                    return Err(anyhow!(
                        "Bundle failed: {:?}", status.err
                    ));
                }
            }
            
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        
        Err(anyhow!("Bundle confirmation timeout after {}s", timeout_secs))
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
    pub fn get_tip_instruction(&self) -> Result<solana_sdk::instruction::Instruction> {
        let tip_account = JitoClient::random_tip_account();
        Ok(system_instruction::transfer(&self.payer, &tip_account, self.tip_lamports))
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
    
    #[test]
    fn test_jito_region_parsing() {
        assert_eq!(JitoRegion::from_str("frankfurt").unwrap(), JitoRegion::Frankfurt);
        assert_eq!(JitoRegion::from_str("amsterdam").unwrap(), JitoRegion::Amsterdam);
        assert_eq!(JitoRegion::from_str("ny").unwrap(), JitoRegion::NewYork);
        assert_eq!(JitoRegion::from_str("tokyo").unwrap(), JitoRegion::Tokyo);
    }
    
    #[test]
    fn test_tip_account_valid() {
        let tip_account = JitoClient::random_tip_account();
        assert!(JITO_TIP_ACCOUNTS.iter().any(|&acc| Pubkey::from_str(acc).unwrap() == tip_account));
    }
}
