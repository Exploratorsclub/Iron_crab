use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_client::client_error::ClientError;
use solana_client::rpc_response::Response;
use solana_sdk::{hash::Hash, message::Message, pubkey::Pubkey, signature::Signature};
use solana_client::rpc_config::RpcTransactionConfig;
// no UiTransactionEncoding needed here
use std::sync::Arc;
use rand::{Rng, SeedableRng};
use std::time::Duration;

#[derive(Clone)]
pub struct SolanaRpc {
    pub rpc: Arc<RpcClient>,
}

impl SolanaRpc {
    pub fn new(url: &str) -> Self {
        let rpc = RpcClient::new(url.to_string());
        Self { rpc: Arc::new(rpc) }
    }

    async fn sleep_with_backoff(attempt: u32) {
        let base = (2u64.pow(attempt.min(6)) * 100).min(2_000); // 100ms, 200ms, 400ms, ... up to 2s
        let mut rng = rand::rngs::StdRng::from_entropy();
        let jitter: u64 = rng.gen_range(0..100);
        tokio::time::sleep(Duration::from_millis(base + jitter)).await;
    }

    fn is_transient_error(_e: &ClientError) -> bool {
        // Conservative: treat errors as transient up to max attempts; specialize later by matching error kinds/codes
        true
    }

    pub async fn get_latest_blockhash_retry(&self) -> Result<Hash, ClientError> {
        let mut attempt = 0u32;
        loop {
            match self.rpc.get_latest_blockhash().await {
                Ok(h) => return Ok(h),
                Err(e) => {
                    if !Self::is_transient_error(&e) || attempt >= 2 { return Err(e); }
                    attempt += 1;
                    crate::metrics::RPC_RETRY_ATTEMPTS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Self::sleep_with_backoff(attempt).await;
                }
            }
        }
    }

    pub async fn get_fee_for_message_retry(&self, msg: &Message) -> Result<u64, ClientError> {
        let mut attempt = 0u32;
        loop {
            match self.rpc.get_fee_for_message(msg).await {
                Ok(f) => return Ok(f),
                Err(e) => {
                    if !Self::is_transient_error(&e) || attempt >= 2 { return Err(e); }
                    attempt += 1;
                    crate::metrics::RPC_RETRY_ATTEMPTS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Self::sleep_with_backoff(attempt).await;
                }
            }
        }
    }

    pub async fn get_account_retry(&self, key: &Pubkey) -> Result<solana_sdk::account::Account, ClientError> {
        let mut attempt = 0u32;
        loop {
            match self.rpc.get_account(key).await {
                Ok(acc) => return Ok(acc),
                Err(e) => {
                    if !Self::is_transient_error(&e) || attempt >= 2 { return Err(e); }
                    attempt += 1;
                    crate::metrics::RPC_RETRY_ATTEMPTS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Self::sleep_with_backoff(attempt).await;
                }
            }
        }
    }

    pub async fn get_balance_retry(&self, key: &Pubkey) -> Result<u64, ClientError> {
        let mut attempt = 0u32;
        loop {
            match self.rpc.get_balance(key).await {
                Ok(b) => return Ok(b),
                Err(e) => {
                    if !Self::is_transient_error(&e) || attempt >= 2 { return Err(e); }
                    attempt += 1;
                    crate::metrics::RPC_RETRY_ATTEMPTS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Self::sleep_with_backoff(attempt).await;
                }
            }
        }
    }

    pub async fn get_signature_statuses_retry(&self, sigs: &[Signature]) -> Result<Response<Vec<Option<solana_transaction_status::TransactionStatus>>>, ClientError> {
        let mut attempt = 0u32;
        loop {
            match self.rpc.get_signature_statuses(sigs).await {
                Ok(s) => return Ok(s),
                Err(e) => {
                    if !Self::is_transient_error(&e) || attempt >= 2 { return Err(e); }
                    attempt += 1;
                    crate::metrics::RPC_RETRY_ATTEMPTS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Self::sleep_with_backoff(attempt).await;
                }
            }
        }
    }

    pub async fn get_transaction_with_config_retry(&self, sig: &Signature, cfg: RpcTransactionConfig) -> Result<solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta, ClientError> {
        let mut attempt = 0u32;
        loop {
            match self.rpc.get_transaction_with_config(sig, cfg.clone()).await {
                Ok(tx) => return Ok(tx),
                Err(e) => {
                    if !Self::is_transient_error(&e) || attempt >= 2 { return Err(e); }
                    attempt += 1;
                    crate::metrics::RPC_RETRY_ATTEMPTS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Self::sleep_with_backoff(attempt).await;
                }
            }
        }
    }
}
