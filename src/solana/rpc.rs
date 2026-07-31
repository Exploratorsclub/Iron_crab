use solana_client::client_error::ClientError;
use solana_client::rpc_config::RpcProgramAccountsConfig;
use solana_client::rpc_config::RpcTransactionConfig;
use solana_client::rpc_response::Response;
use solana_commitment_config::CommitmentConfig;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{hash::Hash, message::Message, pubkey::Pubkey, signature::Signature};
// no UiTransactionEncoding needed here
use rand::{Rng, SeedableRng};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct AdaptiveLimiterOptions {
    pub min: usize,
    pub max: usize,
    pub initial: usize,
    pub inc_every_successes: usize,
    pub dec_on_rate_limit: usize,
    pub timeout_ms: u64,
}

impl Default for AdaptiveLimiterOptions {
    fn default() -> Self {
        Self {
            min: 8,
            max: 64,
            initial: 16,
            inc_every_successes: 64,
            dec_on_rate_limit: 4,
            timeout_ms: 10_000,
        }
    }
}

#[derive(Debug)]
pub struct AdaptivePermit<'a> {
    inflight: &'a AtomicUsize,
}

impl<'a> Drop for AdaptivePermit<'a> {
    fn drop(&mut self) {
        self.inflight.fetch_sub(1, AtomicOrdering::Relaxed);
        crate::metrics::RPC_INFLIGHT_GAUGE.store(
            self.inflight.load(AtomicOrdering::Relaxed) as u64,
            AtomicOrdering::Relaxed,
        );
    }
}

#[derive(Debug)]
pub struct AdaptiveLimiter {
    allowed: AtomicUsize,
    inflight: AtomicUsize,
    success_streak: AtomicUsize,
    opts: AdaptiveLimiterOptions,
}

impl AdaptiveLimiter {
    pub fn new(opts: AdaptiveLimiterOptions) -> Self {
        let allowed = if opts.initial < opts.min {
            opts.min
        } else if opts.initial > opts.max {
            opts.max
        } else {
            opts.initial
        };
        crate::metrics::RPC_ALLOWED_CONCURRENCY.store(allowed as u64, AtomicOrdering::Relaxed);
        Self {
            allowed: AtomicUsize::new(allowed),
            inflight: AtomicUsize::new(0),
            success_streak: AtomicUsize::new(0),
            opts,
        }
    }

    pub fn allowed(&self) -> usize {
        self.allowed.load(AtomicOrdering::Relaxed)
    }
    pub fn inflight(&self) -> usize {
        self.inflight.load(AtomicOrdering::Relaxed)
    }

    pub async fn acquire(&self) -> AdaptivePermit<'_> {
        // Simple spin-wait with small async sleep when saturated
        loop {
            let allowed = self.allowed.load(AtomicOrdering::Relaxed);
            let cur = self.inflight.load(AtomicOrdering::Relaxed);
            if cur < allowed {
                if self
                    .inflight
                    .compare_exchange(
                        cur,
                        cur + 1,
                        AtomicOrdering::Acquire,
                        AtomicOrdering::Relaxed,
                    )
                    .is_ok()
                {
                    crate::metrics::RPC_INFLIGHT_GAUGE
                        .store((cur + 1) as u64, AtomicOrdering::Relaxed);
                    return AdaptivePermit {
                        inflight: &self.inflight,
                    };
                }
            } else {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }
    }

    pub fn on_success(&self) {
        let streak = self.success_streak.fetch_add(1, AtomicOrdering::Relaxed) + 1;
        if streak >= self.opts.inc_every_successes {
            self.success_streak.store(0, AtomicOrdering::Relaxed);
            let mut allowed = self.allowed.load(AtomicOrdering::Relaxed);
            if allowed < self.opts.max {
                allowed += 1;
                self.allowed.store(allowed, AtomicOrdering::Relaxed);
                crate::metrics::RPC_CONCURRENCY_ADJUSTMENTS_TOTAL
                    .fetch_add(1, AtomicOrdering::Relaxed);
                crate::metrics::RPC_ALLOWED_CONCURRENCY
                    .store(allowed as u64, AtomicOrdering::Relaxed);
            }
        }
    }

    pub fn on_rate_limit(&self) {
        self.success_streak.store(0, AtomicOrdering::Relaxed);
        let allowed = self.allowed.load(AtomicOrdering::Relaxed);
        let dec = self.opts.dec_on_rate_limit.max(1);
        let new_allowed = allowed.saturating_sub(dec).max(self.opts.min);
        if new_allowed != allowed {
            self.allowed.store(new_allowed, AtomicOrdering::Relaxed);
            crate::metrics::RPC_CONCURRENCY_ADJUSTMENTS_TOTAL.fetch_add(1, AtomicOrdering::Relaxed);
            crate::metrics::RPC_ALLOWED_CONCURRENCY
                .store(new_allowed as u64, AtomicOrdering::Relaxed);
        }
    }

    pub fn on_timeout(&self) {
        self.on_rate_limit();
    }
    pub fn timeout_ms(&self) -> u64 {
        self.opts.timeout_ms
    }
}

#[derive(Clone)]
pub struct SolanaRpc {
    pub rpc: Arc<RpcClient>,
    limiter: Arc<AdaptiveLimiter>,
    ws_primary_url: Option<String>, // if provided, preferred WS endpoint
    ws_failover_urls: Arc<Vec<String>>, // additional endpoints for PubSub
    ws_connect_timeout_ms: u64,
    ws_max_backoff_ms: u64,
    ws_headers: Arc<std::collections::HashMap<String, String>>,
}

impl SolanaRpc {
    pub fn new(url: &str) -> Self {
        let rpc = RpcClient::new(url.to_string());
        let limiter = AdaptiveLimiter::new(AdaptiveLimiterOptions::default());
        Self {
            rpc: Arc::new(rpc),
            limiter: Arc::new(limiter),
            ws_primary_url: None,
            ws_failover_urls: Arc::new(Vec::new()),
            ws_connect_timeout_ms: 8_000,
            ws_max_backoff_ms: 15_000,
            ws_headers: Arc::new(Default::default()),
        }
    }

    pub fn from_cfg(cfg: &crate::config::SolanaCfg) -> Self {
        let rpc = RpcClient::new(cfg.rpc_url.clone());
        let opts = AdaptiveLimiterOptions {
            min: cfg.rpc_min_concurrency.unwrap_or(8),
            max: cfg.rpc_max_concurrency.unwrap_or(64),
            initial: cfg.rpc_initial_concurrency.unwrap_or(16),
            inc_every_successes: cfg.rpc_inc_every_successes.unwrap_or(64),
            dec_on_rate_limit: cfg.rpc_dec_on_rate_limit.unwrap_or(4),
            timeout_ms: cfg.rpc_timeout_ms.unwrap_or(10_000),
        };
        let limiter = AdaptiveLimiter::new(opts);
        let ws_primary_url = Some(cfg.ws_url.clone());
        let ws_failover_urls = cfg.ws_failover_urls.clone().unwrap_or_default();
        let ws_connect_timeout_ms = cfg.ws_connect_timeout_ms.unwrap_or(8_000);
        let ws_max_backoff_ms = cfg.ws_max_backoff_ms.unwrap_or(15_000);
        let ws_headers = cfg.ws_headers.clone().unwrap_or_default();
        Self {
            rpc: Arc::new(rpc),
            limiter: Arc::new(limiter),
            ws_primary_url,
            ws_failover_urls: Arc::new(ws_failover_urls),
            ws_connect_timeout_ms,
            ws_max_backoff_ms,
            ws_headers: Arc::new(ws_headers),
        }
    }

    pub fn ws_failovers(&self) -> Arc<Vec<String>> {
        self.ws_failover_urls.clone()
    }
    pub fn primary_ws_url(&self) -> Option<String> {
        self.ws_primary_url.clone()
    }
    pub fn ws_connect_timeout_ms(&self) -> u64 {
        self.ws_connect_timeout_ms
    }
    pub fn ws_max_backoff_ms(&self) -> u64 {
        self.ws_max_backoff_ms
    }
    pub fn ws_headers(&self) -> Arc<std::collections::HashMap<String, String>> {
        self.ws_headers.clone()
    }

    pub async fn sleep_with_backoff(attempt: u32, class: ErrorClass) {
        // Adaptive base backoff based on error class / HTTP status codes.
        let base: u64 = match class {
            ErrorClass::RateLimited | ErrorClass::Http(429) => {
                // Back off more aggressively on rate limiting signals
                (2u64.pow(attempt.min(6)) * 300).min(5_000)
            }
            ErrorClass::Http(code) if code == 503 || code == 504 => {
                // Service unavailable / gateway timeout -> medium aggressive
                (2u64.pow(attempt.min(6)) * 250).min(4_000)
            }
            ErrorClass::Timeout => (2u64.pow(attempt.min(6)) * 200).min(3_000),
            ErrorClass::Http(code) if code == 500 || code == 502 => {
                // Internal error / bad gateway – usually brief
                (2u64.pow(attempt.min(6)) * 150).min(2_500)
            }
            _ => (2u64.pow(attempt.min(6)) * 100).min(2_000),
        };
        let mut rng = rand::rngs::StdRng::from_entropy();
        let jitter: u64 = rng.gen_range(0..100);
        crate::metrics::RPC_BACKOFF_MS_TOTAL
            .fetch_add(base + jitter, std::sync::atomic::Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(base + jitter)).await;
    }

    /// Best-effort classification for structured diagnostics (Cold Path logging).
    pub fn classify_client_error(e: &ClientError) -> ErrorClass {
        let s = format!("{e}").to_lowercase();
        if s.contains("timeout")
            || s.contains("timed out")
            || s.contains("deadline has elapsed")
            || s.contains("deadline exceeded")
        {
            return ErrorClass::Timeout;
        }
        if let Some(code) = Self::extract_http_status(&s) {
            return ErrorClass::Http(code);
        }
        if s.contains("too many requests") || s.contains("rate limit") || s.contains("throttl") {
            return ErrorClass::RateLimited;
        }
        ErrorClass::Other
    }

    fn extract_http_status(s: &str) -> Option<u16> {
        // Best-effort parse: look for common HTTP status markers in error strings
        // Examples: "status 429", "http 503", "(504 Gateway Timeout)"
        const CODES: [u16; 7] = [429, 408, 500, 502, 503, 504, 520];
        for c in CODES.iter() {
            let needle = c.to_string();
            if s.contains(&format!(" {} ", needle))
                || s.contains(&format!("({})", needle))
                || s.contains(&format!("status {}", needle))
                || s.contains(&format!("http {}", needle))
                || s.ends_with(&format!(" {}", needle))
            {
                return Some(*c);
            }
        }
        // Keep the earlier heuristic for 429 if phrased without explicit "status"
        if s.contains("429") {
            return Some(429);
        }
        None
    }

    fn is_transient_error(e: &ClientError) -> bool {
        match Self::classify_client_error(e) {
            ErrorClass::Timeout | ErrorClass::RateLimited => true,
            ErrorClass::Http(code) => code == 429 || code == 408 || (500..=599).contains(&code),
            ErrorClass::Other => true,
        }
    }

    async fn with_timeout<F, T>(&self, fut: F) -> Option<Result<T, ClientError>>
    where
        F: std::future::Future<Output = Result<T, ClientError>>,
    {
        let dur = Duration::from_millis(self.limiter.timeout_ms());
        match tokio::time::timeout(dur, fut).await {
            Ok(r) => Some(r),
            Err(_elapsed) => {
                crate::metrics::RPC_TIMEOUTS_TOTAL
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.limiter.on_timeout();
                None
            }
        }
    }

    pub async fn get_latest_blockhash_retry(&self) -> Result<Hash, ClientError> {
        let _permit = self.limiter.acquire().await;
        let mut attempt = 0u32;
        loop {
            match self.with_timeout(self.rpc.get_latest_blockhash()).await {
                Some(Ok(h)) => {
                    self.limiter.on_success();
                    return Ok(h);
                }
                Some(Err(e)) => {
                    let class = Self::classify_client_error(&e);
                    match class {
                        ErrorClass::RateLimited
                        | ErrorClass::Http(429)
                        | ErrorClass::Http(503)
                        | ErrorClass::Http(504) => {
                            self.limiter.on_rate_limit();
                            crate::metrics::RPC_RATE_LIMIT_HITS_TOTAL
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        ErrorClass::Timeout => {
                            self.limiter.on_timeout();
                        }
                        ErrorClass::Other => {}
                        ErrorClass::Http(_) => {}
                    }
                    if !Self::is_transient_error(&e) || attempt >= 2 {
                        return Err(e);
                    }
                    attempt += 1;
                    crate::metrics::RPC_RETRY_ATTEMPTS_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Self::sleep_with_backoff(attempt, class).await;
                }
                None => {
                    // Timeout path handled via limiter + metrics; retry until attempts exhausted
                    if attempt >= 2 {
                        return Err(e_against_timeout());
                    }
                    attempt += 1;
                    crate::metrics::RPC_RETRY_ATTEMPTS_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Self::sleep_with_backoff(attempt, ErrorClass::Timeout).await;
                }
            }
        }
    }

    pub async fn get_fee_for_message_retry(&self, msg: &Message) -> Result<u64, ClientError> {
        let _permit = self.limiter.acquire().await;
        let mut attempt = 0u32;
        loop {
            match self.with_timeout(self.rpc.get_fee_for_message(msg)).await {
                Some(Ok(f)) => {
                    self.limiter.on_success();
                    return Ok(f);
                }
                Some(Err(e)) => {
                    let class = Self::classify_client_error(&e);
                    match class {
                        ErrorClass::RateLimited
                        | ErrorClass::Http(429)
                        | ErrorClass::Http(503)
                        | ErrorClass::Http(504) => {
                            self.limiter.on_rate_limit();
                            crate::metrics::RPC_RATE_LIMIT_HITS_TOTAL
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        ErrorClass::Timeout => {
                            self.limiter.on_timeout();
                        }
                        ErrorClass::Other => {}
                        ErrorClass::Http(_) => {}
                    }
                    if !Self::is_transient_error(&e) || attempt >= 2 {
                        return Err(e);
                    }
                    attempt += 1;
                    crate::metrics::RPC_RETRY_ATTEMPTS_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Self::sleep_with_backoff(attempt, class).await;
                }
                None => {
                    if attempt >= 2 {
                        return Err(e_against_timeout());
                    }
                    attempt += 1;
                    crate::metrics::RPC_RETRY_ATTEMPTS_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Self::sleep_with_backoff(attempt, ErrorClass::Timeout).await;
                }
            }
        }
    }

    /// Get account without any retry logic - for time-sensitive operations like sniping
    /// Returns the raw result from RPC without rate limiting or retries
    pub async fn get_account(
        &self,
        key: &Pubkey,
    ) -> Result<solana_sdk::account::Account, ClientError> {
        self.rpc.get_account(key).await
    }

    pub async fn get_account_retry(
        &self,
        key: &Pubkey,
    ) -> Result<solana_sdk::account::Account, ClientError> {
        Ok(self.get_account_with_slot_retry(key).await?.0)
    }

    /// Like [`get_account_retry`] but also returns the RPC response `context.slot` (Cold Path).
    pub async fn get_account_with_slot_retry(
        &self,
        key: &Pubkey,
    ) -> Result<(solana_sdk::account::Account, u64), ClientError> {
        let commitment = CommitmentConfig::confirmed();
        let _permit = self.limiter.acquire().await;
        let mut attempt = 0u32;
        loop {
            match self
                .with_timeout(self.rpc.get_account_with_commitment(key, commitment))
                .await
            {
                Some(Ok(response)) => {
                    self.limiter.on_success();
                    let slot = response.context.slot;
                    let acc = response.value.ok_or_else(|| {
                        ClientError::from(solana_client::client_error::ClientErrorKind::Custom(
                            "account not found".into(),
                        ))
                    })?;
                    return Ok((acc, slot));
                }
                Some(Err(e)) => {
                    let class = Self::classify_client_error(&e);
                    match class {
                        ErrorClass::RateLimited
                        | ErrorClass::Http(429)
                        | ErrorClass::Http(503)
                        | ErrorClass::Http(504) => {
                            self.limiter.on_rate_limit();
                            crate::metrics::RPC_RATE_LIMIT_HITS_TOTAL
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        ErrorClass::Timeout => {
                            self.limiter.on_timeout();
                        }
                        ErrorClass::Other => {}
                        ErrorClass::Http(_) => {}
                    }
                    if !Self::is_transient_error(&e) || attempt >= 2 {
                        return Err(e);
                    }
                    attempt += 1;
                    crate::metrics::RPC_RETRY_ATTEMPTS_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Self::sleep_with_backoff(attempt, class).await;
                }
                None => {
                    if attempt >= 2 {
                        return Err(e_against_timeout());
                    }
                    attempt += 1;
                    crate::metrics::RPC_RETRY_ATTEMPTS_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Self::sleep_with_backoff(attempt, ErrorClass::Timeout).await;
                }
            }
        }
    }

    /// Chain head slot via RPC (Cold Path fallback when account context and Geyser head are zero).
    pub async fn get_slot_retry(&self) -> Result<u64, ClientError> {
        let _permit = self.limiter.acquire().await;
        let mut attempt = 0u32;
        loop {
            match self.with_timeout(self.rpc.get_slot()).await {
                Some(Ok(slot)) => {
                    self.limiter.on_success();
                    return Ok(slot);
                }
                Some(Err(e)) => {
                    let class = Self::classify_client_error(&e);
                    match class {
                        ErrorClass::RateLimited
                        | ErrorClass::Http(429)
                        | ErrorClass::Http(503)
                        | ErrorClass::Http(504) => {
                            self.limiter.on_rate_limit();
                            crate::metrics::RPC_RATE_LIMIT_HITS_TOTAL
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        ErrorClass::Timeout => {
                            self.limiter.on_timeout();
                        }
                        ErrorClass::Other => {}
                        ErrorClass::Http(_) => {}
                    }
                    if !Self::is_transient_error(&e) || attempt >= 2 {
                        return Err(e);
                    }
                    attempt += 1;
                    crate::metrics::RPC_RETRY_ATTEMPTS_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Self::sleep_with_backoff(attempt, class).await;
                }
                None => {
                    if attempt >= 2 {
                        return Err(e_against_timeout());
                    }
                    attempt += 1;
                    crate::metrics::RPC_RETRY_ATTEMPTS_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Self::sleep_with_backoff(attempt, ErrorClass::Timeout).await;
                }
            }
        }
    }

    pub async fn get_balance_retry(&self, key: &Pubkey) -> Result<u64, ClientError> {
        let _permit = self.limiter.acquire().await;
        let mut attempt = 0u32;
        loop {
            match self.with_timeout(self.rpc.get_balance(key)).await {
                Some(Ok(b)) => {
                    self.limiter.on_success();
                    return Ok(b);
                }
                Some(Err(e)) => {
                    let class = Self::classify_client_error(&e);
                    match class {
                        ErrorClass::RateLimited
                        | ErrorClass::Http(429)
                        | ErrorClass::Http(503)
                        | ErrorClass::Http(504) => {
                            self.limiter.on_rate_limit();
                            crate::metrics::RPC_RATE_LIMIT_HITS_TOTAL
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        ErrorClass::Timeout => {
                            self.limiter.on_timeout();
                        }
                        ErrorClass::Other => {}
                        ErrorClass::Http(_) => {}
                    }
                    if !Self::is_transient_error(&e) || attempt >= 2 {
                        return Err(e);
                    }
                    attempt += 1;
                    crate::metrics::RPC_RETRY_ATTEMPTS_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Self::sleep_with_backoff(attempt, class).await;
                }
                None => {
                    if attempt >= 2 {
                        return Err(e_against_timeout());
                    }
                    attempt += 1;
                    crate::metrics::RPC_RETRY_ATTEMPTS_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Self::sleep_with_backoff(attempt, ErrorClass::Timeout).await;
                }
            }
        }
    }

    pub async fn get_signature_statuses_retry(
        &self,
        sigs: &[Signature],
    ) -> Result<Response<Vec<Option<solana_transaction_status::TransactionStatus>>>, ClientError>
    {
        let _permit = self.limiter.acquire().await;
        let mut attempt = 0u32;
        loop {
            match self
                .with_timeout(self.rpc.get_signature_statuses(sigs))
                .await
            {
                Some(Ok(s)) => {
                    self.limiter.on_success();
                    return Ok(s);
                }
                Some(Err(e)) => {
                    let class = Self::classify_client_error(&e);
                    match class {
                        ErrorClass::RateLimited
                        | ErrorClass::Http(429)
                        | ErrorClass::Http(503)
                        | ErrorClass::Http(504) => {
                            self.limiter.on_rate_limit();
                            crate::metrics::RPC_RATE_LIMIT_HITS_TOTAL
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        ErrorClass::Timeout => {
                            self.limiter.on_timeout();
                        }
                        ErrorClass::Other => {}
                        ErrorClass::Http(_) => {}
                    }
                    if !Self::is_transient_error(&e) || attempt >= 2 {
                        return Err(e);
                    }
                    attempt += 1;
                    crate::metrics::RPC_RETRY_ATTEMPTS_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Self::sleep_with_backoff(attempt, class).await;
                }
                None => {
                    if attempt >= 2 {
                        return Err(e_against_timeout());
                    }
                    attempt += 1;
                    crate::metrics::RPC_RETRY_ATTEMPTS_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Self::sleep_with_backoff(attempt, ErrorClass::Timeout).await;
                }
            }
        }
    }

    pub async fn get_transaction_with_config_retry(
        &self,
        sig: &Signature,
        cfg: RpcTransactionConfig,
    ) -> Result<solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta, ClientError>
    {
        let _permit = self.limiter.acquire().await;
        let mut attempt = 0u32;
        loop {
            match self
                .with_timeout(self.rpc.get_transaction_with_config(sig, cfg))
                .await
            {
                Some(Ok(tx)) => {
                    self.limiter.on_success();
                    return Ok(tx);
                }
                Some(Err(e)) => {
                    let class = Self::classify_client_error(&e);
                    match class {
                        ErrorClass::RateLimited
                        | ErrorClass::Http(429)
                        | ErrorClass::Http(503)
                        | ErrorClass::Http(504) => {
                            self.limiter.on_rate_limit();
                            crate::metrics::RPC_RATE_LIMIT_HITS_TOTAL
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        ErrorClass::Timeout => {
                            self.limiter.on_timeout();
                        }
                        ErrorClass::Other => {}
                        ErrorClass::Http(_) => {}
                    }
                    if !Self::is_transient_error(&e) || attempt >= 2 {
                        return Err(e);
                    }
                    attempt += 1;
                    crate::metrics::RPC_RETRY_ATTEMPTS_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Self::sleep_with_backoff(attempt, class).await;
                }
                None => {
                    if attempt >= 2 {
                        return Err(e_against_timeout());
                    }
                    attempt += 1;
                    crate::metrics::RPC_RETRY_ATTEMPTS_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Self::sleep_with_backoff(attempt, ErrorClass::Timeout).await;
                }
            }
        }
    }

    /// Heavy call: wraps get_program_accounts_with_config with timeout, retries and backoff.
    pub async fn get_program_accounts_with_config_retry(
        &self,
        program_id: &Pubkey,
        cfg: RpcProgramAccountsConfig,
    ) -> Result<Vec<(Pubkey, solana_sdk::account::Account)>, ClientError> {
        let _permit = self.limiter.acquire().await;
        let mut attempt = 0u32;
        loop {
            match self
                .with_timeout(
                    self.rpc
                        .get_program_accounts_with_config(program_id, cfg.clone()),
                )
                .await
            {
                Some(Ok(v)) => {
                    self.limiter.on_success();
                    return Ok(v);
                }
                Some(Err(e)) => {
                    let class = Self::classify_client_error(&e);
                    match class {
                        ErrorClass::RateLimited
                        | ErrorClass::Http(429)
                        | ErrorClass::Http(503)
                        | ErrorClass::Http(504) => {
                            self.limiter.on_rate_limit();
                            crate::metrics::RPC_RATE_LIMIT_HITS_TOTAL
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        ErrorClass::Timeout => {
                            self.limiter.on_timeout();
                        }
                        ErrorClass::Other => {}
                        ErrorClass::Http(_) => {}
                    }
                    // Allow one more retry for heavy calls
                    if !Self::is_transient_error(&e) || attempt >= 3 {
                        return Err(e);
                    }
                    attempt += 1;
                    crate::metrics::RPC_RETRY_ATTEMPTS_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Self::sleep_with_backoff(attempt, class).await;
                }
                None => {
                    if attempt >= 3 {
                        return Err(e_against_timeout());
                    }
                    attempt += 1;
                    crate::metrics::RPC_RETRY_ATTEMPTS_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Self::sleep_with_backoff(attempt, ErrorClass::Timeout).await;
                }
            }
        }
    }

    // ========================================================================
    // AccountJanitor Support Methods
    // ========================================================================

    /// Get all token accounts owned by a wallet
    pub async fn get_token_accounts_by_owner(
        &self,
        owner: &Pubkey,
    ) -> Result<Vec<(Pubkey, solana_sdk::account::Account)>, ClientError> {
        use solana_account_decoder::UiAccountEncoding;
        use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
        use solana_client::rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType};

        let _permit = self.limiter.acquire().await;

        // Filter for token accounts owned by this wallet
        // Token account layout: mint (32) + owner (32) + ...
        // Owner is at offset 32
        let filters = vec![
            RpcFilterType::DataSize(165), // SPL Token account size
            RpcFilterType::Memcmp(Memcmp::new(
                32, // owner offset
                MemcmpEncodedBytes::Base58(owner.to_string()),
            )),
        ];

        let config = RpcProgramAccountsConfig {
            filters: Some(filters),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                ..Default::default()
            },
            ..Default::default()
        };

        let token_program = solana_sdk::pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
        let accounts = self
            .rpc
            .get_program_accounts_with_config(&token_program, config)
            .await
            .map_err(|e| {
                tracing::warn!(error = %e, "get_token_accounts_by_owner failed");
                e
            })?;

        self.limiter.on_success();
        Ok(accounts)
    }

    /// Get signatures for an address (for estimating account age)
    pub async fn get_signatures_for_address(
        &self,
        address: &Pubkey,
        limit: Option<usize>,
    ) -> Result<
        Vec<solana_client::rpc_response::RpcConfirmedTransactionStatusWithSignature>,
        ClientError,
    > {
        self.get_signatures_for_address_with_config(address, limit, None, None)
            .await
    }

    /// Get signatures for an address with pagination (before/until)
    pub async fn get_signatures_for_address_with_config(
        &self,
        address: &Pubkey,
        limit: Option<usize>,
        before: Option<&Signature>,
        until: Option<&Signature>,
    ) -> Result<
        Vec<solana_client::rpc_response::RpcConfirmedTransactionStatusWithSignature>,
        ClientError,
    > {
        use solana_commitment_config::CommitmentConfig;
        use solana_rpc_client::rpc_client::GetConfirmedSignaturesForAddress2Config;

        let _permit = self.limiter.acquire().await;

        let config = GetConfirmedSignaturesForAddress2Config {
            limit,
            before: before.cloned(),
            until: until.cloned(),
            commitment: Some(CommitmentConfig::confirmed()),
        };

        let sigs = self
            .rpc
            .get_signatures_for_address_with_config(address, config)
            .await
            .map_err(|e| {
                tracing::debug!(error = %e, address = %address, "get_signatures_for_address failed");
                e
            })?;

        self.limiter.on_success();
        Ok(sigs)
    }

    /// Get account with retry; returns None if account does not exist
    pub async fn get_account_opt_retry(
        &self,
        key: &Pubkey,
    ) -> Result<Option<solana_sdk::account::Account>, ClientError> {
        match self.get_account_retry(key).await {
            Ok(acc) => Ok(Some(acc)),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("could not find")
                    || msg.contains("AccountNotFound")
                    || msg.contains("account not found")
                {
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Get token accounts by owner with optional mint filter (for pumpfun_amm compatibility)
    pub async fn get_token_accounts_by_owner_with_filter(
        &self,
        owner: &Pubkey,
        program_id: &Pubkey,
    ) -> Result<Vec<(Pubkey, solana_sdk::account::Account)>, ClientError> {
        use solana_account_decoder::UiAccountEncoding;
        use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
        use solana_client::rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType};

        let _permit = self.limiter.acquire().await;

        let filters = vec![
            RpcFilterType::DataSize(165),
            RpcFilterType::Memcmp(Memcmp::new(
                32,
                MemcmpEncodedBytes::Base58(owner.to_string()),
            )),
        ];

        let config = RpcProgramAccountsConfig {
            filters: Some(filters),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                ..Default::default()
            },
            ..Default::default()
        };

        let accounts = self
            .rpc
            .get_program_accounts_with_config(program_id, config)
            .await
            .map_err(|e| {
                tracing::warn!(error = %e, "get_token_accounts_by_owner failed");
                e
            })?;

        self.limiter.on_success();
        Ok(accounts)
    }

    /// Send and confirm a transaction
    pub async fn send_and_confirm_transaction(
        &self,
        tx: &solana_sdk::transaction::Transaction,
    ) -> Result<Signature, ClientError> {
        let _permit = self.limiter.acquire().await;

        let sig = self
            .rpc
            .send_and_confirm_transaction(tx)
            .await
            .map_err(|e| {
                tracing::warn!(error = %e, "send_and_confirm_transaction failed");
                e
            })?;

        self.limiter.on_success();
        Ok(sig)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    RateLimited,
    Timeout,
    Http(u16),
    Other,
}

fn e_against_timeout() -> ClientError {
    // Fallback: map to a generic reqwest error string using ClientError's string-based constructor path.
    // We avoid tight coupling; this will be replaced by the real underlying error on next attempt in practice.
    ClientError::from(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "rpc timeout",
    ))
}
