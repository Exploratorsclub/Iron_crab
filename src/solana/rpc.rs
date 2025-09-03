
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::sync::Arc;

#[derive(Clone)]
pub struct SolanaRpc {
    pub rpc: Arc<RpcClient>,
}

impl SolanaRpc {
    pub fn new(url: &str) -> Self {
        let rpc = RpcClient::new(url.to_string());
        Self { rpc: Arc::new(rpc) }
    }
}
