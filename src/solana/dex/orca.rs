
//! Orca Connector – Skeleton (Whirlpool/Classic)

use std::sync::Arc;
use anyhow::Result;
use async_trait::async_trait;

use crate::solana::rpc::SolanaRpc;
use super::{Dex, Quote};
use solana_sdk::instruction::Instruction;

pub const ORCA_WHIRLPOOL_PROGRAM: &str = "whirLbMiicV3QDeqAD9nukHf8stYwh5GozfX6rS3SAm"; // verify

pub struct Orca {
    rpc: Arc<SolanaRpc>,
}

impl Orca {
    pub fn new(rpc: Arc<SolanaRpc>) -> Self { Self { rpc } }
}

#[async_trait]
impl Dex for Orca {
    async fn refresh_pools(&self) -> Result<()> {
        tracing::trace!("orca.refresh_pools()");
        Ok(())
    }

    async fn quote_exact_in(&self, _input_mint: &str, _output_mint: &str, _amount_in: u64) -> Result<Option<Quote>> {
        Ok(None)
    }

    fn build_swap_ix(&self, _input_mint: &str, _output_mint: &str, _amount_in: u64, _min_out: u64) -> Result<Vec<Instruction>> {
        Ok(vec![])
    }
}
