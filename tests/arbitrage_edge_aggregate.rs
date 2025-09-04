use ironcrab::solana::dex::Dex;
use ironcrab::solana::arbitrage::ArbitrageEngine; // re-export via crate root
use ironcrab::solana::rpc::SolanaRpc;
use ironcrab::solana::dex::raydium::Raydium; // placeholders (we'll mock Dex below)
use async_trait::async_trait;
use anyhow::Result;
use std::sync::Arc;
use solana_sdk::instruction::Instruction;

#[derive(Clone)]
struct MockDex { pair: (String,String), out_amount: u64 }

#[async_trait]
impl Dex for MockDex {
    async fn refresh_pools(&self) -> Result<()> { Ok(()) }
    async fn quote_exact_in(&self, input_mint: &str, output_mint: &str, amount_in: u64) -> Result<Option<ironcrab::solana::dex::Quote>> {
        if (input_mint, output_mint) == (self.pair.0.as_str(), self.pair.1.as_str()) {
            Ok(Some(ironcrab::solana::dex::Quote { amount_out: self.out_amount + amount_in/1000, price_impact_bps: 10, route: vec!["mock".into()], fee_bps: 30, in_reserve: 1_000_000_000, out_reserve: 1_000_000_000, input_mint: input_mint.into(), output_mint: output_mint.into() }))
        } else { Ok(None) }
    }
    fn build_swap_ix(&self, _i:&str, _o:&str, _a:u64, _m:u64) -> Result<Vec<Instruction>> { Ok(vec![]) }
    fn list_pairs(&self) -> Vec<(String,String)> { vec![self.pair.clone()] }
}

#[tokio::test]
async fn aggregate_picks_higher_output() {
    let rpc = Arc::new(SolanaRpc::new("http://localhost:8899"));
    let d1 = Arc::new(MockDex { pair: ("A".into(), "B".into()), out_amount: 10_000 });
    let d2 = Arc::new(MockDex { pair: ("A".into(), "B".into()), out_amount: 12_000 });
    let engine = ArbitrageEngine::new(rpc, vec![d1, d2]);
    let res = engine.aggregate_best_edges(&[("A".into(), "B".into())], 100_000).await.unwrap();
    assert_eq!(res.len(), 1);
    assert!(res[0].quote.amount_out >= 12_000 + 100_000/1000 - 1); // include formula headroom
}