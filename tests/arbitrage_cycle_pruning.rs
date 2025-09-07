use anyhow::Result;
use async_trait::async_trait;
use ironcrab::solana::arbitrage::ArbitrageEngine;
use ironcrab::solana::dex::{Dex, Quote};
use ironcrab::solana::rpc::SolanaRpc;
use solana_sdk::instruction::Instruction;
use std::sync::Arc;

#[derive(Clone)]
struct EdgeDex {
    a: String,
    b: String,
    mul_bps: u64,
}
#[async_trait]
impl Dex for EdgeDex {
    async fn refresh_pools(&self) -> Result<()> {
        Ok(())
    }
    async fn quote_exact_in(&self, i: &str, o: &str, amt: u64) -> Result<Option<Quote>> {
        if i == self.a && o == self.b {
            let out = amt.saturating_mul(self.mul_bps) / 10_000;
            return Ok(Some(Quote {
                amount_out: out,
                price_impact_bps: 10,
                route: vec!["e".into()],
                fee_bps: 30,
                in_reserve: 1_000_000_000,
                out_reserve: 1_000_000_000,
                input_mint: i.into(),
                output_mint: o.into(),
                tick_spacing: None,
            }));
        }
        if i == self.b && o == self.a {
            let out = amt.saturating_mul(10_000) / self.mul_bps.max(1);
            return Ok(Some(Quote {
                amount_out: out,
                price_impact_bps: 10,
                route: vec!["e".into()],
                fee_bps: 30,
                in_reserve: 1_000_000_000,
                out_reserve: 1_000_000_000,
                input_mint: i.into(),
                output_mint: o.into(),
                tick_spacing: None,
            }));
        }
        Ok(None)
    }
    fn build_swap_ix(&self, _: &str, _: &str, _: u64, _: u64) -> Result<Vec<Instruction>> {
        Ok(vec![])
    }
    fn list_pairs(&self) -> Vec<(String, String)> {
        vec![(self.a.clone(), self.b.clone())]
    }
}

#[tokio::test]
async fn pruning_keeps_profitable_cycle() {
    // Construct two parallel paths; one clearly inferior. Dominance should prune inferior without losing profit cycle.
    let rpc = Arc::new(SolanaRpc::new("http://localhost:8899"));
    let connectors: Vec<Arc<dyn Dex>> = vec![
        Arc::new(EdgeDex {
            a: "A".into(),
            b: "B".into(),
            mul_bps: 10_200,
        }),
        Arc::new(EdgeDex {
            a: "B".into(),
            b: "C".into(),
            mul_bps: 10_100,
        }),
        Arc::new(EdgeDex {
            a: "C".into(),
            b: "A".into(),
            mul_bps: 10_150,
        }),
        // Inferior duplicate edges (lower multipliers) that should be dominated
        Arc::new(EdgeDex {
            a: "A".into(),
            b: "B".into(),
            mul_bps: 10_050,
        }),
        Arc::new(EdgeDex {
            a: "B".into(),
            b: "C".into(),
            mul_bps: 10_020,
        }),
    ];
    let engine = ArbitrageEngine::new(rpc, connectors).with_profit_params(0, 0);
    let cycles = engine
        .enumerate_cycles_generic(&["A".into()], 1_000_000, 5, 50)
        .await
        .unwrap();
    assert!(
        cycles
            .iter()
            .any(|c| c.path.len() == 4 && c.gross_profit > 0),
        "Expected profitable triangular cycle retained"
    );
}
