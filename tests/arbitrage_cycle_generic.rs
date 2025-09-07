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
    async fn quote_exact_in(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
    ) -> Result<Option<Quote>> {
        if input_mint == self.a && output_mint == self.b {
            let out = amount_in.saturating_mul(self.mul_bps) / 10_000;
            return Ok(Some(Quote {
                amount_out: out,
                price_impact_bps: 10,
                route: vec!["e".into()],
                fee_bps: 30,
                in_reserve: 1_000_000_000,
                out_reserve: 1_000_000_000,
                input_mint: input_mint.into(),
                output_mint: output_mint.into(),
                tick_spacing: None,
            }));
        }
        if input_mint == self.b && output_mint == self.a {
            let out = amount_in.saturating_mul(10_000) / self.mul_bps.max(1);
            return Ok(Some(Quote {
                amount_out: out,
                price_impact_bps: 10,
                route: vec!["e".into()],
                fee_bps: 30,
                in_reserve: 1_000_000_000,
                out_reserve: 1_000_000_000,
                input_mint: input_mint.into(),
                output_mint: output_mint.into(),
                tick_spacing: None,
            }));
        }
        Ok(None)
    }
    fn build_swap_ix(&self, _i: &str, _o: &str, _a: u64, _m: u64) -> Result<Vec<Instruction>> {
        Ok(vec![])
    }
    fn list_pairs(&self) -> Vec<(String, String)> {
        vec![(self.a.clone(), self.b.clone())]
    }
}

#[tokio::test]
async fn enumerate_4hop_cycle() {
    // Construct A-B, B-C, C-D, D-A edges with multipliers >1 to yield profit on 4-hop cycle.
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
            b: "D".into(),
            mul_bps: 10_050,
        }),
        Arc::new(EdgeDex {
            a: "D".into(),
            b: "A".into(),
            mul_bps: 10_020,
        }),
    ];
    let engine = ArbitrageEngine::new(rpc, connectors).with_profit_params(0, 0);
    let cycles = engine
        .enumerate_cycles_generic(&["A".into()], 1_000_000u64, 5, 20)
        .await
        .unwrap();
    assert!(!cycles.is_empty());
    let has_abcd = cycles.iter().any(|c| {
        c.path.len() == 5
            && c.path[0] == "A"
            && c.path[1] == "B"
            && c.path[2] == "C"
            && c.path[3] == "D"
            && c.path[4] == "A"
    });
    assert!(has_abcd, "expected A->B->C->D->A cycle present");
    let profitable = cycles.iter().any(|c| c.gross_profit > 0);
    assert!(profitable, "expected positive profit on 4-hop cycle");
}
