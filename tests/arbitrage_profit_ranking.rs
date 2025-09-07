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
    multiplier: u64,
} // multiplier in bps (10000 = 1x)

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
        // Provide both directions; inverse multiplier for reverse
        if input_mint == self.a && output_mint == self.b {
            let out = amount_in.saturating_mul(self.multiplier) / 10_000;
            return Ok(Some(Quote {
                amount_out: out,
                price_impact_bps: 10,
                route: vec!["edge".into()],
                fee_bps: 30,
                in_reserve: 1_000_000_000,
                out_reserve: 1_000_000_000,
                input_mint: input_mint.into(),
                output_mint: output_mint.into(),
                tick_spacing: None,
            }));
        }
        if input_mint == self.b && output_mint == self.a {
            // approximate inverse
            let out = amount_in.saturating_mul(10_000) / self.multiplier.max(1);
            return Ok(Some(Quote {
                amount_out: out,
                price_impact_bps: 10,
                route: vec!["edge".into()],
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
async fn profit_ranking_orders_cycles() {
    // Cycle1: A-B-C-A multipliers => 1.02 * 1.01 * 1.02 ≈ 1.050
    // Cycle2: A-X-Y-A multipliers => 1.03 * 1.02 * 1.02 ≈ 1.072 (higher profit)
    let rpc = Arc::new(SolanaRpc::new("http://localhost:8899"));
    let connectors: Vec<Arc<dyn Dex>> = vec![
        Arc::new(EdgeDex {
            a: "A".into(),
            b: "B".into(),
            multiplier: 10_200,
        }),
        Arc::new(EdgeDex {
            a: "B".into(),
            b: "C".into(),
            multiplier: 10_100,
        }),
        Arc::new(EdgeDex {
            a: "C".into(),
            b: "A".into(),
            multiplier: 10_200,
        }),
        Arc::new(EdgeDex {
            a: "A".into(),
            b: "X".into(),
            multiplier: 10_300,
        }),
        Arc::new(EdgeDex {
            a: "X".into(),
            b: "Y".into(),
            multiplier: 10_200,
        }),
        Arc::new(EdgeDex {
            a: "Y".into(),
            b: "A".into(),
            multiplier: 10_200,
        }),
    ];
    let engine = ArbitrageEngine::new(rpc, connectors).with_profit_params(0, 0);
    let ranked = engine
        .rank_triangular_cycles(&["A".into()], 1_000_000u64, 5)
        .await
        .unwrap();
    assert!(!ranked.is_empty());
    // Ensure top path contains X or Y (higher profit cycle)
    let top = &ranked[0];
    let (_a, m1, m2) = &top.path;
    assert!(
        m1 == "X" || m2 == "Y" || m1 == "Y" || m2 == "X",
        "expected higher profit cycle with X/Y in top rank"
    );
    // Net or gross profit should be > 0
    assert!(top.gross_profit > 0);
}
