use clap::Parser;
use ironcrab::metrics::{record_quote_latency, record_swap_latency, snapshot};
use ironcrab::solana::dex::{orca::Orca, raydium::Raydium, router::Router, Dex};
use ironcrab::solana::rpc::SolanaRpc;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Parser, Debug, Clone)]
#[command(
    author,
    version,
    about = "IronCrab – Load/Stress: Quote & Swap Plan Latency under load"
)]
struct Args {
    /// RPC URL (falls back to SOLANA_RPC_URL env if present)
    #[arg(long)]
    rpc_url: Option<String>,
    /// Test duration in seconds
    #[arg(long, default_value_t = 20)]
    duration_secs: u64,
    /// Concurrency (number of worker tasks)
    #[arg(long, default_value_t = 32)]
    concurrency: usize,
    /// Amount-in (raw units)
    #[arg(long, default_value_t = 1_000_000u64)]
    amount_in: u64,
    /// Slippage bps used when building multi-hop swap plans
    #[arg(long, default_value_t = 100u32)]
    slippage_bps: u32,
    /// Random seed (for reproducibility). If 0, a time-based seed is used.
    #[arg(long, default_value_t = 0u64)]
    seed: u64,
    /// Pin specific mint pairs (format: MINT_IN->MINT_OUT). Multiple values separated by comma or repeated flags.
    #[arg(long, value_delimiter = ',')]
    pairs: Option<Vec<String>>,
    /// Operation mix weights: single-hop quote
    #[arg(long, default_value_t = 1)]
    w_single: u32,
    /// Operation mix weights: hops2 quote
    #[arg(long, default_value_t = 1)]
    w_hops2: u32,
    /// Operation mix weights: hops3 quote
    #[arg(long, default_value_t = 1)]
    w_hops3: u32,
    /// Operation mix weights: build hops2 swap-plan
    #[arg(long, default_value_t = 1)]
    w_plan2: u32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let rpc_url = args
        .rpc_url
        .clone()
        .or_else(|| std::env::var("SOLANA_RPC_URL").ok())
        .unwrap_or_else(|| "https://api.mainnet-beta.solana.com".to_string());
    let rpc = Arc::new(SolanaRpc::new(&rpc_url));

    // Initialize connectors and refresh pools once before the stress run.
    let ray = Arc::new(Raydium::new(rpc.clone()));
    let orc = Arc::new(Orca::new(rpc.clone()));

    // Initial refresh in parallel – we ignore individual errors, the goal is to populate some pools.
    let t0 = Instant::now();
    let (r1, r2) = tokio::join!(ray.refresh_pools(), orc.refresh_pools());
    if let Err(e) = r1 {
        eprintln!("raydium refresh error: {e:?}");
    }
    if let Err(e) = r2 {
        eprintln!("orca refresh error: {e:?}");
    }
    eprintln!("initial refresh took {} ms", t0.elapsed().as_millis());

    // Router across both DEXs
    let router = Arc::new(Router::new(vec![
        ray.clone() as Arc<dyn Dex>,
        orc.clone() as Arc<dyn Dex>,
    ]));

    // Build a pool of pairs from both connectors for random selection (include both directions).
    let mut set: HashSet<(String, String)> = HashSet::new();
    for (a, b) in ray
        .list_pairs()
        .into_iter()
        .chain(orc.list_pairs().into_iter())
    {
        set.insert((a.clone(), b.clone()));
        set.insert((b, a));
    }
    let mut pairs: Vec<(String, String)> = set.into_iter().collect();
    pairs.sort_unstable();
    // Apply pinning if provided
    if let Some(pins) = &args.pairs {
        let mut pin_tuples: HashSet<(String, String)> = HashSet::new();
        for p in pins {
            // Accept formats: A->B or A:B or A,B
            let (a, b) = if let Some((a, b)) = p.split_once("->") {
                (a.trim(), b.trim())
            } else if let Some((a, b)) = p.split_once(':') {
                (a.trim(), b.trim())
            } else if let Some((a, b)) = p.split_once(',') {
                (a.trim(), b.trim())
            } else {
                eprintln!("invalid pair format: {p}; expected A->B");
                continue;
            };
            pin_tuples.insert((a.to_string(), b.to_string()));
        }
        let mut filtered: Vec<(String, String)> = Vec::new();
        let mut missing: Vec<(String, String)> = Vec::new();
        for t in pin_tuples.iter() {
            if pairs.contains(t) {
                filtered.push(t.clone());
            } else {
                missing.push(t.clone());
            }
        }
        if !missing.is_empty() {
            eprintln!(
                "warning: {} pinned pairs not found in discovered set",
                missing.len()
            );
        }
        pairs = filtered;
    }
    if pairs.is_empty() {
        eprintln!("No pairs discovered. Exiting. Make sure RPC URL is valid.");
        return Ok(());
    }
    eprintln!(
        "using {} pairs ({} pinned)",
        pairs.len(),
        args.pairs.as_ref().map(|v| v.len()).unwrap_or(0)
    );

    // Prepare workers
    let deadline = Instant::now() + Duration::from_secs(args.duration_secs);
    let amount_in = args.amount_in;
    let slippage_bps = args.slippage_bps;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(u64, u64)>(); // (quote_ns, swapplan_ns)

    let total_w = (args.w_single as u64)
        + (args.w_hops2 as u64)
        + (args.w_hops3 as u64)
        + (args.w_plan2 as u64);
    for wid in 0..args.concurrency {
        let router = router.clone();
        let pairs = pairs.clone();
        let tx = tx.clone();
        let w_single = args.w_single;
        let w_hops2 = args.w_hops2;
        let w_hops3 = args.w_hops3;
        let w_plan2 = args.w_plan2;
        let seed = if args.seed == 0 {
            // Derive a per-worker seed from time and worker id
            (Instant::now().elapsed().as_nanos() as u64) ^ ((wid as u64) << 32)
        } else {
            args.seed ^ ((wid as u64) << 32)
        };
        tokio::spawn(async move {
            let mut rng = StdRng::seed_from_u64(seed);
            while Instant::now() < deadline {
                // Weighted op pick among: 0=single,1=hops2,2=hops3,3=plan2
                let r = rng.gen_range(0..total_w);
                let mut acc = 0u64;
                let choose = |acc: &mut u64, w: u32, tag: u8, r: u64| -> Option<u8> {
                    *acc += w as u64;
                    if r < *acc {
                        Some(tag)
                    } else {
                        None
                    }
                };
                let op = choose(&mut acc, w_single, 0, r)
                    .or_else(|| choose(&mut acc, w_hops2, 1, r))
                    .or_else(|| choose(&mut acc, w_hops3, 2, r))
                    .or_else(|| choose(&mut acc, w_plan2, 3, r))
                    .unwrap_or(3);
                let (a, b) = &pairs[rng.gen_range(0..pairs.len())];
                match op {
                    0 => {
                        let t = Instant::now();
                        let _ = router.best_quote_exact_in(a, b, amount_in).await;
                        let ns = t.elapsed().as_nanos() as u64;
                        let _ = tx.send((ns, 0));
                        record_quote_latency(ns);
                    }
                    1 => {
                        let t = Instant::now();
                        let _ = router.best_quote_exact_in_hops2(a, b, amount_in).await;
                        let ns = t.elapsed().as_nanos() as u64;
                        let _ = tx.send((ns, 0));
                        record_quote_latency(ns);
                    }
                    2 => {
                        let t = Instant::now();
                        let _ = router.best_quote_exact_in_hops3(a, b, amount_in).await;
                        let ns = t.elapsed().as_nanos() as u64;
                        let _ = tx.send((ns, 0));
                        record_quote_latency(ns);
                    }
                    _ => {
                        let t = Instant::now();
                        let _ = router
                            .build_best_hops2_plan_exact_in(a, b, amount_in, slippage_bps)
                            .await;
                        let ns = t.elapsed().as_nanos() as u64;
                        let _ = tx.send((0, ns));
                        record_swap_latency(ns);
                    }
                }
                // Small jitter sleep to avoid a tight loop starving other tasks
                let sleep_ms: u64 = rng.gen_range(0..2);
                if sleep_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                }
            }
        });
    }
    drop(tx);

    // Aggregate results until channel closes
    let mut quote_samples: Vec<u64> = Vec::with_capacity(args.concurrency * 512);
    let mut swapplan_samples: Vec<u64> = Vec::with_capacity(args.concurrency * 256);
    while let Some((q, s)) = rx.recv().await {
        if q > 0 {
            quote_samples.push(q);
        }
        if s > 0 {
            swapplan_samples.push(s);
        }
    }

    // Summaries
    quote_samples.sort_unstable();
    swapplan_samples.sort_unstable();
    let p = |v: &Vec<u64>, pct: f64| -> u64 {
        if v.is_empty() {
            return 0;
        }
        let idx = ((v.len() as f64 - 1.0) * pct).round() as usize;
        v[idx]
    };
    let avg = |v: &Vec<u64>| -> u64 {
        if v.is_empty() {
            return 0;
        }
        v.iter().sum::<u64>() / (v.len() as u64)
    };

    println!("=== Load/Stress Summary ===");
    println!("duration_s={} concurrency={} pairs={} amount_in={} slippage_bps={} mix=[single:{} hops2:{} hops3:{} plan2:{}]",
        args.duration_secs, args.concurrency, pairs.len(), amount_in, slippage_bps,
        args.w_single, args.w_hops2, args.w_hops3, args.w_plan2);
    let snap = snapshot();
    println!(
        "quote_reqs={} quote_ok={}",
        snap.quote_requests, snap.quote_successes
    );
    if !quote_samples.is_empty() {
        println!(
            "quote latency ns: avg={} p50={} p90={} p99={}",
            avg(&quote_samples),
            p(&quote_samples, 0.50),
            p(&quote_samples, 0.90),
            p(&quote_samples, 0.99)
        );
    } else {
        println!("no quote samples recorded");
    }
    if !swapplan_samples.is_empty() {
        println!(
            "swap-plan latency ns: avg={} p50={} p90={} p99={}",
            avg(&swapplan_samples),
            p(&swapplan_samples, 0.50),
            p(&swapplan_samples, 0.90),
            p(&swapplan_samples, 0.99)
        );
    } else {
        println!("no swap-plan samples recorded");
    }

    Ok(())
}
