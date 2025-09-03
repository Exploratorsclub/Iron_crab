
use std::env;
use std::str::FromStr;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use ironcrab::solana::dex::raydium::{reader, RAYDIUM_AMM_V4};

fn usage() -> ! {
    eprintln!("Usage:
  raydium_pools --mint <MINT> [--active]
  raydium_pools --pair <MINT_A> <MINT_B> [--active] [--either]");
    std::process::exit(2);
}

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 { usage(); }

    let rpc_url = env::var("RPC_URL").unwrap_or_else(|_| "http://127.0.0.1:8899".to_string());
    let rpc = RpcClient::new(rpc_url.clone());

    let mut active_only = false;
    let mut either = false;
    let mut i = 1;
    enum Mode { Mint(Pubkey), Pair(Pubkey, Pubkey) }
    let mut mode: Option<Mode> = None;

    while i < args.len() {
        match args[i].as_str() {
            "--active" => { active_only = true; i += 1; }
            "--either" => { either = true; i += 1; }
            "--mint" => {
                if i+1 >= args.len() { usage(); }
                let mint = Pubkey::from_str(&args[i+1])?;
                mode = Some(Mode::Mint(mint));
                i += 2;
            }
            "--pair" => {
                if i+2 >= args.len() { usage(); }
                let a = Pubkey::from_str(&args[i+1])?;
                let b = Pubkey::from_str(&args[i+2])?;
                mode = Some(Mode::Pair(a,b));
                i += 3;
            }
            _ => { usage(); }
        }
    }

    let program_id = Pubkey::from_str(RAYDIUM_AMM_V4)?;

    match mode {
        Some(Mode::Mint(m)) => {
            let pools = reader::fetch_pools(&rpc, Some(m), None, active_only, false, program_id)?;
            println!("Found {} pools matching baseMint={}", pools.len(), m);
            for p in pools {
                println!("{} | baseMint={} quoteMint={} lpMint={} market={}",
                    p.address, p.base_mint, p.quote_mint, p.lp_mint, p.market_id);
            }
        }
        Some(Mode::Pair(a,b)) => {
            let mut v = reader::fetch_pools(&rpc, Some(a), Some(b), active_only, false, program_id)?;
            if either {
                let mut r = reader::fetch_pools(&rpc, Some(b), Some(a), active_only, false, program_id)?;
                v.append(&mut r);
                v.sort_by_key(|p| p.address);
                v.dedup_by_key(|p| p.address);
            }
            println!("Found {} pools for pair ({}, {}){}", v.len(), a, b, if either { " [either]" } else { "" });
            for p in v {
                println!("{} | baseMint={} quoteMint={} lpMint={} market={}",
                    p.address, p.base_mint, p.quote_mint, p.lp_mint, p.market_id);
            }
        }
        None => usage()
    }

    Ok(())
}
