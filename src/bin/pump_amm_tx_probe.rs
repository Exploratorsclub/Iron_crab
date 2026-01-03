use anyhow::{anyhow, Context, Result};
use clap::Parser;
use reqwest::Client;
use serde_json::{json, Value};
use solana_sdk::hash::hash;
use solana_sdk::pubkey::Pubkey;
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

#[derive(Parser, Debug)]
#[command(about = "Probe a Pump.fun AMM tx and extract program/accounts/discriminators")]
struct Args {
    #[arg(long)]
    sig: String,

    #[arg(long, env = "SOLANA_RPC_URL")]
    rpc_url: String,

    /// How many recent transactions to sample for the pool address to find other discriminators.
    #[arg(long, default_value_t = 25)]
    sample: usize,
}

fn anchor_discriminator(ix_name: &str) -> [u8; 8] {
    let out = hash(format!("global:{ix_name}").as_bytes());
    let mut disc = [0u8; 8];
    disc.copy_from_slice(&out.as_ref()[..8]);
    disc
}

fn fmt_disc(d: [u8; 8]) -> String {
    d.iter().map(|b| format!("{b:02x}")).collect::<Vec<_>>().join("")
}

async fn rpc_call(client: &Client, rpc_url: &str, method: &str, params: Value) -> Result<Value> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    });
    let resp = client
        .post(rpc_url)
        .json(&body)
        .send()
        .await
        .map_err(|e| anyhow!("RPC http error: {e}"))?;
    let status = resp.status();
    let v: Value = resp
        .json()
        .await
        .map_err(|e| anyhow!("RPC json decode error: {e}"))?;
    if !status.is_success() {
        return Err(anyhow!("RPC http status {status}: {v}"));
    }
    if !v.get("error").is_none() {
        return Err(anyhow!("RPC error: {v}"));
    }
    Ok(v)
}

fn parse_account_keys(msg: &Value) -> Result<Vec<String>> {
    let keys = msg
        .get("accountKeys")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("missing message.accountKeys"))?;
    let mut out = Vec::with_capacity(keys.len());
    for k in keys {
        if let Some(s) = k.as_str() {
            out.push(s.to_string());
        } else if let Some(obj) = k.as_object() {
            let s = obj
                .get("pubkey")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("accountKeys element missing pubkey"))?;
            out.push(s.to_string());
        } else {
            return Err(anyhow!("unexpected accountKeys element: {k}"));
        }
    }
    Ok(out)
}

fn parse_instructions(msg: &Value) -> Result<Vec<Value>> {
    msg.get("instructions")
        .and_then(|v| v.as_array())
        .cloned()
        .ok_or_else(|| anyhow!("missing message.instructions"))
}

fn decode_u64_le_args(data: &[u8]) -> Vec<u64> {
    let mut out = Vec::new();
    let mut i = 8usize;
    while i + 8 <= data.len() {
        out.push(u64::from_le_bytes(data[i..i + 8].try_into().unwrap()));
        i += 8;
    }
    out
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let client = Client::new();
    // Validate signature formatting early.
    let _ = bs58::decode(&args.sig)
        .into_vec()
        .context("invalid signature (base58)")?;

    let tx = rpc_call(
        &client,
        &args.rpc_url,
        "getTransaction",
        json!([
            args.sig,
            {"encoding": "json", "maxSupportedTransactionVersion": 0}
        ]),
    )
    .await?;

    let msg = tx
        .get("result")
        .and_then(|r| r.get("transaction"))
        .and_then(|t| t.get("message"))
        .ok_or_else(|| anyhow!("missing result.transaction.message"))?;

    let account_keys = parse_account_keys(msg)?;
    let instructions = parse_instructions(msg)?;

    println!("=== Account keys (index -> pubkey) ===");
    for (i, k) in account_keys.iter().enumerate() {
        println!("{i:>3}: {k}");
    }

    println!("\n=== Top-level instructions ===");

    // Heuristic: find the instruction that looks like the Pump.fun AMM swap from the screenshots:
    // It has many accounts and begins with an Anchor discriminator.
    let mut candidate: Option<(usize, String, Vec<usize>, Vec<u8>)> = None;

    for (ix_idx, ix) in instructions.iter().enumerate() {
        let program_id_index = ix
            .get("programIdIndex")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| anyhow!("ix missing programIdIndex"))? as usize;
        let program = account_keys
            .get(program_id_index)
            .cloned()
            .unwrap_or_else(|| "<bad index>".to_string());

        let accs: Vec<usize> = ix
            .get("accounts")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("ix missing accounts"))?
            .iter()
            .filter_map(|v| v.as_u64().map(|x| x as usize))
            .collect();
        let data_str = ix.get("data").and_then(|v| v.as_str()).unwrap_or("");
        let data = bs58::decode(data_str).into_vec().unwrap_or_default();
        let disc = data.get(0..8).unwrap_or(&[]).to_vec();

        println!(
            "ix[{ix_idx}]: program={program} accounts={} data_len={} disc={}...",
            accs.len(),
            data.len(),
            disc.iter()
                .map(|b| format!("{b:02x}"))
                .collect::<Vec<_>>()
                .join("")
        );

        // Prefer the instruction with the largest account set and non-empty data.
        if data.len() >= 8 {
            let better = match &candidate {
                None => true,
                Some((_, _, best_accs, best_data)) => {
                    accs.len() > best_accs.len() || (accs.len() == best_accs.len() && data.len() > best_data.len())
                }
            };

            if better {
                candidate = Some((ix_idx, program, accs, data));
            }
        }
    }

    let (ix_idx, program_id_str, ix_accounts, ix_data) = candidate
        .ok_or_else(|| anyhow!("No suitable instruction candidate found (need Json encoding)") )?;

    let disc_bytes: [u8; 8] = ix_data[0..8].try_into().unwrap();
    let disc_hex = fmt_disc(disc_bytes);

    println!("\n=== Candidate swap instruction ===");
    println!("ix_index: {ix_idx}");
    println!("program_id: {program_id_str}");
    println!("discriminator_hex: {disc_hex}");
    println!("accounts (in order):");
    for (pos, acc_i) in ix_accounts.iter().enumerate() {
        let pk = account_keys.get(*acc_i).cloned().unwrap_or_default();
        println!("  {pos:>2}: {pk}");
    }

    let buy_disc = anchor_discriminator("buy_exact_quote_in");
    println!(
        "\nanchor(global:buy_exact_quote_in) discriminator_hex: {} (matches_candidate={})",
        fmt_disc(buy_disc),
        buy_disc == disc_bytes
    );

    // Try to identify a SELL discriminator by sampling other txs involving the pool address.
    // From the screenshots, the pool/market account is account #0 in the instruction account list.
    let pool_pk = ix_accounts
        .first()
        .and_then(|i| account_keys.get(*i))
        .ok_or_else(|| anyhow!("missing pool account"))
        .and_then(|s| Pubkey::from_str(s).map_err(|e| anyhow!("bad pool pubkey: {e}")))?;

    println!("\n=== Sampling recent txs for pool {pool_pk} ===");

    let sigs_v = rpc_call(
        &client,
        &args.rpc_url,
        "getSignaturesForAddress",
        json!([
            pool_pk.to_string(),
            {"limit": args.sample.min(100)}
        ]),
    )
    .await?;
    let sigs = sigs_v
        .get("result")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("missing result for getSignaturesForAddress"))?;

    let mut discs: BTreeMap<String, usize> = BTreeMap::new();
    let mut unknown_discs: BTreeSet<String> = BTreeSet::new();
    let mut examples: BTreeMap<String, (String, usize, Vec<u64>)> = BTreeMap::new();

    for s in sigs.iter().take(args.sample) {
        let sig = match s.get("signature").and_then(|v| v.as_str()) {
            Some(v) => v,
            None => continue,
        };

        let tx = match rpc_call(
            &client,
            &args.rpc_url,
            "getTransaction",
            json!([sig, {"encoding": "json", "maxSupportedTransactionVersion": 0}]),
        )
        .await
        {
            Ok(v) => v,
            Err(_) => continue,
        };
        let msg = match tx
            .get("result")
            .and_then(|r| r.get("transaction"))
            .and_then(|t| t.get("message"))
        {
            Some(v) => v,
            None => continue,
        };
        let account_keys = match parse_account_keys(msg) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let instructions = match parse_instructions(msg) {
            Ok(v) => v,
            Err(_) => continue,
        };

        for ix in instructions.iter() {
            let program_id_index = match ix.get("programIdIndex").and_then(|v| v.as_u64()) {
                Some(v) => v as usize,
                None => continue,
            };
            let program = account_keys.get(program_id_index).cloned().unwrap_or_default();
            if program != program_id_str {
                continue;
            }
            let data_str = match ix.get("data").and_then(|v| v.as_str()) {
                Some(v) => v,
                None => continue,
            };
            let data = match bs58::decode(data_str).into_vec() {
                Ok(v) => v,
                Err(_) => continue,
            };
            if data.len() < 8 {
                continue;
            }
            let disc: [u8; 8] = data[0..8].try_into().unwrap();
            let disc_hex = fmt_disc(disc);
            *discs.entry(disc_hex.clone()).or_insert(0) += 1;
            examples.entry(disc_hex.clone()).or_insert_with(|| {
                let args = decode_u64_le_args(&data);
                (sig.to_string(), data.len(), args)
            });
            if disc != buy_disc {
                unknown_discs.insert(disc_hex);
            }
        }
    }

    println!("\n=== Discriminators seen for program {program_id_str} (count) ===");
    for (k, v) in discs.iter() {
        if let Some((ex_sig, data_len, args)) = examples.get(k) {
            println!("{k}: {v} (example_sig={ex_sig} data_len={data_len} u64_args={args:?})");
        } else {
            println!("{k}: {v}");
        }
    }

    if !unknown_discs.is_empty() {
        println!("\n=== Trying to match non-buy discriminators to common SELL names ===");
        let candidates = [
            "sell_exact_base_in",
            "sell_exact_in",
            "sell_exact_token_in",
            "sell_exact_base_in_v2",
            "sell_exact_in_v2",
            "sell_exact_base_in_with_fee",
            "sell_exact_in_with_fee",
            "sell",
        ];
        for disc_hex in unknown_discs.iter() {
            println!("disc {disc_hex}:");
            for name in candidates.iter() {
                let d = anchor_discriminator(name);
                if fmt_disc(d) == *disc_hex {
                    println!("  MATCH: global:{name}");
                }
            }
        }
    } else {
        println!("\nNo non-buy discriminators found in sampled txs.");
    }

    Ok(())
}
