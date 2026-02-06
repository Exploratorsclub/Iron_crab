#!/usr/bin/env cargo
//! Purge POOL_CACHE JetStream to clear old pools with incorrect fee_config

use anyhow::Result;
use ironcrab::nats::{NatsClient, NatsConfig};

#[tokio::main]
async fn main() -> Result<()> {
    println!("🔥 Purging POOL_CACHE JetStream...");

    let config = NatsConfig::new("nats://127.0.0.1:4222", "purge-nats-pool-cache");
    let mut nats_client = NatsClient::new(config);
    nats_client.connect().await?;

    let jetstream = async_nats::jetstream::new(nats_client.client().clone());

    let mut stream = jetstream.get_stream("POOL_CACHE").await?;
    let before_messages = stream.info().await?.state.messages;
    println!("📊 Before purge: {} messages", before_messages);

    stream.purge().await?;

    let after_messages = stream.info().await?.state.messages;
    println!(
        "✅ After purge: {} messages (purged {})",
        after_messages,
        before_messages.saturating_sub(after_messages)
    );
    println!("🎯 POOL_CACHE purged successfully - all pools will be re-discovered with correct fee_config");

    Ok(())
}
