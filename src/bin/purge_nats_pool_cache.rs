#!/usr/bin/env cargo
//! Purge POOL_CACHE JetStream to clear old pools with incorrect fee_config

use ironcrab::nats::{NatsClient, NatsConfig};
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    println!("🔥 Purging POOL_CACHE JetStream...");
    
    let config = NatsConfig {
        url: "nats://127.0.0.1:4222".to_string(),
    };
    
    let nats_client = NatsClient::new(config).await?;
    let jetstream = async_nats::jetstream::new(nats_client.client.clone());
    
    let stream = jetstream.get_stream("POOL_CACHE").await?;
    let info_before = stream.info().await?;
    println!("📊 Before purge: {} messages", info_before.state.messages);
    
    stream.purge().await?;
    
    let info_after = stream.info().await?;
    println!("✅ After purge: {} messages (purged {})", 
        info_after.state.messages,
        info_before.state.messages - info_after.state.messages
    );
    println!("🎯 POOL_CACHE purged successfully - all pools will be re-discovered with correct fee_config");
    
    Ok(())
}
