//! JetStream bootstrap + apply for observed token program per mint (A.57).

use crate::execution::live_pool_cache::LivePoolCache;
use crate::ipc::TokenProgramUpdate;
use crate::nats::{
    observed_spl_token_program_str, token_program_bootstrap_consumer_config,
    token_program_live_consumer_config, NatsClient, TOKEN_PROGRAM_STREAM_NAME,
    TOPIC_TOKEN_PROGRAM_PATTERN,
};
use anyhow::{Context, Result};
use async_nats::jetstream;
use futures::StreamExt;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tracing::{info, warn};

const BOOTSTRAP_MAX_MESSAGES: usize = 100_000;
const BOOTSTRAP_MAX_DURATION: Duration = Duration::from_secs(30);

/// Returns mint + token_program when the update is acceptable (Tokenkeg or Token-2022 only).
#[must_use]
pub fn acceptable_token_program_update(update: &TokenProgramUpdate) -> Option<(&str, &str)> {
    if observed_spl_token_program_str(&update.token_program) {
        Some((update.mint.as_str(), update.token_program.as_str()))
    } else {
        None
    }
}

/// Apply JetStream payload to SLAVE `LivePoolCache` mint program (idempotent).
#[must_use]
pub fn apply_token_program_jetstream_update_live_pool_cache(
    cache: &LivePoolCache,
    update: &TokenProgramUpdate,
) -> bool {
    let Some((mint_str, program_str)) = acceptable_token_program_update(update) else {
        return false;
    };
    let Ok(mint) = Pubkey::from_str(mint_str) else {
        return false;
    };
    let Ok(program) = Pubkey::from_str(program_str) else {
        return false;
    };
    cache.update_mint_program(&mint, program);
    true
}

/// Pull last-known program per mint (`LastPerSubject`) until consumer idle or limits hit.
pub async fn bootstrap_token_program_from_jetstream<F>(
    nats_client: &NatsClient,
    mut apply: F,
) -> Result<u64>
where
    F: FnMut(&TokenProgramUpdate) -> bool,
{
    let jetstream = jetstream::new(nats_client.client().clone());

    let stream = match jetstream.get_stream(TOKEN_PROGRAM_STREAM_NAME).await {
        Ok(s) => s,
        Err(e) => {
            warn!(
                error = %e,
                stream = TOKEN_PROGRAM_STREAM_NAME,
                "Token program JetStream stream not found (market-data may not be running)"
            );
            return Ok(0);
        }
    };

    let consumer = stream
        .create_consumer(token_program_bootstrap_consumer_config())
        .await
        .context("create ephemeral token program bootstrap consumer")?;

    let started = Instant::now();
    let mut applied_mints = 0u64;
    let mut messages_seen = 0usize;
    let batch_size = 1000usize;

    loop {
        if messages_seen >= BOOTSTRAP_MAX_MESSAGES || started.elapsed() >= BOOTSTRAP_MAX_DURATION {
            warn!(
                applied_mints,
                messages_seen,
                subject_pattern = TOPIC_TOKEN_PROGRAM_PATTERN,
                "Token program bootstrap stopped at safety limit"
            );
            break;
        }

        let mut messages = consumer
            .fetch()
            .max_messages(batch_size)
            .expires(Duration::from_secs(2))
            .messages()
            .await
            .context("fetch token program bootstrap batch")?;

        let mut batch_count = 0usize;
        while let Some(msg_result) = messages.next().await {
            batch_count += 1;
            messages_seen += 1;
            if messages_seen > BOOTSTRAP_MAX_MESSAGES {
                break;
            }

            let msg = match msg_result {
                Ok(m) => m,
                Err(e) => {
                    warn!(error = %e, "Token program bootstrap message error");
                    continue;
                }
            };

            let update: TokenProgramUpdate = match serde_json::from_slice(&msg.payload) {
                Ok(u) => u,
                Err(e) => {
                    warn!(error = %e, "Failed to deserialize TokenProgramUpdate");
                    if let Err(ack_err) = msg.ack().await {
                        warn!(error = %ack_err, "Token program bootstrap ack failed");
                    }
                    continue;
                }
            };

            if apply(&update) {
                applied_mints += 1;
            }

            if let Err(ack_err) = msg.ack().await {
                warn!(error = %ack_err, "Token program bootstrap ack failed");
            }
        }

        if batch_count == 0 {
            break;
        }
    }

    if applied_mints > 0 {
        info!(
            applied_mints,
            messages_seen, "Token program bootstrap: applied JetStream snapshots"
        );
    }

    Ok(applied_mints)
}

/// Create an ephemeral live consumer for incremental token-program updates (`DeliverPolicy::New`).
pub async fn create_token_program_live_consumer(
    nats_client: &NatsClient,
) -> Result<
    Option<
        async_nats::jetstream::consumer::Consumer<async_nats::jetstream::consumer::pull::Config>,
    >,
> {
    let jetstream = jetstream::new(nats_client.client().clone());
    let stream = match jetstream.get_stream(TOKEN_PROGRAM_STREAM_NAME).await {
        Ok(s) => s,
        Err(e) => {
            warn!(
                error = %e,
                stream = TOKEN_PROGRAM_STREAM_NAME,
                "Token program stream not found for live consumer"
            );
            return Ok(None);
        }
    };

    stream
        .create_consumer(token_program_live_consumer_config())
        .await
        .map(Some)
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::live_pool_cache::create_shared_cache;
    use crate::nats::{
        token_program_jetstream_enqueue_state_reset_for_tests,
        token_program_jetstream_enqueue_wanted, token_program_jetstream_mark_enqueued,
    };
    use spl_token_2022;

    const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

    fn sample_update(mint: &str) -> TokenProgramUpdate {
        TokenProgramUpdate::new("market-data", "test", "run", 1, mint, TOKEN_2022)
    }

    #[test]
    fn vault_owner_token_2022_enqueue_and_apply_empty_cache() {
        token_program_jetstream_enqueue_state_reset_for_tests();
        let mint = Pubkey::new_unique();
        let program = Pubkey::new_from_array(spl_token_2022::id().to_bytes());
        assert!(token_program_jetstream_enqueue_wanted(mint, program));

        let cache = create_shared_cache();
        let update = TokenProgramUpdate::new(
            "market-data",
            "test",
            "run",
            1,
            &mint.to_string(),
            TOKEN_2022,
        );
        assert!(apply_token_program_jetstream_update_live_pool_cache(
            &cache, &update
        ));
        assert_eq!(cache.get_mint_program(&mint), Some(program));
    }

    #[test]
    fn same_mint_same_program_already_enqueued_no_second_publish() {
        token_program_jetstream_enqueue_state_reset_for_tests();
        let mint = Pubkey::new_unique();
        let program = Pubkey::new_from_array(spl_token_2022::id().to_bytes());
        token_program_jetstream_mark_enqueued(mint, program);
        assert!(!token_program_jetstream_enqueue_wanted(mint, program));

        let cache = create_shared_cache();
        let update = sample_update(&mint.to_string());
        assert!(apply_token_program_jetstream_update_live_pool_cache(
            &cache, &update
        ));
        assert_eq!(cache.get_mint_program(&mint), Some(program));
    }

    #[test]
    fn unknown_program_not_accepted_for_apply_or_enqueue() {
        token_program_jetstream_enqueue_state_reset_for_tests();
        let mint = Pubkey::new_unique();
        let bogus = Pubkey::new_unique();
        assert!(!token_program_jetstream_enqueue_wanted(mint, bogus));

        let cache = create_shared_cache();
        let update = TokenProgramUpdate::new(
            "market-data",
            "test",
            "run",
            1,
            &mint.to_string(),
            &bogus.to_string(),
        );
        assert!(!apply_token_program_jetstream_update_live_pool_cache(
            &cache, &update
        ));
        assert_eq!(cache.get_mint_program(&mint), None);
        assert!(acceptable_token_program_update(&update).is_none());
    }
}
