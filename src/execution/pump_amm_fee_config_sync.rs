//! JetStream bootstrap + apply for Pump AMM global FeeConfig (A.56).
//!
//! Durable consumers with `DeliverPolicy::Last` only deliver the last message when the durable
//! is first created; after ack + process restart the static cache is empty while the durable
//! continues from ack. Bootstrap therefore uses an **ephemeral** `Last` consumer every startup.

use crate::ipc::PumpAmmGlobalFeeConfigUpdate;
use crate::nats::{
    pump_amm_global_fee_config_bootstrap_consumer_config,
    pump_amm_global_fee_config_live_consumer_config, NatsClient,
    PUMP_AMM_GLOBAL_FEE_CONFIG_STREAM_NAME, TOPIC_PUMP_AMM_GLOBAL_FEE_CONFIG,
};
use crate::solana::dex::pumpfun_amm::pump_amm_update_global_fee_config_account;
use anyhow::{Context, Result};
use async_nats::jetstream;
use futures::StreamExt;
use tracing::{info, warn};

/// Apply a JetStream FeeConfig payload to the process-static cache (idempotent).
#[must_use]
pub fn apply_pump_amm_global_fee_config_jetstream_update(
    update: &PumpAmmGlobalFeeConfigUpdate,
) -> bool {
    pump_amm_update_global_fee_config_account(&update.account_data)
}

/// Pull the latest message on `ironcrab.pump_amm.global_fee_config` from the stream (not durable ack cursor).
pub async fn bootstrap_pump_amm_global_fee_config_from_jetstream(
    nats_client: &NatsClient,
) -> Result<bool> {
    let jetstream = jetstream::new(nats_client.client().clone());

    let stream = match jetstream
        .get_stream(PUMP_AMM_GLOBAL_FEE_CONFIG_STREAM_NAME)
        .await
    {
        Ok(s) => s,
        Err(e) => {
            warn!(
                error = %e,
                stream = PUMP_AMM_GLOBAL_FEE_CONFIG_STREAM_NAME,
                "FeeConfig JetStream stream not found (market-data may not be running)"
            );
            return Ok(false);
        }
    };

    let consumer = stream
        .create_consumer(pump_amm_global_fee_config_bootstrap_consumer_config())
        .await
        .context("create ephemeral FeeConfig bootstrap consumer")?;

    let mut messages = consumer
        .fetch()
        .max_messages(1)
        .expires(std::time::Duration::from_secs(2))
        .messages()
        .await
        .context("fetch FeeConfig bootstrap message")?;

    let Some(msg_result) = messages.next().await else {
        info!(
            subject = TOPIC_PUMP_AMM_GLOBAL_FEE_CONFIG,
            "FeeConfig bootstrap: no message on stream yet"
        );
        return Ok(false);
    };

    let msg = match msg_result {
        Ok(m) => m,
        Err(e) => {
            return Err(anyhow::anyhow!("FeeConfig bootstrap message error: {e}"));
        }
    };
    let update: PumpAmmGlobalFeeConfigUpdate =
        serde_json::from_slice(&msg.payload).context("deserialize PumpAmmGlobalFeeConfigUpdate")?;

    let applied = apply_pump_amm_global_fee_config_jetstream_update(&update);
    if applied {
        info!(
            slot = update.slot,
            subject = TOPIC_PUMP_AMM_GLOBAL_FEE_CONFIG,
            "FeeConfig bootstrap: applied last JetStream snapshot to process cache"
        );
    } else {
        warn!(
            slot = update.slot,
            "FeeConfig bootstrap: JetStream payload did not parse as FeeConfig (cache unchanged)"
        );
    }

    if let Err(e) = msg.ack().await {
        warn!(error = %e, "FeeConfig bootstrap: ack failed (ephemeral consumer)");
    }

    Ok(applied)
}

/// Create an ephemeral live consumer for incremental FeeConfig updates (`DeliverPolicy::New`).
pub async fn create_pump_amm_global_fee_config_live_consumer(
    nats_client: &NatsClient,
) -> Result<
    Option<
        async_nats::jetstream::consumer::Consumer<async_nats::jetstream::consumer::pull::Config>,
    >,
> {
    let jetstream = jetstream::new(nats_client.client().clone());
    let stream = match jetstream
        .get_stream(PUMP_AMM_GLOBAL_FEE_CONFIG_STREAM_NAME)
        .await
    {
        Ok(s) => s,
        Err(e) => {
            warn!(
                error = %e,
                stream = PUMP_AMM_GLOBAL_FEE_CONFIG_STREAM_NAME,
                "FeeConfig stream not found for live consumer"
            );
            return Ok(None);
        }
    };

    stream
        .create_consumer(pump_amm_global_fee_config_live_consumer_config())
        .await
        .map(Some)
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ipc::PumpAmmGlobalFeeConfigUpdate;
    use crate::nats::{
        pump_amm_global_fee_config_bootstrap_consumer_config,
        pump_amm_global_fee_config_live_consumer_config,
    };
    use crate::solana::dex::pumpfun_amm::{
        pump_amm_global_fee_config_loaded, pump_amm_test_reset_fee_quote_cache,
        PUMP_AMM_FEE_CONFIG_ACCOUNT_DISCRIMINATOR,
    };
    use async_nats::jetstream::consumer::DeliverPolicy;
    use solana_sdk::pubkey::Pubkey;

    fn tier0_fee_config_fixture_bytes() -> Vec<u8> {
        let mut data = Vec::with_capacity(128);
        data.extend_from_slice(&PUMP_AMM_FEE_CONFIG_ACCOUNT_DISCRIMINATOR);
        data.push(255);
        data.extend_from_slice(&Pubkey::default().to_bytes());
        for v in [25u64, 5, 0] {
            data.extend_from_slice(&v.to_le_bytes());
        }
        data.extend_from_slice(&1u32.to_le_bytes());
        data.extend_from_slice(&0u128.to_le_bytes());
        for v in [2u64, 93, 30] {
            data.extend_from_slice(&v.to_le_bytes());
        }
        data
    }

    fn sample_jetstream_fee_config_update() -> PumpAmmGlobalFeeConfigUpdate {
        PumpAmmGlobalFeeConfigUpdate::new(
            "market-data",
            "test",
            "run",
            42,
            tier0_fee_config_fixture_bytes(),
        )
    }

    #[test]
    fn restart_simulation_bootstrap_reapplies_last_snapshot_after_process_cache_cleared() {
        pump_amm_test_reset_fee_quote_cache();
        assert!(!pump_amm_global_fee_config_loaded());

        let last_snapshot = sample_jetstream_fee_config_update();
        assert!(apply_pump_amm_global_fee_config_jetstream_update(
            &last_snapshot
        ));
        assert!(pump_amm_global_fee_config_loaded());

        pump_amm_test_reset_fee_quote_cache();
        assert!(!pump_amm_global_fee_config_loaded());

        assert!(apply_pump_amm_global_fee_config_jetstream_update(
            &last_snapshot
        ));
        assert!(pump_amm_global_fee_config_loaded());
    }

    #[test]
    fn bootstrap_consumer_is_ephemeral_last_not_durable() {
        let cfg = pump_amm_global_fee_config_bootstrap_consumer_config();
        assert!(cfg.durable_name.is_none());
        assert!(matches!(cfg.deliver_policy, DeliverPolicy::Last));
    }

    #[test]
    fn live_consumer_is_ephemeral_new_not_durable() {
        let cfg = pump_amm_global_fee_config_live_consumer_config();
        assert!(cfg.durable_name.is_none());
        assert!(matches!(cfg.deliver_policy, DeliverPolicy::New));
    }
}
