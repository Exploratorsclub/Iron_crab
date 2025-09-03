
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;
use tracing::Instrument;

use crate::types::TradeIntent;
use super::EngineContext;

#[async_trait]
pub trait Strategy: Send + Sync {
    fn name(&self) -> &str;

    /// Wird ca. alle `tick_ms` vom Engine Loop aufgerufen.
    async fn on_tick(&self, ctx: Arc<EngineContext>) -> anyhow::Result<Vec<TradeIntent>>;

    /// Optional: Event‑Hooks (z.B. neue Pools entdeckt, Preisänderung, etc.)
    #[allow(unused)]
    fn on_event(&self, _event: &Value) {}
}

/// Hilfsfunktion für instrumentiertes Ausführen
pub async fn run_strategy_tick<S: Strategy + ?Sized>(
    s: &S,
    ctx: Arc<EngineContext>,
) -> anyhow::Result<Vec<TradeIntent>> {
    let span = tracing::info_span!("strategy_tick", name = s.name());
    s.on_tick(ctx).instrument(span).await
}
