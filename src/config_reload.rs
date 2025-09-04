//! Config reload utilities: diff computation & file watch abstraction.
use crate::solana::sniper::SniperCfg;

/// Compute a human-readable diff between two `SniperCfg` values (only fields that changed).
pub fn diff_sniper_cfg(old: &SniperCfg, new: &SniperCfg) -> String {
    let mut out: Vec<String> = Vec::new();
    macro_rules! chk { ($f:ident) => { if old.$f != new.$f { out.push(format!("{}: {:?} -> {:?}", stringify!($f), old.$f, new.$f)); } }; }
    chk!(max_buy_sol);
    chk!(max_slippage_bps);
    chk!(blacklist_mints);
    chk!(blacklist_owners);
    chk!(min_pool_liquidity_sol);
    chk!(require_freeze_auth_none);
    chk!(require_mint_decimals_range);
    chk!(lp_top1_max_pct);
    chk!(lp_top3_max_pct);
    chk!(lp_top5_max_pct);
    chk!(max_position_sol);
    chk!(stop_loss_bps);
    chk!(take_profit_bps);
    chk!(daily_loss_limit_sol);
    chk!(max_open_positions);
    chk!(per_mint_position_limit);
    chk!(stop_loss_cooldown_secs);
    chk!(drawdown_scale_start);
    chk!(drawdown_max_reduction);
    chk!(rolling_pnl_window);
    chk!(hot_reload_secs);
    chk!(pending_trade_ttl_secs);
    if out.is_empty() { "(no changes)".to_string() } else { out.join(", ") }
}

/// Validate a prospective new `SniperCfg`. Returns Ok(()) if acceptable else Err(reason).
pub fn validate_sniper_cfg(cfg: &SniperCfg) -> Result<(), String> {
    if cfg.max_buy_sol <= 0.0 { return Err("max_buy_sol must be > 0".into()); }
    if cfg.max_slippage_bps == 0 || cfg.max_slippage_bps > 10_000 { return Err("max_slippage_bps must be 1..=10000".into()); }
    if let Some(mps) = cfg.max_position_sol { if mps <= 0.0 { return Err("max_position_sol must be > 0 if set".into()); } }
    if let Some(w) = cfg.rolling_pnl_window { if w == 0 { return Err("rolling_pnl_window must be > 0".into()); } }
    if let Some(s) = cfg.drawdown_scale_start { if !(0.0..1.0).contains(&s) { return Err("drawdown_scale_start must be in (0,1)".into()); } }
    if let Some(r) = cfg.drawdown_max_reduction { if !(0.0..1.0).contains(&r) { return Err("drawdown_max_reduction must be in (0,1)".into()); } }
    if let (Some(s), Some(r)) = (cfg.drawdown_scale_start, cfg.drawdown_max_reduction) { if r > 0.95 && s < 0.2 { return Err("drawdown settings too aggressive".into()); } }
    Ok(())
}

#[cfg(unix)]
/// Spawn a SIGHUP listener that triggers the provided async reload fn when signal received.
pub fn spawn_sighup_reload(path: std::path::PathBuf, apply: std::sync::Arc<dyn Fn(SniperCfg, String) + Send + Sync>) {
    use tokio::signal::unix::{signal, SignalKind};
    use tracing::info;
    tokio::spawn(async move {
        let mut hup = signal(SignalKind::hangup()).expect("sighup");
        info!(?path, "SIGHUP handler initialized for config reload");
        while hup.recv().await.is_some() {
            if let Ok(txt) = std::fs::read_to_string(&path) {
                if let Ok(root) = toml::from_str::<crate::config::Config>(&txt) {
                    if let Some(sn) = root.sniper.clone() {
                        let new_cfg: SniperCfg = (&sn).into();
                        let diff = "(SIGHUP reload)".to_string();
                        (apply)(new_cfg, diff);
                    }
                }
            }
        }
    });
}
#[cfg(feature = "notify_watch")]
pub async fn watch_and_reload(path: std::path::PathBuf, apply: impl Fn(SniperCfg, String) + Send + 'static) -> anyhow::Result<()> {
    use notify::{RecommendedWatcher, Watcher, EventKind};
    use tokio::sync::mpsc;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut watcher: RecommendedWatcher = RecommendedWatcher::new(move |res| {
        if let Ok(ev) = res { let _ = tx.send(ev); }
    }, notify::Config::default())?;
    watcher.watch(&path, notify::RecursiveMode::NonRecursive)?;
    let mut last_cfg: Option<SniperCfg> = None;
    while let Some(ev) = rx.recv().await {
        if matches!(ev.kind, EventKind::Modify(_)) {
            if let Ok(txt) = std::fs::read_to_string(&path) {
                if let Ok(root) = toml::from_str::<crate::config::Config>(&txt) {
                    if let Some(sn) = root.sniper.clone() {
                        let new_cfg: SniperCfg = (&sn).into();
                        let diff = if let Some(ref old) = last_cfg { diff_sniper_cfg(old, &new_cfg) } else { "(initial load file watch)".into() };
                        apply(new_cfg.clone(), diff);
                        last_cfg = Some(new_cfg);
                    }
                }
            }
        }
    }
    Ok(())
}
