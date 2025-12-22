//! Kill Switch Monitor - Geyser-based emergency exit triggers
//! 
//! Monitors token transactions for positions and triggers emergency exits on:
//! 1. Dev/Creator Sell - immediate 100% exit
//! 2. Sell Burst - >N sells of >X SOL within Y slots
//! 3. Flow Kippunkt - buy/sell ratio drops below threshold or N consecutive negative flow slots

use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use parking_lot::RwLock;
use tracing::{debug, info, warn};

/// Trade event from Geyser
#[derive(Debug, Clone)]
pub struct TokenTradeEvent {
    pub mint: Pubkey,
    pub slot: u64,
    pub is_buy: bool,
    pub sol_amount: f64,
    pub trader: Pubkey,
    pub timestamp: i64,
}

/// Kill switch trigger reason
#[derive(Debug, Clone, PartialEq)]
pub enum KillSwitchReason {
    DevSell { dev_address: String, sol_amount: f64 },
    SellBurst { sell_count: u32, total_sol: f64, slots: u64 },
    FlowRatio { ratio: f64, threshold: f64 },
    NegativeFlow { consecutive_slots: u64 },
}

/// Per-token flow tracking
#[derive(Debug, Clone, Default)]
struct TokenFlowState {
    /// Recent trades for this token (slot, is_buy, sol_amount, trader)
    recent_trades: VecDeque<(u64, bool, f64, Pubkey)>,
    /// Slot-by-slot net flow (positive = buys > sells)
    slot_flows: VecDeque<(u64, f64)>,
    /// Creator/dev address if known
    creator: Option<Pubkey>,
    /// Last processed slot
    last_slot: u64,
}

impl TokenFlowState {
    fn new(creator: Option<Pubkey>) -> Self {
        Self {
            recent_trades: VecDeque::with_capacity(100),
            slot_flows: VecDeque::with_capacity(20),
            creator,
            last_slot: 0,
        }
    }
    
    /// Add a trade and update flow state
    fn add_trade(&mut self, slot: u64, is_buy: bool, sol_amount: f64, trader: Pubkey) {
        // Add to recent trades
        self.recent_trades.push_back((slot, is_buy, sol_amount, trader));
        
        // Keep only last 100 trades
        while self.recent_trades.len() > 100 {
            self.recent_trades.pop_front();
        }
        
        // Update slot flow
        let flow = if is_buy { sol_amount } else { -sol_amount };
        
        if let Some((last_slot, last_flow)) = self.slot_flows.back_mut() {
            if *last_slot == slot {
                *last_flow += flow;
            } else {
                self.slot_flows.push_back((slot, flow));
            }
        } else {
            self.slot_flows.push_back((slot, flow));
        }
        
        // Keep only last 20 slots
        while self.slot_flows.len() > 20 {
            self.slot_flows.pop_front();
        }
        
        self.last_slot = slot;
    }
}

/// Kill Switch Monitor
pub struct KillSwitchMonitor {
    /// Per-token flow state
    token_states: Arc<RwLock<HashMap<Pubkey, TokenFlowState>>>,
    
    // Configuration
    dev_sell_enabled: bool,
    sell_burst_count: u32,
    sell_burst_sol: f64,
    sell_burst_slots: u64,
    flow_ratio_min: f64,
    negative_flow_slots: u64,
}

impl KillSwitchMonitor {
    pub fn new(
        dev_sell_enabled: bool,
        sell_burst_count: Option<u32>,
        sell_burst_sol: Option<f64>,
        sell_burst_slots: Option<u64>,
        flow_ratio_min: Option<f64>,
        negative_flow_slots: Option<u64>,
    ) -> Self {
        Self {
            token_states: Arc::new(RwLock::new(HashMap::new())),
            dev_sell_enabled,
            sell_burst_count: sell_burst_count.unwrap_or(3),
            sell_burst_sol: sell_burst_sol.unwrap_or(0.5),
            sell_burst_slots: sell_burst_slots.unwrap_or(5),
            flow_ratio_min: flow_ratio_min.unwrap_or(0.6),
            negative_flow_slots: negative_flow_slots.unwrap_or(3),
        }
    }
    
    /// Register a new position to monitor
    pub fn register_position(&self, mint: Pubkey, creator: Option<Pubkey>) {
        let mut states = self.token_states.write();
        if !states.contains_key(&mint) {
            info!(mint=%mint, creator=?creator, "kill_switch: registered new position for monitoring");
            states.insert(mint, TokenFlowState::new(creator));
        }
    }
    
    /// Unregister a position (after exit)
    pub fn unregister_position(&self, mint: &Pubkey) {
        let mut states = self.token_states.write();
        if states.remove(mint).is_some() {
            debug!(mint=%mint, "kill_switch: unregistered position");
        }
    }
    
    /// Process a trade event and check for kill switch triggers
    pub fn process_trade(&self, event: TokenTradeEvent) -> Option<KillSwitchReason> {
        let mut states = self.token_states.write();
        
        let state = match states.get_mut(&event.mint) {
            Some(s) => s,
            None => return None, // Not monitoring this token
        };
        
        // Add trade to state
        state.add_trade(event.slot, event.is_buy, event.sol_amount, event.trader);
        
        // Check kill switch conditions
        
        // 1. Dev/Creator Sell
        if self.dev_sell_enabled && !event.is_buy {
            if let Some(creator) = &state.creator {
                if event.trader == *creator {
                    warn!(
                        mint=%event.mint,
                        dev=%event.trader,
                        sol=event.sol_amount,
                        "KILL SWITCH: Dev sell detected!"
                    );
                    return Some(KillSwitchReason::DevSell {
                        dev_address: creator.to_string(),
                        sol_amount: event.sol_amount,
                    });
                }
            }
        }
        
        // 2. Sell Burst
        if !event.is_buy {
            let current_slot = event.slot;
            let min_slot = current_slot.saturating_sub(self.sell_burst_slots);
            
            let (sell_count, total_sol): (u32, f64) = state.recent_trades.iter()
                .filter(|(slot, is_buy, _, _)| *slot >= min_slot && !is_buy)
                .fold((0, 0.0), |(count, sol), (_, _, amount, _)| (count + 1, sol + amount));
            
            if sell_count >= self.sell_burst_count && total_sol >= self.sell_burst_sol {
                warn!(
                    mint=%event.mint,
                    sell_count,
                    total_sol,
                    slots=self.sell_burst_slots,
                    "KILL SWITCH: Sell burst detected!"
                );
                return Some(KillSwitchReason::SellBurst {
                    sell_count,
                    total_sol,
                    slots: self.sell_burst_slots,
                });
            }
        }
        
        // 3. Flow Ratio (buy/sell ratio)
        let (total_buys, total_sells): (f64, f64) = state.recent_trades.iter()
            .fold((0.0, 0.0), |(buys, sells), (_, is_buy, sol, _)| {
                if *is_buy {
                    (buys + sol, sells)
                } else {
                    (buys, sells + sol)
                }
            });
        
        if total_sells > 0.0 {
            let ratio = total_buys / total_sells;
            if ratio < self.flow_ratio_min && total_sells > 0.1 {
                // Only trigger if significant sell volume (>0.1 SOL)
                warn!(
                    mint=%event.mint,
                    ratio,
                    threshold=self.flow_ratio_min,
                    buys=total_buys,
                    sells=total_sells,
                    "KILL SWITCH: Flow ratio below threshold!"
                );
                return Some(KillSwitchReason::FlowRatio {
                    ratio,
                    threshold: self.flow_ratio_min,
                });
            }
        }
        
        // 4. Consecutive Negative Flow Slots
        if state.slot_flows.len() >= self.negative_flow_slots as usize {
            let recent_slots: Vec<_> = state.slot_flows.iter()
                .rev()
                .take(self.negative_flow_slots as usize)
                .collect();
            
            let all_negative = recent_slots.iter().all(|(_, flow)| *flow < 0.0);
            let total_negative: f64 = recent_slots.iter()
                .map(|(_, flow)| flow.abs())
                .sum();
            
            if all_negative && total_negative > 0.1 {
                // Only trigger if significant volume
                warn!(
                    mint=%event.mint,
                    consecutive_slots=self.negative_flow_slots,
                    total_negative,
                    "KILL SWITCH: Consecutive negative flow detected!"
                );
                return Some(KillSwitchReason::NegativeFlow {
                    consecutive_slots: self.negative_flow_slots,
                });
            }
        }
        
        None
    }
    
    /// Get all monitored mints
    pub fn get_monitored_mints(&self) -> Vec<Pubkey> {
        self.token_states.read().keys().cloned().collect()
    }
    
    /// Check if a mint is being monitored
    pub fn is_monitoring(&self, mint: &Pubkey) -> bool {
        self.token_states.read().contains_key(mint)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey;
    
    #[test]
    fn test_dev_sell_detection() {
        let monitor = KillSwitchMonitor::new(true, Some(3), Some(0.5), Some(5), Some(0.6), Some(3));
        
        let mint = pubkey!("TokenMint1111111111111111111111111111111111");
        let dev = pubkey!("DevWallet111111111111111111111111111111111");
        let random_trader = pubkey!("RandomTrader11111111111111111111111111111");
        
        monitor.register_position(mint, Some(dev));
        
        // Random trader sell - no trigger
        let event = TokenTradeEvent {
            mint,
            slot: 100,
            is_buy: false,
            sol_amount: 1.0,
            trader: random_trader,
            timestamp: 0,
        };
        assert!(monitor.process_trade(event).is_none());
        
        // Dev sell - should trigger
        let event = TokenTradeEvent {
            mint,
            slot: 101,
            is_buy: false,
            sol_amount: 0.5,
            trader: dev,
            timestamp: 0,
        };
        let result = monitor.process_trade(event);
        assert!(matches!(result, Some(KillSwitchReason::DevSell { .. })));
    }
    
    #[test]
    fn test_sell_burst_detection() {
        let monitor = KillSwitchMonitor::new(false, Some(3), Some(0.5), Some(5), Some(0.6), Some(3));
        
        let mint = pubkey!("TokenMint1111111111111111111111111111111111");
        let trader = pubkey!("RandomTrader11111111111111111111111111111");
        
        monitor.register_position(mint, None);
        
        // 3 sells of 0.2 SOL each within 5 slots = 0.6 SOL > 0.5 threshold
        for i in 0..3 {
            let event = TokenTradeEvent {
                mint,
                slot: 100 + i,
                is_buy: false,
                sol_amount: 0.2,
                trader,
                timestamp: 0,
            };
            let result = monitor.process_trade(event);
            if i == 2 {
                assert!(matches!(result, Some(KillSwitchReason::SellBurst { .. })));
            }
        }
    }
}
