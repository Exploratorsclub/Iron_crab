//! Startup restore / Geyser-connect barrier (I-MD-6): readiness only after physical publish.

use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

const BARRIER_PENDING: u8 = 0;
const BARRIER_READY: u8 = 1;
const BARRIER_FAILED: u8 = 2;

/// One-shot barrier: `md-track-worker` signals after admitted explicit set is physically published.
#[derive(Debug)]
pub struct GeyserConnectBarrier {
    state: AtomicU8,
}

impl Default for GeyserConnectBarrier {
    fn default() -> Self {
        Self::new()
    }
}

impl GeyserConnectBarrier {
    pub fn new() -> Self {
        Self {
            state: AtomicU8::new(BARRIER_PENDING),
        }
    }

    pub fn mark_ready(&self) {
        self.state.store(BARRIER_READY, Ordering::Release);
    }

    pub fn mark_failed(&self) {
        self.state.store(BARRIER_FAILED, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.state.load(Ordering::Acquire) == BARRIER_READY
    }

    pub fn wait_ready(&self, timeout: Duration) -> Result<(), &'static str> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            match self.state.load(Ordering::Acquire) {
                BARRIER_READY => return Ok(()),
                BARRIER_FAILED => return Err("geyser_explicit_barrier_failed"),
                _ => std::thread::sleep(Duration::from_millis(5)),
            }
        }
        Err("geyser_explicit_barrier_timeout")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn barrier_ready_after_mark() {
        let b = GeyserConnectBarrier::new();
        b.mark_ready();
        assert!(b.is_ready());
        assert!(b.wait_ready(Duration::from_millis(50)).is_ok());
    }

    #[test]
    fn barrier_failed_returns_error() {
        let b = GeyserConnectBarrier::new();
        b.mark_failed();
        assert!(!b.is_ready());
        assert_eq!(
            b.wait_ready(Duration::from_millis(20)),
            Err("geyser_explicit_barrier_failed")
        );
    }
}
