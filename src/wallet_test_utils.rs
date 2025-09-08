#![cfg(any(test, feature = "test_helpers"))]

use crate::wallet::Treasury;
use solana_sdk::signer::keypair::Keypair;
use std::sync::Arc;

impl Treasury {
    pub fn new_for_tests() -> Self {
        // Unsafe dummy: ephemeral keypair, zero balances; only for unit tests that don't hit RPC
        let kp = Arc::new(Keypair::new());
        Treasury::new(kp)
    }
}
