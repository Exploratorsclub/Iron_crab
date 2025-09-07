use rand::{rngs::SmallRng, Rng, SeedableRng};

/// Configuration for impact/slippage noise during backtests.
#[derive(Debug, Clone)]
pub struct ImpactSettings {
    /// Deterministic seed for RNG; if None, randomized.
    pub seed: Option<u64>,
    /// Add stochastic shortfall noise in basis points to min_out checks.
    pub noise_bps_mean: f32,
    pub noise_bps_std: f32,
    /// Optional fixed latency in ms to emulate price staleness (applied as N slots back if slot_ms provided by driver).
    pub emulate_latency_ms: Option<u64>,
    /// Extra protocol/referral fee bps to subtract from outputs (on top of pool fee_bps).
    pub extra_fee_bps: u32,
    /// Slot duration in ms for latency modeling (default 400 ms if not provided by driver).
    pub slot_ms: Option<u64>,
}

impl Default for ImpactSettings {
    fn default() -> Self {
        Self {
            seed: None,
            noise_bps_mean: 0.0,
            noise_bps_std: 0.0,
            emulate_latency_ms: None,
            extra_fee_bps: 0,
            slot_ms: Some(400),
        }
    }
}

pub struct NoiseSampler {
    rng: SmallRng,
    mean: f32,
    std: f32,
}

impl NoiseSampler {
    pub fn new(seed: Option<u64>, mean: f32, std: f32) -> Self {
        let rng = match seed {
            Some(s) => SmallRng::seed_from_u64(s),
            None => SmallRng::from_entropy(),
        };
        Self { rng, mean, std }
    }
    /// Sample a shortfall in bps (>= 0). Uses a truncated normal at 0.
    pub fn sample_bps(&mut self) -> u32 {
        if self.std <= 0.0 {
            return self.mean.max(0.0) as u32;
        }
        // Box-Muller transform for normal sampling
        let mut u1: f32 = self.rng.gen();
        if u1 < 1.0e-6 {
            u1 = 1.0e-6;
        }
        if u1 > 1.0 - 1.0e-6 {
            u1 = 1.0 - 1.0e-6;
        }
        let u2: f32 = self.rng.gen();
        let z0 = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f32::consts::PI * u2).cos();
        let v = self.mean + z0 * self.std;
        if v < 0.0 {
            0
        } else {
            v as u32
        }
    }
}
