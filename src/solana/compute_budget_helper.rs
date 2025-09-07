use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
pub const COMPUTE_BUDGET_PROGRAM_ID_STR: &str = "ComputeBudget111111111111111111111111111111";
pub fn program_id() -> Pubkey {
    Pubkey::from_str(COMPUTE_BUDGET_PROGRAM_ID_STR).expect("compute budget pid")
}
pub fn set_compute_unit_limit(units: u32) -> Instruction {
    let mut data = Vec::with_capacity(1 + 4);
    data.push(2u8);
    data.extend_from_slice(&units.to_le_bytes());
    Instruction {
        program_id: program_id(),
        accounts: vec![],
        data,
    }
}
pub fn set_compute_unit_price(micro_lamports: u64) -> Instruction {
    let mut data = Vec::with_capacity(1 + 8);
    data.push(3u8);
    data.extend_from_slice(&micro_lamports.to_le_bytes());
    Instruction {
        program_id: program_id(),
        accounts: vec![],
        data,
    }
}
