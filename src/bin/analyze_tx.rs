use solana_client::rpc_client::RpcClient;
use solana_transaction_status::UiTransactionEncoding;
use solana_sdk::signature::Signature;
use std::str::FromStr;

fn main() {
    // Use a known Pump.fun Create transaction
    // This is the transaction from the user's screenshot
    let sig_str = "HhXpmJsfqteFXVyS9fZPskTFHZ2kn2MEwkW8VBzJVVnv99uuzpump";
    
    let rpc = RpcClient::new("https://api.mainnet-beta.solana.com".to_string());
    
    println!("Fetching transaction {}...", sig_str);
    
    let sig = match Signature::from_str(sig_str) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Invalid signature: {}", e);
            return;
        }
    };
    
    match rpc.get_transaction_with_config(
        &sig,
        solana_client::rpc_config::RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::JsonParsed),
            commitment: Some(solana_sdk::commitment_config::CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
        },
    ) {
        Ok(tx) => {
            println!("✅ Transaction found!");
            if let Some(transaction) = tx.transaction.transaction {
                println!("\n=== TRANSACTION ACCOUNT KEYS ===");
                match transaction {
                    solana_transaction_status::EncodedTransaction::Json(ui_tx) => {
                        if let solana_transaction_status::UiMessage::Parsed(parsed) = ui_tx.message {
                            for (i, acc) in parsed.account_keys.iter().enumerate() {
                                println!("[{}] {} (signer:{}, writable:{})", 
                                    i, 
                                    acc.pubkey,
                                    acc.signer,
                                    acc.writable
                                );
                            }
                            
                            // Also show instructions
                            println!("\n=== INSTRUCTIONS ===");
                            for (i, ix) in parsed.instructions.iter().enumerate() {
                                println!("Instruction #{}: {:?}", i, ix);
                            }
                        }
                    },
                    _ => println!("Non-parsed transaction"),
                }
            }
        },
        Err(e) => eprintln!("❌ Error: {}", e),
    }
}
