# Vendored crates (Cargo `[patch.crates-io]`)

These are **unmodified upstream sources** from crates.io (same versions as published), plus **minimal security/build patches** tracked in git:

- `async-nats` — `rustls-webpki` 0.103.10, `aws-lc-rs` feature flag rename (`aws_lc_rs` → `aws-lc-rs`), TLS helpers updated for `rustls-pki-types` / PEM API.
- `solana-pubsub-client` — `tokio-tungstenite` / `tungstenite` 0.24, `connect`/`connect_async` use `url.as_str()`, HTTP types from `tungstenite::http`.
- `five8` — pin `five8_core` to 1.0.0 (crates.io 1.0.0 allows 0.1.x; Solana `solana-keypair` needs `DecodeError: std::error::Error`).

Remove these patches when upstream publishes equivalent fixes.
