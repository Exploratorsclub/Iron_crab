use anyhow::Result;
use std::fmt;
use tracing::{Event, Subscriber};
use tracing::field::Field;
use tracing_subscriber::{
    fmt::{FmtContext, format::{FormatEvent, Writer, FormatFields}},
    EnvFilter, registry::LookupSpan,
};
use tracing::field::Visit;

/// Initialize global logging with a redacting formatter that prevents secrets from leaking.
pub fn init_redacting_logging(default_level: &str) -> Result<()> {
    let level = std::env::var("RUST_LOG").unwrap_or_else(|_| default_level.to_string());
    let filter = EnvFilter::try_new(level)?;
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .event_format(RedactingFormatter)
        .init();
    Ok(())
}

struct RedactingFormatter;

impl<S, N> FormatEvent<S, N> for RedactingFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(&self, ctx: &FmtContext<S, N>, mut writer: Writer, event: &Event) -> fmt::Result {
        // Timestamp and level
        let meta = event.metadata();
        write!(writer, "{} {} ", meta.level(), meta.target())?;

        // Span context (current span only for compactness)
        if let Some(span) = ctx.lookup_current() {
            write!(writer, "[{}] ", span.metadata().name())?;
        }

        // Fields with redaction
        let mut visitor = RedactionVisitor { out: String::new(), first: true };
        event.record(&mut visitor);
        writer.write_str(&visitor.out)?;
        writeln!(writer)
    }
}

struct RedactionVisitor { out: String, first: bool }

impl Visit for RedactionVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.push_kv(field.name(), value);
    }
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        // Format debug into string, then apply redaction heuristic
        let s = format!("{:?}", value);
        self.push_kv(field.name(), &s);
    }
}

impl RedactionVisitor {
    fn push_kv(&mut self, key: &str, value: &str) {
        if !self.first { self.out.push(' '); } else { self.first = false; }
        let redacted = redact_if_sensitive(key, value);
        // Avoid adding quotes around already quoted debug strings to keep compact output
        self.out.push_str(key);
        self.out.push('=');
        self.out.push_str(&redacted);
    }
}

fn redact_if_sensitive(key: &str, val: &str) -> String {
    let lname = key.to_ascii_lowercase();
    // Remove surrounding quotes from debug strings
    let v = val.trim_matches('"');
    let key_hint = ["secret", "private", "priv", "seed", "mnemonic", "keypair", "sk", "kp"]
        .iter()
        .any(|k| lname.contains(k));
    if key_hint { return "***REDACTED***".into(); }

    // Heuristic: JSON-like large numeric arrays (e.g., keypair bytes)
    if v.starts_with('[') && v.ends_with(']') && v.len() >= 64 && v.contains(',') {
        return "[***REDACTED BYTES***]".into();
    }

    v.to_string()
}
