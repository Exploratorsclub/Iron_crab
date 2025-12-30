# Role Separation & Least Privilege

**Source of Truth für Architektur:** `docs/TARGET_ARCHITECTURE.md`

Dieses Dokument beschreibt die **Sicherheitsarchitektur** und **Zugriffsrechte** der IronCrab Multi-Prozess-Architektur.

## P0 Non-Negotiables

### Single Signer Prinzip
- **NUR `execution-engine`** hat Zugriff auf Wallet-Keys
- Alle anderen Prozesse (market-data, momentum-bot, control-plane) sind **KEYLESS**
- Verstoß = sofortiger Prozessabbruch (exit 1)

### Intent-Only Pattern
- Strategien (momentum-bot) erzeugen ausschließlich `TradeIntent`s
- Keine direkten RPC/TPU/Jito Sends außerhalb der Execution Engine
- Intents werden über NATS IPC übermittelt

## Prozess-Zugriffsmatrix

| Prozess           | Wallet Keys | NATS Publish          | NATS Subscribe        | Tx Sign/Send |
|-------------------|-------------|-----------------------|-----------------------|--------------|
| execution-engine  | ✅ JA       | ExecutionResults      | TradeIntents, Control | ✅ JA        |
| market-data       | ❌ NEIN     | MarketEvents          | (keine)               | ❌ NEIN      |
| momentum-bot      | ❌ NEIN     | TradeIntents          | MarketEvents          | ❌ NEIN      |
| control-plane     | ❌ NEIN     | Control Commands      | (Status Replies)      | ❌ NEIN      |

## Environment Variables

### execution-engine (einziger Key-Halter)
```bash
# Genau EINER dieser muss gesetzt sein:
IRONCRAB_KEYPAIR_JSON=     # JSON array [1,2,3,...] 32 oder 64 bytes
IRONCRAB_KEYPAIR_B64=      # Base64 encoded keypair
IRONCRAB_KEYPAIR_PATH=     # Pfad zu keypair.json
IRONCRAB_KEYPAIR_BASE58=   # Base58 encoded secret

# Optional für strengere Pfad-Validierung:
IRONCRAB_KEYPAIR_STRICT=1
IRONCRAB_KEYPAIR_ALLOWED_DIRS=/home/ironcrab/.config/solana
```

### Alle anderen Prozesse
```bash
# KEINE der obigen KEYPAIR Variablen setzen!
# Prozess crasht mit exit(1) wenn Keys erkannt werden.
```

## Enforcement im Code

### market-data (src/bin/market_data.rs)
```rust
if std::env::var("IRONCRAB_KEYPAIR_JSON").is_ok()
    || std::env::var("IRONCRAB_KEYPAIR_B64").is_ok()
    || std::env::var("IRONCRAB_KEYPAIR_PATH").is_ok()
{
    error!("market-data is KEYLESS per architecture");
    std::process::exit(1);
}
```

### momentum-bot (src/bin/momentum_bot.rs)
```rust
// Identische Prüfung mit exit(1)
```

### control-plane (Python)
```python
# Prüfung beim Startup
forbidden_vars = ["IRONCRAB_KEYPAIR_JSON", "IRONCRAB_KEYPAIR_B64", 
                  "IRONCRAB_KEYPAIR_PATH", "IRONCRAB_KEYPAIR_BASE58"]
if any(os.getenv(v) for v in forbidden_vars):
    raise RuntimeError("Control Plane cannot start with wallet keys")
```

## NATS ACL Konfiguration (Production)

Für Production sollte NATS mit ACLs konfiguriert werden:

```hcl
# /etc/nats/nats.conf

authorization {
  users = [
    # execution-engine: Kann Intents empfangen, Results senden
    {
      user: "execution-engine"
      password: "$EXEC_NATS_PASS"
      permissions: {
        subscribe: ["ironcrab.intents.>", "ironcrab.control.>"]
        publish: ["ironcrab.results.>", "_INBOX.>"]
      }
    }
    
    # market-data: Kann nur Events publizieren
    {
      user: "market-data"
      password: "$MD_NATS_PASS"
      permissions: {
        subscribe: []
        publish: ["ironcrab.market.events"]
      }
    }
    
    # momentum-bot: Empfängt Events, sendet Intents
    {
      user: "momentum-bot"
      password: "$MB_NATS_PASS"
      permissions: {
        subscribe: ["ironcrab.market.events"]
        publish: ["ironcrab.intents.>"]
      }
    }
    
    # control-plane: Control Commands, keine Trading Topics
    {
      user: "control-plane"
      password: "$CP_NATS_PASS"
      permissions: {
        subscribe: ["_INBOX.>"]
        publish: ["ironcrab.control.>"]
        # KEIN Zugriff auf: ironcrab.intents.> oder ironcrab.results.>
      }
    }
  ]
}
```

## Audit Logging

### Control Plane Audit Log
Alle administrativen Aktionen werden in `control_plane_audit.log` geloggt:

```
2024-12-30 10:15:00 - AUDIT - STARTUP: Control Plane started (keyless mode verified)
2024-12-30 10:20:00 - AUDIT - COMMAND: component=momentum-bot, command=pause, params={}
2024-12-30 10:25:00 - AUDIT - CONFIG_UPDATE: component=execution-engine, keys=['max_position_sol']
2024-12-30 11:00:00 - AUDIT - KILL_SWITCH_ACTIVATED: reason='Manual intervention', liquidate=True
```

### Decision Records (execution-engine)
Jede Trade-Entscheidung wird in `trade_logs/decisions/` aufgezeichnet:
- Input-Snapshot (Intent + Marktdaten)
- Reason Code (ACCEPTED, REJECTED_*)
- Outcome (Signature oder Error)

## Systemd Hardening

### Keypair File Permissions
```bash
# Nur ironcrab User kann lesen
chmod 600 /home/ironcrab/.config/solana/id.json
chown ironcrab:ironcrab /home/ironcrab/.config/solana/id.json
```

### Service-spezifische Umgebung
```ini
# execution-engine.service
Environment=IRONCRAB_KEYPAIR_PATH=/home/ironcrab/.config/solana/id.json

# momentum-bot.service  
# KEINE KEYPAIR Variable!

# market-data.service
# KEINE KEYPAIR Variable!
```

### Zusätzliche Hardening-Optionen
```ini
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=strict
ProtectHome=read-only
```

## Verifikation

### Test: Key-Isolation prüfen
```bash
# Auf dem Server: Prüfen dass nur execution-engine Keys hat
sudo -u ironcrab printenv | grep KEYPAIR
# Sollte nur in execution-engine Kontext erscheinen

# Prüfen dass momentum-bot ohne Keys startet
systemctl status momentum-bot
# Log sollte KEINE Keypair-Warnungen zeigen
```

### Test: NATS ACL prüfen (wenn konfiguriert)
```bash
# Mit momentum-bot Credentials sollte publish auf intents.> funktionieren
nats pub ironcrab.intents.test "test" --user momentum-bot --password $MB_PASS
# Aber NICHT auf control.>
nats pub ironcrab.control.test "test" --user momentum-bot --password $MB_PASS
# Expected: Permissions Violation
```

## Checkliste vor Go-Live

- [ ] Keypair-Datei nur für ironcrab User lesbar (chmod 600)
- [ ] Nur execution-engine.service hat KEYPAIR Environment Variable
- [ ] market-data und momentum-bot crashen mit exit(1) wenn Keys erkannt
- [ ] control-plane crasht beim Start wenn Keys erkannt
- [ ] Audit-Log aktiviert und rotiert
- [ ] (Optional) NATS ACLs konfiguriert
- [ ] Decision Records werden geschrieben
