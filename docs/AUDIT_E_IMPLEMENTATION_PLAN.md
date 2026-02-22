# Audit-E: cleanup_wallet_after_liquidation – Implementierungsplan

## Kontext

**Audit-E** ([ARCHITECTURE_AUDIT_2026-02-07.md](ARCHITECTURE_AUDIT_2026-02-07.md), [BUGS_FIXES.md](BUGS_FIXES.md)):
- **Problem**: `cleanup_wallet_after_liquidation()` nutzt RPC (`get_token_accounts_by_owner`, `get_account(wsol_ata)`) statt Geyser/JetStream
- **Vorgeschlagene Alternative**: Wallet-Snapshots aus market-data/JetStream

## Was macht cleanup_wallet_after_liquidation?

Wird **nach Liquidation** (Killswitch mit liquidate) aufgerufen, nach 15 Sekunden Wartezeit auf TX-Bestätigung:

1. **WSOL unwrap**: Schließt die WSOL-ATA, um Wrapped SOL in natives SOL zu konvertieren
2. **Leere ATAs schließen**: Schließt Token-Accounts mit `balance=0`, um Rent zurückzuholen

**Aktuell genutzte RPC-Calls:**
- `get_token_accounts_by_owner` (2×: SPL Token + Token-2022) – Liste aller Token-Accounts
- `get_account(&wsol_ata)` – Prüfung, ob die WSOL-ATA existiert

## Architektur-Einordnung

| Regel | Bedeutung |
|-------|-----------|
| **Cold Path** (Liquidation, manuelle Aktionen) | **RPC ist erlaubt**. Sicherheit und Korrektheit haben Vorrang vor Geschwindigkeit. |
| Quelle: ironcrab-core.mdc | |

**Folge**: Die Nutzung von RPC in `cleanup_wallet_after_liquidation` verstößt nicht gegen die Architektur. Es ist ein Cold-Path-Flow.

## Warum trotzdem Audit-E?

- **Konsistenz**: Liquidation verwendet an anderer Stelle JetStream/WalletBalanceSnapshot; der Cleanup hingegen macht einen eigenen RPC-Scan.
- **Geyser-First**: Wo möglich soll Geyser/JetStream genutzt werden statt RPC.

## Analyse: Ist ein Wechsel auf JetStream sinnvoll?

### Pro JetStream

- Konsistenz mit Geyser-First-Architektur
- Weniger RPC-Last
- Nutzung von Daten, die market-data ohnehin publiziert
- Nach 15 Sekunden Sleep sollten Geyser-Events angekommen sein (Balance-Updates nach Sells)

### Contra JetStream / Risiken

1. **Cold Path**: RPC ist laut Regeln erlaubt – kein Zwang zur Umstellung.
2. **Sicherheit**: RPC liefert den aktuellen on-chain-Zustand. JetStream-Daten können verzögert sein.
3. **WSOL-ATA**: Existenz der ATA muss geprüft werden. LockManager speichert nur die Balance, nicht die Existenz.
4. **Leere ATAs**: Für ein Close brauchen wir `mint`, `token_program` und die ATA-Adresse. `LockManager` speichert nur `mint → balance`, kein `token_program`.
5. **Completeness**: Manche ATAs könnten in JetStream noch nicht vorkommen (z.B. kurz nach Sell, bevor Geyser-Event ankommt).

### Bestehende Datenquellen

- **LockManager** (`storage/locks.rs`): mint → balance (aus WalletBalanceSnapshot). Kein token_program.
- **WalletBalanceSnapshot** (JetStream): mint, balance_raw, decimals, token_program.
- **WalletSnapshotComplete**: Liste aller Mints im Wallet (periodischer Scan).

## Option A: RPC beibehalten, Audit-E als akzeptiert markieren

**Begründung**: Cold Path, Safety > Speed. RPC liefert den autoritativen Zustand nach der Liquidation.

**Änderungen**:
- In BUGS_FIXES.md: Audit-E als „BEHOBEN (by design)“ oder „AKZEPTIERT“ markieren
- Begründung: „Liquidation-Cleanup ist Cold Path. RPC für autoritativen Post-Liquidation-Zustand ist per Architektur erlaubt.“

## Option B: JetStream-First mit RPC-Fallback (Implementierung)

### Ablauf

1. **Vor dem Cleanup** (nach 15 Sekunden Sleep):
   - Letzte WalletBalanceSnapshot-Nachrichten für die Wallet aus JetStream lesen.
   - Daraus Liste aller Mints mit `balance_raw = 0` (ohne WSOL, ohne NATIVE_SOL).
   - Jedes Event enthält `token_program`.

2. **WSOL unwrap**:
   - `LockManager.wsol_balance()` prüfen. Wenn > 0: WSOL-ATA schließen (SPL Token).
   - Wenn = 0: Trotzdem versuchen, ATA zu schließen (Rent-Rückgewinnung), wenn die ATA leer ist. Zuerst simulieren; bei Fehler (Account nicht vorhanden) überspringen.
   - Kein `get_account` nötig: Close-IX bauen, simulieren, bei Erfolg senden.

3. **Leere ATAs schließen**:
   - Aus JetStream: `(mint, token_program)` mit `balance_raw = 0`
   - ATA ableiten: `ata_for_owner_mint(wallet, mint, token_program)`
   - Close-IX bauen, simulieren, bei Erfolg senden
   - Simulation schützt vor falschen Closes (z.B. Balance ≠ 0)

4. **RPC-Fallback**:
   - Wenn keine/wenige WalletBalanceSnapshot-Events für die Wallet gefunden werden (z.B. direkt nach Start), Cleanup wie bisher per RPC durchführen.
   - Wenn JetStream-Flow aus anderen Gründen nicht nutzbar ist, ebenfalls RPC-Fallback.

### Benötigte Änderungen

| Komponente | Änderung |
|------------|----------|
| `cleanup_wallet_after_liquidation` | 1. Versuch: WalletBalanceSnapshot aus JetStream holen und daraus `(mint, token_program)` mit balance=0 bauen. 2. Fallback: Bestehende RPC-Logik. |
| `ExecutionContext` | Zugriff auf JetStream/NATS-Client für Wallet-Subject der Wallet. |
| Keine neue Infrastruktur | Nutzung des vorhandenen WalletBalanceSnapshot-Subjects. |

### Daten-Fetch aus JetStream

- Subject-Pattern: `ironcrab.wallet_snapshot.{wallet}.*`
- Consumer mit `filter_subject` für diese Wallet
- `fetch()` mit `max_messages` (z.B. 200) und `expires` (z.B. 2 Sekunden)
- Aus Messages: `MarketEventKind::WalletBalanceSnapshot { mint, balance_raw, token_program }` extrahieren
- Sammeln: `HashMap<mint, (balance_raw, token_program)>` (neueste pro Mint behalten)

### Abnahmekriterien

- Wenn JetStream hinreichend Daten liefert: Kein RPC in `get_token_accounts_by_owner` und kein `get_account(wsol_ata)`.
- RPC-Fallback funktioniert bei fehlenden/ungenügenden JetStream-Daten.
- WSOL unwrap und Close von leeren ATAs verhalten sich funktional wie bisher.
- Keine Änderung an der 15-Sekunden-Wartezeit vor Cleanup.

## Empfehlung

**Option A** ist die pragmatische Variante: Cold Path erlaubt RPC, der aktuelle Ansatz ist sicher und klar.

**Option B** lohnt sich, wenn ihr stärker auf Geyser-First und weniger RPC ausgerichtet sein wollt. Der Aufwand ist moderat (ca. 1–2 Tage) und das Risiko überschaubar durch den RPC-Fallback.

## Entscheidung (2026-02)

**Option A wurde gewählt.** Cleanup betrifft nur den Post-Liquidation-Flow. Es ist zentral, dass alle offen gebliebenen leeren ATAs geschlossen werden. RPC liefert den autoritativen Zustand; Geyser/JetStream birgt das Risiko von Stale-Daten und übersehenen ATAs. Cold Path erlaubt RPC. Keine Code-Änderung erforderlich.
