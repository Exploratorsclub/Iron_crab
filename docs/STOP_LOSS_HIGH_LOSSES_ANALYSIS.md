# Warum STOP_LOSS oft erst bei -60 % oder -80 % auslöst

## Beobachtung

- **Konfiguriert**: `hard_stop_loss_pct = 15` (Exit bei -15 %)
- **Dashboard-Detail**: z.B. „Hard stop hit: -37.5 % loss (limit: -15.0 %)“ oder -80.3 %
- **Realized PnL**: oft -12 %, -40 %, -86 %

## Hauptursache: Einzelne Trades bewegen den Preis massiv

### Bonding-Curve-Charakteristik (PumpFun)

- Liquidity ist sehr dünn
- Ein Swap von 0.5–2 SOL kann den Preis um **30–60 %** verschieben
- Es gibt keinen „sanften“ Abstieg: von -10 % auf -60 % in **einer Transaktion**

### Ablauf

1. Letzter Preis: ca. -8 % (noch unter Limit)
2. Ein großer Sell passiert → Preis springt in einem Block auf -60 %
3. Geyser sendet Account-Update → market-data → PoolCacheUpdate → momentum_bot
4. Bot sieht bei nächstem Check `pnl_pct = -60 %` → STOP_LOSS
5. Der Bot hat nie einen Zwischenzustand bei -15 % gesehen

**Ergebnis:** Die Verzögerung kommt weniger von unserer Latenz als vom Markt: der Sprung findet in einem einzigen Trade statt.

---

## Pipeline-Latenz (nach Fix: event-driven)

### Event-driven Exit-Check (seit Fix)

Der Exit-Check wird **bei jedem Preis-Update** ausgeführt, nicht mehr alle 2 s:

- **PoolCacheUpdate** (JetStream): Nach jedem Pool-Reserve-Update → sofort `check_for_exits()`
- **Trade** (Core NATS): Nach jedem Trade-Event, das eine Position aktualisiert → sofort `check_for_exits()`
- **strategy_interval**: 500 ms Fallback, wenn keine Events ankommen (z. B. stille Pools)

### Zeit von „Preisänderung“ bis „Exit-TX“ (event-driven)

| Schritt | Typische Dauer |
|--------|-----------------|
| Geyser → market-data | &lt;100 ms |
| market-data → JetStream/NATS | &lt;50 ms |
| momentum_bot empfängt Update | &lt;50 ms |
| **Exit-Check** | **sofort** (event-driven) |
| momentum_bot → execution-engine (JetStream) | &lt;100 ms |
| Simulation (RPC) | ~200–500 ms |
| send_transaction + Bestätigung | ~400 ms (1 Block) |
| **Summe** | **&lt;500 ms** (ohne Simulation), ~700 ms–1 s (mit TX) |

### Vor dem Fix

- strategy_interval 2 s → Worst-Case 2 s Verzögerung vor dem ersten Check.
- Jetzt: Reaktion beim ersten relevanten Event.

---

## Weitere Ursache: Stale Price bei wenig Volumen

### Mechanismus

- **PoolCacheUpdate** kommt, wenn Geyser ein Pool-Account-Update meldet.
- Das passiert nur bei Swaps (Bonding-Curve ändert sich).
- Bei sehr wenig Volumen: lange keine Trades → **kein Update** → `current_price` bleibt alt.

### Beispiel

1. 0–60 s: keine Trades auf dem Token
2. `current_price` bleibt z.B. bei -5 %
3. Ein großer Sell (2 SOL) → Preis fällt in einem TX auf -70 %
4. Erster Update nach 60 s → wir sehen direkt -70 %

**Ergebnis:** Wir haben nie einen Zwischenstand bei -15 % erhalten.

---

## RPC im Hot Path?

### momentum_bot

- **Kein RPC**
- Daten nur über MarketEvents / Geyser → market-data → JetStream
- Kein Architekturverstoß hier

### execution-engine (BUY/SELL)

- **Simulation**: RPC `simulate_transaction` – notwendig für simulate-gated Safety
- **Send**: RPC `send_transaction` – notwendig
- **Andere RPCs** im Intent-Pfad: z.B. `get_account` für Bonding Curve, Token Account etc.

Die Simulation ist erforderlich und begründet. Zusätzliche RPCs im Hot Path könnten Latenz erhöhen, lösen aber nicht das Kernproblem der großen Einzeltrades.

---

## Zusammenfassung

| Ursache | Gewicht | Kann man verbessern? |
|---------|---------|----------------------|
| **Sprüche in einem Trade** | Hoch | Kaum – Marktstruktur |
| **strategy_interval (Fallback)** | Gering | Event-driven + 500 ms Fallback |
| **Stale Price bei wenig Volumen** | Mittel | Schwer – braucht mehr Updates als nur Swaps |
| **RPC-Latenz (Simulation)** | Gering | Nur mit Trade-off (z.B. weniger Simulationssicherheit) |

---

## Empfehlungen

1. **Event-driven Exit-Check (umgesetzt)**  
   Exit-Check bei jedem PoolCacheUpdate und Trade-Event, strategy_interval 500 ms als Fallback.

2. **Realistisch bleiben**  
   Bei illiquiden Meme-Tokens sind Verluste von 30–80 % trotz Stop-Loss möglich, weil der Preis oft in einem einzelnen Trade fällt.

3. **Optional: Periodische Preisabfrage**  
   Für Positionen ohne Pool-Updates könnte man z.B. alle 5 s einen Preis „anstoßen“ (z.B. über Trade-Event-Subscription oder ähnliches), um Stale-Price-Situationen zu verringern – technisch aufwändig.

4. **Logging nutzen**  
   `entry_price`, `current_price`, `pnl_pct` bei STOP_LOSS/TAKE_PROFIT in den Logs prüfen, um zu sehen, ob der Preis sprungartig oder graduell gefallen ist.
