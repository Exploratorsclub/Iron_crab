# Handoff: Scope 46 - PumpSwap Sell auf dynamisches Layout umstellen, Cashback-/Sharing-Merkmale in market-data erkennen und cachen

WICHTIG: Lies und befolge die STOP-CHECK Regeln in `AGENTS.md` und `.cursor/rules/ironcrab-core.mdc` BEVOR du eine Datei aenderst. Wenn eine geplante Aenderung gegen eine Regel verstoesst, STOPPE sofort und melde den Verstoss statt die Aenderung durchzufuehren.

## Task-Beschreibung

Die aktuelle Root Cause ist nicht mehr Discovery oder ein falscher einzelner v14-Account, sondern ein **zu statisch gebautes PumpSwap-Sell-Layout**.

Der User hat neue Runtime-Evidenz geliefert:

- erfolgreiche direkte PumpSwap-`sell`-TXs mit **21 Accounts**
- erfolgreiche direkte PumpSwap-`sell`-TXs mit **24 Accounts**
- die betroffenen Problemtoken gehoeren zur 24-Account-Klasse

Der naechste Scope ist daher:

1. `pump_amm`-Sell darf **nicht mehr global als "immer 21 Accounts"** gebaut werden.
2. `market-data` muss die fuer das PumpSwap-Sell-Layout relevanten Feature-Merkmale **autoritaetstreu erkennen** und **im Cache / JetStream / SLAVE** verfuegbar machen.
3. `tx_builder` / PumpSwap-Builder muessen daraus das **dynamische Sell-Layout** bauen:
   - Standardfall: 21
   - erweiterter Fall: zusaetzliche Remaining-/Kontext-Accounts, wenn onchain fuer diesen Pool / Coin erforderlich
4. Kein blindes Hardcoding "immer 24" als Gegensymptom-Fix.

## Harte Runtime-Evidenz

### Offizielle Pump-Doku

Es gibt offizielle Pump-Dokumentation ausserhalb des Repos:

- `idl/pump_amm.ts`
- `docs/PUMP_CASHBACK_README.md`
- `docs/PUMP_SWAP_README.md`

Wesentliche, fuer diesen Scope bereits verifizierte Aussagen:

1. Die offizielle PumpSwap-IDL beschreibt fuer `sell` ein **21-Account-Basislayout**.
2. Die offizielle Cashback-Doku sagt fuer **Pump Swap Sell**:
   - `remaining_accounts[0]` = WSOL ATA des `UserVolumeAccumulator` fuer das Pump AMM Programm
   - `remaining_accounts[1]` = `UserVolumeAccumulator` fuer das Pump AMM Programm
3. Das bedeutet: der dokumentierte `sell` ist **nicht starr nur 21**, sondern kann zusaetzliche Remaining Accounts brauchen.

### Mainnet-Referenzen

Es gibt jetzt drei relevante Mainnet-Signaturen:

#### A. 21-Account-Sell

Signatur:

- `4GshJEMQztyguGRX9dBoJtBTbC3Ywb8bwjF57MMRoVdrFZLrvDdAknHCAGbaz8fdKk5eg6KEV2PWHgwZaxWMRUJ3`

Befund:

- echte `pump_amm` `Instruction: Sell`
- genau **21 Accounts**
- Standardfall, passt zur offiziellen IDL

#### B. 24-Account-Sell

Signaturen:

- `2CCmRDScAErjuBLnVJbGEyV3jsWbuNZpniZ5iTLSwZoE84nmyf285hqJXjRStMHJUaJ9Ex7EvL9fgwAVM83qGd3o`
- `S1P8nMjnNhV8zPxAXWrfwf1gX2Dy7vNMEyPKrFB3kByFFTNjQ3ccZFBKpRRmDxZ88yqwBLz4D1VB53LXXjUFB9t`

Befund:

- echte `pump_amm` `Instruction: Sell`
- genau **24 Accounts**
- die drei zusaetzlichen trailing Accounts sind beobachtbar
- zwei davon passen bereits fachlich exakt zur offiziellen Cashback-Doku:
  - WSOL ATA des `UserVolumeAccumulator`
  - `UserVolumeAccumulator`
- das dritte readonly Zusatzkonto ist sehr wahrscheinlich Teil eines neueren Sharing-/Creator-Fee-/Volume-Tracking-Kontexts und muss fachlich korrekt identifiziert werden

### Wichtige Schlussfolgerung

Die alte Annahme:

- "PumpSwap SELL = 21"

ist fuer heutige Mainnet-Realitaet **zu eng**.

Die korrekte Arbeitsannahme fuer diesen Scope ist:

- **21 ist das Basislayout**
- fuer bestimmte Coins / Pools werden **zusatzliche Accounts** benoetigt
- diese Zusatzaccounts muessen **onchain / aus offizieller Programmlogik** erkannt werden
- `market-data` muss diese Information autoritaetstreu fuer spaetere Sells in den Cache bringen

## Ziel dieses Scopes

Bitte setze einen **engen, sauberen Architektur-Fix** um:

1. `market-data` erkennt fuer PumpSwap autoritativ, ob ein Pool / Coin den erweiterten Sell-Pfad braucht.
2. Diese Information wird im MASTER-State und ueber JetStream in den SLAVE-Cache propagiert.
3. Der Sell-Builder baut das Layout **dynamisch**:
   - Basislayout 21
   - plus zusaetzliche Accounts nur dann, wenn der onchain-/cache-gestuetzte Zustand das verlangt
4. Wenn fuer den erweiterten Pfad notwendige Informationen nicht autoritativ verfuegbar sind, muss der Pfad **sauber fehlschlagen** statt ein semantisch falsches 21er- oder 24er-Layout zu bauen.

## Relevante Invarianten (Volltext)

### I-4 Hot Path = Geyser-First
HOT PATH (Discovery, Buy, Sell, Monitoring): GEYSER-ONLY. Keine neuen blockierenden RPC-Calls. Wenn ein Pfad sowohl Hot als auch Cold Path beruehrt, darf der Fix keinen neuen blockierenden Engine-RPC oder externen RPC in den Hot Path schleusen.

### I-5 Cold Path
COLD PATH (Liquidation, Manual Actions, Bootstrap): RPC erlaubt. Safety und correctness vor Speed. Autoritativer On-Chain-State darf hier nachgeladen werden.

### I-7 Hot-Path RPC-Freiheit
Nie RPC im normalen Trading-Hot-Path ohne explizite Freigabe. Keine neue Feature-Erkennung, die versehentlich im Hot Path unbedingte RPCs ausloest.

### I-9 Simulation-Gate
Wenn Simulation fehlschlaegt, darf keine Transaktion gesendet werden. Dieser Scope darf das Simulations-Gate nicht aufweichen oder bypassen.

### I-12 Decision Record
Wenn der Sell-Pfad wegen fehlender autoritativer Layout-Information fehlschlaegt, muessen Decision-Record und bestehende Reject-Pfade erhalten bleiben. Keine stille Ablehnung.

### I-24d Cold-Path Discovery nur per Request/Reply
`execution-engine` darf fehlende oder unbrauchbare PumpSwap-Accounts im Cold Path weder selbst discovern noch lokal als Truth in den SLAVE Cache schreiben. Discovery, MASTER-Write und JetStream-Publikation bleiben bei `market-data`. `execution-engine` darf nur den korrelierten Request/Reply-Pfad anstossen und bounded auf die autoritative Antwort warten.

## Bestehendes Pattern, das du explizit wiederverwenden sollst

### A. Cache-Hit ist nicht automatisch ready

Siehe bekannte Patterns fuer PumpFun Cashback und PumpSwap Recovery:

- ein Cache-Hit darf nicht blind bedeuten "Layout ist vollstaendig / sell-ready"
- Feature-Flags oder Zusatzaccounts muessen autoritaetstreu vorliegen
- fehlt diese Information, darf ein Hot Path nicht blind raten; ein Cold Path darf bounded verifizieren

### B. JetStream-Propagation statt lokale Engine-Wahrheit

Analog zum bereits umgesetzten `cashback_enabled`-Fix im PumpFun-Pfad:

- relevante onchain-Feature-Erkennung gehoert in `market-data`
- der MASTER publiziert die Information
- der SLAVE liest sie aus JetStream / Cache-Metadata
- der Builder verwendet den gecachten, autoritativen Zustand

### C. DEX-Account-Order nur gegen IDL + echte Mainnet-Referenz

Kein Fix "nach Gefuehl".

Fuer diesen Scope ist der richtige Standard:

- offizielle Pump-Docs / IDL
- erfolgreiche echte Mainnet-Sell-Referenzen
- keine globale starre Annahme `SELL == 21`
- aber auch keine globale starre Annahme `SELL == 24`

## Erwartete technische Richtung

Bitte arbeite in dieser Reihenfolge:

### A. Erweiterten Sell-Pfad fachlich sauber modellieren

Klaere im Code explizit:

- was das **21-Account-Basislayout** ist
- welche **zusaetzlichen Accounts** fuer den erweiterten Pfad benoetigt werden
- welche davon deterministisch aus `user`, Programm-ID und Mint ableitbar sind
- welches zusaetzliche readonly Konto bzw. welcher Kontextaccount fuer den beobachteten 24er-Fall gebraucht wird

Wichtig:

- nicht einfach "wenn Token-2022 dann 24"
- nicht einfach "wenn cashback dann immer genau 24"
- erst das reale onchain-Merkmal und die reale Programmlogik bestimmen

### B. PumpSwap-Feature-Erkennung in `market-data`

`market-data` soll fuer wallet-relevante PumpSwap-Pools die Merkmale erkennen, die spaeter den Sell-Builder steuern.

Das kann z. B. in dieser Form enden:

- `cashback_enabled` oder aequivalente PumpSwap-spezifische Sell-Layout-Flags
- `requires_user_volume_accumulator_wsol_ata`
- `requires_user_volume_accumulator`
- ggf. zusaetzlicher `sharing`-/`creator-fee`-/`volume-tracking` Kontext, wenn fachlich wirklich erforderlich

Die konkrete Datenform darfst du passend zum bestehenden Pattern waehlen, aber:

- die Information muss vom MASTER in den Cache / JetStream publiziert werden
- der SLAVE muss sie korrekt rekonstruieren
- sie darf nicht nur lokal im Builder "nebenbei" per spekulativer Heuristik erraten werden

### C. Dynamischer Sell-Builder

Der PumpSwap-Sell-Builder soll:

1. immer das offizielle 21er-Basislayout bauen
2. bei autoritativ erkanntem erweitertem Pfad die noetigen Zusatzaccounts anhaengen
3. fuer deterministische Zusatzaccounts PDAs / ATAs sauber ableiten
4. fuer nicht-deterministische / featuregebundene Zusatzaccounts nur onchain-/cache-autorisierte Werte verwenden

Wenn ein erweiterter Pfad angezeigt ist, die noetige Information aber fehlt:

- lieber klarer Fehler
- nicht still auf 21 zurueckfallen
- nicht blind 24 mit erfundenem Konto bauen

### D. Markt-/Cache-Zukunftssicherheit

Der User will explizit, dass die Sells in Zukunft **wieder korrekt funktionieren**, nicht nur in diesem einen Run.

Deshalb muss der Fix nicht nur den Builder selbst, sondern auch die **Cache-Pipeline** adressieren:

- `market-data` erkennt das Sell-Layout-relevante Feature
- MASTER publiziert es
- SLAVE behält es ueber JetStream / Bootstrap
- spaetere Sells koennen ohne neue lokale Truth-Heilung korrekt bauen

## Relevante Bug-Patterns

- `KNOWN_BUG_PATTERNS.md` #14
  - alte Account-Count-Annahmen fuer PumpFun/PumpSwap koennen veralten; BUY/SELL duerfen nicht an veralteten Guard-/Count-Annahmen haengen
- `KNOWN_BUG_PATTERNS.md` #19
  - kein Fix ohne harte Runtime-Evidenz
- `KNOWN_BUG_PATTERNS.md` #20
  - DEX-Account-Order / Account-Satz nur gegen offizielle IDL und echte Mainnet-Referenz korrigieren
- `KNOWN_BUG_PATTERNS.md` #25
  - analoges Pattern: Feature-Flag (`cashback_enabled`) muss ueber market-data -> JetStream -> SLAVE propagiert werden; Cache-Hit darf keinen notwendigen autoritativen Pfad verdecken
- `KNOWN_BUG_PATTERNS.md` #29
  - keine statische Token-Program-Annahme fuer PumpSwap
- `KNOWN_BUG_PATTERNS.md` #34
  - Cold-Path Recovery darf nicht erneut denselben kaputten Satz liefern
- `KNOWN_BUG_PATTERNS.md` #36
  - Cache-Hit ist nicht automatisch `ready`

OpenBrain-relevante Treffer:

- "Cold-path publish path still allows heuristic PumpSwap field resolution ..."
- "degenerate Cache Reserves / cache-hit prevents fallback"
- "JetStream cache hardcodes cashback_enabled=false, Cache-HIT prevents RPC fallback"

Das Muster ist konsistent:

- Ein Teilzustand im Cache wird zu frueh wie vollstaendiger Wahrheit behandelt.

## Akzeptanzkriterien

- PumpSwap-SELL ist nicht mehr global hart auf 21 festgelegt
- der Builder nutzt ein **dynamisches** Layout nach autoritativ erkanntem Zustand
- `market-data` erkennt die fuer PumpSwap-SELL relevanten Cashback-/Sharing-/Tracking-Merkmale korrekt
- diese Merkmale werden ueber MASTER -> JetStream -> SLAVE propagiert
- Standard-Pools bleiben beim 21er-Basislayout
- erweiterte Pools bauen die zusaetzlichen Accounts korrekt
- kein blindes globales Hardcoding "immer 24"
- kein neuer Hot-Path-RPC
- keine lokale Engine-Truth
- kein Simulation-Bypass

## Erlaubte Dateien

- `Iron_crab/src/solana/dex/pumpfun_amm.rs`
- `Iron_crab/src/bin/market_data.rs`
- `Iron_crab/src/execution/pool_cache_sync.rs`
- `Iron_crab/src/execution/tx_builder.rs` nur falls fuer die Zusatzaccounts / Builder-Weitergabe wirklich minimal noetig
- `Iron_crab/src/bin/execution_engine.rs` nur falls fuer bestehenden Cold-Path-Request/Reply-/Fehlerpfad minimal noetig
- enge Tests im selben Repo, wenn sie genau diesen PumpSwap-Contract absichern

## Verboten

- Kein Eval-Repo
- Kein globales Hardcoding "PumpSwap SELL = 24"
- Kein spekulativer / heuristischer Blind-Fix ohne autoritativen Merkmal-Nachweis
- Kein neuer lokaler Discovery-/Write-Pfad in `execution-engine`
- Kein Hot-Path-RPC
- Kein unbounded externer Scan
- Keine Aufweichung des Simulation-Gates
- Kein grosser Multi-DEX-Refactor

## Pruef-Befehle

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --quiet
cargo test --features test_helpers --quiet
```

## Erwarteter Abschlussbericht

Bitte am Ende kurz nennen:

- welche STOP-CHECKs geprueft wurden
- welches onchain-/cache-Reife-Merkmal jetzt zwischen Basislayout und erweitertem Sell-Layout unterscheidet
- welche Zusatzaccounts fuer den erweiterten Pfad gebaut werden und aus welcher Quelle sie kommen
- wie `market-data` diese Information in MASTER / JetStream / SLAVE propagiert
- ob der Standardfall weiter 21 bleibt
- ob der erweiterte Fall beobachtbar korrekt zusaetzliche Accounts anhaengt
- welche Tests / Checks gelaufen sind
