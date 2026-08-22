# Spezifikation – Standort (Level 5)

Die vollständige Spezifikation liegt im Evaluator-Repo:

**https://github.com/Exploratorsclub/Iron_crab-eval** → `docs/spec/` (Branch `main`)

## Dokumente

| Datei | Inhalt |
|--------|--------|
| `INVARIANTS.md` | Lebender Invarianten-Katalog (eval-getestet + Leitlinien). Zuerst lesen. |
| `TARGET_ARCHITECTURE.md` | Zielarchitektur; aktueller Betriebsstand steht oben im Dokument. |
| `DEFINITION_OF_DONE.md` | Historische Abnahme-Checkliste des Umbaus. |
| `ROLE_SEPARATION.md` | Rollen / Keyless |
| `STORAGE_CONVENTIONS.md` | Persistenz, JSONL, Schema |
| `ARB_QUOTE_CONTRACT.md` | Arb-Quote-Vertrag |
| `ARB_TRACK_REQUESTS.md` | Arb Track-Requests |
| `MOMENTUM_ACTIVE_POOLS.md` | Momentum Active Pools |
| `TRAILING_SESSION_HIGH.md` | Trailing Session High |

Onboarding für Mitentwickler: [CONTRIBUTING.md](../CONTRIBUTING.md) (dieses Repo) und [Iron_crab-eval/CONTRIBUTING.md](https://github.com/Exploratorsclub/Iron_crab-eval/blob/main/CONTRIBUTING.md).

## Level-5

Test Authority arbeitet mit Spec + Tests im Eval-Repo. Implementation Agent (dieses Repo) sieht den Eval-Test-Code nicht. Spec unter `Iron_crab-eval/docs/` darf gelesen werden; `Iron_crab-eval/tests/` nicht.
