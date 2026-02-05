import json
import subprocess
import time
from pathlib import Path

DEBUG_LOG_PATH = Path(r"c:\Users\Robert Onuk\Desktop\Trading_bot\Iron_crab\.cursor\debug.log")
SESSION_ID = "debug-session"


def _now_ms() -> int:
    return int(time.time() * 1000)


def log(hypothesis_id: str, location: str, message: str, data: dict, run_id: str) -> None:
    payload = {
        "sessionId": SESSION_ID,
        "runId": run_id,
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
        "timestamp": _now_ms(),
    }
    DEBUG_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with DEBUG_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def ssh(host: str, cmd: str) -> str:
    p = subprocess.run(
        ["ssh", host, cmd],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    out = (p.stdout or "") + (("\n" + p.stderr) if p.stderr else "")
    return out.strip()


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="ironcrab-prod")
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--mint", default="2KkoQ5ErFA2zritMNb4A9JjrzhSJogzt1GQiy9W9EL1J")
    args = ap.parse_args()

    host = args.host
    run_id = args.run_id
    mint = args.mint

    # H0: Ensure we know what is deployed.
    out0 = ssh(host, "cd ~/Iron_crab && git rev-parse HEAD && git log -1 --oneline || true")
    log("H0", "tools/debug_collect_rejects.py:main", "Server git HEAD", {"text": out0}, run_id)
    out0b = ssh(host, "systemctl is-active market-data execution-engine momentum-bot 2>/dev/null || true")
    log("H0", "tools/debug_collect_rejects.py:main", "Systemd active status", {"text": out0b}, run_id)

    # H1: Wallet bootstrap overwrites non-ATA balances with 0 -> LOCK_CAPITAL_CONFLICT storms.
    # Collect decision reason distribution.
    out = ssh(
        host,
        r"""python3 - <<'PY'
import os,glob,json,collections
base=os.path.expanduser('~/Iron_crab/trade_logs/decisions')
files=sorted(glob.glob(os.path.join(base,'decision_records-*.jsonl')))
latest=files[-1]
from collections import deque
q=deque(maxlen=6000)
with open(latest,'rb') as f:
    for line in f: q.append(line)
counts=collections.Counter()
reject=collections.Counter()
simerr=collections.Counter()
for raw in list(q)[-2000:]:
    try: obj=json.loads(raw)
    except Exception: continue
    outc=obj.get('outcome')
    counts[outc]+=1
    rr=obj.get('primary_reject_reason')
    if rr: reject[rr]+=1
    sim=obj.get('simulate') or {}
    if sim and not sim.get('success',True):
        simerr[(sim.get('error_code') or 'unknown')]+=1
print(json.dumps({
  "latest": latest,
  "outcomes": dict(counts),
  "top_reject": reject.most_common(10),
  "top_sim_error": simerr.most_common(10),
}, ensure_ascii=False))
PY""",
    )
    try:
        summary = json.loads(out.splitlines()[-1])
    except Exception:
        summary = {"parse_error": True, "raw": out[-2000:]}
    log("H1", "tools/debug_collect_rejects.py:main", "DecisionRecords summary", summary, run_id)

    # Sample capital lock conflicts details.
    out2 = ssh(
        host,
        r"""python3 - <<'PY'
import os,glob,json
base=os.path.expanduser('~/Iron_crab/trade_logs/decisions')
latest=sorted(glob.glob(os.path.join(base,'decision_records-*.jsonl')))[-1]
rows=[]
with open(latest,'rb') as f:
    for line in f:
        try: o=json.loads(line)
        except Exception: continue
        if o.get('primary_reject_reason')!='LOCK_CAPITAL_CONFLICT': continue
        for c in (o.get('checks') or []):
            if c.get('check_name')=='capital_lock':
                rows.append({
                    "ts": o.get("ts_unix_ms"),
                    "decision_id": o.get("decision_id"),
                    "intent_id": o.get("intent_id"),
                    "details": c.get("details"),
                })
                break
print(json.dumps({"count": len(rows), "last": rows[-5:]}, ensure_ascii=False))
PY""",
    )
    try:
        conflicts = json.loads(out2.splitlines()[-1])
    except Exception:
        conflicts = {"parse_error": True, "raw": out2[-2000:]}
    log("H1", "tools/debug_collect_rejects.py:main", "Capital lock conflicts sample", conflicts, run_id)

    # Check wallet snapshot history for mint (first non-zero scan vs latest bootstrap).
    out3 = ssh(
        host,
        f"""python3 - <<'PY'
import os,glob,json
mint='{mint}'
path=sorted(glob.glob(os.path.expanduser('~/Iron_crab/trade_logs/market_events/market_events-*.jsonl')))[-1]
rows=[]
with open(path,'rb') as f:
    for line in f:
        if mint.encode() not in line: continue
        if b'WalletBalanceSnapshot' not in line: continue
        try: o=json.loads(line)
        except Exception: continue
        rows.append({{
            "ts": o.get("ts_unix_ms"),
            "source": o.get("source"),
            "balance_raw": o.get("balance_raw"),
            "decimals": o.get("decimals"),
            "token_program": o.get("token_program"),
        }})
rows.sort(key=lambda r: r.get("ts") or 0)
nz=[r for r in rows if (r.get("balance_raw") or 0)>0]
payload={{
  "snapshots": len(rows),
  "first": rows[0] if rows else None,
  "last": rows[-1] if rows else None,
  "non_zero": len(nz),
  "last_non_zero": nz[-1] if nz else None,
}}
print(json.dumps(payload, ensure_ascii=False))
PY""",
    )
    try:
        snaps = json.loads(out3.splitlines()[-1])
    except Exception:
        snaps = {"parse_error": True, "raw": out3[-2000:]}
    log("H1", "tools/debug_collect_rejects.py:main", "WalletBalanceSnapshot history for mint", snaps, run_id)

    # H2: IncorrectProgramId sims -> Token-2022 mints lacking token_program override.
    out4 = ssh(
        host,
        r"""python3 - <<'PY'
import os,glob,json
base=os.path.expanduser('~/Iron_crab/trade_logs/decisions')
latest=sorted(glob.glob(os.path.join(base,'decision_records-*.jsonl')))[-1]
rows=[]
with open(latest,'rb') as f:
    for line in f:
        try: o=json.loads(line)
        except Exception: continue
        if o.get('outcome')!='SimFailed': continue
        rr=o.get('primary_reject_reason') or ''
        if 'IncorrectProgramId' not in rr: continue
        rows.append({
            "ts": o.get("ts_unix_ms"),
            "decision_id": o.get("decision_id"),
            "intent_id": o.get("intent_id"),
            "reason": rr,
        })
print(json.dumps({"count": len(rows), "last": rows[-5:]}, ensure_ascii=False))
PY""",
    )
    try:
        simfailed = json.loads(out4.splitlines()[-1])
    except Exception:
        simfailed = {"parse_error": True, "raw": out4[-2000:]}
    log("H2", "tools/debug_collect_rejects.py:main", "SimFailed IncorrectProgramId sample", simfailed, run_id)


if __name__ == "__main__":
    main()

