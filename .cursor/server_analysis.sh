#!/usr/bin/env bash
set -euo pipefail

ssh ironcrab-prod "python3 - <<'PY'
from pathlib import Path
import json
from collections import Counter
from datetime import datetime, timezone

base = Path('Iron_crab')
decisions_dir = base / 'trade_logs' / 'decisions'
intents_dir = base / 'trade_logs' / 'intents'
exec_dir = base / 'trade_logs' / 'executions'

def list_latest(dir_path, label):
    files = sorted([f.name for f in dir_path.iterdir() if f.is_file()])
    print(f'{label}: {files[-3:] if files else []}')
    return files

dec_files = list_latest(decisions_dir, 'decisions')
int_files = list_latest(intents_dir, 'intents')
exe_files = list_latest(exec_dir, 'executions')

latest_dec = dec_files[-1] if dec_files else None
latest_int = int_files[-1] if int_files else None

def parse_jsonl(path):
    for line in path.open():
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            continue

if latest_dec and latest_int:
    dec_path = decisions_dir / latest_dec
    int_path = intents_dir / latest_int

    intents = {}
    side_counts = Counter()
    entry_kind = Counter()
    for obj in parse_jsonl(int_path):
        iid = obj.get('intent_id')
        intents[iid] = obj
        side_counts[obj.get('side')] += 1
        kind = (obj.get('metadata') or {}).get('entry_kind')
        if kind:
            entry_kind[kind] += 1

    outcome_counts = Counter()
    reject_counts = Counter()
    sell_decisions = 0
    for obj in parse_jsonl(dec_path):
        outcome_counts[obj.get('outcome')] += 1
        if obj.get('primary_reject_reason'):
            reject_counts[obj.get('primary_reject_reason')] += 1
        iid = obj.get('intent_id')
        if intents.get(iid, {}).get('side') == 'Sell':
            sell_decisions += 1

    print('latest_intents_file:', latest_int)
    print('latest_decisions_file:', latest_dec)
    print('intents_total:', len(intents))
    print('intents_by_side:', dict(side_counts))
    print('entry_kind:', dict(entry_kind))
    print('decisions_outcome:', dict(outcome_counts))
    print('top_rejects:', reject_counts.most_common(5))
    print('sell_decisions_count:', sell_decisions)

cfg_path = base / 'control_plane' / 'control_plane_configs.json'
if cfg_path.exists():
    try:
        cfg = json.loads(cfg_path.read_text())
        for key in ['max_hold_time_secs','exit_eval_interval_secs','pending_trade_ttl_secs']:
            if key in cfg:
                print('control_plane_config', key, cfg[key])
    except Exception as e:
        print('control_plane_configs.json parse error:', e)
else:
    print('control_plane_configs.json not found')

log_path = base / 'logs' / 'execution_engine.log'
if log_path.exists():
    from collections import deque
    lines = deque(log_path.open(), 300)
    keywords = ('wsol', 'WSOL', 'wrap', 'Wrap', 'wsol_manager', 'WsolManager')
    matches = [l for l in lines if any(k in l for k in keywords)]
    print('execution_engine.log wsol matches:')
    for l in matches[-30:]:
        print(l.rstrip())
else:
    print('execution_engine.log not found')

PY"
