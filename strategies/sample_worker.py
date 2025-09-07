import sys, json

# Simple line protocol worker
# - Input: a JSON object per line
# - Emits: for tick/event, one JSON object per line representing StrategyDecision { actions: [...] }

# Minimal echo strategy: no actions, but validates protocol and prints decisions

def handle(obj):
    kind = obj.get("kind")
    # For backtest event passthrough, SimEvent will be passed directly; produce empty decision
    return {"actions": []}

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        obj = json.loads(line)
    except Exception:
        # emit empty decision to keep the pipe flowing
        print(json.dumps({"actions": []}), flush=True)
        continue
    out = handle(obj)
    print(json.dumps(out), flush=True)
