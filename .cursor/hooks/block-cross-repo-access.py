#!/usr/bin/env python3
"""Block Cloud Agent shell commands that cross the Impl/Eval repo boundary."""

import json
import re
import sys


def response(permission, user_message=None, agent_message=None):
    payload = {"permission": permission}
    if user_message:
        payload["user_message"] = user_message
    if agent_message:
        payload["agent_message"] = agent_message
    print(json.dumps(payload))


def collect_strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for item in value.values():
            yield from collect_strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from collect_strings(item)


raw = sys.stdin.read()
try:
    event = json.loads(raw or "{}")
except json.JSONDecodeError:
    response(
        "deny",
        "Shell command blocked because the hook could not parse the request.",
        "The repository shell guard failed closed on invalid hook input.",
    )
    sys.exit(0)

command = event.get("command")
if not isinstance(command, str):
    command = "\n".join(collect_strings(event))

command_lc = command.lower()

blocked_patterns = [
    (r"\bgit\s+clone\b", "Impl agents must not clone additional repositories."),
    (r"\bgh\s+repo\s+clone\b", "Impl agents must not clone additional repositories."),
    (r"\bgit\s+submodule\b", "Impl agents must not fetch nested repositories."),
    (
        r"exploratorsclub[/\\]iron_crab-eval\b",
        "Impl agents must not access the Iron_crab-eval repository.",
    ),
    (
        r"\biron_crab-eval\b",
        "Impl agents must not access the Iron_crab-eval repository.",
    ),
]

for pattern, reason in blocked_patterns:
    if re.search(pattern, command_lc):
        response(
            "deny",
            reason,
            (
                f"{reason} Stop and report the need for supervisor/test-authority "
                "coordination instead of reading or modifying the eval repo."
            ),
        )
        sys.exit(0)

response("allow")
