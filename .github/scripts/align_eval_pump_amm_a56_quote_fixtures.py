#!/usr/bin/env python3
"""CI-only: patch ironcrab-eval tests for A.56 Pump AMM ExecutableMarginal FeeConfig fixtures."""

from __future__ import annotations

import re
import sys
from pathlib import Path

MARKER = "pump_amm_reload_tier0_bootstrap_fee_config_fixture"
# Eval blackbox tests use a placeholder mint string; 41-char form is not a valid 32-byte base58 pubkey.
FIXTURE_MINT_INVALID = "TokenMint11111111111111111111111111111111"
FIXTURE_MINT = "TokenMint1111111111111111111111111111111111"
SUPPLY: int = 1_000_000_000_000_000

PUMP_USE = """use ironcrab::solana::dex::pumpfun_amm::{
    pump_amm_canonical_pool_creator_for_base_mint, pump_amm_register_mint_supply_for_quote,
    pump_amm_register_pool_creator_for_quote, pump_amm_reload_tier0_bootstrap_fee_config_fixture,
};
"""

SAMPLE_POOL_SEED = f"""    if dex == "pump_amm" {{
        {MARKER}();
        if let Ok(mint) = Pubkey::from_str("{FIXTURE_MINT}") {{
            let creator = pump_amm_canonical_pool_creator_for_base_mint(&mint);
            pump_amm_register_pool_creator_for_quote(mint, creator);
            pump_amm_register_mint_supply_for_quote(mint, {SUPPLY});
        }}
    }}
"""

CACHE_RESERVES_SEED = f"""    {MARKER}();
    let creator = pump_amm_canonical_pool_creator_for_base_mint(&base_mint);
    pump_amm_register_pool_creator_for_quote(base_mint, creator);
    pump_amm_register_mint_supply_for_quote(base_mint, {SUPPLY});
"""


def _needs_pump_imports(text: str) -> bool:
    return (
        "pump_amm_canonical_pool_creator_for_base_mint" not in text
        or MARKER not in text
        or "pump_amm_register_pool_creator_for_quote" not in text
    )


def _insert_pump_use_block(text: str) -> str:
    if not _needs_pump_imports(text):
        return text
    anchor = re.search(r"^(fn |const |#\[test\])", text, re.MULTILINE)
    pos = anchor.start() if anchor else len(text)
    return text[:pos] + PUMP_USE + "\n" + text[pos:]


def _ensure_from_str_import(text: str) -> str:
    if "use std::str::FromStr;" in text:
        return text
    if "Pubkey::from_str" not in text:
        return text
    anchor = re.search(r"^(fn |const |#\[test\])", text, re.MULTILINE)
    pos = anchor.start() if anchor else len(text)
    return text[:pos] + "use std::str::FromStr;\n" + text[pos:]


def _ensure_pubkey_import(text: str) -> str:
    if "use solana_sdk::pubkey::Pubkey;" in text:
        return text
    if "Pubkey::" not in text:
        return text
    anchor = re.search(r"^(fn |const |#\[test\])", text, re.MULTILINE)
    pos = anchor.start() if anchor else len(text)
    return text[:pos] + "use solana_sdk::pubkey::Pubkey;\n" + text[pos:]


def _function_body_span(text: str, fn_name: str) -> tuple[int, int] | None:
    m = re.search(rf"\bfn {re.escape(fn_name)}\(", text)
    if not m:
        return None
    brace = text.find("{", m.end())
    if brace < 0:
        return None
    depth = 0
    for offset, ch in enumerate(text[brace:], start=0):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return brace + 1, brace + offset
    return None


SAMPLE_POOL_FN = re.compile(
    r"fn sample_pool\(\s*dex:\s*&str[^)]*\)\s*->\s*QuotePoolInput\s*\{"
)


def _file_uses_pump_amm_sample_pool(text: str) -> bool:
    return 'sample_pool("pump_amm"' in text or "sample_pool(\"pump_amm\"" in text


def _sample_pool_already_seeded(text: str) -> bool:
    m = SAMPLE_POOL_FN.search(text)
    if not m:
        return False
    head = text[m.end() : m.end() + 600]
    return MARKER in head and 'if dex == "pump_amm"' in head


ORPHAN_PUMP_SEED = re.compile(
    r"\n\s*if dex == \"pump_amm\" \{[^}]*"
    + re.escape(MARKER)
    + r"[^}]*\}\s*\n"
)


def _strip_orphan_pump_amm_seed(text: str) -> str:
    """Remove a prior mis-patch that inserted the seed outside `sample_pool(dex: ...)`."""
    if _sample_pool_already_seeded(text):
        return text
    return ORPHAN_PUMP_SEED.sub("\n", text, count=1)


def _strip_unused_pump_use_block(text: str) -> str:
    if _file_uses_pump_amm_sample_pool(text):
        return text
    if PUMP_USE.strip() not in text:
        return text
    return text.replace(PUMP_USE + "\n", "", 1)


def _ensure_imports_before_sample_pool(text: str) -> str:
    m = SAMPLE_POOL_FN.search(text)
    if not m:
        return text
    prefix = text[: m.start()]
    suffix = text[m.start() :]
    inserts: list[str] = []
    if "Pubkey::from_str" in text or MARKER in text:
        if "use solana_sdk::pubkey::Pubkey;" not in text:
            inserts.append("use solana_sdk::pubkey::Pubkey;")
        if "use std::str::FromStr;" not in text:
            inserts.append("use std::str::FromStr;")
    if not inserts:
        return text
    block = "\n".join(inserts) + "\n\n"
    return prefix + block + suffix


def _normalize_fixture_mint_literals(text: str) -> str:
    if not _file_uses_pump_amm_sample_pool(text):
        return text
    if FIXTURE_MINT in text:
        return text
    return text.replace(f'"{FIXTURE_MINT_INVALID}"', f'"{FIXTURE_MINT}"')


def patch_sample_pool(text: str) -> str:
    text = _normalize_fixture_mint_literals(text)
    text = _strip_orphan_pump_amm_seed(text)
    if not _file_uses_pump_amm_sample_pool(text):
        return _strip_unused_pump_use_block(text)
    if _sample_pool_already_seeded(text):
        return text
    m = SAMPLE_POOL_FN.search(text)
    if not m:
        return text
    text = _insert_pump_use_block(text)
    text = _ensure_imports_before_sample_pool(text)
    m = SAMPLE_POOL_FN.search(text)
    if not m:
        return text
    return text[: m.end()] + "\n" + SAMPLE_POOL_SEED + text[m.end() :]


def patch_make_pump_amm_cache_with_reserves(text: str) -> str:
    span = _function_body_span(text, "make_pump_amm_cache_with_reserves")
    if span is None:
        return text
    body_start, body_end = span
    if MARKER in text[body_start:body_end]:
        return text
    text = _insert_pump_use_block(text)
    text = _ensure_pubkey_import(text)
    span = _function_body_span(text, "make_pump_amm_cache_with_reserves")
    if span is None:
        return text
    body_start, body_end = span
    body = text[body_start:body_end]
    new_body = CACHE_RESERVES_SEED + body
    new_body = re.sub(
        r"creator:\s*None,",
        "creator: Some(creator),",
        new_body,
        count=1,
    )
    return text[:body_start] + "\n" + new_body + text[body_end:]


def patch_file(path: Path) -> bool:
    original = path.read_text(encoding="utf-8")
    updated = original
    updated = patch_sample_pool(updated)
    if path.name in (
        "pump_amm_geyser_first.rs",
        "invariants_pumpswap_amm_liquidation.rs",
    ):
        updated = patch_make_pump_amm_cache_with_reserves(updated)
    if updated == original:
        return False
    path.write_text(updated, encoding="utf-8")
    return True


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: align_eval_pump_amm_a56_quote_fixtures.py EVAL_ROOT", file=sys.stderr)
        return 2
    eval_root = Path(sys.argv[1])
    tests = eval_root / "tests"
    if not tests.is_dir():
        print(f"missing tests dir: {tests}", file=sys.stderr)
        return 1
    changed = 0
    for path in sorted(tests.glob("*.rs")):
        if patch_file(path):
            changed += 1
            print(f"aligned {path.relative_to(eval_root)}")
    print(f"align_eval_pump_amm_a56: {changed} file(s) updated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
