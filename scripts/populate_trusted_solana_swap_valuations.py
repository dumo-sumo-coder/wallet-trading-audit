"""Populate trusted valuation rows for wrapped-SOL swaps using explicit SOL/USD lookup."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
SRC = ROOT / "src"
for search_path in (SCRIPT_DIR, SRC):
    if str(search_path) not in sys.path:
        sys.path.insert(0, str(search_path))

from valuation.sol_usd_lookup import (  # noqa: E402
    SolUsdLookupError,
    lookup_sol_usd_at_timestamp,
)
from valuation.solana_valuation import VALUATION_STATUS_TRUSTED  # noqa: E402

SOLANA_WRAPPED_SOL_MINT = "So11111111111111111111111111111111111111112"
DEFAULT_TRUSTED_VALUATION_DIR = ROOT / "data" / "raw" / "solana" / "test_wallet"


@dataclass(frozen=True, slots=True)
class PopulateTrustedSolanaSwapValuationsResult:
    trusted_valuation_path: str
    rows_processed: int
    wrapped_sol_rows: int
    trusted_rows_populated: int
    skipped_existing_trusted_rows: int
    failed_lookup_rows: int


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Populate trusted valuation rows for wrapped-SOL swaps using explicit SOL/USD lookup.",
    )
    parser.add_argument(
        "--trusted-valuation-path",
        type=Path,
        default=None,
        help="Optional explicit trusted valuation file. Defaults to the latest single-wallet file.",
    )
    parser.add_argument(
        "--overwrite-existing",
        action="store_true",
        help="Allow overwriting already-trusted rows.",
    )
    return parser.parse_args(argv)


def find_latest_trusted_valuation_path(
    valuation_dir: Path = DEFAULT_TRUSTED_VALUATION_DIR,
) -> Path:
    valuation_paths = sorted(
        path
        for path in valuation_dir.glob("wallet_snapshot_*_trusted_valuations.json")
        if "_proposed_" not in path.stem
    )
    if not valuation_paths:
        raise ValueError(f"No trusted valuation files found under {valuation_dir}")
    return valuation_paths[-1]


def populate_trusted_solana_swap_valuations(
    *,
    trusted_valuation_path: Path | None = None,
    overwrite_existing: bool = False,
) -> PopulateTrustedSolanaSwapValuationsResult:
    resolved_path = trusted_valuation_path or find_latest_trusted_valuation_path()
    payload = json.loads(resolved_path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("Trusted valuation file must be a JSON object")
    raw_rows = payload.get("valuations")
    if not isinstance(raw_rows, list):
        raise ValueError("Trusted valuation file must contain a 'valuations' list")

    wrapped_sol_rows = 0
    trusted_rows_populated = 0
    skipped_existing_trusted_rows = 0
    failed_lookup_rows = 0

    updated_rows: list[object] = []
    for item in raw_rows:
        if not isinstance(item, Mapping):
            raise ValueError("Each trusted valuation row must be a JSON object")
        row = dict(item)
        status = _optional_text(row.get("valuation_status"))
        if status is None:
            raise ValueError("Trusted valuation row is missing valuation_status")

        if not _has_wrapped_sol_leg(row):
            updated_rows.append(row)
            continue

        wrapped_sol_rows += 1
        if status == VALUATION_STATUS_TRUSTED and not overwrite_existing:
            skipped_existing_trusted_rows += 1
            updated_rows.append(row)
            continue

        try:
            lookup = lookup_sol_usd_at_timestamp(_require_datetime(row, "block_time"))
        except SolUsdLookupError:
            failed_lookup_rows += 1
            updated_rows.append(row)
            continue

        sol_amount = _extract_sol_amount(row)
        usd_value = lookup.reference_price_usd * sol_amount
        row["usd_value"] = _decimal_to_text(usd_value)
        row["valuation_source"] = (
            f"{lookup.source_name};product_id={lookup.product_id};"
            f"price_reference={lookup.price_reference_kind};"
            f"candle_start={lookup.reference_candle_start.isoformat()};"
            f"looked_up_at={lookup.lookup_timestamp.isoformat()}"
        )
        row["valuation_status"] = VALUATION_STATUS_TRUSTED
        trusted_rows_populated += 1
        updated_rows.append(row)

    payload_to_write = dict(payload)
    payload_to_write["valuations"] = updated_rows
    resolved_path.write_text(json.dumps(payload_to_write, indent=2), encoding="utf-8")

    return PopulateTrustedSolanaSwapValuationsResult(
        trusted_valuation_path=_relative_path_text(resolved_path),
        rows_processed=len(raw_rows),
        wrapped_sol_rows=wrapped_sol_rows,
        trusted_rows_populated=trusted_rows_populated,
        skipped_existing_trusted_rows=skipped_existing_trusted_rows,
        failed_lookup_rows=failed_lookup_rows,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = populate_trusted_solana_swap_valuations(
            trusted_valuation_path=args.trusted_valuation_path,
            overwrite_existing=args.overwrite_existing,
        )
    except (ValueError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(f"Trusted valuation file: {result.trusted_valuation_path}")
    print(f"Rows processed: {result.rows_processed}")
    print(f"Wrapped SOL rows: {result.wrapped_sol_rows}")
    print(f"Trusted rows populated: {result.trusted_rows_populated}")
    print(f"Skipped existing trusted rows: {result.skipped_existing_trusted_rows}")
    print(f"Failed lookup rows: {result.failed_lookup_rows}")
    return 0


def _has_wrapped_sol_leg(row: Mapping[str, object]) -> bool:
    return (
        _optional_text(row.get("token_in_address")) == SOLANA_WRAPPED_SOL_MINT
        or _optional_text(row.get("token_out_address")) == SOLANA_WRAPPED_SOL_MINT
    )


def _extract_sol_amount(row: Mapping[str, object]) -> Decimal:
    if _optional_text(row.get("token_in_address")) == SOLANA_WRAPPED_SOL_MINT:
        return _require_decimal(row, "amount_in")
    if _optional_text(row.get("token_out_address")) == SOLANA_WRAPPED_SOL_MINT:
        return _require_decimal(row, "amount_out")
    raise ValueError("Wrapped SOL leg is required to extract SOL amount")


def _require_decimal(row: Mapping[str, object], key: str) -> Decimal:
    value = row.get(key)
    if value is None:
        raise ValueError(f"Trusted valuation row is missing decimal field: {key}")
    text = str(value).strip()
    if not text:
        raise ValueError(f"Trusted valuation decimal field '{key}' cannot be blank")
    return Decimal(text)


def _require_datetime(row: Mapping[str, object], key: str) -> datetime:
    value = row.get(key)
    if value is None:
        raise ValueError(f"Trusted valuation row is missing field: {key}")
    text = str(value).strip()
    if not text:
        raise ValueError(f"Trusted valuation field '{key}' cannot be blank")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"Trusted valuation field '{key}' must be timezone-aware")
    return parsed


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _decimal_to_text(value: Decimal) -> str:
    return format(value, "f")


def _relative_path_text(path: Path) -> str:
    try:
        return path.relative_to(ROOT).as_posix()
    except ValueError:
        return str(path)


if __name__ == "__main__":
    raise SystemExit(main())
