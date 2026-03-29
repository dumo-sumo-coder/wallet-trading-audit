"""Populate trusted valuation rows for wrapped-SOL swaps using explicit SOL/USD lookup."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
SRC = ROOT / "src"
for search_path in (SCRIPT_DIR, SRC):
    if str(search_path) not in sys.path:
        sys.path.insert(0, str(search_path))

from valuation.solana_valuation import (  # noqa: E402
    load_valuation_records,
    populate_wrapped_sol_trusted_values,
    write_valuation_records,
)
from valuation.sol_usd_lookup import lookup_sol_usd_at_timestamp  # noqa: E402
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
    valuation_records = load_valuation_records(resolved_path, include_all_statuses=True)
    population_result = populate_wrapped_sol_trusted_values(
        valuation_records,
        overwrite_existing=overwrite_existing,
        lookup_fn=lookup_sol_usd_at_timestamp,
    )
    write_valuation_records(resolved_path, population_result.records)

    return PopulateTrustedSolanaSwapValuationsResult(
        trusted_valuation_path=_relative_path_text(resolved_path),
        rows_processed=len(valuation_records),
        wrapped_sol_rows=population_result.wrapped_sol_rows,
        trusted_rows_populated=population_result.trusted_rows_populated,
        skipped_existing_trusted_rows=population_result.skipped_existing_trusted_rows,
        failed_lookup_rows=population_result.failed_lookup_rows,
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
def _relative_path_text(path: Path) -> str:
    try:
        return path.relative_to(ROOT).as_posix()
    except ValueError:
        return str(path)


if __name__ == "__main__":
    raise SystemExit(main())
