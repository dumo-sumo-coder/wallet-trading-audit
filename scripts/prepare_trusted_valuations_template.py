"""Prepare a manual trusted valuation template for one Solana wallet snapshot."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
SRC = ROOT / "src"
for search_path in (SCRIPT_DIR, SRC):
    if str(search_path) not in sys.path:
        sys.path.insert(0, str(search_path))

from analyze_single_wallet_snapshot import analyze_snapshot_path, find_latest_snapshot_path  # noqa: E402
from valuation.solana_valuation import VALUATION_STATUS_PENDING, SolanaValuationRecord  # noqa: E402


@dataclass(frozen=True, slots=True)
class TrustedValuationTemplateResult:
    snapshot_path: str
    template_path: str | None
    valuation_rows_count: int
    status: str


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare a manual trusted valuation template for the latest Solana test-wallet snapshot.",
    )
    parser.add_argument(
        "--snapshot-path",
        type=Path,
        default=None,
        help="Optional explicit snapshot path. Defaults to the latest test_wallet snapshot.",
    )
    return parser.parse_args(argv)


def prepare_trusted_valuations_template(
    *,
    snapshot_path: Path | None = None,
) -> TrustedValuationTemplateResult:
    resolved_snapshot_path = snapshot_path or find_latest_snapshot_path()
    analysis = analyze_snapshot_path(resolved_snapshot_path)
    rows_requiring_valuation = analysis.valuation_summary.rows_requiring_valuation_after

    if not rows_requiring_valuation:
        return TrustedValuationTemplateResult(
            snapshot_path=_relative_path_text(resolved_snapshot_path),
            template_path=None,
            valuation_rows_count=0,
            status="no_rows_require_valuation",
        )

    template_path = resolved_snapshot_path.with_name(
        f"{resolved_snapshot_path.stem}_trusted_valuations.json"
    )
    if template_path.exists():
        raise ValueError(
            f"Trusted valuation template already exists: {_relative_path_text(template_path)}"
        )

    template_payload = {
        "valuations": [_build_template_row(record) for record in rows_requiring_valuation]
    }
    template_path.write_text(
        json.dumps(template_payload, indent=2),
        encoding="utf-8",
    )

    return TrustedValuationTemplateResult(
        snapshot_path=_relative_path_text(resolved_snapshot_path),
        template_path=_relative_path_text(template_path),
        valuation_rows_count=len(rows_requiring_valuation),
        status="template_created",
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = prepare_trusted_valuations_template(snapshot_path=args.snapshot_path)
    except (ValueError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(f"Snapshot analyzed: {result.snapshot_path}")
    print(f"Status: {result.status}")
    print(f"Valuation rows included: {result.valuation_rows_count}")
    if result.template_path is not None:
        print(f"Template path: {result.template_path}")
        print("Next step: fill usd_value, valuation_source, and change valuation_status to trusted.")
        print("Then rerun: python scripts/analyze_single_wallet_snapshot.py")
    else:
        print("No template created because no rows currently require valuation.")
    return 0


def _build_template_row(record: SolanaValuationRecord) -> dict[str, object]:
    return {
        "tx_hash": record.tx_hash,
        "wallet": record.wallet,
        "block_time": record.block_time.isoformat(),
        "token_in_address": record.token_in_address,
        "token_out_address": record.token_out_address,
        "amount_in": str(record.amount_in),
        "amount_out": str(record.amount_out),
        "usd_value": None,
        "valuation_source": None,
        "valuation_status": VALUATION_STATUS_PENDING,
    }


def _relative_path_text(path: Path) -> str:
    try:
        return path.relative_to(ROOT).as_posix()
    except ValueError:
        return str(path)


if __name__ == "__main__":
    raise SystemExit(main())
