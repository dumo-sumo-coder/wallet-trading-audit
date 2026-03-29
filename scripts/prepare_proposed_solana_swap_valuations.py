"""Prepare review-only proposed valuations for Solana swaps with a wrapped SOL leg."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
SRC = ROOT / "src"
for search_path in (SCRIPT_DIR, SRC):
    if str(search_path) not in sys.path:
        sys.path.insert(0, str(search_path))

from normalize.transactions import SOLANA_WRAPPED_SOL_MINT  # noqa: E402

DEFAULT_TRUSTED_VALUATION_DIR = ROOT / "data" / "raw" / "solana" / "test_wallet"


@dataclass(frozen=True, slots=True)
class ProposedSolanaSwapValuation:
    tx_hash: str
    block_time: str
    wallet: str
    token_in_address: str
    token_out_address: str
    amount_in: str
    amount_out: str
    sol_amount: str
    proposed_usd_value: str | None
    proposed_valuation_source: str | None
    valuation_status: str


@dataclass(frozen=True, slots=True)
class ProposedSolanaSwapValuationResult:
    trusted_valuation_path: str
    proposed_valuation_path: str | None
    rows_processed: int
    rows_with_wrapped_sol_leg: int
    rows_with_proposed_usd_value: int
    rows_missing_external_sol_usd_reference: int
    recommendation: str


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare review-only proposed Solana swap valuations from the latest trusted-valuation template.",
    )
    parser.add_argument(
        "--trusted-valuation-path",
        type=Path,
        default=None,
        help="Optional explicit trusted valuation template path. Defaults to the latest single-wallet template.",
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
        raise ValueError(f"No trusted valuation template files found under {valuation_dir}")
    return valuation_paths[-1]


def prepare_proposed_solana_swap_valuations(
    *,
    trusted_valuation_path: Path | None = None,
) -> ProposedSolanaSwapValuationResult:
    resolved_trusted_valuation_path = trusted_valuation_path or find_latest_trusted_valuation_path()
    raw_payload = json.loads(resolved_trusted_valuation_path.read_text(encoding="utf-8"))
    valuation_rows = _load_pending_or_trusted_rows(raw_payload)

    proposed_rows: list[ProposedSolanaSwapValuation] = []
    rows_with_wrapped_sol_leg = 0
    rows_with_proposed_usd_value = 0
    rows_missing_external_reference = 0

    sol_usd_reference = resolve_local_sol_usd_reference_source()

    for row in valuation_rows:
        if row["valuation_status"] == "trusted":
            continue

        wrapped_sol_leg = detect_wrapped_sol_leg(row)
        if wrapped_sol_leg is None:
            continue

        rows_with_wrapped_sol_leg += 1
        sol_amount = extract_sol_amount(row)
        proposed_usd_value = None
        proposed_valuation_source = None

        if sol_usd_reference is None:
            rows_missing_external_reference += 1
        else:  # pragma: no cover - no current repo-backed price source
            proposed_usd_value = None
            proposed_valuation_source = sol_usd_reference

        if proposed_usd_value is not None:
            rows_with_proposed_usd_value += 1

        proposed_rows.append(
            ProposedSolanaSwapValuation(
                tx_hash=row["tx_hash"],
                block_time=row["block_time"],
                wallet=row["wallet"],
                token_in_address=row["token_in_address"],
                token_out_address=row["token_out_address"],
                amount_in=row["amount_in"],
                amount_out=row["amount_out"],
                sol_amount=str(sol_amount),
                proposed_usd_value=proposed_usd_value,
                proposed_valuation_source=proposed_valuation_source,
                valuation_status="proposed",
            )
        )

    if not proposed_rows:
        return ProposedSolanaSwapValuationResult(
            trusted_valuation_path=_relative_path_text(resolved_trusted_valuation_path),
            proposed_valuation_path=None,
            rows_processed=len(valuation_rows),
            rows_with_wrapped_sol_leg=0,
            rows_with_proposed_usd_value=0,
            rows_missing_external_sol_usd_reference=0,
            recommendation=(
                "No wrapped-SOL valuation rows were found. Manual promotion into the trusted "
                "valuation file is not needed for this snapshot."
            ),
        )

    proposed_valuation_path = resolved_trusted_valuation_path.with_name(
        resolved_trusted_valuation_path.name.replace(
            "_trusted_valuations.json",
            "_proposed_valuations.json",
        )
    )
    proposal_payload = {
        "notes": [
            "This is a review-only proposal file. Do not treat proposed values as trusted automatically.",
            "No local or provider-backed SOL/USD reference source exists in this repo yet.",
            "proposed_usd_value remains null until an explicit external SOL/USD reference is verified manually.",
        ],
        "proposals": [_proposal_to_row(item) for item in proposed_rows],
    }
    proposed_valuation_path.write_text(
        json.dumps(proposal_payload, indent=2),
        encoding="utf-8",
    )

    return ProposedSolanaSwapValuationResult(
        trusted_valuation_path=_relative_path_text(resolved_trusted_valuation_path),
        proposed_valuation_path=_relative_path_text(proposed_valuation_path),
        rows_processed=len(valuation_rows),
        rows_with_wrapped_sol_leg=rows_with_wrapped_sol_leg,
        rows_with_proposed_usd_value=rows_with_proposed_usd_value,
        rows_missing_external_sol_usd_reference=rows_missing_external_reference,
        recommendation=(
            "The repo still needs a minimal SOL/USD lookup utility first, or you can manually "
            "review these proposals against an external verified SOL/USD reference and then copy "
            "approved values into the trusted valuation file."
        ),
    )


def detect_wrapped_sol_leg(row: Mapping[str, str]) -> str | None:
    token_in_address = row.get("token_in_address")
    token_out_address = row.get("token_out_address")
    if token_in_address == SOLANA_WRAPPED_SOL_MINT:
        return "token_in"
    if token_out_address == SOLANA_WRAPPED_SOL_MINT:
        return "token_out"
    return None


def extract_sol_amount(row: Mapping[str, str]) -> Decimal:
    wrapped_sol_leg = detect_wrapped_sol_leg(row)
    if wrapped_sol_leg == "token_in":
        return _require_decimal_text(row, "amount_in")
    if wrapped_sol_leg == "token_out":
        return _require_decimal_text(row, "amount_out")
    raise ValueError("Wrapped SOL leg is required to extract sol_amount")


def resolve_local_sol_usd_reference_source() -> str | None:
    """Return a local SOL/USD reference source if one exists in the repo.

    The current repository does not yet contain a provider-backed or local
    historical SOL/USD lookup utility, so proposals remain review-only with
    `proposed_usd_value=null`.
    """

    return None


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = prepare_proposed_solana_swap_valuations(
            trusted_valuation_path=args.trusted_valuation_path
        )
    except (ValueError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(f"Trusted valuation template: {result.trusted_valuation_path}")
    print(f"Rows processed: {result.rows_processed}")
    print(f"Rows with wrapped SOL leg: {result.rows_with_wrapped_sol_leg}")
    print(f"Rows with proposed_usd_value populated: {result.rows_with_proposed_usd_value}")
    print(
        "Rows still missing external SOL/USD reference: "
        f"{result.rows_missing_external_sol_usd_reference}"
    )
    if result.proposed_valuation_path is not None:
        print(f"Proposal path: {result.proposed_valuation_path}")
    else:
        print("Proposal path: none")
    print(f"Recommendation: {result.recommendation}")
    return 0


def _load_pending_or_trusted_rows(raw_payload: object) -> list[dict[str, str]]:
    if not isinstance(raw_payload, Mapping):
        raise ValueError("Trusted valuation template must be a JSON object")
    raw_rows = raw_payload.get("valuations")
    if not isinstance(raw_rows, list):
        raise ValueError("Trusted valuation template must contain a 'valuations' list")

    rows: list[dict[str, str]] = []
    for item in raw_rows:
        if not isinstance(item, Mapping):
            raise ValueError("Each valuation row must be a JSON object")
        row = {
            key: _require_text(item, key)
            for key in (
                "tx_hash",
                "wallet",
                "block_time",
                "token_in_address",
                "token_out_address",
                "amount_in",
                "amount_out",
                "valuation_status",
            )
        }
        if row["valuation_status"] not in {"pending", "trusted"}:
            raise ValueError(
                "Trusted valuation template rows must use valuation_status 'pending' or 'trusted'"
            )
        rows.append(row)
    return rows


def _proposal_to_row(proposal: ProposedSolanaSwapValuation) -> dict[str, object]:
    return {
        "tx_hash": proposal.tx_hash,
        "block_time": proposal.block_time,
        "wallet": proposal.wallet,
        "token_in_address": proposal.token_in_address,
        "token_out_address": proposal.token_out_address,
        "amount_in": proposal.amount_in,
        "amount_out": proposal.amount_out,
        "sol_amount": proposal.sol_amount,
        "proposed_usd_value": proposal.proposed_usd_value,
        "proposed_valuation_source": proposal.proposed_valuation_source,
        "valuation_status": proposal.valuation_status,
    }


def _require_text(mapping: Mapping[str, object], key: str) -> str:
    value = mapping.get(key)
    if value is None:
        raise ValueError(f"Missing required valuation template field: {key}")
    text = str(value).strip()
    if not text:
        raise ValueError(f"Valuation template field '{key}' cannot be blank")
    return text


def _require_decimal_text(mapping: Mapping[str, str], key: str) -> Decimal:
    text = mapping.get(key)
    if text is None:
        raise ValueError(f"Missing required decimal field: {key}")
    normalized_text = str(text).strip()
    if not normalized_text:
        raise ValueError(f"Decimal field '{key}' cannot be blank")
    return Decimal(normalized_text)


def _relative_path_text(path: Path) -> str:
    try:
        return path.relative_to(ROOT).as_posix()
    except ValueError:
        return str(path)


if __name__ == "__main__":
    raise SystemExit(main())
