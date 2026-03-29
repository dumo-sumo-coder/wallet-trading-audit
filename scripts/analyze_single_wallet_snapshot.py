"""Analyze one raw Solana wallet snapshot for normalization and FIFO coverage."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ingestion.solana_review import load_json_mapping  # noqa: E402
from normalize.schema import EventType, NormalizedTransaction  # noqa: E402
from normalize.transactions import normalize_transaction  # noqa: E402
from pnl.pipeline import run_fifo_pipeline  # noqa: E402
from valuation.solana_valuation import (  # noqa: E402
    SolanaValuationRecord,
    apply_trusted_usd_values,
    find_local_trusted_valuation_path,
    load_trusted_valuation_records,
    summarize_valuation_readiness,
)

DEFAULT_SNAPSHOT_DIR = ROOT / "data" / "raw" / "solana" / "test_wallet"


@dataclass(frozen=True, slots=True)
class UnsupportedSnapshotTransaction:
    index: int
    tx_hash: str | None
    reason: str


@dataclass(frozen=True, slots=True)
class UnsupportedReasonCount:
    reason: str
    count: int


@dataclass(frozen=True, slots=True)
class FifoCoverageSummary:
    status: str
    fifo_candidate_transactions_count: int
    skipped_missing_valuation_count: int
    skipped_missing_valuation_tx_hashes: tuple[str, ...]
    realized_pnl_usd: Decimal | None
    trade_matches_count: int
    remaining_positions_count: int
    recorded_fees_count: int
    error: str | None
    meaningful: bool


@dataclass(frozen=True, slots=True)
class SnapshotValuationSummary:
    valuation_path: str | None
    local_trusted_valuation_records_count: int
    local_trusted_valuations_applied_count: int
    swap_rows_total: int
    swap_rows_already_valued_count: int
    rows_requiring_valuation_before_count: int
    rows_requiring_valuation_after_count: int
    rows_requiring_valuation_after: tuple[SolanaValuationRecord, ...]
    applied_valuation_records: tuple[SolanaValuationRecord, ...]


@dataclass(frozen=True, slots=True)
class SingleWalletSnapshotAnalysis:
    snapshot_path: str
    summary_path: str
    valuation_path: str | None
    wallet: str
    total_raw_transactions: int
    normalized_transactions_count: int
    unsupported_transactions_count: int
    unsupported_reason_counts: tuple[UnsupportedReasonCount, ...]
    unsupported_transactions: tuple[UnsupportedSnapshotTransaction, ...]
    valuation_summary: SnapshotValuationSummary
    fifo_summary: FifoCoverageSummary


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Measure conservative normalization coverage on one raw Solana wallet snapshot.",
    )
    parser.add_argument(
        "--snapshot-path",
        type=Path,
        default=None,
        help="Optional explicit snapshot path. Defaults to the latest test_wallet snapshot.",
    )
    parser.add_argument(
        "--valuation-path",
        type=Path,
        default=None,
        help=(
            "Optional explicit trusted valuation file. Defaults to a sibling "
            "'*_trusted_valuations.json' file when present."
        ),
    )
    return parser.parse_args(argv)


def find_latest_snapshot_path(snapshot_dir: Path = DEFAULT_SNAPSHOT_DIR) -> Path:
    snapshot_paths = sorted(
        path
        for path in snapshot_dir.glob("wallet_snapshot_*.json")
        if "_analysis_summary" not in path.stem
        and "_trusted_valuations" not in path.stem
        and "_proposed_valuations" not in path.stem
    )
    if not snapshot_paths:
        raise ValueError(f"No wallet snapshots found under {snapshot_dir}")
    return snapshot_paths[-1]


def analyze_snapshot_path(
    snapshot_path: Path,
    *,
    valuation_path: Path | None = None,
) -> SingleWalletSnapshotAnalysis:
    snapshot = load_json_mapping(snapshot_path)
    analysis = analyze_snapshot_mapping(
        snapshot,
        snapshot_path=snapshot_path,
        valuation_path=valuation_path,
    )
    write_analysis_summary(analysis, summary_path=snapshot_path.with_name(f"{snapshot_path.stem}_analysis_summary.json"))
    return analysis


def analyze_snapshot_mapping(
    snapshot: Mapping[str, object],
    *,
    snapshot_path: Path,
    valuation_path: Path | None = None,
) -> SingleWalletSnapshotAnalysis:
    wallet = _require_text(snapshot, "wallet")
    transaction_responses = snapshot.get("transaction_responses")
    if not isinstance(transaction_responses, list):
        raise ValueError("Solana snapshot must contain a list at 'transaction_responses'")

    normalized_transactions: list[NormalizedTransaction] = []
    unsupported_transactions: list[UnsupportedSnapshotTransaction] = []

    for index, payload in enumerate(transaction_responses):
        tx_hash = _extract_tx_hash(payload)
        if not isinstance(payload, Mapping):
            unsupported_transactions.append(
                UnsupportedSnapshotTransaction(
                    index=index,
                    tx_hash=tx_hash,
                    reason="Solana transaction response must be an object",
                )
            )
            continue

        try:
            normalized = normalize_transaction(
                {
                    "chain": "solana",
                    "wallet": wallet,
                    **payload,
                }
            )
        except ValueError as exc:
            unsupported_transactions.append(
                UnsupportedSnapshotTransaction(
                    index=index,
                    tx_hash=tx_hash,
                    reason=str(exc),
                )
            )
            continue

        normalized_transactions.append(normalized)

    resolved_valuation_path = valuation_path or find_local_trusted_valuation_path(snapshot_path)
    trusted_valuation_records = (
        load_trusted_valuation_records(resolved_valuation_path)
        if resolved_valuation_path is not None
        else ()
    )
    readiness_before = summarize_valuation_readiness(normalized_transactions)
    valuation_application_result = apply_trusted_usd_values(
        normalized_transactions,
        trusted_valuation_records,
    )
    valued_transactions = valuation_application_result.transactions
    readiness_after = summarize_valuation_readiness(valued_transactions)

    unsupported_reason_counts = _group_unsupported_reasons(unsupported_transactions)
    valuation_summary = SnapshotValuationSummary(
        valuation_path=(
            _relative_path_text(resolved_valuation_path)
            if resolved_valuation_path is not None
            else None
        ),
        local_trusted_valuation_records_count=len(trusted_valuation_records),
        local_trusted_valuations_applied_count=len(valuation_application_result.applied_records),
        swap_rows_total=readiness_before.swap_transactions,
        swap_rows_already_valued_count=readiness_before.swap_rows_already_valued_count,
        rows_requiring_valuation_before_count=readiness_before.rows_requiring_valuation_count,
        rows_requiring_valuation_after_count=readiness_after.rows_requiring_valuation_count,
        rows_requiring_valuation_after=readiness_after.rows_requiring_valuation,
        applied_valuation_records=valuation_application_result.applied_records,
    )
    fifo_summary = _analyze_fifo_coverage(valued_transactions)

    return SingleWalletSnapshotAnalysis(
        snapshot_path=_relative_path_text(snapshot_path),
        summary_path=_relative_path_text(
            snapshot_path.with_name(f"{snapshot_path.stem}_analysis_summary.json")
        ),
        valuation_path=valuation_summary.valuation_path,
        wallet=wallet,
        total_raw_transactions=len(transaction_responses),
        normalized_transactions_count=len(normalized_transactions),
        unsupported_transactions_count=len(unsupported_transactions),
        unsupported_reason_counts=unsupported_reason_counts,
        unsupported_transactions=tuple(unsupported_transactions),
        valuation_summary=valuation_summary,
        fifo_summary=fifo_summary,
    )


def write_analysis_summary(
    analysis: SingleWalletSnapshotAnalysis,
    *,
    summary_path: Path,
) -> Path:
    summary_path.write_text(
        json.dumps(_jsonify(asdict(analysis)), indent=2),
        encoding="utf-8",
    )
    return summary_path


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        snapshot_path = args.snapshot_path or find_latest_snapshot_path()
        analysis = analyze_snapshot_path(
            snapshot_path,
            valuation_path=args.valuation_path,
        )
    except (ValueError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(f"Snapshot analyzed: {analysis.snapshot_path}")
    print(f"Summary path: {analysis.summary_path}")
    print(f"Wallet: {analysis.wallet}")
    print(f"Total raw transactions: {analysis.total_raw_transactions}")
    print(f"Normalized transactions: {analysis.normalized_transactions_count}")
    print(f"Unsupported transactions: {analysis.unsupported_transactions_count}")
    print("Unsupported reasons:")
    if not analysis.unsupported_reason_counts:
        print("  none")
    else:
        for item in analysis.unsupported_reason_counts[:5]:
            print(f"  {item.count} x {item.reason}")

    valuation_summary = analysis.valuation_summary
    print(f"Valuation file: {analysis.valuation_path or 'none'}")
    print(f"Swap rows total: {valuation_summary.swap_rows_total}")
    print(
        "Swap rows already valued: "
        f"{valuation_summary.swap_rows_already_valued_count}"
    )
    print(
        "Rows requiring valuation before local file: "
        f"{valuation_summary.rows_requiring_valuation_before_count}"
    )
    print(
        "Local trusted valuation records applied: "
        f"{valuation_summary.local_trusted_valuations_applied_count}"
    )
    print(
        "Rows still requiring valuation after local file: "
        f"{valuation_summary.rows_requiring_valuation_after_count}"
    )

    fifo_summary = analysis.fifo_summary
    print(f"FIFO status: {fifo_summary.status}")
    print(f"FIFO candidate transactions: {fifo_summary.fifo_candidate_transactions_count}")
    print(
        "Skipped for missing valuation: "
        f"{fifo_summary.skipped_missing_valuation_count}"
    )
    if fifo_summary.skipped_missing_valuation_count > 0:
        print("FIFO realized PnL: not meaningful without trusted usd_value on skipped swap rows")
    else:
        print(f"FIFO realized PnL: {fifo_summary.realized_pnl_usd}")
    print(f"FIFO trade matches: {fifo_summary.trade_matches_count}")
    print(f"FIFO remaining positions: {fifo_summary.remaining_positions_count}")
    print(f"Recorded fees: {fifo_summary.recorded_fees_count}")
    if fifo_summary.error is not None:
        print(f"FIFO error: {fifo_summary.error}")
    return 0


def _analyze_fifo_coverage(
    normalized_transactions: Sequence[NormalizedTransaction],
) -> FifoCoverageSummary:
    skipped_missing_valuation = [
        transaction
        for transaction in normalized_transactions
        if transaction.event_type == EventType.SWAP and transaction.usd_value is None
    ]
    fifo_candidates = [
        transaction
        for transaction in normalized_transactions
        if transaction not in skipped_missing_valuation
    ]

    if skipped_missing_valuation:
        fifo_result = None
        fifo_error = None
        if fifo_candidates:
            try:
                fifo_result = run_fifo_pipeline(fifo_candidates)
            except ValueError as exc:
                fifo_error = str(exc)
        return FifoCoverageSummary(
            status="not_meaningful_missing_valuation",
            fifo_candidate_transactions_count=len(fifo_candidates),
            skipped_missing_valuation_count=len(skipped_missing_valuation),
            skipped_missing_valuation_tx_hashes=tuple(
                transaction.tx_hash for transaction in skipped_missing_valuation
            ),
            realized_pnl_usd=None,
            trade_matches_count=(
                len(fifo_result.fifo_result.trade_matches) if fifo_result is not None else 0
            ),
            remaining_positions_count=(
                len(fifo_result.remaining_positions) if fifo_result is not None else 0
            ),
            recorded_fees_count=(
                len(fifo_result.fifo_result.recorded_fees) if fifo_result is not None else 0
            ),
            error=fifo_error,
            meaningful=False,
        )

    if not fifo_candidates:
        return FifoCoverageSummary(
            status="not_applicable_no_fifo_rows",
            fifo_candidate_transactions_count=0,
            skipped_missing_valuation_count=0,
            skipped_missing_valuation_tx_hashes=(),
            realized_pnl_usd=Decimal("0"),
            trade_matches_count=0,
            remaining_positions_count=0,
            recorded_fees_count=0,
            error=None,
            meaningful=False,
        )

    try:
        fifo_result = run_fifo_pipeline(fifo_candidates)
    except ValueError as exc:
        return FifoCoverageSummary(
            status="error",
            fifo_candidate_transactions_count=len(fifo_candidates),
            skipped_missing_valuation_count=0,
            skipped_missing_valuation_tx_hashes=(),
            realized_pnl_usd=None,
            trade_matches_count=0,
            remaining_positions_count=0,
            recorded_fees_count=0,
            error=str(exc),
            meaningful=False,
        )

    return FifoCoverageSummary(
        status="computed",
        fifo_candidate_transactions_count=len(fifo_candidates),
        skipped_missing_valuation_count=0,
        skipped_missing_valuation_tx_hashes=(),
        realized_pnl_usd=fifo_result.realized_pnl_usd,
        trade_matches_count=len(fifo_result.fifo_result.trade_matches),
        remaining_positions_count=len(fifo_result.remaining_positions),
        recorded_fees_count=len(fifo_result.fifo_result.recorded_fees),
        error=None,
        meaningful=True,
    )


def _group_unsupported_reasons(
    unsupported_transactions: Sequence[UnsupportedSnapshotTransaction],
) -> tuple[UnsupportedReasonCount, ...]:
    reason_counts = Counter(item.reason for item in unsupported_transactions)
    return tuple(
        UnsupportedReasonCount(reason=reason, count=count)
        for reason, count in sorted(
            reason_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )
    )


def _extract_tx_hash(payload: object) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    result = payload.get("result")
    if not isinstance(result, Mapping):
        return None
    transaction = result.get("transaction")
    if not isinstance(transaction, Mapping):
        return None
    signatures = transaction.get("signatures")
    if not isinstance(signatures, list) or not signatures:
        return None
    first_signature = signatures[0]
    if not isinstance(first_signature, str):
        return None
    trimmed_signature = first_signature.strip()
    return trimmed_signature or None


def _relative_path_text(path: Path) -> str:
    try:
        return path.relative_to(ROOT).as_posix()
    except ValueError:
        return str(path)


def _require_text(mapping: Mapping[str, object], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Solana snapshot is missing a non-empty '{key}' field")
    return value.strip()


def _jsonify(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, tuple):
        return [_jsonify(item) for item in value]
    if isinstance(value, list):
        return [_jsonify(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _jsonify(item) for key, item in value.items()}
    return value


if __name__ == "__main__":
    raise SystemExit(main())
