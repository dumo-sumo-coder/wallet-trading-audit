"""Analyze one raw Solana wallet snapshot for normalization and FIFO coverage."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ingestion.solana_review import load_json_mapping  # noqa: E402
from analytics.trade_diagnostics import (  # noqa: E402
    TradeDiagnosticReport,
    TradeDiagnosticSummary,
    build_trade_diagnostic_report,
)
from analytics.trade_filter_simulation import (  # noqa: E402
    TradeFilterSimulationReport,
    TradeFilterSimulationSummary,
    build_default_trade_filter_simulation_report,
)
from analytics.rules_report import (  # noqa: E402
    WalletRulesReport,
    build_wallet_rules_report,
    render_wallet_rules_markdown,
)
from analytics.wallet_behavior import (  # noqa: E402
    WalletBehaviorReport,
    WalletBehaviorSummary,
    build_wallet_behavior_report,
)
from normalize.schema import EventType, NormalizedTransaction  # noqa: E402
from normalize.transactions import SOLANA_WRAPPED_SOL_MINT, normalize_transaction  # noqa: E402
from pnl.fifo_engine import InsufficientInventoryError  # noqa: E402
from pnl.pipeline import FifoPipelineResult, run_fifo_pipeline  # noqa: E402
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
    fifo_executed_transactions_count: int
    skipped_missing_valuation_count: int
    skipped_missing_valuation_tx_hashes: tuple[str, ...]
    unsupported_fifo_transactions_count: int
    unsupported_fifo_transactions: tuple[UnsupportedSnapshotTransaction, ...]
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
class TradeDiagnosticArtifacts:
    trade_report_json_path: str
    trade_report_csv_path: str
    report_summary: TradeDiagnosticSummary


@dataclass(frozen=True, slots=True)
class BehaviorDiagnosticArtifacts:
    behavior_report_json_path: str
    behavior_report_csv_path: str
    report_summary: WalletBehaviorSummary


@dataclass(frozen=True, slots=True)
class SimulationDiagnosticArtifacts:
    simulation_report_json_path: str
    simulation_report_csv_path: str
    report_summary: TradeFilterSimulationSummary


@dataclass(frozen=True, slots=True)
class RulesDiagnosticArtifacts:
    rules_report_json_path: str
    rules_report_markdown_path: str
    report_summary: WalletRulesReport


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
    trade_diagnostics: TradeDiagnosticArtifacts
    behavior_diagnostics: BehaviorDiagnosticArtifacts
    simulation_diagnostics: SimulationDiagnosticArtifacts
    rules_diagnostics: RulesDiagnosticArtifacts


@dataclass(frozen=True, slots=True)
class _FifoCoverageComputation:
    summary: FifoCoverageSummary
    pipeline_result: FifoPipelineResult | None


@dataclass(frozen=True, slots=True)
class _SnapshotAnalysisComputation:
    analysis: SingleWalletSnapshotAnalysis
    trade_report: TradeDiagnosticReport
    behavior_report: WalletBehaviorReport
    simulation_report: TradeFilterSimulationReport
    rules_report: WalletRulesReport


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
        "--fetch-metadata-path",
        type=Path,
        default=None,
        help=(
            "Optional explicit multi-page fetch metadata path. When provided, "
            "all referenced page snapshots are combined before analysis."
        ),
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


def find_latest_fetch_metadata_path(snapshot_dir: Path = DEFAULT_SNAPSHOT_DIR) -> Path | None:
    metadata_paths = sorted(snapshot_dir.glob("wallet_fetch_metadata_*.json"))
    if not metadata_paths:
        return None
    return metadata_paths[-1]


def analyze_fetch_metadata_path(
    fetch_metadata_path: Path,
    *,
    valuation_path: Path | None = None,
) -> SingleWalletSnapshotAnalysis:
    fetch_metadata = load_json_mapping(fetch_metadata_path)
    combined_snapshot = _combine_snapshot_pages_from_metadata(
        fetch_metadata,
        fetch_metadata_path=fetch_metadata_path,
    )
    computation = _analyze_snapshot_mapping_with_report(
        combined_snapshot,
        snapshot_path=fetch_metadata_path,
        valuation_path=valuation_path,
    )
    analysis = computation.analysis
    write_analysis_summary(
        analysis,
        summary_path=fetch_metadata_path.with_name(f"{fetch_metadata_path.stem}_analysis_summary.json"),
    )
    write_trade_diagnostic_report(
        computation.trade_report,
        json_path=fetch_metadata_path.with_name(f"{fetch_metadata_path.stem}_trade_report.json"),
        csv_path=fetch_metadata_path.with_name(f"{fetch_metadata_path.stem}_trade_report.csv"),
    )
    write_behavior_diagnostic_report(
        computation.behavior_report,
        json_path=fetch_metadata_path.with_name(f"{fetch_metadata_path.stem}_behavior_report.json"),
        csv_path=fetch_metadata_path.with_name(f"{fetch_metadata_path.stem}_behavior_report.csv"),
    )
    write_simulation_diagnostic_report(
        computation.simulation_report,
        json_path=fetch_metadata_path.with_name(f"{fetch_metadata_path.stem}_simulation_report.json"),
        csv_path=fetch_metadata_path.with_name(f"{fetch_metadata_path.stem}_simulation_report.csv"),
    )
    write_rules_diagnostic_report(
        computation.rules_report,
        json_path=fetch_metadata_path.with_name(f"{fetch_metadata_path.stem}_rules_report.json"),
        markdown_path=fetch_metadata_path.with_name(f"{fetch_metadata_path.stem}_rules_report.md"),
    )
    return analysis


def analyze_snapshot_path(
    snapshot_path: Path,
    *,
    valuation_path: Path | None = None,
) -> SingleWalletSnapshotAnalysis:
    snapshot = load_json_mapping(snapshot_path)
    computation = _analyze_snapshot_mapping_with_report(
        snapshot,
        snapshot_path=snapshot_path,
        valuation_path=valuation_path,
    )
    analysis = computation.analysis
    write_analysis_summary(analysis, summary_path=snapshot_path.with_name(f"{snapshot_path.stem}_analysis_summary.json"))
    write_trade_diagnostic_report(
        computation.trade_report,
        json_path=snapshot_path.with_name(f"{snapshot_path.stem}_trade_report.json"),
        csv_path=snapshot_path.with_name(f"{snapshot_path.stem}_trade_report.csv"),
    )
    write_behavior_diagnostic_report(
        computation.behavior_report,
        json_path=snapshot_path.with_name(f"{snapshot_path.stem}_behavior_report.json"),
        csv_path=snapshot_path.with_name(f"{snapshot_path.stem}_behavior_report.csv"),
    )
    write_simulation_diagnostic_report(
        computation.simulation_report,
        json_path=snapshot_path.with_name(f"{snapshot_path.stem}_simulation_report.json"),
        csv_path=snapshot_path.with_name(f"{snapshot_path.stem}_simulation_report.csv"),
    )
    write_rules_diagnostic_report(
        computation.rules_report,
        json_path=snapshot_path.with_name(f"{snapshot_path.stem}_rules_report.json"),
        markdown_path=snapshot_path.with_name(f"{snapshot_path.stem}_rules_report.md"),
    )
    return analysis


def _combine_snapshot_pages_from_metadata(
    fetch_metadata: Mapping[str, object],
    *,
    fetch_metadata_path: Path,
) -> dict[str, object]:
    wallet = _require_text(fetch_metadata, "wallet")
    page_snapshot_paths = fetch_metadata.get("page_snapshot_paths")
    if not isinstance(page_snapshot_paths, list) or not page_snapshot_paths:
        raise ValueError("Fetch metadata must contain a non-empty 'page_snapshot_paths' list")

    deduplicated_payloads: list[tuple[datetime, str, int, int, Mapping[str, object]]] = []
    seen_tx_hashes: set[str] = set()

    for page_index, page_snapshot_path_text in enumerate(page_snapshot_paths):
        if not isinstance(page_snapshot_path_text, str) or not page_snapshot_path_text.strip():
            raise ValueError("Fetch metadata page_snapshot_paths entries must be non-empty strings")
        page_snapshot_path = _resolve_metadata_relative_path(
            fetch_metadata_path=fetch_metadata_path,
            relative_or_absolute_path=page_snapshot_path_text,
        )
        page_snapshot = load_json_mapping(page_snapshot_path)
        if _require_text(page_snapshot, "wallet") != wallet:
            raise ValueError("All snapshot pages in fetch metadata must belong to the same wallet")
        transaction_responses = page_snapshot.get("transaction_responses")
        if not isinstance(transaction_responses, list):
            raise ValueError("Each snapshot page must contain a list at 'transaction_responses'")

        for item_index, payload in enumerate(transaction_responses):
            if not isinstance(payload, Mapping):
                raise ValueError("Each Solana transaction response must be an object")
            tx_hash = _extract_tx_hash(payload)
            if tx_hash is not None and tx_hash in seen_tx_hashes:
                continue
            if tx_hash is not None:
                seen_tx_hashes.add(tx_hash)

            deduplicated_payloads.append(
                (
                    _extract_sortable_block_time(payload),
                    tx_hash or "",
                    page_index,
                    item_index,
                    payload,
                )
            )

    deduplicated_payloads.sort(
        key=lambda item: (item[0], item[1], item[2], item[3])
    )
    combined_responses = [payload for _, _, _, _, payload in deduplicated_payloads]
    tested_at = fetch_metadata.get("tested_at")
    if not isinstance(tested_at, str) or not tested_at.strip():
        tested_at = None

    return {
        "wallet": wallet,
        "fetched_at_utc": tested_at,
        "capture": {
            "combined_pages": True,
            "page_count": len(page_snapshot_paths),
            "deduplicated_by_tx_hash": True,
            "ordering": "chronological_ascending",
        },
        "transaction_responses": combined_responses,
    }


def analyze_snapshot_mapping(
    snapshot: Mapping[str, object],
    *,
    snapshot_path: Path,
    valuation_path: Path | None = None,
) -> SingleWalletSnapshotAnalysis:
    return _analyze_snapshot_mapping_with_report(
        snapshot,
        snapshot_path=snapshot_path,
        valuation_path=valuation_path,
    ).analysis


def _analyze_snapshot_mapping_with_report(
    snapshot: Mapping[str, object],
    *,
    snapshot_path: Path,
    valuation_path: Path | None = None,
) -> _SnapshotAnalysisComputation:
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
    fifo_computation = _analyze_fifo_coverage(valued_transactions)
    trade_report = (
        build_trade_diagnostic_report(fifo_computation.pipeline_result.fifo_result)
        if fifo_computation.pipeline_result is not None
        else TradeDiagnosticReport(
            matched_trades=(),
            summary=TradeDiagnosticSummary(
                total_matched_trades=0,
                winners_count=0,
                losers_count=0,
                avg_winner_usd=None,
                avg_loser_usd=None,
                largest_win_usd=None,
                largest_loss_usd=None,
                pnl_by_token=(),
            ),
        )
    )
    behavior_report = build_wallet_behavior_report(trade_report.matched_trades)
    simulation_report = build_default_trade_filter_simulation_report(
        trade_report.matched_trades
    )
    rules_report = build_wallet_rules_report(
        behavior_report.summary,
        simulation_report.summary,
    )

    return _SnapshotAnalysisComputation(
        analysis=SingleWalletSnapshotAnalysis(
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
            fifo_summary=fifo_computation.summary,
            trade_diagnostics=TradeDiagnosticArtifacts(
                trade_report_json_path=_relative_path_text(
                    snapshot_path.with_name(f"{snapshot_path.stem}_trade_report.json")
                ),
                trade_report_csv_path=_relative_path_text(
                    snapshot_path.with_name(f"{snapshot_path.stem}_trade_report.csv")
                ),
                report_summary=trade_report.summary,
            ),
            behavior_diagnostics=BehaviorDiagnosticArtifacts(
                behavior_report_json_path=_relative_path_text(
                    snapshot_path.with_name(f"{snapshot_path.stem}_behavior_report.json")
                ),
                behavior_report_csv_path=_relative_path_text(
                    snapshot_path.with_name(f"{snapshot_path.stem}_behavior_report.csv")
                ),
                report_summary=behavior_report.summary,
            ),
            simulation_diagnostics=SimulationDiagnosticArtifacts(
                simulation_report_json_path=_relative_path_text(
                    snapshot_path.with_name(f"{snapshot_path.stem}_simulation_report.json")
                ),
                simulation_report_csv_path=_relative_path_text(
                    snapshot_path.with_name(f"{snapshot_path.stem}_simulation_report.csv")
                ),
                report_summary=simulation_report.summary,
            ),
            rules_diagnostics=RulesDiagnosticArtifacts(
                rules_report_json_path=_relative_path_text(
                    snapshot_path.with_name(f"{snapshot_path.stem}_rules_report.json")
                ),
                rules_report_markdown_path=_relative_path_text(
                    snapshot_path.with_name(f"{snapshot_path.stem}_rules_report.md")
                ),
                report_summary=rules_report,
            ),
        ),
        trade_report=trade_report,
        behavior_report=behavior_report,
        simulation_report=simulation_report,
        rules_report=rules_report,
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


def write_trade_diagnostic_report(
    report: TradeDiagnosticReport,
    *,
    json_path: Path,
    csv_path: Path,
) -> tuple[Path, Path]:
    json_path.write_text(
        json.dumps(_jsonify(asdict(report)), indent=2),
        encoding="utf-8",
    )

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                "token_address",
                "opening_tx_hash",
                "closing_tx_hash",
                "open_timestamp",
                "close_timestamp",
                "holding_duration_seconds",
                "quantity_matched",
                "cost_basis_usd",
                "proceeds_usd",
                "realized_pnl_usd",
                "opening_fee_native",
                "opening_fee_usd",
                "closing_fee_native",
                "closing_fee_usd",
            ),
        )
        writer.writeheader()
        for matched_trade in report.matched_trades:
            writer.writerow(
                {
                    "token_address": matched_trade.token_address,
                    "opening_tx_hash": matched_trade.opening_tx_hash,
                    "closing_tx_hash": matched_trade.closing_tx_hash,
                    "open_timestamp": matched_trade.open_timestamp.isoformat(),
                    "close_timestamp": matched_trade.close_timestamp.isoformat(),
                    "holding_duration_seconds": matched_trade.holding_duration_seconds,
                    "quantity_matched": str(matched_trade.quantity_matched),
                    "cost_basis_usd": _csv_value(matched_trade.cost_basis_usd),
                    "proceeds_usd": _csv_value(matched_trade.proceeds_usd),
                    "realized_pnl_usd": _csv_value(matched_trade.realized_pnl_usd),
                    "opening_fee_native": _csv_value(matched_trade.opening_fee_native),
                    "opening_fee_usd": _csv_value(matched_trade.opening_fee_usd),
                    "closing_fee_native": _csv_value(matched_trade.closing_fee_native),
                    "closing_fee_usd": _csv_value(matched_trade.closing_fee_usd),
                }
            )

    return json_path, csv_path


def write_behavior_diagnostic_report(
    report: WalletBehaviorReport,
    *,
    json_path: Path,
    csv_path: Path,
) -> tuple[Path, Path]:
    json_path.write_text(
        json.dumps(_jsonify(asdict(report)), indent=2),
        encoding="utf-8",
    )

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                "token_address",
                "opening_tx_hash",
                "closing_tx_hash",
                "open_timestamp",
                "close_timestamp",
                "close_day",
                "holding_duration_seconds",
                "holding_time_bucket",
                "quantity_matched",
                "cost_basis_usd",
                "cost_basis_bucket",
                "proceeds_usd",
                "realized_pnl_usd",
                "outcome",
                "prior_trade_outcome",
            ),
        )
        writer.writeheader()
        for trade_row in report.trade_rows:
            writer.writerow(
                {
                    "token_address": trade_row.token_address,
                    "opening_tx_hash": trade_row.opening_tx_hash,
                    "closing_tx_hash": trade_row.closing_tx_hash,
                    "open_timestamp": trade_row.open_timestamp.isoformat(),
                    "close_timestamp": trade_row.close_timestamp.isoformat(),
                    "close_day": trade_row.close_day,
                    "holding_duration_seconds": trade_row.holding_duration_seconds,
                    "holding_time_bucket": trade_row.holding_time_bucket,
                    "quantity_matched": str(trade_row.quantity_matched),
                    "cost_basis_usd": _csv_value(trade_row.cost_basis_usd),
                    "cost_basis_bucket": trade_row.cost_basis_bucket,
                    "proceeds_usd": _csv_value(trade_row.proceeds_usd),
                    "realized_pnl_usd": _csv_value(trade_row.realized_pnl_usd),
                    "outcome": trade_row.outcome,
                    "prior_trade_outcome": trade_row.prior_trade_outcome or "",
                }
            )

    return json_path, csv_path


def write_simulation_diagnostic_report(
    report: TradeFilterSimulationReport,
    *,
    json_path: Path,
    csv_path: Path,
) -> tuple[Path, Path]:
    json_path.write_text(
        json.dumps(_jsonify(asdict(report)), indent=2),
        encoding="utf-8",
    )

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                "scenario_name",
                "rule_type",
                "threshold_value",
                "original_trade_count",
                "remaining_trade_count",
                "filtered_out_trade_count",
                "original_realized_pnl_usd",
                "filtered_out_realized_pnl_usd",
                "new_realized_pnl_usd",
                "delta_vs_original_pnl_usd",
                "excluded_tokens",
            ),
        )
        writer.writeheader()
        for scenario_result in report.summary.scenario_results:
            writer.writerow(
                {
                    "scenario_name": scenario_result.scenario_name,
                    "rule_type": scenario_result.rule_type,
                    "threshold_value": _csv_value(scenario_result.threshold_value),
                    "original_trade_count": scenario_result.original_trade_count,
                    "remaining_trade_count": scenario_result.remaining_trade_count,
                    "filtered_out_trade_count": scenario_result.filtered_out_trade_count,
                    "original_realized_pnl_usd": str(
                        scenario_result.original_realized_pnl_usd
                    ),
                    "filtered_out_realized_pnl_usd": str(
                        scenario_result.filtered_out_realized_pnl_usd
                    ),
                    "new_realized_pnl_usd": str(scenario_result.new_realized_pnl_usd),
                    "delta_vs_original_pnl_usd": str(
                        scenario_result.delta_vs_original_pnl_usd
                    ),
                    "excluded_tokens": ",".join(scenario_result.excluded_tokens),
                }
            )

    return json_path, csv_path


def write_rules_diagnostic_report(
    report: WalletRulesReport,
    *,
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.write_text(
        json.dumps(_jsonify(asdict(report)), indent=2),
        encoding="utf-8",
    )
    markdown_path.write_text(
        render_wallet_rules_markdown(report),
        encoding="utf-8",
    )
    return json_path, markdown_path


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.fetch_metadata_path is not None:
            analysis = analyze_fetch_metadata_path(
                args.fetch_metadata_path,
                valuation_path=args.valuation_path,
            )
        else:
            latest_fetch_metadata_path = find_latest_fetch_metadata_path()
            if args.snapshot_path is None and latest_fetch_metadata_path is not None:
                analysis = analyze_fetch_metadata_path(
                    latest_fetch_metadata_path,
                    valuation_path=args.valuation_path,
                )
            else:
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
    print(f"FIFO executed transactions: {fifo_summary.fifo_executed_transactions_count}")
    print(
        "Skipped for missing valuation: "
        f"{fifo_summary.skipped_missing_valuation_count}"
    )
    print(
        "Skipped unsupported FIFO rows: "
        f"{fifo_summary.unsupported_fifo_transactions_count}"
    )
    if fifo_summary.unsupported_fifo_transactions:
        for item in fifo_summary.unsupported_fifo_transactions[:5]:
            print(f"  unsupported FIFO row {item.tx_hash}: {item.reason}")
    if fifo_summary.skipped_missing_valuation_count > 0:
        print("FIFO realized PnL: not meaningful without trusted usd_value on skipped swap rows")
    else:
        print(f"FIFO realized PnL: {fifo_summary.realized_pnl_usd}")
    print(f"FIFO trade matches: {fifo_summary.trade_matches_count}")
    print(f"FIFO remaining positions: {fifo_summary.remaining_positions_count}")
    print(f"Recorded fees: {fifo_summary.recorded_fees_count}")
    if fifo_summary.error is not None:
        print(f"FIFO error: {fifo_summary.error}")
    trade_diagnostics = analysis.trade_diagnostics
    print(f"Trade report JSON: {trade_diagnostics.trade_report_json_path}")
    print(f"Trade report CSV: {trade_diagnostics.trade_report_csv_path}")
    print(f"Matched trades in report: {trade_diagnostics.report_summary.total_matched_trades}")
    print(
        "Winners vs losers: "
        f"{trade_diagnostics.report_summary.winners_count} / "
        f"{trade_diagnostics.report_summary.losers_count}"
    )
    print(f"Largest win: {trade_diagnostics.report_summary.largest_win_usd}")
    print(f"Largest loss: {trade_diagnostics.report_summary.largest_loss_usd}")
    if trade_diagnostics.report_summary.pnl_by_token:
        print("PnL by token:")
        for item in trade_diagnostics.report_summary.pnl_by_token[:5]:
            print(
                f"  {item.token_address}: "
                f"{item.realized_pnl_usd} across {item.matched_trades} matched trades"
            )
    behavior_diagnostics = analysis.behavior_diagnostics
    print(f"Behavior report JSON: {behavior_diagnostics.behavior_report_json_path}")
    print(f"Behavior report CSV: {behavior_diagnostics.behavior_report_csv_path}")
    print(
        "Longest losing streak: "
        f"{behavior_diagnostics.report_summary.streak_diagnostics.longest_losing_streak}"
    )
    print(
        "Average cost basis: "
        f"{behavior_diagnostics.report_summary.notional_diagnostics.average_cost_basis_usd}"
    )
    print("PnL by holding-time bucket:")
    for bucket in behavior_diagnostics.report_summary.holding_time_buckets:
        print(
            f"  {bucket.bucket}: {bucket.total_pnl_usd} "
            f"across {bucket.trade_count} trades"
        )
    top_loss_concentration = (
        behavior_diagnostics.report_summary.concentration_diagnostics
        .top_5_losing_tokens_contribution_pct
    )
    print(f"Top loss concentration: {top_loss_concentration}")
    simulation_diagnostics = analysis.simulation_diagnostics
    print(f"Simulation report JSON: {simulation_diagnostics.simulation_report_json_path}")
    print(f"Simulation report CSV: {simulation_diagnostics.simulation_report_csv_path}")
    print(
        "Best improvement scenario: "
        f"{simulation_diagnostics.report_summary.best_improvement_scenario_name}"
    )
    print(
        "Best improvement delta: "
        f"{simulation_diagnostics.report_summary.best_improvement_delta_usd}"
    )
    print(
        "Best improvement new realized PnL: "
        f"{simulation_diagnostics.report_summary.best_improvement_new_realized_pnl_usd}"
    )
    rules_diagnostics = analysis.rules_diagnostics
    print(f"Rules report JSON: {rules_diagnostics.rules_report_json_path}")
    print(f"Rules report Markdown: {rules_diagnostics.rules_report_markdown_path}")
    print("Top leak patterns:")
    if not rules_diagnostics.report_summary.top_leak_patterns:
        print("  none")
    else:
        for item in rules_diagnostics.report_summary.top_leak_patterns:
            print(
                f"  {item.rank}. {item.title} "
                f"({item.category}, estimated drag {item.estimated_pnl_drag_usd})"
            )
    print("Top candidate rules:")
    if not rules_diagnostics.report_summary.top_candidate_rules:
        print("  none")
    else:
        for item in rules_diagnostics.report_summary.top_candidate_rules:
            print(
                f"  {item.rank}. {item.title} "
                f"({item.category}, +{item.estimated_pnl_improvement_usd})"
            )
    if rules_diagnostics.report_summary.next_test_rule_categories:
        print("Explore next:")
        for item in rules_diagnostics.report_summary.next_test_rule_categories:
            print(f"  {item}")
    return 0


def _analyze_fifo_coverage(
    normalized_transactions: Sequence[NormalizedTransaction],
) -> _FifoCoverageComputation:
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
        fifo_result, fifo_error, unsupported_fifo_transactions, executed_count = (
            _run_fifo_on_supported_subset(fifo_candidates)
        )
        return _FifoCoverageComputation(
            summary=FifoCoverageSummary(
                status="not_meaningful_missing_valuation",
                fifo_candidate_transactions_count=len(fifo_candidates),
                fifo_executed_transactions_count=executed_count,
                skipped_missing_valuation_count=len(skipped_missing_valuation),
                skipped_missing_valuation_tx_hashes=tuple(
                    transaction.tx_hash for transaction in skipped_missing_valuation
                ),
                unsupported_fifo_transactions_count=len(unsupported_fifo_transactions),
                unsupported_fifo_transactions=unsupported_fifo_transactions,
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
            ),
            pipeline_result=fifo_result,
        )

    if not fifo_candidates:
        return _FifoCoverageComputation(
            summary=FifoCoverageSummary(
                status="not_applicable_no_fifo_rows",
                fifo_candidate_transactions_count=0,
                fifo_executed_transactions_count=0,
                skipped_missing_valuation_count=0,
                skipped_missing_valuation_tx_hashes=(),
                unsupported_fifo_transactions_count=0,
                unsupported_fifo_transactions=(),
                realized_pnl_usd=Decimal("0"),
                trade_matches_count=0,
                remaining_positions_count=0,
                recorded_fees_count=0,
                error=None,
                meaningful=False,
            ),
            pipeline_result=None,
        )

    fifo_result, fifo_error, unsupported_fifo_transactions, executed_count = (
        _run_fifo_on_supported_subset(fifo_candidates)
    )
    if fifo_error is not None:
        return _FifoCoverageComputation(
            summary=FifoCoverageSummary(
                status="error",
                fifo_candidate_transactions_count=len(fifo_candidates),
                fifo_executed_transactions_count=executed_count,
                skipped_missing_valuation_count=0,
                skipped_missing_valuation_tx_hashes=(),
                unsupported_fifo_transactions_count=len(unsupported_fifo_transactions),
                unsupported_fifo_transactions=unsupported_fifo_transactions,
                realized_pnl_usd=None,
                trade_matches_count=0,
                remaining_positions_count=0,
                recorded_fees_count=0,
                error=fifo_error,
                meaningful=False,
            ),
            pipeline_result=None,
        )

    status = "computed_supported_subset" if unsupported_fifo_transactions else "computed"
    return _FifoCoverageComputation(
        summary=FifoCoverageSummary(
            status=status,
            fifo_candidate_transactions_count=len(fifo_candidates),
            fifo_executed_transactions_count=executed_count,
            skipped_missing_valuation_count=0,
            skipped_missing_valuation_tx_hashes=(),
            unsupported_fifo_transactions_count=len(unsupported_fifo_transactions),
            unsupported_fifo_transactions=unsupported_fifo_transactions,
            realized_pnl_usd=fifo_result.realized_pnl_usd,
            trade_matches_count=len(fifo_result.fifo_result.trade_matches),
            remaining_positions_count=len(fifo_result.remaining_positions),
            recorded_fees_count=len(fifo_result.fifo_result.recorded_fees),
            error=None,
            meaningful=True,
        ),
        pipeline_result=fifo_result,
    )


def _run_fifo_on_supported_subset(
    fifo_candidates: Sequence[NormalizedTransaction],
) -> tuple[object | None, str | None, tuple[UnsupportedSnapshotTransaction, ...], int]:
    working_transactions = list(fifo_candidates)
    unsupported_fifo_transactions: list[UnsupportedSnapshotTransaction] = []

    while True:
        try:
            fifo_result = run_fifo_pipeline(working_transactions)
        except InsufficientInventoryError as exc:
            offending_transaction = next(
                (
                    transaction
                    for transaction in working_transactions
                    if transaction.tx_hash == exc.tx_hash
                ),
                None,
            )
            if (
                offending_transaction is None
                or not _is_wrapped_sol_quote_swap(offending_transaction)
            ):
                return (
                    None,
                    str(exc),
                    tuple(unsupported_fifo_transactions),
                    len(working_transactions),
                )

            unsupported_fifo_transactions.append(
                UnsupportedSnapshotTransaction(
                    index=-1,
                    tx_hash=exc.tx_hash,
                    reason=(
                        "Unsupported FIFO subset case: wrapped-SOL token disposal has "
                        "no opening inventory in the current transaction set."
                    ),
                )
            )
            working_transactions = [
                transaction
                for transaction in working_transactions
                if transaction.tx_hash != exc.tx_hash
            ]
            continue
        except ValueError as exc:
            return (
                None,
                str(exc),
                tuple(unsupported_fifo_transactions),
                len(working_transactions),
            )

        return (
            fifo_result,
            None,
            tuple(unsupported_fifo_transactions),
            len(working_transactions),
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


def _is_wrapped_sol_quote_swap(transaction: NormalizedTransaction) -> bool:
    return transaction.event_type == EventType.SWAP and (
        (
            transaction.token_in_address == SOLANA_WRAPPED_SOL_MINT
            and transaction.token_out_address is not None
            and transaction.token_out_address != SOLANA_WRAPPED_SOL_MINT
        )
        or (
            transaction.token_out_address == SOLANA_WRAPPED_SOL_MINT
            and transaction.token_in_address is not None
            and transaction.token_in_address != SOLANA_WRAPPED_SOL_MINT
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


def _extract_sortable_block_time(payload: Mapping[str, object]) -> datetime:
    result = payload.get("result")
    if not isinstance(result, Mapping):
        raise ValueError("Solana transaction response is missing result")
    block_time = result.get("blockTime")
    if not isinstance(block_time, (int, float)):
        raise ValueError("Solana transaction response is missing a numeric blockTime")
    return datetime.fromtimestamp(block_time, tz=timezone.utc)


def _resolve_metadata_relative_path(
    *,
    fetch_metadata_path: Path,
    relative_or_absolute_path: str,
) -> Path:
    candidate = Path(relative_or_absolute_path)
    if candidate.is_absolute():
        return candidate
    return ROOT / candidate


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


def _csv_value(value: object | None) -> str:
    if value is None:
        return ""
    return str(value)


if __name__ == "__main__":
    raise SystemExit(main())
