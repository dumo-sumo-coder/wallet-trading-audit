"""Analyze many manifest wallets and aggregate portfolio-level diagnostics."""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SCRIPTS = ROOT / "scripts"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import analyze_single_wallet_snapshot as single_wallet_analysis  # noqa: E402
from analytics.manifest_portfolio import (  # noqa: E402
    ManifestPortfolioReport,
    PortfolioWalletSummary,
    STATUS_EXCLUDED_ANALYSIS_FAILED,
    STATUS_EXCLUDED_MISSING_LOCAL_DATA,
    STATUS_EXCLUDED_NOT_MEANINGFUL,
    STATUS_EXCLUDED_UNSUPPORTED_CHAIN,
    STATUS_INCLUDED_COMPLETE,
    STATUS_INCLUDED_SUPPORTED_SUBSET,
    UnsupportedCasePatternCount,
    build_manifest_portfolio_report,
)
from analytics.portfolio_subset_diagnostics import (  # noqa: E402
    PortfolioSubsetBehaviorReport,
    PortfolioSubsetRulesReport,
    PortfolioSubsetSimulationReport,
    build_portfolio_subset_behavior_report,
    build_portfolio_subset_rules_report,
    build_portfolio_subset_simulation_report,
    prepare_portfolio_subset_wallet_diagnostics,
    render_portfolio_subset_rules_markdown,
)
from analytics.trade_diagnostics import (  # noqa: E402
    MatchedTradeDiagnostic,
    TokenPnlDiagnostic,
    TradeDiagnosticReport,
    summarize_trade_diagnostic_report,
)
from config import get_manual_env_load_instructions  # noqa: E402
from ingestion.manifest import (  # noqa: E402
    DEFAULT_WALLET_MANIFEST_PATH,
    WalletManifestEntry,
    filter_wallet_manifest_entries,
    load_wallet_manifest,
    manifest_entry_wallet_directory,
)
from ingestion.solana_client import SolanaRpcClient  # noqa: E402
from valuation.solana_valuation import (  # noqa: E402
    load_valuation_records,
    merge_valuation_records,
    populate_wrapped_sol_trusted_values,
    write_valuation_records,
)

DEFAULT_OUTPUT_DIR = ROOT / "data" / "reports" / "portfolio"
SUPPORTED_ANALYSIS_CHAINS = frozenset({"solana"})
TIMESTAMP_TOKEN_PATTERN = re.compile(r"(\d{8}T\d{6}Z)")


@dataclass(frozen=True, slots=True)
class ManifestPortfolioRun:
    manifest_path: str
    portfolio_summary_json_path: str
    portfolio_summary_csv_path: str
    portfolio_behavior_json_path: str
    portfolio_behavior_csv_path: str
    portfolio_simulation_json_path: str
    portfolio_simulation_csv_path: str
    portfolio_rules_json_path: str
    portfolio_rules_markdown_path: str
    filters: dict[str, object]
    report: ManifestPortfolioReport


@dataclass(frozen=True, slots=True)
class _AnalysisTarget:
    path: Path
    target_type: str
    timestamp: datetime | None


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the current single-wallet analysis pipeline across manifest wallets "
            "and aggregate the results into portfolio diagnostics."
        ),
    )
    parser.add_argument(
        "--manifest-path",
        default=str(ROOT / DEFAULT_WALLET_MANIFEST_PATH),
        help="Wallet manifest CSV to analyze.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory where the aggregated portfolio summary will be written.",
    )
    parser.add_argument(
        "--chain",
        choices=("solana", "bnb_evm"),
        default=None,
        help="Optional exact chain filter.",
    )
    parser.add_argument(
        "--label-filter",
        default=None,
        help="Optional case-insensitive substring filter on manifest labels.",
    )
    parser.add_argument(
        "--group-filter",
        default=None,
        help="Optional case-insensitive substring filter on manifest groups.",
    )
    parser.add_argument(
        "--wallet",
        action="append",
        default=[],
        help="Optional exact wallet filter. Repeat to include more than one wallet.",
    )
    parser.add_argument(
        "--limit-wallets",
        type=int,
        default=None,
        help="Optional limit on how many filtered wallets to analyze.",
    )
    parser.add_argument(
        "--recent-only",
        action="store_true",
        help=(
            "Analyze only wallets whose local raw-data directory has a resolvable "
            "artifact timestamp, sorted newest-first before limit-wallets is applied."
        ),
    )
    parser.add_argument(
        "--fetch-missing",
        action="store_true",
        help=(
            "When a selected Solana wallet has no local snapshot yet, fetch a small "
            "provider-native snapshot first."
        ),
    )
    parser.add_argument(
        "--solana-limit",
        type=int,
        default=50,
        help="Solana transaction limit to use when --fetch-missing is enabled.",
    )
    parser.add_argument(
        "--solana-max-pages",
        type=int,
        default=1,
        help=(
            "Maximum number of paginated Solana history pages to fetch per wallet "
            "when fetch mode is active."
        ),
    )
    parser.add_argument(
        "--refetch-existing",
        action="store_true",
        help=(
            "Refresh selected Solana wallets even when local snapshots already exist, "
            "so deeper history can replace shallow smoke-test data."
        ),
    )
    return parser.parse_args(argv)


def analyze_wallet_manifest_portfolio(
    manifest_path: Path,
    *,
    repository_root: Path = ROOT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    chain: str | None = None,
    label_filter: str | None = None,
    group_filter: str | None = None,
    wallets: Sequence[str] = (),
    limit_wallets: int | None = None,
    recent_only: bool = False,
    fetch_missing: bool = False,
    solana_limit: int = 50,
    solana_max_pages: int = 1,
    refetch_existing: bool = False,
) -> ManifestPortfolioRun:
    entries = load_wallet_manifest(manifest_path)
    filtered_entries = filter_wallet_manifest_entries(
        entries,
        chain=chain,
        label_filter=label_filter,
        group_filter=group_filter,
        wallets=tuple(wallets),
    )
    selected_entries = _select_entries_for_analysis(
        filtered_entries,
        repository_root=repository_root,
        recent_only=recent_only,
        limit_wallets=limit_wallets,
    )

    wallet_summaries = tuple(
        _analyze_manifest_entry(
            entry,
            repository_root=repository_root,
            fetch_missing=fetch_missing,
            solana_limit=solana_limit,
            solana_max_pages=solana_max_pages,
            refetch_existing=refetch_existing,
        )
        for entry in selected_entries
    )
    report = build_manifest_portfolio_report(wallet_summaries)

    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at = _utc_now()
    timestamp_token = generated_at.strftime("%Y%m%dT%H%M%SZ")
    json_path = output_dir / f"manifest_portfolio_{timestamp_token}.json"
    csv_path = output_dir / f"manifest_portfolio_{timestamp_token}.csv"
    behavior_json_path = output_dir / f"manifest_portfolio_{timestamp_token}_behavior_report.json"
    behavior_csv_path = output_dir / f"manifest_portfolio_{timestamp_token}_behavior_report.csv"
    simulation_json_path = output_dir / f"manifest_portfolio_{timestamp_token}_simulation_report.json"
    simulation_csv_path = output_dir / f"manifest_portfolio_{timestamp_token}_simulation_report.csv"
    rules_json_path = output_dir / f"manifest_portfolio_{timestamp_token}_rules_report.json"
    rules_markdown_path = output_dir / f"manifest_portfolio_{timestamp_token}_rules_report.md"
    _write_manifest_portfolio_report(
        report,
        json_path=json_path,
        csv_path=csv_path,
        manifest_path=manifest_path,
        repository_root=repository_root,
        filters={
            "chain": chain,
            "label_filter": label_filter,
            "group_filter": group_filter,
            "wallets": [wallet for wallet in wallets if wallet],
            "limit_wallets": limit_wallets,
            "recent_only": recent_only,
            "fetch_missing": fetch_missing,
            "solana_limit": solana_limit,
            "solana_max_pages": solana_max_pages,
            "refetch_existing": refetch_existing,
        },
    )
    included_wallet_diagnostics = _load_included_wallet_diagnostics(
        report.wallet_summaries,
        repository_root=repository_root,
    )
    portfolio_behavior_report = build_portfolio_subset_behavior_report(
        included_wallet_diagnostics,
        portfolio_report=report,
    )
    portfolio_simulation_report = build_portfolio_subset_simulation_report(
        included_wallet_diagnostics,
    )
    portfolio_rules_report = build_portfolio_subset_rules_report(
        included_wallet_diagnostics,
        portfolio_behavior_report=portfolio_behavior_report,
        portfolio_simulation_report=portfolio_simulation_report,
    )
    _write_portfolio_subset_behavior_report(
        portfolio_behavior_report,
        json_path=behavior_json_path,
        csv_path=behavior_csv_path,
    )
    _write_portfolio_subset_simulation_report(
        portfolio_simulation_report,
        json_path=simulation_json_path,
        csv_path=simulation_csv_path,
    )
    _write_portfolio_subset_rules_report(
        portfolio_rules_report,
        json_path=rules_json_path,
        markdown_path=rules_markdown_path,
    )

    return ManifestPortfolioRun(
        manifest_path=_relative_path_text(manifest_path, repository_root),
        portfolio_summary_json_path=_relative_path_text(json_path, repository_root),
        portfolio_summary_csv_path=_relative_path_text(csv_path, repository_root),
        portfolio_behavior_json_path=_relative_path_text(behavior_json_path, repository_root),
        portfolio_behavior_csv_path=_relative_path_text(behavior_csv_path, repository_root),
        portfolio_simulation_json_path=_relative_path_text(
            simulation_json_path,
            repository_root,
        ),
        portfolio_simulation_csv_path=_relative_path_text(
            simulation_csv_path,
            repository_root,
        ),
        portfolio_rules_json_path=_relative_path_text(rules_json_path, repository_root),
        portfolio_rules_markdown_path=_relative_path_text(
            rules_markdown_path,
            repository_root,
        ),
        filters={
            "chain": chain,
            "label_filter": label_filter,
            "group_filter": group_filter,
            "wallets": tuple(wallet for wallet in wallets if wallet),
            "limit_wallets": limit_wallets,
            "recent_only": recent_only,
            "fetch_missing": fetch_missing,
            "solana_limit": solana_limit,
            "solana_max_pages": solana_max_pages,
            "refetch_existing": refetch_existing,
        },
        report=report,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        run = analyze_wallet_manifest_portfolio(
            Path(args.manifest_path),
            repository_root=ROOT,
            output_dir=Path(args.output_dir),
            chain=args.chain,
            label_filter=args.label_filter,
            group_filter=args.group_filter,
            wallets=tuple(args.wallet),
            limit_wallets=args.limit_wallets,
            recent_only=args.recent_only,
            fetch_missing=args.fetch_missing,
            solana_limit=args.solana_limit,
            solana_max_pages=args.solana_max_pages,
            refetch_existing=args.refetch_existing,
        )
    except (FileNotFoundError, ValueError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        print(
            "If you need provider-backed Solana fetches, load env vars in your terminal first: "
            f"{get_manual_env_load_instructions()}",
            file=sys.stderr,
        )
        return 1

    print(f"Manifest: {run.manifest_path}")
    print(f"Portfolio summary JSON: {run.portfolio_summary_json_path}")
    print(f"Portfolio summary CSV: {run.portfolio_summary_csv_path}")
    print(f"Portfolio behavior JSON: {run.portfolio_behavior_json_path}")
    print(f"Portfolio behavior CSV: {run.portfolio_behavior_csv_path}")
    print(f"Portfolio simulation JSON: {run.portfolio_simulation_json_path}")
    print(f"Portfolio simulation CSV: {run.portfolio_simulation_csv_path}")
    print(f"Portfolio rules JSON: {run.portfolio_rules_json_path}")
    print(f"Portfolio rules Markdown: {run.portfolio_rules_markdown_path}")
    print(f"Wallets analyzed: {run.report.summary.analyzed_wallet_count}")
    print(f"Wallets included in aggregate: {run.report.summary.included_wallet_count}")
    print(f"Aggregate realized PnL: {run.report.summary.aggregate_realized_pnl_usd}")
    print(f"Aggregate matched trades: {run.report.summary.aggregate_matched_trade_count}")
    print("Best wallets by PnL:")
    if not run.report.summary.best_wallets_by_pnl:
        print("  none")
    else:
        for item in run.report.summary.best_wallets_by_pnl:
            print(f"  {item.label}: {item.realized_pnl_usd} ({item.status})")
    print("Worst wallets by PnL:")
    if not run.report.summary.worst_wallets_by_pnl:
        print("  none")
    else:
        for item in run.report.summary.worst_wallets_by_pnl:
            print(f"  {item.label}: {item.realized_pnl_usd} ({item.status})")
    return 0


def _select_entries_for_analysis(
    entries: Sequence[WalletManifestEntry],
    *,
    repository_root: Path,
    recent_only: bool,
    limit_wallets: int | None,
) -> tuple[WalletManifestEntry, ...]:
    if not recent_only:
        selected_entries = list(entries)
        if limit_wallets is not None:
            selected_entries = selected_entries[:limit_wallets]
        return tuple(selected_entries)

    ranked_entries: list[tuple[datetime, int, WalletManifestEntry]] = []
    for index, entry in enumerate(entries):
        analysis_target = _find_local_analysis_target(entry, repository_root=repository_root)
        if analysis_target is None or analysis_target.timestamp is None:
            continue
        ranked_entries.append((analysis_target.timestamp, index, entry))

    ranked_entries.sort(key=lambda item: (item[0], -item[1]), reverse=True)
    selected_entries = [entry for _, _, entry in ranked_entries]
    if limit_wallets is not None:
        selected_entries = selected_entries[:limit_wallets]
    return tuple(selected_entries)


def _analyze_manifest_entry(
    entry: WalletManifestEntry,
    *,
    repository_root: Path,
    fetch_missing: bool,
    solana_limit: int,
    solana_max_pages: int,
    refetch_existing: bool,
) -> PortfolioWalletSummary:
    if entry.chain not in SUPPORTED_ANALYSIS_CHAINS:
        return PortfolioWalletSummary(
            wallet=entry.wallet,
            label=entry.label,
            group=entry.group,
            chain=entry.chain,
            status=STATUS_EXCLUDED_UNSUPPORTED_CHAIN,
            included_in_aggregate=False,
            message=(
                "Current portfolio analysis only supports Solana manifest entries; "
                f"{entry.chain} is not analyzed yet."
            ),
            source_path=None,
            analysis_summary_path=None,
            realized_pnl_usd=None,
            matched_trade_count=0,
            winners_count=0,
            losers_count=0,
            win_rate=None,
            holding_time_buckets=(),
            token_pnl=(),
            top_losing_tokens=(),
            unsupported_transactions_count=None,
            rows_requiring_valuation_after_count=None,
            unsupported_fifo_transactions_count=None,
            skipped_missing_valuation_count=None,
            unsupported_patterns=(),
        )

    analysis_target = _find_local_analysis_target(entry, repository_root=repository_root)
    if refetch_existing and entry.chain == "solana":
        analysis_target = _fetch_solana_history(
            entry,
            repository_root=repository_root,
            limit=solana_limit,
            max_pages=solana_max_pages,
            fetch_mode="manifest_portfolio_refetch_existing",
        )
    elif analysis_target is None and fetch_missing:
        analysis_target = _fetch_solana_history(
            entry,
            repository_root=repository_root,
            limit=solana_limit,
            max_pages=solana_max_pages,
            fetch_mode="manifest_portfolio_fetch_missing",
        )

    if analysis_target is None:
        return PortfolioWalletSummary(
            wallet=entry.wallet,
            label=entry.label,
            group=entry.group,
            chain=entry.chain,
            status=STATUS_EXCLUDED_MISSING_LOCAL_DATA,
            included_in_aggregate=False,
            message=(
                "No local Solana snapshot or fetch metadata was found for this wallet. "
                "Rerun with --fetch-missing to capture a small snapshot first."
            ),
            source_path=None,
            analysis_summary_path=None,
            realized_pnl_usd=None,
            matched_trade_count=0,
            winners_count=0,
            losers_count=0,
            win_rate=None,
            holding_time_buckets=(),
            token_pnl=(),
            top_losing_tokens=(),
            unsupported_transactions_count=None,
            rows_requiring_valuation_after_count=None,
            unsupported_fifo_transactions_count=None,
            skipped_missing_valuation_count=None,
            unsupported_patterns=(),
        )

    try:
        analysis = _analyze_single_wallet_target(analysis_target)
        analysis = _auto_prepare_and_apply_wrapped_sol_valuations(
            analysis_target,
            analysis,
        )
    except (ValueError, OSError) as exc:
        return PortfolioWalletSummary(
            wallet=entry.wallet,
            label=entry.label,
            group=entry.group,
            chain=entry.chain,
            status=STATUS_EXCLUDED_ANALYSIS_FAILED,
            included_in_aggregate=False,
            message=str(exc),
            source_path=_relative_path_text(analysis_target.path, repository_root),
            analysis_summary_path=None,
            realized_pnl_usd=None,
            matched_trade_count=0,
            winners_count=0,
            losers_count=0,
            win_rate=None,
            holding_time_buckets=(),
            token_pnl=(),
            top_losing_tokens=(),
            unsupported_transactions_count=None,
            rows_requiring_valuation_after_count=None,
            unsupported_fifo_transactions_count=None,
            skipped_missing_valuation_count=None,
            unsupported_patterns=(),
        )

    included_in_aggregate = analysis.fifo_summary.meaningful
    if not included_in_aggregate:
        status = STATUS_EXCLUDED_NOT_MEANINGFUL
        realized_pnl_usd = None
    elif (
        analysis.unsupported_transactions_count > 0
        or analysis.fifo_summary.unsupported_fifo_transactions_count > 0
    ):
        status = STATUS_INCLUDED_SUPPORTED_SUBSET
        realized_pnl_usd = analysis.fifo_summary.realized_pnl_usd
    else:
        status = STATUS_INCLUDED_COMPLETE
        realized_pnl_usd = analysis.fifo_summary.realized_pnl_usd

    matched_trade_count = analysis.trade_diagnostics.report_summary.total_matched_trades
    win_rate = (
        Decimal(analysis.trade_diagnostics.report_summary.winners_count)
        / Decimal(matched_trade_count)
        if matched_trade_count > 0
        else None
    )
    token_pnl = analysis.trade_diagnostics.report_summary.pnl_by_token
    top_losing_tokens = tuple(item for item in token_pnl if item.realized_pnl_usd < 0)[:5]

    return PortfolioWalletSummary(
        wallet=entry.wallet,
        label=entry.label,
        group=entry.group,
        chain=entry.chain,
        status=status,
        included_in_aggregate=included_in_aggregate,
        message=None,
        source_path=_relative_path_text(analysis_target.path, repository_root),
        analysis_summary_path=analysis.summary_path,
        realized_pnl_usd=realized_pnl_usd,
        matched_trade_count=matched_trade_count,
        winners_count=analysis.trade_diagnostics.report_summary.winners_count,
        losers_count=analysis.trade_diagnostics.report_summary.losers_count,
        win_rate=win_rate,
        holding_time_buckets=analysis.behavior_diagnostics.report_summary.holding_time_buckets,
        token_pnl=token_pnl,
        top_losing_tokens=top_losing_tokens,
        unsupported_transactions_count=analysis.unsupported_transactions_count,
        rows_requiring_valuation_after_count=(
            analysis.valuation_summary.rows_requiring_valuation_after_count
        ),
        unsupported_fifo_transactions_count=(
            analysis.fifo_summary.unsupported_fifo_transactions_count
        ),
        skipped_missing_valuation_count=analysis.fifo_summary.skipped_missing_valuation_count,
        unsupported_patterns=_build_unsupported_patterns(
            analysis.unsupported_reason_counts
        ),
    )


def _find_local_analysis_target(
    entry: WalletManifestEntry,
    *,
    repository_root: Path,
) -> _AnalysisTarget | None:
    wallet_directory = manifest_entry_wallet_directory(
        entry,
        repository_root=repository_root,
    )
    if not wallet_directory.exists():
        return None

    candidate_targets: list[_AnalysisTarget] = []
    fetch_metadata_path = single_wallet_analysis.find_latest_fetch_metadata_path(wallet_directory)
    if fetch_metadata_path is not None and _is_multipage_fetch_metadata(fetch_metadata_path):
        candidate_targets.append(
            _AnalysisTarget(
                path=fetch_metadata_path,
                target_type="fetch_metadata",
                timestamp=_timestamp_from_artifact_path(fetch_metadata_path),
            )
        )

    try:
        snapshot_path = single_wallet_analysis.find_latest_snapshot_path(wallet_directory)
    except ValueError:
        snapshot_path = None
    if snapshot_path is not None:
        candidate_targets.append(
            _AnalysisTarget(
                path=snapshot_path,
                target_type="snapshot",
                timestamp=_timestamp_from_artifact_path(snapshot_path),
            )
        )

    if not candidate_targets:
        return None

    return max(
        candidate_targets,
        key=lambda item: (
            item.timestamp or datetime.min.replace(tzinfo=UTC),
            1 if item.target_type == "fetch_metadata" else 0,
            item.path.name,
        ),
    )


def _fetch_solana_history(
    entry: WalletManifestEntry,
    *,
    repository_root: Path,
    limit: int,
    max_pages: int,
    fetch_mode: str,
) -> _AnalysisTarget:
    if limit <= 0:
        raise ValueError("solana_limit must be positive.")
    if max_pages <= 0:
        raise ValueError("solana_max_pages must be positive.")

    wallet_directory = manifest_entry_wallet_directory(
        entry,
        repository_root=repository_root,
    )
    wallet_directory.mkdir(parents=True, exist_ok=True)
    fetch_time = _utc_now()
    fetch_time_text = fetch_time.isoformat()
    timestamp_token = fetch_time.strftime("%Y%m%dT%H%M%SZ")
    page_directory = wallet_directory / f"fetch_{timestamp_token}"
    page_directory.mkdir(parents=True, exist_ok=True)
    metadata_path = wallet_directory / f"wallet_fetch_metadata_{timestamp_token}.json"

    client = SolanaRpcClient()
    provider = "solana_json_rpc"
    page_snapshot_paths: list[str] = []
    page_records: list[dict[str, object]] = []
    total_tx_count = 0
    before: str | None = None
    last_snapshot_fetched_at = fetch_time_text

    for page_number in range(1, max_pages + 1):
        snapshot = client.fetch_recent_transaction_history(entry.wallet, limit=limit, before=before)
        source = snapshot.get("source")
        if isinstance(source, dict):
            source_provider = source.get("provider")
            if isinstance(source_provider, str) and source_provider.strip():
                provider = source_provider

        tx_count = _count_transaction_responses(snapshot)
        if tx_count == 0:
            break

        snapshot_path = page_directory / f"wallet_snapshot_page_{page_number:03d}.json"
        snapshot_path.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
        page_snapshot_paths.append(_relative_path_text(snapshot_path, repository_root))
        total_tx_count += tx_count
        last_snapshot_fetched_at = _extract_fetched_at(snapshot, default=last_snapshot_fetched_at)
        page_records.append(
            {
                "page_number": page_number,
                "before": before,
                "tx_count": tx_count,
                "snapshot_path": _relative_path_text(snapshot_path, repository_root),
                "first_tx_hash": _extract_first_tx_hash(snapshot),
                "last_tx_hash": _extract_last_tx_hash(snapshot),
            }
        )

        if tx_count < limit:
            break
        before = _extract_last_signature(snapshot)
        if before is None:
            break

    if not page_snapshot_paths:
        raise ValueError("Solana fetch returned no transaction responses.")

    metadata_path.write_text(
        json.dumps(
            {
                "wallet": entry.wallet,
                "chain": entry.chain,
                "label": entry.label,
                "group": entry.group,
                "notes": entry.notes,
                "provider": provider,
                "fetched_at": last_snapshot_fetched_at,
                "status": "success",
                "snapshot_path": page_snapshot_paths[0],
                "page_snapshot_paths": page_snapshot_paths,
                "fetch_directory": _relative_path_text(page_directory, repository_root),
                "fetch_mode": fetch_mode,
                "limit": limit,
                "max_pages_requested": max_pages,
                "total_pages_fetched": len(page_records),
                "total_tx_count": total_tx_count,
                "pages": page_records,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return _AnalysisTarget(
        path=metadata_path,
        target_type="fetch_metadata",
        timestamp=fetch_time,
    )


def _analyze_single_wallet_target(
    analysis_target: _AnalysisTarget,
    *,
    valuation_path: Path | None = None,
) -> single_wallet_analysis.SingleWalletSnapshotAnalysis:
    if analysis_target.target_type == "fetch_metadata":
        return single_wallet_analysis.analyze_fetch_metadata_path(
            analysis_target.path,
            valuation_path=valuation_path,
        )
    return single_wallet_analysis.analyze_snapshot_path(
        analysis_target.path,
        valuation_path=valuation_path,
    )


def _auto_prepare_and_apply_wrapped_sol_valuations(
    analysis_target: _AnalysisTarget,
    analysis: single_wallet_analysis.SingleWalletSnapshotAnalysis,
) -> single_wallet_analysis.SingleWalletSnapshotAnalysis:
    rows_requiring_valuation = analysis.valuation_summary.rows_requiring_valuation_after
    if not rows_requiring_valuation:
        return analysis

    valuation_path = analysis_target.path.with_name(
        f"{analysis_target.path.stem}_trusted_valuations.json"
    )
    existing_records = (
        load_valuation_records(valuation_path, include_all_statuses=True)
        if valuation_path.exists()
        else ()
    )
    merged_records = merge_valuation_records(existing_records, rows_requiring_valuation)
    populated_records = populate_wrapped_sol_trusted_values(merged_records)

    if not valuation_path.exists() or populated_records.records != existing_records:
        write_valuation_records(valuation_path, populated_records.records)

    if populated_records.trusted_rows_populated <= 0:
        return analysis
    return _analyze_single_wallet_target(analysis_target, valuation_path=valuation_path)


def _build_unsupported_patterns(
    unsupported_reason_counts: Sequence[object],
) -> tuple[UnsupportedCasePatternCount, ...]:
    pattern_totals: dict[tuple[str, str], int] = {}
    for item in unsupported_reason_counts:
        reason = getattr(item, "reason", None)
        count = getattr(item, "count", None)
        if not isinstance(reason, str) or not isinstance(count, int):
            continue
        pattern_key, label = _unsupported_pattern_from_reason(reason)
        pattern_totals[(pattern_key, label)] = pattern_totals.get((pattern_key, label), 0) + count

    ranked_patterns = sorted(
        pattern_totals.items(),
        key=lambda item: (-item[1], item[0][1], item[0][0]),
    )
    return tuple(
        UnsupportedCasePatternCount(
            pattern_key=pattern_key,
            label=label,
            count=count,
        )
        for (pattern_key, label), count in ranked_patterns
    )


def _unsupported_pattern_from_reason(reason: str) -> tuple[str, str]:
    normalized_reason = reason.lower()
    if "failed transactions are out of scope" in normalized_reason:
        return ("failed_transactions", "failed_transactions")
    if "multiple wallet token balance deltas detected" in normalized_reason:
        return ("multiple_token_deltas", "multiple_token_deltas_or_multi_leg")
    if "token inflow with net sol inflow is ambiguous" in normalized_reason:
        return ("token_and_sol_inflow", "token_and_sol_inflow")
    if "token outflow with net sol outflow is ambiguous" in normalized_reason:
        return ("token_and_sol_outflow", "token_and_sol_outflow")
    return ("other_unsupported", "other_unsupported")


def _write_manifest_portfolio_report(
    report: ManifestPortfolioReport,
    *,
    json_path: Path,
    csv_path: Path,
    manifest_path: Path,
    repository_root: Path,
    filters: dict[str, object],
) -> tuple[Path, Path]:
    json_path.write_text(
        json.dumps(
            _jsonify(
                {
                    "manifest_path": _relative_path_text(manifest_path, repository_root),
                    "filters": filters,
                    "report": asdict(report),
                }
            ),
            indent=2,
        ),
        encoding="utf-8",
    )

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                "wallet",
                "label",
                "group",
                "chain",
                "status",
                "included_in_aggregate",
                "source_path",
                "analysis_summary_path",
                "realized_pnl_usd",
                "matched_trade_count",
                "winners_count",
                "losers_count",
                "win_rate",
                "unsupported_transactions_count",
                "rows_requiring_valuation_after_count",
                "unsupported_fifo_transactions_count",
                "skipped_missing_valuation_count",
                "unsupported_patterns",
                "top_losing_tokens",
                "message",
            ),
        )
        writer.writeheader()
        for wallet_summary in report.wallet_summaries:
            writer.writerow(
                {
                    "wallet": wallet_summary.wallet,
                    "label": wallet_summary.label,
                    "group": wallet_summary.group or "",
                    "chain": wallet_summary.chain,
                    "status": wallet_summary.status,
                    "included_in_aggregate": str(wallet_summary.included_in_aggregate).lower(),
                    "source_path": wallet_summary.source_path or "",
                    "analysis_summary_path": wallet_summary.analysis_summary_path or "",
                    "realized_pnl_usd": _csv_value(wallet_summary.realized_pnl_usd),
                    "matched_trade_count": wallet_summary.matched_trade_count,
                    "winners_count": wallet_summary.winners_count,
                    "losers_count": wallet_summary.losers_count,
                    "win_rate": _csv_value(wallet_summary.win_rate),
                    "unsupported_transactions_count": _csv_value(
                        wallet_summary.unsupported_transactions_count
                    ),
                    "rows_requiring_valuation_after_count": _csv_value(
                        wallet_summary.rows_requiring_valuation_after_count
                    ),
                    "unsupported_fifo_transactions_count": _csv_value(
                        wallet_summary.unsupported_fifo_transactions_count
                    ),
                    "skipped_missing_valuation_count": _csv_value(
                        wallet_summary.skipped_missing_valuation_count
                    ),
                    "unsupported_patterns": ",".join(
                        f"{item.label}:{item.count}" for item in wallet_summary.unsupported_patterns
                    ),
                    "top_losing_tokens": ",".join(
                        item.token_address for item in wallet_summary.top_losing_tokens
                    ),
                    "message": wallet_summary.message or "",
                }
            )

    return json_path, csv_path


def _load_included_wallet_diagnostics(
    wallet_summaries: Sequence[PortfolioWalletSummary],
    *,
    repository_root: Path,
) -> tuple[object, ...]:
    included_wallet_diagnostics = []
    for wallet_summary in wallet_summaries:
        if not wallet_summary.included_in_aggregate:
            continue
        if wallet_summary.analysis_summary_path is None:
            continue
        analysis_summary_path = _resolve_repository_relative_path(
            wallet_summary.analysis_summary_path,
            repository_root=repository_root,
        )
        trade_report_path = _analysis_summary_to_sibling_path(
            analysis_summary_path,
            "_trade_report.json",
        )
        trade_report = _load_trade_diagnostic_report(trade_report_path)
        included_wallet_diagnostics.append(
            prepare_portfolio_subset_wallet_diagnostics(
                wallet=wallet_summary.wallet,
                label=wallet_summary.label,
                group=wallet_summary.group,
                trade_report=trade_report,
            )
        )
    return tuple(included_wallet_diagnostics)


def _analysis_summary_to_sibling_path(
    analysis_summary_path: Path,
    suffix: str,
) -> Path:
    marker = "_analysis_summary.json"
    if not analysis_summary_path.name.endswith(marker):
        raise ValueError(
            "Analysis summary path does not follow the expected '*_analysis_summary.json' pattern: "
            f"{analysis_summary_path}"
        )
    stem = analysis_summary_path.name[: -len(marker)]
    return analysis_summary_path.with_name(f"{stem}{suffix}")


def _load_trade_diagnostic_report(path: Path) -> TradeDiagnosticReport:
    payload = json.loads(path.read_text(encoding="utf-8"))
    matched_trades_payload = payload.get("matched_trades")
    if not isinstance(matched_trades_payload, list):
        raise ValueError(f"Trade report is missing a 'matched_trades' list: {path}")
    matched_trades = tuple(
        _parse_matched_trade(row, path=path)
        for row in matched_trades_payload
    )
    return TradeDiagnosticReport(
        matched_trades=matched_trades,
        summary=summarize_trade_diagnostic_report(matched_trades),
    )


def _parse_matched_trade(
    row: object,
    *,
    path: Path,
) -> MatchedTradeDiagnostic:
    if not isinstance(row, Mapping):
        raise ValueError(f"Trade report rows must be objects: {path}")
    try:
        return MatchedTradeDiagnostic(
            token_address=_require_text(row, "token_address"),
            opening_tx_hash=_require_text(row, "opening_tx_hash"),
            closing_tx_hash=_require_text(row, "closing_tx_hash"),
            open_timestamp=datetime.fromisoformat(_require_text(row, "open_timestamp")),
            close_timestamp=datetime.fromisoformat(_require_text(row, "close_timestamp")),
            holding_duration_seconds=_require_int(row, "holding_duration_seconds"),
            quantity_matched=_require_decimal(row, "quantity_matched"),
            cost_basis_usd=_optional_decimal(row.get("cost_basis_usd")),
            proceeds_usd=_optional_decimal(row.get("proceeds_usd")),
            realized_pnl_usd=_optional_decimal(row.get("realized_pnl_usd")),
            opening_fee_native=_optional_decimal(row.get("opening_fee_native")),
            opening_fee_usd=_optional_decimal(row.get("opening_fee_usd")),
            closing_fee_native=_optional_decimal(row.get("closing_fee_native")),
            closing_fee_usd=_optional_decimal(row.get("closing_fee_usd")),
        )
    except ValueError as exc:
        raise ValueError(f"Invalid matched trade row in {path}: {exc}") from exc


def _write_portfolio_subset_behavior_report(
    report: PortfolioSubsetBehaviorReport,
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
                "wallet",
                "label",
                "group",
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
                    "wallet": trade_row.wallet,
                    "label": trade_row.label,
                    "group": trade_row.group or "",
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


def _write_portfolio_subset_simulation_report(
    report: PortfolioSubsetSimulationReport,
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


def _write_portfolio_subset_rules_report(
    report: PortfolioSubsetRulesReport,
    *,
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.write_text(
        json.dumps(_jsonify(asdict(report)), indent=2),
        encoding="utf-8",
    )
    markdown_path.write_text(
        render_portfolio_subset_rules_markdown(report),
        encoding="utf-8",
    )
    return json_path, markdown_path


def _timestamp_from_artifact_path(path: Path) -> datetime | None:
    match = TIMESTAMP_TOKEN_PATTERN.search(path.stem)
    if match is None:
        return None
    return datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)


def _is_multipage_fetch_metadata(path: Path) -> bool:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return False
    page_snapshot_paths = payload.get("page_snapshot_paths")
    return isinstance(page_snapshot_paths, list) and bool(page_snapshot_paths)


def _count_transaction_responses(snapshot: Mapping[str, object]) -> int:
    responses = snapshot.get("transaction_responses")
    if not isinstance(responses, list):
        raise ValueError("Solana snapshot is missing transaction_responses.")
    return len(responses)


def _extract_fetched_at(snapshot: Mapping[str, object], *, default: str) -> str:
    fetched_at = snapshot.get("fetched_at_utc")
    if not isinstance(fetched_at, str) or not fetched_at.strip():
        return default
    return fetched_at


def _extract_last_signature(snapshot: Mapping[str, object]) -> str | None:
    signatures_response = snapshot.get("signatures_response")
    if not isinstance(signatures_response, Mapping):
        return None
    result = signatures_response.get("result")
    if not isinstance(result, list) or not result:
        return None
    last_row = result[-1]
    if not isinstance(last_row, Mapping):
        return None
    signature = last_row.get("signature")
    if not isinstance(signature, str) or not signature.strip():
        return None
    return signature


def _extract_first_tx_hash(snapshot: Mapping[str, object]) -> str | None:
    responses = snapshot.get("transaction_responses")
    if not isinstance(responses, list) or not responses:
        return None
    return _extract_tx_hash_from_payload(responses[0])


def _extract_last_tx_hash(snapshot: Mapping[str, object]) -> str | None:
    responses = snapshot.get("transaction_responses")
    if not isinstance(responses, list) or not responses:
        return None
    return _extract_tx_hash_from_payload(responses[-1])


def _extract_tx_hash_from_payload(payload: object) -> str | None:
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
    if not isinstance(first_signature, str) or not first_signature.strip():
        return None
    return first_signature


def _relative_path_text(path: Path, repository_root: Path) -> str:
    try:
        return path.relative_to(repository_root).as_posix()
    except ValueError:
        return str(path)


def _resolve_repository_relative_path(path_text: str, *, repository_root: Path) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path
    return repository_root / path


def _require_text(payload: Mapping[str, object], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Missing required text field '{field_name}'")
    return value


def _require_int(payload: Mapping[str, object], field_name: str) -> int:
    value = payload.get(field_name)
    if not isinstance(value, int):
        raise ValueError(f"Missing required integer field '{field_name}'")
    return value


def _require_decimal(payload: Mapping[str, object], field_name: str) -> Decimal:
    value = payload.get(field_name)
    decimal_value = _optional_decimal(value)
    if decimal_value is None:
        raise ValueError(f"Missing required decimal field '{field_name}'")
    return decimal_value


def _optional_decimal(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float, str)):
        return Decimal(str(value))
    raise ValueError(f"Expected a decimal-compatible value, got {type(value).__name__}")


def _jsonify(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _jsonify(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_jsonify(item) for item in value]
    if isinstance(value, list):
        return [_jsonify(item) for item in value]
    return value


def _csv_value(value: object | None) -> str:
    if value is None:
        return ""
    return str(value)


def _utc_now() -> datetime:
    return datetime.now(UTC)


if __name__ == "__main__":
    raise SystemExit(main())
