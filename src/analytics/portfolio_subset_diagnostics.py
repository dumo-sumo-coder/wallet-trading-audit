"""Aggregate behavior, simulation, and coaching diagnostics for wallet subsets."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Sequence

from .manifest_portfolio import (
    ManifestPortfolioReport,
    PortfolioTokenLossConcentration,
    PortfolioWalletLossConcentration,
)
from .rules_report import build_wallet_rules_report
from .trade_diagnostics import (
    MatchedTradeDiagnostic,
    TradeDiagnosticReport,
    TradeDiagnosticSummary,
    summarize_trade_diagnostic_report,
)
from .trade_filter_simulation import (
    TradeFilterSimulationReport,
    TradeFilterSimulationSummary,
    build_default_trade_filter_simulation_report,
)
from .wallet_behavior import (
    BehaviorTradeRow,
    CostBasisBucketDiagnostic,
    FastRotationDiagnostic,
    HoldingTimeBucketDiagnostic,
    StreakDiagnostics,
    WalletBehaviorReport,
    build_wallet_behavior_report,
)

ZERO = Decimal("0")


@dataclass(frozen=True, slots=True)
class PortfolioSubsetWalletDiagnostics:
    wallet: str
    label: str
    group: str | None
    trade_report: TradeDiagnosticReport


@dataclass(frozen=True, slots=True)
class PortfolioSubsetBehaviorTradeRow:
    wallet: str
    label: str
    group: str | None
    token_address: str
    opening_tx_hash: str
    closing_tx_hash: str
    open_timestamp: object
    close_timestamp: object
    close_day: str
    holding_duration_seconds: int
    holding_time_bucket: str
    quantity_matched: Decimal
    cost_basis_usd: Decimal | None
    cost_basis_bucket: str
    proceeds_usd: Decimal | None
    realized_pnl_usd: Decimal | None
    outcome: str
    prior_trade_outcome: str | None


@dataclass(frozen=True, slots=True)
class PortfolioSubsetWalletStreakSummary:
    wallet: str
    label: str
    group: str | None
    longest_losing_streak: int
    longest_winning_streak: int


@dataclass(frozen=True, slots=True)
class PortfolioSubsetBehaviorSummary:
    included_wallet_count: int
    included_wallet_labels: tuple[str, ...]
    total_matched_trades: int
    aggregate_realized_pnl_usd: Decimal
    winners_count: int
    losers_count: int
    overall_win_rate: Decimal | None
    holding_time_buckets: tuple[HoldingTimeBucketDiagnostic, ...]
    fast_rotation_diagnostics: tuple[FastRotationDiagnostic, ...]
    cost_basis_buckets: tuple[CostBasisBucketDiagnostic, ...]
    longest_losing_streak_overall: int
    longest_winning_streak_overall: int
    streaks_by_wallet: tuple[PortfolioSubsetWalletStreakSummary, ...]
    loss_concentration_by_wallet: tuple[PortfolioWalletLossConcentration, ...]
    loss_concentration_by_token: tuple[PortfolioTokenLossConcentration, ...]


@dataclass(frozen=True, slots=True)
class PortfolioSubsetBehaviorReport:
    trade_rows: tuple[PortfolioSubsetBehaviorTradeRow, ...]
    summary: PortfolioSubsetBehaviorSummary


@dataclass(frozen=True, slots=True)
class PortfolioSubsetSimulationReport:
    included_wallet_count: int
    included_wallet_labels: tuple[str, ...]
    summary: TradeFilterSimulationSummary


@dataclass(frozen=True, slots=True)
class PortfolioSubsetLeakPatternRecommendation:
    rank: int
    title: str
    category: str
    evidence: str
    estimated_pnl_drag_usd: Decimal
    affected_wallet_count: int
    total_wallet_count: int


@dataclass(frozen=True, slots=True)
class PortfolioSubsetCandidateRuleRecommendation:
    rank: int
    title: str
    category: str
    scenario_name: str
    estimated_pnl_improvement_usd: Decimal
    estimated_new_realized_pnl_usd: Decimal
    filtered_out_trade_count: int
    affected_wallet_count: int
    total_wallet_count: int
    rationale: str
    excluded_tokens: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PortfolioSubsetRulesReport:
    included_wallet_count: int
    included_wallet_labels: tuple[str, ...]
    original_realized_pnl_usd: Decimal
    top_leak_patterns: tuple[PortfolioSubsetLeakPatternRecommendation, ...]
    top_candidate_rules: tuple[PortfolioSubsetCandidateRuleRecommendation, ...]
    next_test_rule_categories: tuple[str, ...]
    caution_notes: tuple[str, ...]


def prepare_portfolio_subset_wallet_diagnostics(
    *,
    wallet: str,
    label: str,
    group: str | None,
    trade_report: TradeDiagnosticReport,
) -> PortfolioSubsetWalletDiagnostics:
    return PortfolioSubsetWalletDiagnostics(
        wallet=wallet,
        label=label,
        group=group,
        trade_report=trade_report,
    )


def build_portfolio_subset_behavior_report(
    wallet_diagnostics: Sequence[PortfolioSubsetWalletDiagnostics],
    *,
    portfolio_report: ManifestPortfolioReport,
) -> PortfolioSubsetBehaviorReport:
    combined_trades = _combine_matched_trades(wallet_diagnostics)
    combined_trade_summary = summarize_trade_diagnostic_report(combined_trades)
    aggregate_behavior_report = build_wallet_behavior_report(combined_trades)

    trade_rows: list[PortfolioSubsetBehaviorTradeRow] = []
    streaks_by_wallet: list[PortfolioSubsetWalletStreakSummary] = []
    for wallet_item in wallet_diagnostics:
        wallet_behavior_report = build_wallet_behavior_report(wallet_item.trade_report.matched_trades)
        streaks_by_wallet.append(
            PortfolioSubsetWalletStreakSummary(
                wallet=wallet_item.wallet,
                label=wallet_item.label,
                group=wallet_item.group,
                longest_losing_streak=wallet_behavior_report.summary.streak_diagnostics.longest_losing_streak,
                longest_winning_streak=wallet_behavior_report.summary.streak_diagnostics.longest_winning_streak,
            )
        )
        trade_rows.extend(
            _portfolio_subset_trade_rows(
                wallet_item,
                wallet_behavior_report.trade_rows,
            )
        )

    trade_rows.sort(
        key=lambda item: (
            item.close_timestamp,
            item.open_timestamp,
            item.label,
            item.closing_tx_hash,
            item.opening_tx_hash,
            item.token_address,
        )
    )
    ranked_streaks = tuple(
        sorted(
            streaks_by_wallet,
            key=lambda item: (
                -item.longest_losing_streak,
                -item.longest_winning_streak,
                item.label,
                item.wallet,
            ),
        )
    )

    return PortfolioSubsetBehaviorReport(
        trade_rows=tuple(trade_rows),
        summary=PortfolioSubsetBehaviorSummary(
            included_wallet_count=len(wallet_diagnostics),
            included_wallet_labels=tuple(item.label for item in wallet_diagnostics),
            total_matched_trades=combined_trade_summary.total_matched_trades,
            aggregate_realized_pnl_usd=_sum_realized_pnls(combined_trades),
            winners_count=combined_trade_summary.winners_count,
            losers_count=combined_trade_summary.losers_count,
            overall_win_rate=(
                Decimal(combined_trade_summary.winners_count)
                / Decimal(combined_trade_summary.total_matched_trades)
                if combined_trade_summary.total_matched_trades > 0
                else None
            ),
            holding_time_buckets=aggregate_behavior_report.summary.holding_time_buckets,
            fast_rotation_diagnostics=aggregate_behavior_report.summary.fast_rotation_diagnostics,
            cost_basis_buckets=aggregate_behavior_report.summary.notional_diagnostics.cost_basis_buckets,
            longest_losing_streak_overall=aggregate_behavior_report.summary.streak_diagnostics.longest_losing_streak,
            longest_winning_streak_overall=aggregate_behavior_report.summary.streak_diagnostics.longest_winning_streak,
            streaks_by_wallet=ranked_streaks,
            loss_concentration_by_wallet=portfolio_report.summary.loss_concentration_by_wallet,
            loss_concentration_by_token=portfolio_report.summary.loss_concentration_by_token,
        ),
    )


def build_portfolio_subset_simulation_report(
    wallet_diagnostics: Sequence[PortfolioSubsetWalletDiagnostics],
) -> PortfolioSubsetSimulationReport:
    combined_trades = _combine_matched_trades(wallet_diagnostics)
    simulation_report = build_default_trade_filter_simulation_report(combined_trades)
    return PortfolioSubsetSimulationReport(
        included_wallet_count=len(wallet_diagnostics),
        included_wallet_labels=tuple(item.label for item in wallet_diagnostics),
        summary=simulation_report.summary,
    )


def build_portfolio_subset_rules_report(
    wallet_diagnostics: Sequence[PortfolioSubsetWalletDiagnostics],
    *,
    portfolio_behavior_report: PortfolioSubsetBehaviorReport,
    portfolio_simulation_report: PortfolioSubsetSimulationReport,
) -> PortfolioSubsetRulesReport:
    combined_trades = _combine_matched_trades(wallet_diagnostics)
    aggregate_behavior_summary = build_wallet_behavior_report(combined_trades).summary
    base_report = build_wallet_rules_report(
        aggregate_behavior_summary,
        portfolio_simulation_report.summary,
    )
    per_wallet_simulations = {
        item.wallet: build_default_trade_filter_simulation_report(
            item.trade_report.matched_trades
        ).summary
        for item in wallet_diagnostics
    }
    total_wallet_count = len(wallet_diagnostics)

    candidate_rules = tuple(
        PortfolioSubsetCandidateRuleRecommendation(
            rank=item.rank,
            title=item.title,
            category=item.category,
            scenario_name=item.scenario_name,
            estimated_pnl_improvement_usd=item.estimated_pnl_improvement_usd,
            estimated_new_realized_pnl_usd=item.estimated_new_realized_pnl_usd,
            filtered_out_trade_count=item.filtered_out_trade_count,
            affected_wallet_count=_count_wallets_improved_by_scenario(
                wallet_diagnostics,
                per_wallet_simulations,
                item.scenario_name,
            ),
            total_wallet_count=total_wallet_count,
            rationale=item.rationale,
            excluded_tokens=item.excluded_tokens,
        )
        for item in base_report.top_candidate_rules
    )
    affected_wallets_by_category = {
        item.category: item.affected_wallet_count for item in candidate_rules
    }

    leak_patterns = tuple(
        PortfolioSubsetLeakPatternRecommendation(
            rank=item.rank,
            title=item.title,
            category=item.category,
            evidence=_augment_leak_evidence(
                item.evidence,
                affected_wallet_count=affected_wallets_by_category.get(item.category, 0),
                total_wallet_count=total_wallet_count,
            ),
            estimated_pnl_drag_usd=item.estimated_pnl_drag_usd,
            affected_wallet_count=affected_wallets_by_category.get(item.category, 0),
            total_wallet_count=total_wallet_count,
        )
        for item in base_report.top_leak_patterns
    )

    return PortfolioSubsetRulesReport(
        included_wallet_count=total_wallet_count,
        included_wallet_labels=tuple(item.label for item in wallet_diagnostics),
        original_realized_pnl_usd=portfolio_simulation_report.summary.original_realized_pnl_usd,
        top_leak_patterns=leak_patterns,
        top_candidate_rules=candidate_rules,
        next_test_rule_categories=base_report.next_test_rule_categories,
        caution_notes=(
            f"These recommendations are retrospective and based on a {total_wallet_count}-wallet recent subset, not the full wallet manifest; they may overfit this sample.",
            "Excluded-trade simulations assume the remaining matched trades are unchanged, which may not hold in live trading.",
            "Supported-subset caveats, unsupported raw transactions, and skipped FIFO rows remain outside these coaching rules.",
        ),
    )


def render_portfolio_subset_rules_markdown(report: PortfolioSubsetRulesReport) -> str:
    lines = [
        "# Portfolio Subset Rules Report",
        "",
        f"Included wallets: {report.included_wallet_count}",
        f"Original realized PnL: {report.original_realized_pnl_usd}",
        "",
        "## Top Repeated Leak Patterns",
    ]
    if not report.top_leak_patterns:
        lines.append("- No repeated leak patterns were identified from the included wallet subset.")
    else:
        for item in report.top_leak_patterns:
            lines.append(
                f"- {item.rank}. {item.title} [{item.category}] "
                f"(estimated drag {item.estimated_pnl_drag_usd}, repeated in "
                f"{item.affected_wallet_count}/{item.total_wallet_count} wallets)"
            )
            lines.append(f"  Evidence: {item.evidence}")

    lines.extend(["", "## Top Candidate Rules"])
    if not report.top_candidate_rules:
        lines.append("- No positive-improvement rule candidates were identified.")
    else:
        for item in report.top_candidate_rules:
            lines.append(
                f"- {item.rank}. {item.title} [{item.category}] "
                f"(+{item.estimated_pnl_improvement_usd} to "
                f"{item.estimated_new_realized_pnl_usd}; improved "
                f"{item.affected_wallet_count}/{item.total_wallet_count} wallets)"
            )
            lines.append(f"  Rationale: {item.rationale}")

    lines.extend(["", "## Explore Next"])
    if not report.next_test_rule_categories:
        lines.append("- No additional rule categories were flagged from this subset.")
    else:
        for item in report.next_test_rule_categories:
            lines.append(f"- {item}")

    lines.extend(["", "## Cautions"])
    for item in report.caution_notes:
        lines.append(f"- {item}")

    return "\n".join(lines) + "\n"


def _combine_matched_trades(
    wallet_diagnostics: Sequence[PortfolioSubsetWalletDiagnostics],
) -> tuple[MatchedTradeDiagnostic, ...]:
    combined: list[MatchedTradeDiagnostic] = []
    for item in wallet_diagnostics:
        combined.extend(item.trade_report.matched_trades)
    combined.sort(
        key=lambda trade: (
            trade.close_timestamp,
            trade.open_timestamp,
            trade.closing_tx_hash,
            trade.opening_tx_hash,
            trade.token_address,
        )
    )
    return tuple(combined)


def _portfolio_subset_trade_rows(
    wallet_item: PortfolioSubsetWalletDiagnostics,
    trade_rows: Sequence[BehaviorTradeRow],
) -> tuple[PortfolioSubsetBehaviorTradeRow, ...]:
    return tuple(
        PortfolioSubsetBehaviorTradeRow(
            wallet=wallet_item.wallet,
            label=wallet_item.label,
            group=wallet_item.group,
            token_address=trade_row.token_address,
            opening_tx_hash=trade_row.opening_tx_hash,
            closing_tx_hash=trade_row.closing_tx_hash,
            open_timestamp=trade_row.open_timestamp,
            close_timestamp=trade_row.close_timestamp,
            close_day=trade_row.close_day,
            holding_duration_seconds=trade_row.holding_duration_seconds,
            holding_time_bucket=trade_row.holding_time_bucket,
            quantity_matched=trade_row.quantity_matched,
            cost_basis_usd=trade_row.cost_basis_usd,
            cost_basis_bucket=trade_row.cost_basis_bucket,
            proceeds_usd=trade_row.proceeds_usd,
            realized_pnl_usd=trade_row.realized_pnl_usd,
            outcome=trade_row.outcome,
            prior_trade_outcome=trade_row.prior_trade_outcome,
        )
        for trade_row in trade_rows
    )


def _sum_realized_pnls(matched_trades: Sequence[MatchedTradeDiagnostic]) -> Decimal:
    return sum(
        (trade.realized_pnl_usd for trade in matched_trades if trade.realized_pnl_usd is not None),
        ZERO,
    )


def _count_wallets_improved_by_scenario(
    wallet_diagnostics: Sequence[PortfolioSubsetWalletDiagnostics],
    per_wallet_simulations: dict[str, TradeFilterSimulationSummary],
    scenario_name: str,
) -> int:
    improved_wallets = 0
    for item in wallet_diagnostics:
        simulation = per_wallet_simulations[item.wallet]
        if any(
            scenario.scenario_name == scenario_name and scenario.delta_vs_original_pnl_usd > 0
            for scenario in simulation.scenario_results
        ):
            improved_wallets += 1
    return improved_wallets


def _augment_leak_evidence(
    evidence: str,
    *,
    affected_wallet_count: int,
    total_wallet_count: int,
) -> str:
    if total_wallet_count <= 0:
        return evidence
    return (
        f"{evidence} Positive improvement was observed in "
        f"{affected_wallet_count}/{total_wallet_count} included wallets."
    )
