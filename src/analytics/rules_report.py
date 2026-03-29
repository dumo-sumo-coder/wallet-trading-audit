"""Rules-oriented coaching report built from behavior and simulation summaries."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Sequence

from .trade_filter_simulation import (
    RULE_EXCLUDE_COST_BASIS_ABOVE_USD,
    RULE_EXCLUDE_HOLD_UNDER_SECONDS,
    RULE_EXCLUDE_TOKENS_LOSS_ABOVE_USD,
    RULE_EXCLUDE_WORST_N_TRADES,
    TradeFilterScenarioResult,
    TradeFilterSimulationSummary,
)
from .wallet_behavior import (
    FastRotationDiagnostic,
    StreakDiagnostics,
    TokenConcentrationDiagnostic,
    WalletBehaviorSummary,
)


@dataclass(frozen=True, slots=True)
class LeakPatternRecommendation:
    rank: int
    title: str
    category: str
    evidence: str
    estimated_pnl_drag_usd: Decimal


@dataclass(frozen=True, slots=True)
class CandidateRuleRecommendation:
    rank: int
    title: str
    category: str
    scenario_name: str
    estimated_pnl_improvement_usd: Decimal
    estimated_new_realized_pnl_usd: Decimal
    filtered_out_trade_count: int
    rationale: str
    excluded_tokens: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class WalletRulesReport:
    original_realized_pnl_usd: Decimal
    top_leak_patterns: tuple[LeakPatternRecommendation, ...]
    top_candidate_rules: tuple[CandidateRuleRecommendation, ...]
    next_test_rule_categories: tuple[str, ...]
    caution_notes: tuple[str, ...]


def build_wallet_rules_report(
    behavior_summary: WalletBehaviorSummary,
    simulation_summary: TradeFilterSimulationSummary,
) -> WalletRulesReport:
    best_hold_scenario = _best_scenario_by_rule_type(
        simulation_summary.scenario_results,
        RULE_EXCLUDE_HOLD_UNDER_SECONDS,
    )
    best_cost_basis_scenario = _best_scenario_by_rule_type(
        simulation_summary.scenario_results,
        RULE_EXCLUDE_COST_BASIS_ABOVE_USD,
    )
    best_token_blacklist_scenario = _best_scenario_by_rule_type(
        simulation_summary.scenario_results,
        RULE_EXCLUDE_TOKENS_LOSS_ABOVE_USD,
    )

    candidate_rules = [
        item
        for item in (
            _build_candidate_rule_recommendation(
                best_token_blacklist_scenario,
                category="token/setup blacklist threshold",
                title=_token_blacklist_title(best_token_blacklist_scenario),
                rationale=_token_blacklist_rationale(
                    behavior_summary,
                    best_token_blacklist_scenario,
                ),
            ),
            _build_candidate_rule_recommendation(
                best_cost_basis_scenario,
                category="max cost basis",
                title=_cost_basis_title(best_cost_basis_scenario),
                rationale=_cost_basis_rationale(best_cost_basis_scenario),
            ),
            _build_candidate_rule_recommendation(
                best_hold_scenario,
                category="hold-time floor",
                title=_hold_time_title(best_hold_scenario),
                rationale=_hold_time_rationale(
                    behavior_summary,
                    best_hold_scenario,
                ),
            ),
        )
        if item is not None
    ]
    candidate_rules = sorted(
        candidate_rules,
        key=lambda item: (
            item.estimated_pnl_improvement_usd,
            -item.filtered_out_trade_count,
            item.title,
        ),
        reverse=True,
    )
    ranked_candidate_rules = tuple(
        CandidateRuleRecommendation(
            rank=index,
            title=item.title,
            category=item.category,
            scenario_name=item.scenario_name,
            estimated_pnl_improvement_usd=item.estimated_pnl_improvement_usd,
            estimated_new_realized_pnl_usd=item.estimated_new_realized_pnl_usd,
            filtered_out_trade_count=item.filtered_out_trade_count,
            rationale=item.rationale,
            excluded_tokens=item.excluded_tokens,
        )
        for index, item in enumerate(candidate_rules[:3], start=1)
    )

    leak_patterns = [
        item
        for item in (
            _build_token_concentration_leak_pattern(
                behavior_summary,
                best_token_blacklist_scenario,
            ),
            _build_cost_basis_leak_pattern(best_cost_basis_scenario),
            _build_fast_rotation_leak_pattern(
                behavior_summary,
                best_hold_scenario,
            ),
        )
        if item is not None
    ]
    leak_patterns = sorted(
        leak_patterns,
        key=lambda item: (item.estimated_pnl_drag_usd, item.title),
        reverse=True,
    )
    ranked_leak_patterns = tuple(
        LeakPatternRecommendation(
            rank=index,
            title=item.title,
            category=item.category,
            evidence=item.evidence,
            estimated_pnl_drag_usd=item.estimated_pnl_drag_usd,
        )
        for index, item in enumerate(leak_patterns[:3], start=1)
    )

    next_test_rule_categories = _build_next_test_rule_categories(
        behavior_summary.streak_diagnostics,
    )

    return WalletRulesReport(
        original_realized_pnl_usd=simulation_summary.original_realized_pnl_usd,
        top_leak_patterns=ranked_leak_patterns,
        top_candidate_rules=ranked_candidate_rules,
        next_test_rule_categories=next_test_rule_categories,
        caution_notes=(
            "These recommendations are retrospective and based on one wallet only; they may overfit a 46-trade sample.",
            "Excluded-trade simulations assume the remaining matched trades are unchanged, which may not hold in live trading.",
            "Unsupported raw transactions and skipped FIFO rows are out of scope for these coaching rules.",
        ),
    )


def render_wallet_rules_markdown(report: WalletRulesReport) -> str:
    lines = [
        "# Wallet Rules Coaching Report",
        "",
        f"Original realized PnL: {report.original_realized_pnl_usd}",
        "",
        "## Top Leak Patterns",
    ]
    if not report.top_leak_patterns:
        lines.append("- No leak patterns identified from the current summaries.")
    else:
        for item in report.top_leak_patterns:
            lines.append(
                f"- {item.rank}. {item.title} [{item.category}] "
                f"(estimated drag {item.estimated_pnl_drag_usd})"
            )
            lines.append(f"  Evidence: {item.evidence}")

    lines.extend(["", "## Top Candidate Rules"])
    if not report.top_candidate_rules:
        lines.append("- No positive-improvement rule candidates were identified.")
    else:
        for item in report.top_candidate_rules:
            lines.append(
                f"- {item.rank}. {item.title} [{item.category}] "
                f"(+{item.estimated_pnl_improvement_usd} to {item.estimated_new_realized_pnl_usd})"
            )
            lines.append(f"  Rationale: {item.rationale}")

    lines.extend(["", "## Explore Next"])
    if not report.next_test_rule_categories:
        lines.append("- No additional unestimated rule categories flagged.")
    else:
        for item in report.next_test_rule_categories:
            lines.append(f"- {item}")

    lines.extend(["", "## Cautions"])
    for item in report.caution_notes:
        lines.append(f"- {item}")

    return "\n".join(lines) + "\n"


def _best_scenario_by_rule_type(
    scenario_results: Sequence[TradeFilterScenarioResult],
    rule_type: str,
) -> TradeFilterScenarioResult | None:
    matching_results = [
        item
        for item in scenario_results
        if item.rule_type == rule_type and item.delta_vs_original_pnl_usd > 0
    ]
    if not matching_results:
        return None
    return max(
        matching_results,
        key=lambda item: (
            item.delta_vs_original_pnl_usd,
            -item.filtered_out_trade_count,
            item.scenario_name,
        ),
    )


def _build_candidate_rule_recommendation(
    scenario: TradeFilterScenarioResult | None,
    *,
    category: str,
    title: str,
    rationale: str,
) -> CandidateRuleRecommendation | None:
    if scenario is None:
        return None
    return CandidateRuleRecommendation(
        rank=0,
        title=title,
        category=category,
        scenario_name=scenario.scenario_name,
        estimated_pnl_improvement_usd=scenario.delta_vs_original_pnl_usd,
        estimated_new_realized_pnl_usd=scenario.new_realized_pnl_usd,
        filtered_out_trade_count=scenario.filtered_out_trade_count,
        rationale=rationale,
        excluded_tokens=scenario.excluded_tokens,
    )


def _build_fast_rotation_leak_pattern(
    behavior_summary: WalletBehaviorSummary,
    scenario: TradeFilterScenarioResult | None,
) -> LeakPatternRecommendation | None:
    if scenario is None:
        return None
    fast_rotation_bucket = _find_fast_rotation_bucket(
        behavior_summary.fast_rotation_diagnostics,
        scenario_name=scenario.scenario_name,
    )
    if fast_rotation_bucket is None:
        evidence = (
            f"Removing {scenario.filtered_out_trade_count} very short trades "
            f"would have improved PnL by {scenario.delta_vs_original_pnl_usd}."
        )
    else:
        evidence = (
            f"{fast_rotation_bucket.trade_count} trades in the matched fast-rotation bucket "
            f"contributed {fast_rotation_bucket.pnl_contribution_usd}."
        )
    return LeakPatternRecommendation(
        rank=0,
        title="Fast rotations were a recurring leak",
        category="hold-time floor",
        evidence=evidence,
        estimated_pnl_drag_usd=scenario.delta_vs_original_pnl_usd,
    )


def _build_cost_basis_leak_pattern(
    scenario: TradeFilterScenarioResult | None,
) -> LeakPatternRecommendation | None:
    if scenario is None:
        return None
    return LeakPatternRecommendation(
        rank=0,
        title="Higher-cost entries lost money disproportionately",
        category="max cost basis",
        evidence=(
            f"Removing {scenario.filtered_out_trade_count} trades above the tested cost-basis "
            f"threshold would have changed realized PnL by {scenario.delta_vs_original_pnl_usd}."
        ),
        estimated_pnl_drag_usd=scenario.delta_vs_original_pnl_usd,
    )


def _build_token_concentration_leak_pattern(
    behavior_summary: WalletBehaviorSummary,
    scenario: TradeFilterScenarioResult | None,
) -> LeakPatternRecommendation | None:
    if scenario is None:
        return None
    top_loss_concentration = (
        behavior_summary.concentration_diagnostics.top_5_losing_tokens_contribution_pct
    )
    top_losing_tokens = behavior_summary.concentration_diagnostics.top_losing_tokens[:3]
    token_labels = ", ".join(item.token_address for item in top_losing_tokens)
    evidence = (
        f"Top losing tokens contributed {top_loss_concentration} of total losses; "
        f"the heaviest leaks included {token_labels}."
    )
    return LeakPatternRecommendation(
        rank=0,
        title="Losses were concentrated in a small token subset",
        category="token/setup blacklist threshold",
        evidence=evidence,
        estimated_pnl_drag_usd=scenario.delta_vs_original_pnl_usd,
    )


def _build_next_test_rule_categories(
    streak_diagnostics: StreakDiagnostics,
) -> tuple[str, ...]:
    if (
        streak_diagnostics.longest_losing_streak >= 3
        and streak_diagnostics.avg_pnl_after_prior_loss_usd is not None
        and streak_diagnostics.avg_pnl_after_prior_loss_usd < 0
    ):
        return (
            "daily stop / losing-streak stop: the wallet hit a long losing streak and average PnL after prior losses stayed negative; simulate this next before adopting it.",
        )
    return ()


def _find_fast_rotation_bucket(
    fast_rotation_diagnostics: Sequence[FastRotationDiagnostic],
    *,
    scenario_name: str,
) -> FastRotationDiagnostic | None:
    scenario_to_bucket = {
        "exclude_hold_under_30s": "under_30s",
        "exclude_hold_under_60s": "under_60s",
        "exclude_hold_under_5m": "under_5m",
    }
    expected_bucket = scenario_to_bucket.get(scenario_name)
    if expected_bucket is None:
        return None
    for bucket in fast_rotation_diagnostics:
        if bucket.bucket == expected_bucket:
            return bucket
    return None


def _hold_time_title(scenario: TradeFilterScenarioResult | None) -> str:
    if scenario is None:
        return "Hold-time floor"
    return f"Require a minimum hold time of {_format_duration(int(scenario.threshold_value))}"


def _cost_basis_title(scenario: TradeFilterScenarioResult | None) -> str:
    if scenario is None:
        return "Max cost basis"
    return f"Avoid entries above ${scenario.threshold_value} cost basis"


def _token_blacklist_title(scenario: TradeFilterScenarioResult | None) -> str:
    if scenario is None:
        return "Token/setup blacklist threshold"
    return (
        "Blacklist tokens after cumulative matched-trade loss exceeds "
        f"${scenario.threshold_value}"
    )


def _hold_time_rationale(
    behavior_summary: WalletBehaviorSummary,
    scenario: TradeFilterScenarioResult | None,
) -> str:
    if scenario is None:
        return "No positive hold-time simulation was available."
    bucket = _find_fast_rotation_bucket(
        behavior_summary.fast_rotation_diagnostics,
        scenario_name=scenario.scenario_name,
    )
    if bucket is None:
        return (
            f"Removing short-hold trades improved PnL by {scenario.delta_vs_original_pnl_usd}."
        )
    return (
        f"{bucket.trade_count} trades inside the tested fast-rotation bucket contributed "
        f"{bucket.pnl_contribution_usd}, and removing them would have improved PnL by "
        f"{scenario.delta_vs_original_pnl_usd}."
    )


def _cost_basis_rationale(scenario: TradeFilterScenarioResult | None) -> str:
    if scenario is None:
        return "No positive cost-basis simulation was available."
    return (
        f"Removing {scenario.filtered_out_trade_count} higher-notional trades would have "
        f"improved PnL by {scenario.delta_vs_original_pnl_usd}."
    )


def _token_blacklist_rationale(
    behavior_summary: WalletBehaviorSummary,
    scenario: TradeFilterScenarioResult | None,
) -> str:
    if scenario is None:
        return "No positive token blacklist simulation was available."
    top_loser = _top_token_label(
        behavior_summary.concentration_diagnostics.top_losing_tokens
    )
    return (
        f"Losses were concentrated enough that removing {scenario.filtered_out_trade_count} "
        f"trades across {len(scenario.excluded_tokens)} flagged tokens, led by {top_loser}, "
        f"would have improved PnL by {scenario.delta_vs_original_pnl_usd}."
    )


def _top_token_label(tokens: Sequence[TokenConcentrationDiagnostic]) -> str:
    if not tokens:
        return "no dominant loser"
    return tokens[0].token_address


def _format_duration(seconds: int) -> str:
    if seconds % 3600 == 0 and seconds >= 3600:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0 and seconds >= 60:
        return f"{seconds // 60}m"
    return f"{seconds}s"
