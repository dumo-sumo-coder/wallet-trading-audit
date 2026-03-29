"""Trade-filter simulations built from matched trade diagnostics only."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Sequence

from .trade_diagnostics import MatchedTradeDiagnostic

ZERO = Decimal("0")

RULE_EXCLUDE_HOLD_UNDER_SECONDS = "exclude_hold_under_seconds"
RULE_EXCLUDE_COST_BASIS_ABOVE_USD = "exclude_cost_basis_above_usd"
RULE_EXCLUDE_WORST_N_TRADES = "exclude_worst_n_trades"
RULE_EXCLUDE_TOKENS_LOSS_ABOVE_USD = "exclude_tokens_loss_above_usd"


@dataclass(frozen=True, slots=True)
class TradeFilterScenario:
    name: str
    rule_type: str
    threshold_value: Decimal | int


@dataclass(frozen=True, slots=True)
class TradeFilterScenarioResult:
    scenario_name: str
    rule_type: str
    threshold_value: Decimal | int
    original_trade_count: int
    remaining_trade_count: int
    filtered_out_trade_count: int
    original_realized_pnl_usd: Decimal
    filtered_out_realized_pnl_usd: Decimal
    new_realized_pnl_usd: Decimal
    delta_vs_original_pnl_usd: Decimal
    excluded_tokens: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TradeFilterSimulationSummary:
    original_trade_count: int
    original_realized_pnl_usd: Decimal
    scenario_results: tuple[TradeFilterScenarioResult, ...]
    best_improvement_scenario_name: str | None
    best_improvement_delta_usd: Decimal | None
    best_improvement_new_realized_pnl_usd: Decimal | None


@dataclass(frozen=True, slots=True)
class TradeFilterSimulationReport:
    summary: TradeFilterSimulationSummary


def build_default_trade_filter_simulation_report(
    matched_trades: Sequence[MatchedTradeDiagnostic],
) -> TradeFilterSimulationReport:
    return build_trade_filter_simulation_report(
        matched_trades,
        scenarios=default_trade_filter_scenarios(),
    )


def build_trade_filter_simulation_report(
    matched_trades: Sequence[MatchedTradeDiagnostic],
    *,
    scenarios: Sequence[TradeFilterScenario],
) -> TradeFilterSimulationReport:
    original_realized_pnl_usd = _sum_realized_pnls(matched_trades)
    scenario_results = tuple(
        _run_scenario(
            matched_trades,
            scenario=scenario,
            original_realized_pnl_usd=original_realized_pnl_usd,
        )
        for scenario in scenarios
    )
    best_scenario = _select_best_improvement_scenario(scenario_results)

    return TradeFilterSimulationReport(
        summary=TradeFilterSimulationSummary(
            original_trade_count=len(matched_trades),
            original_realized_pnl_usd=original_realized_pnl_usd,
            scenario_results=scenario_results,
            best_improvement_scenario_name=(
                best_scenario.scenario_name if best_scenario is not None else None
            ),
            best_improvement_delta_usd=(
                best_scenario.delta_vs_original_pnl_usd
                if best_scenario is not None
                else None
            ),
            best_improvement_new_realized_pnl_usd=(
                best_scenario.new_realized_pnl_usd
                if best_scenario is not None
                else None
            ),
        )
    )


def default_trade_filter_scenarios() -> tuple[TradeFilterScenario, ...]:
    return (
        TradeFilterScenario(
            name="exclude_hold_under_30s",
            rule_type=RULE_EXCLUDE_HOLD_UNDER_SECONDS,
            threshold_value=30,
        ),
        TradeFilterScenario(
            name="exclude_hold_under_60s",
            rule_type=RULE_EXCLUDE_HOLD_UNDER_SECONDS,
            threshold_value=60,
        ),
        TradeFilterScenario(
            name="exclude_hold_under_5m",
            rule_type=RULE_EXCLUDE_HOLD_UNDER_SECONDS,
            threshold_value=300,
        ),
        TradeFilterScenario(
            name="exclude_cost_basis_above_5_usd",
            rule_type=RULE_EXCLUDE_COST_BASIS_ABOVE_USD,
            threshold_value=Decimal("5"),
        ),
        TradeFilterScenario(
            name="exclude_cost_basis_above_10_usd",
            rule_type=RULE_EXCLUDE_COST_BASIS_ABOVE_USD,
            threshold_value=Decimal("10"),
        ),
        TradeFilterScenario(
            name="exclude_worst_1_trade",
            rule_type=RULE_EXCLUDE_WORST_N_TRADES,
            threshold_value=1,
        ),
        TradeFilterScenario(
            name="exclude_worst_3_trades",
            rule_type=RULE_EXCLUDE_WORST_N_TRADES,
            threshold_value=3,
        ),
        TradeFilterScenario(
            name="exclude_worst_5_trades",
            rule_type=RULE_EXCLUDE_WORST_N_TRADES,
            threshold_value=5,
        ),
        TradeFilterScenario(
            name="exclude_tokens_losing_more_than_3_usd",
            rule_type=RULE_EXCLUDE_TOKENS_LOSS_ABOVE_USD,
            threshold_value=Decimal("3"),
        ),
        TradeFilterScenario(
            name="exclude_tokens_losing_more_than_5_usd",
            rule_type=RULE_EXCLUDE_TOKENS_LOSS_ABOVE_USD,
            threshold_value=Decimal("5"),
        ),
        TradeFilterScenario(
            name="exclude_tokens_losing_more_than_10_usd",
            rule_type=RULE_EXCLUDE_TOKENS_LOSS_ABOVE_USD,
            threshold_value=Decimal("10"),
        ),
    )


def _run_scenario(
    matched_trades: Sequence[MatchedTradeDiagnostic],
    *,
    scenario: TradeFilterScenario,
    original_realized_pnl_usd: Decimal,
) -> TradeFilterScenarioResult:
    threshold_value = _normalize_threshold_value(scenario)
    excluded_trades, excluded_tokens = _select_excluded_trades(
        matched_trades,
        scenario=scenario,
        threshold_value=threshold_value,
    )
    excluded_trade_keys = {
        (trade.opening_tx_hash, trade.closing_tx_hash, trade.token_address)
        for trade in excluded_trades
    }
    remaining_trades = [
        trade
        for trade in matched_trades
        if (trade.opening_tx_hash, trade.closing_tx_hash, trade.token_address)
        not in excluded_trade_keys
    ]
    filtered_out_realized_pnl_usd = _sum_realized_pnls(excluded_trades)
    new_realized_pnl_usd = _sum_realized_pnls(remaining_trades)

    return TradeFilterScenarioResult(
        scenario_name=scenario.name,
        rule_type=scenario.rule_type,
        threshold_value=threshold_value,
        original_trade_count=len(matched_trades),
        remaining_trade_count=len(remaining_trades),
        filtered_out_trade_count=len(excluded_trades),
        original_realized_pnl_usd=original_realized_pnl_usd,
        filtered_out_realized_pnl_usd=filtered_out_realized_pnl_usd,
        new_realized_pnl_usd=new_realized_pnl_usd,
        delta_vs_original_pnl_usd=new_realized_pnl_usd - original_realized_pnl_usd,
        excluded_tokens=excluded_tokens,
    )


def _select_excluded_trades(
    matched_trades: Sequence[MatchedTradeDiagnostic],
    *,
    scenario: TradeFilterScenario,
    threshold_value: Decimal | int,
) -> tuple[tuple[MatchedTradeDiagnostic, ...], tuple[str, ...]]:
    if scenario.rule_type == RULE_EXCLUDE_HOLD_UNDER_SECONDS:
        if not isinstance(threshold_value, int):
            raise ValueError("Holding-time simulation thresholds must be integers")
        excluded_trades = tuple(
            trade
            for trade in matched_trades
            if trade.holding_duration_seconds < threshold_value
        )
        return excluded_trades, ()

    if scenario.rule_type == RULE_EXCLUDE_COST_BASIS_ABOVE_USD:
        if not isinstance(threshold_value, Decimal):
            raise ValueError("Cost-basis simulation thresholds must be Decimal values")
        excluded_trades = tuple(
            trade
            for trade in matched_trades
            if trade.cost_basis_usd is not None and trade.cost_basis_usd > threshold_value
        )
        return excluded_trades, ()

    if scenario.rule_type == RULE_EXCLUDE_WORST_N_TRADES:
        if not isinstance(threshold_value, int):
            raise ValueError("Worst-trade simulation thresholds must be integers")
        valued_trades = [
            trade for trade in matched_trades if trade.realized_pnl_usd is not None
        ]
        sorted_trades = sorted(
            valued_trades,
            key=lambda trade: (
                trade.realized_pnl_usd,
                trade.close_timestamp,
                trade.open_timestamp,
                trade.closing_tx_hash,
                trade.opening_tx_hash,
                trade.token_address,
            ),
        )
        return tuple(sorted_trades[:threshold_value]), ()

    if scenario.rule_type == RULE_EXCLUDE_TOKENS_LOSS_ABOVE_USD:
        if not isinstance(threshold_value, Decimal):
            raise ValueError("Token-loss simulation thresholds must be Decimal values")
        pnl_by_token: dict[str, Decimal] = defaultdict(lambda: ZERO)
        for trade in matched_trades:
            if trade.realized_pnl_usd is None:
                continue
            pnl_by_token[trade.token_address] += trade.realized_pnl_usd

        excluded_tokens = tuple(
            sorted(
                token_address
                for token_address, realized_pnl_usd in pnl_by_token.items()
                if realized_pnl_usd < -threshold_value
            )
        )
        excluded_trades = tuple(
            trade
            for trade in matched_trades
            if trade.token_address in excluded_tokens
        )
        return excluded_trades, excluded_tokens

    raise ValueError(f"Unsupported simulation rule type: {scenario.rule_type}")


def _normalize_threshold_value(scenario: TradeFilterScenario) -> Decimal | int:
    if scenario.rule_type in {
        RULE_EXCLUDE_HOLD_UNDER_SECONDS,
        RULE_EXCLUDE_WORST_N_TRADES,
    }:
        if not isinstance(scenario.threshold_value, int) or scenario.threshold_value < 0:
            raise ValueError(
                f"Simulation scenario '{scenario.name}' requires a non-negative integer threshold"
            )
        return scenario.threshold_value

    if scenario.rule_type in {
        RULE_EXCLUDE_COST_BASIS_ABOVE_USD,
        RULE_EXCLUDE_TOKENS_LOSS_ABOVE_USD,
    }:
        if isinstance(scenario.threshold_value, Decimal):
            threshold_value = scenario.threshold_value
        elif isinstance(scenario.threshold_value, int):
            threshold_value = Decimal(scenario.threshold_value)
        else:
            raise ValueError(
                f"Simulation scenario '{scenario.name}' requires a Decimal-compatible threshold"
            )
        if threshold_value < ZERO:
            raise ValueError(
                f"Simulation scenario '{scenario.name}' requires a non-negative threshold"
            )
        return threshold_value

    raise ValueError(f"Unsupported simulation rule type: {scenario.rule_type}")


def _select_best_improvement_scenario(
    scenario_results: Sequence[TradeFilterScenarioResult],
) -> TradeFilterScenarioResult | None:
    if not scenario_results:
        return None
    return max(
        scenario_results,
        key=lambda item: (
            item.delta_vs_original_pnl_usd,
            -item.filtered_out_trade_count,
            item.scenario_name,
        ),
    )


def _sum_realized_pnls(matched_trades: Sequence[MatchedTradeDiagnostic]) -> Decimal:
    return sum(
        (
            trade.realized_pnl_usd
            for trade in matched_trades
            if trade.realized_pnl_usd is not None
        ),
        ZERO,
    )
