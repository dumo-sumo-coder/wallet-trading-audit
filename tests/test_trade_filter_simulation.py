"""Tests for matched-trade filter simulation scenarios."""

from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from analytics.trade_diagnostics import MatchedTradeDiagnostic  # noqa: E402
from analytics.trade_filter_simulation import (  # noqa: E402
    RULE_EXCLUDE_COST_BASIS_ABOVE_USD,
    RULE_EXCLUDE_HOLD_UNDER_SECONDS,
    RULE_EXCLUDE_TOKENS_LOSS_ABOVE_USD,
    RULE_EXCLUDE_WORST_N_TRADES,
    TradeFilterScenario,
    build_trade_filter_simulation_report,
)


def build_trade(
    *,
    token_address: str,
    opening_tx_hash: str,
    closing_tx_hash: str,
    close_timestamp: datetime,
    holding_duration_seconds: int,
    cost_basis_usd: str,
    realized_pnl_usd: str,
) -> MatchedTradeDiagnostic:
    cost_basis = Decimal(cost_basis_usd)
    realized_pnl = Decimal(realized_pnl_usd)
    return MatchedTradeDiagnostic(
        token_address=token_address,
        opening_tx_hash=opening_tx_hash,
        closing_tx_hash=closing_tx_hash,
        open_timestamp=close_timestamp - timedelta(seconds=holding_duration_seconds),
        close_timestamp=close_timestamp,
        holding_duration_seconds=holding_duration_seconds,
        quantity_matched=Decimal("1"),
        cost_basis_usd=cost_basis,
        proceeds_usd=cost_basis + realized_pnl,
        realized_pnl_usd=realized_pnl,
        opening_fee_native=None,
        opening_fee_usd=None,
        closing_fee_native=None,
        closing_fee_usd=None,
    )


def build_mixed_trades() -> tuple[MatchedTradeDiagnostic, ...]:
    return (
        build_trade(
            token_address="TokenA",
            opening_tx_hash="open-1",
            closing_tx_hash="close-1",
            close_timestamp=datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc),
            holding_duration_seconds=10,
            cost_basis_usd="0.5",
            realized_pnl_usd="-5",
        ),
        build_trade(
            token_address="TokenB",
            opening_tx_hash="open-2",
            closing_tx_hash="close-2",
            close_timestamp=datetime(2025, 1, 1, 12, 2, tzinfo=timezone.utc),
            holding_duration_seconds=40,
            cost_basis_usd="2",
            realized_pnl_usd="-3",
        ),
        build_trade(
            token_address="TokenC",
            opening_tx_hash="open-3",
            closing_tx_hash="close-3",
            close_timestamp=datetime(2025, 1, 1, 12, 10, tzinfo=timezone.utc),
            holding_duration_seconds=240,
            cost_basis_usd="8",
            realized_pnl_usd="4",
        ),
        build_trade(
            token_address="TokenD",
            opening_tx_hash="open-4",
            closing_tx_hash="close-4",
            close_timestamp=datetime(2025, 1, 2, 13, 0, tzinfo=timezone.utc),
            holding_duration_seconds=4000,
            cost_basis_usd="20",
            realized_pnl_usd="7",
        ),
        build_trade(
            token_address="TokenA",
            opening_tx_hash="open-5",
            closing_tx_hash="close-5",
            close_timestamp=datetime(2025, 1, 2, 14, 0, tzinfo=timezone.utc),
            holding_duration_seconds=90000,
            cost_basis_usd="30",
            realized_pnl_usd="-2",
        ),
    )


class TradeFilterSimulationTests(unittest.TestCase):
    def test_hold_time_filter_scenarios_recompute_pnl(self) -> None:
        report = build_trade_filter_simulation_report(
            build_mixed_trades(),
            scenarios=(
                TradeFilterScenario(
                    name="exclude_under_30s",
                    rule_type=RULE_EXCLUDE_HOLD_UNDER_SECONDS,
                    threshold_value=30,
                ),
                TradeFilterScenario(
                    name="exclude_under_60s",
                    rule_type=RULE_EXCLUDE_HOLD_UNDER_SECONDS,
                    threshold_value=60,
                ),
                TradeFilterScenario(
                    name="exclude_cost_basis_above_10",
                    rule_type=RULE_EXCLUDE_COST_BASIS_ABOVE_USD,
                    threshold_value=Decimal("10"),
                ),
            ),
        )

        self.assertEqual(report.summary.original_trade_count, 5)
        self.assertEqual(report.summary.original_realized_pnl_usd, Decimal("1"))
        self.assertEqual(report.summary.best_improvement_scenario_name, "exclude_under_60s")
        self.assertEqual(report.summary.best_improvement_delta_usd, Decimal("8"))
        self.assertEqual(report.summary.best_improvement_new_realized_pnl_usd, Decimal("9"))
        self.assertEqual(
            [
                (
                    item.scenario_name,
                    item.filtered_out_trade_count,
                    item.remaining_trade_count,
                    item.new_realized_pnl_usd,
                    item.delta_vs_original_pnl_usd,
                )
                for item in report.summary.scenario_results
            ],
            [
                ("exclude_under_30s", 1, 4, Decimal("6"), Decimal("5")),
                ("exclude_under_60s", 2, 3, Decimal("9"), Decimal("8")),
                ("exclude_cost_basis_above_10", 2, 3, Decimal("-4"), Decimal("-5")),
            ],
        )

    def test_worst_trade_exclusion_scenarios_are_deterministic(self) -> None:
        report = build_trade_filter_simulation_report(
            build_mixed_trades(),
            scenarios=(
                TradeFilterScenario(
                    name="exclude_worst_two",
                    rule_type=RULE_EXCLUDE_WORST_N_TRADES,
                    threshold_value=2,
                ),
            ),
        )

        result = report.summary.scenario_results[0]
        self.assertEqual(result.filtered_out_trade_count, 2)
        self.assertEqual(result.filtered_out_realized_pnl_usd, Decimal("-8"))
        self.assertEqual(result.new_realized_pnl_usd, Decimal("9"))
        self.assertEqual(result.delta_vs_original_pnl_usd, Decimal("8"))
        self.assertEqual(report.summary.best_improvement_scenario_name, "exclude_worst_two")

    def test_token_loss_threshold_exclusion_filters_losing_tokens(self) -> None:
        report = build_trade_filter_simulation_report(
            build_mixed_trades(),
            scenarios=(
                TradeFilterScenario(
                    name="exclude_tokens_losing_more_than_4",
                    rule_type=RULE_EXCLUDE_TOKENS_LOSS_ABOVE_USD,
                    threshold_value=Decimal("4"),
                ),
                TradeFilterScenario(
                    name="exclude_tokens_losing_more_than_10",
                    rule_type=RULE_EXCLUDE_TOKENS_LOSS_ABOVE_USD,
                    threshold_value=Decimal("10"),
                ),
            ),
        )

        first_result, second_result = report.summary.scenario_results
        self.assertEqual(first_result.excluded_tokens, ("TokenA",))
        self.assertEqual(first_result.filtered_out_trade_count, 2)
        self.assertEqual(first_result.new_realized_pnl_usd, Decimal("8"))
        self.assertEqual(first_result.delta_vs_original_pnl_usd, Decimal("7"))
        self.assertEqual(second_result.excluded_tokens, ())
        self.assertEqual(second_result.filtered_out_trade_count, 0)
        self.assertEqual(second_result.new_realized_pnl_usd, Decimal("1"))
        self.assertEqual(second_result.delta_vs_original_pnl_usd, Decimal("0"))

    def test_simulation_is_safe_on_empty_trade_sets(self) -> None:
        report = build_trade_filter_simulation_report(
            (),
            scenarios=(
                TradeFilterScenario(
                    name="exclude_under_30s",
                    rule_type=RULE_EXCLUDE_HOLD_UNDER_SECONDS,
                    threshold_value=30,
                ),
            ),
        )

        self.assertEqual(report.summary.original_trade_count, 0)
        self.assertEqual(report.summary.original_realized_pnl_usd, Decimal("0"))
        self.assertEqual(report.summary.scenario_results[0].filtered_out_trade_count, 0)
        self.assertEqual(report.summary.scenario_results[0].remaining_trade_count, 0)
        self.assertEqual(report.summary.scenario_results[0].new_realized_pnl_usd, Decimal("0"))
        self.assertEqual(report.summary.best_improvement_delta_usd, Decimal("0"))


if __name__ == "__main__":
    unittest.main()
