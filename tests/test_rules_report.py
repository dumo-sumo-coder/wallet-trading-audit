"""Tests for wallet rules coaching recommendations."""

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

from analytics.rules_report import (  # noqa: E402
    build_wallet_rules_report,
    render_wallet_rules_markdown,
)
from analytics.trade_diagnostics import MatchedTradeDiagnostic  # noqa: E402
from analytics.trade_filter_simulation import (  # noqa: E402
    RULE_EXCLUDE_COST_BASIS_ABOVE_USD,
    RULE_EXCLUDE_HOLD_UNDER_SECONDS,
    RULE_EXCLUDE_TOKENS_LOSS_ABOVE_USD,
    RULE_EXCLUDE_WORST_N_TRADES,
    TradeFilterScenarioResult,
    TradeFilterSimulationSummary,
)
from analytics.wallet_behavior import build_wallet_behavior_report  # noqa: E402


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


def build_behavior_summary():
    trades = (
        build_trade(
            token_address="TokenA",
            opening_tx_hash="open-1",
            closing_tx_hash="close-1",
            close_timestamp=datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc),
            holding_duration_seconds=10,
            cost_basis_usd="12",
            realized_pnl_usd="-5",
        ),
        build_trade(
            token_address="TokenB",
            opening_tx_hash="open-2",
            closing_tx_hash="close-2",
            close_timestamp=datetime(2025, 1, 1, 12, 1, tzinfo=timezone.utc),
            holding_duration_seconds=40,
            cost_basis_usd="8",
            realized_pnl_usd="-4",
        ),
        build_trade(
            token_address="TokenA",
            opening_tx_hash="open-3",
            closing_tx_hash="close-3",
            close_timestamp=datetime(2025, 1, 1, 12, 4, tzinfo=timezone.utc),
            holding_duration_seconds=120,
            cost_basis_usd="6",
            realized_pnl_usd="-3",
        ),
        build_trade(
            token_address="TokenC",
            opening_tx_hash="open-4",
            closing_tx_hash="close-4",
            close_timestamp=datetime(2025, 1, 1, 12, 20, tzinfo=timezone.utc),
            holding_duration_seconds=600,
            cost_basis_usd="2",
            realized_pnl_usd="2",
        ),
        build_trade(
            token_address="TokenD",
            opening_tx_hash="open-5",
            closing_tx_hash="close-5",
            close_timestamp=datetime(2025, 1, 1, 13, 0, tzinfo=timezone.utc),
            holding_duration_seconds=1200,
            cost_basis_usd="1",
            realized_pnl_usd="1",
        ),
    )
    return build_wallet_behavior_report(trades).summary


def build_simulation_summary() -> TradeFilterSimulationSummary:
    return TradeFilterSimulationSummary(
        original_trade_count=5,
        original_realized_pnl_usd=Decimal("-9"),
        scenario_results=(
            TradeFilterScenarioResult(
                scenario_name="exclude_hold_under_30s",
                rule_type=RULE_EXCLUDE_HOLD_UNDER_SECONDS,
                threshold_value=30,
                original_trade_count=5,
                remaining_trade_count=4,
                filtered_out_trade_count=1,
                original_realized_pnl_usd=Decimal("-9"),
                filtered_out_realized_pnl_usd=Decimal("-5"),
                new_realized_pnl_usd=Decimal("-4"),
                delta_vs_original_pnl_usd=Decimal("5"),
                excluded_tokens=(),
            ),
            TradeFilterScenarioResult(
                scenario_name="exclude_hold_under_5m",
                rule_type=RULE_EXCLUDE_HOLD_UNDER_SECONDS,
                threshold_value=300,
                original_trade_count=5,
                remaining_trade_count=2,
                filtered_out_trade_count=3,
                original_realized_pnl_usd=Decimal("-9"),
                filtered_out_realized_pnl_usd=Decimal("-12"),
                new_realized_pnl_usd=Decimal("3"),
                delta_vs_original_pnl_usd=Decimal("12"),
                excluded_tokens=(),
            ),
            TradeFilterScenarioResult(
                scenario_name="exclude_cost_basis_above_5_usd",
                rule_type=RULE_EXCLUDE_COST_BASIS_ABOVE_USD,
                threshold_value=Decimal("5"),
                original_trade_count=5,
                remaining_trade_count=2,
                filtered_out_trade_count=3,
                original_realized_pnl_usd=Decimal("-9"),
                filtered_out_realized_pnl_usd=Decimal("-20"),
                new_realized_pnl_usd=Decimal("11"),
                delta_vs_original_pnl_usd=Decimal("20"),
                excluded_tokens=(),
            ),
            TradeFilterScenarioResult(
                scenario_name="exclude_worst_3_trades",
                rule_type=RULE_EXCLUDE_WORST_N_TRADES,
                threshold_value=3,
                original_trade_count=5,
                remaining_trade_count=2,
                filtered_out_trade_count=3,
                original_realized_pnl_usd=Decimal("-9"),
                filtered_out_realized_pnl_usd=Decimal("-18"),
                new_realized_pnl_usd=Decimal("9"),
                delta_vs_original_pnl_usd=Decimal("18"),
                excluded_tokens=(),
            ),
            TradeFilterScenarioResult(
                scenario_name="exclude_tokens_losing_more_than_3_usd",
                rule_type=RULE_EXCLUDE_TOKENS_LOSS_ABOVE_USD,
                threshold_value=Decimal("3"),
                original_trade_count=5,
                remaining_trade_count=2,
                filtered_out_trade_count=3,
                original_realized_pnl_usd=Decimal("-9"),
                filtered_out_realized_pnl_usd=Decimal("-30"),
                new_realized_pnl_usd=Decimal("21"),
                delta_vs_original_pnl_usd=Decimal("30"),
                excluded_tokens=("TokenA", "TokenB"),
            ),
        ),
        best_improvement_scenario_name="exclude_tokens_losing_more_than_3_usd",
        best_improvement_delta_usd=Decimal("30"),
        best_improvement_new_realized_pnl_usd=Decimal("21"),
    )


class WalletRulesReportTests(unittest.TestCase):
    def test_build_wallet_rules_report_ranks_candidate_rules_by_improvement(self) -> None:
        report = build_wallet_rules_report(
            build_behavior_summary(),
            build_simulation_summary(),
        )

        self.assertEqual(
            [item.category for item in report.top_candidate_rules],
            [
                "token/setup blacklist threshold",
                "max cost basis",
                "hold-time floor",
            ],
        )
        self.assertEqual(
            [item.scenario_name for item in report.top_candidate_rules],
            [
                "exclude_tokens_losing_more_than_3_usd",
                "exclude_cost_basis_above_5_usd",
                "exclude_hold_under_5m",
            ],
        )
        self.assertEqual(
            [item.estimated_pnl_improvement_usd for item in report.top_candidate_rules],
            [Decimal("30"), Decimal("20"), Decimal("12")],
        )
        self.assertNotIn(
            "exclude_worst_3_trades",
            [item.scenario_name for item in report.top_candidate_rules],
        )

    def test_build_wallet_rules_report_includes_leak_patterns_and_overfitting_caveats(self) -> None:
        report = build_wallet_rules_report(
            build_behavior_summary(),
            build_simulation_summary(),
        )

        self.assertEqual(
            [item.category for item in report.top_leak_patterns],
            [
                "token/setup blacklist threshold",
                "max cost basis",
                "hold-time floor",
            ],
        )
        self.assertIn("TokenA", report.top_leak_patterns[0].evidence)
        self.assertEqual(len(report.next_test_rule_categories), 1)
        self.assertIn("losing-streak stop", report.next_test_rule_categories[0])
        self.assertTrue(
            any("overfit" in item or "one wallet" in item for item in report.caution_notes)
        )

    def test_render_wallet_rules_markdown_contains_ranked_rules_and_cautions(self) -> None:
        report = build_wallet_rules_report(
            build_behavior_summary(),
            build_simulation_summary(),
        )

        rendered = render_wallet_rules_markdown(report)

        self.assertIn("# Wallet Rules Coaching Report", rendered)
        self.assertIn("Blacklist tokens after cumulative matched-trade loss exceeds $3", rendered)
        self.assertIn("Avoid entries above $5 cost basis", rendered)
        self.assertIn("Require a minimum hold time of 5m", rendered)
        self.assertIn("overfit", rendered.lower())


if __name__ == "__main__":
    unittest.main()
