"""Tests for portfolio-subset behavior, simulation, and rules aggregation."""

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

from analytics.manifest_portfolio import (  # noqa: E402
    PortfolioWalletSummary,
    STATUS_INCLUDED_COMPLETE,
    build_manifest_portfolio_report,
)
from analytics.portfolio_subset_diagnostics import (  # noqa: E402
    build_portfolio_subset_behavior_report,
    build_portfolio_subset_rules_report,
    build_portfolio_subset_simulation_report,
    prepare_portfolio_subset_wallet_diagnostics,
)
from analytics.trade_diagnostics import (  # noqa: E402
    MatchedTradeDiagnostic,
    TradeDiagnosticReport,
    summarize_trade_diagnostic_report,
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


def build_trade_report(
    trades: tuple[MatchedTradeDiagnostic, ...],
) -> TradeDiagnosticReport:
    return TradeDiagnosticReport(
        matched_trades=trades,
        summary=summarize_trade_diagnostic_report(trades),
    )


def build_wallet_summary(
    *,
    wallet: str,
    label: str,
    group: str | None,
    trade_report: TradeDiagnosticReport,
) -> PortfolioWalletSummary:
    realized_pnl_usd = sum(
        (
            trade.realized_pnl_usd
            for trade in trade_report.matched_trades
            if trade.realized_pnl_usd is not None
        ),
        Decimal("0"),
    )
    behavior_summary = build_wallet_behavior_report(trade_report.matched_trades).summary
    matched_trade_count = trade_report.summary.total_matched_trades
    return PortfolioWalletSummary(
        wallet=wallet,
        label=label,
        group=group,
        chain="solana",
        status=STATUS_INCLUDED_COMPLETE,
        included_in_aggregate=True,
        message=None,
        source_path=None,
        analysis_summary_path=None,
        realized_pnl_usd=realized_pnl_usd,
        matched_trade_count=matched_trade_count,
        winners_count=trade_report.summary.winners_count,
        losers_count=trade_report.summary.losers_count,
        win_rate=(
            Decimal(trade_report.summary.winners_count) / Decimal(matched_trade_count)
            if matched_trade_count > 0
            else None
        ),
        holding_time_buckets=behavior_summary.holding_time_buckets,
        token_pnl=trade_report.summary.pnl_by_token,
        top_losing_tokens=tuple(
            item for item in trade_report.summary.pnl_by_token if item.realized_pnl_usd < 0
        )[:5],
        unsupported_transactions_count=0,
        rows_requiring_valuation_after_count=0,
        unsupported_fifo_transactions_count=0,
        skipped_missing_valuation_count=0,
        unsupported_patterns=(),
    )


def build_fixture_inputs():
    wallet_a_trades = (
        build_trade(
            token_address="TokenA",
            opening_tx_hash="a-open-1",
            closing_tx_hash="a-close-1",
            close_timestamp=datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc),
            holding_duration_seconds=10,
            cost_basis_usd="12",
            realized_pnl_usd="-5",
        ),
        build_trade(
            token_address="TokenB",
            opening_tx_hash="a-open-2",
            closing_tx_hash="a-close-2",
            close_timestamp=datetime(2025, 1, 1, 12, 1, tzinfo=timezone.utc),
            holding_duration_seconds=40,
            cost_basis_usd="8",
            realized_pnl_usd="-4",
        ),
        build_trade(
            token_address="TokenC",
            opening_tx_hash="a-open-3",
            closing_tx_hash="a-close-3",
            close_timestamp=datetime(2025, 1, 1, 12, 20, tzinfo=timezone.utc),
            holding_duration_seconds=600,
            cost_basis_usd="2",
            realized_pnl_usd="2",
        ),
    )
    wallet_b_trades = (
        build_trade(
            token_address="TokenA",
            opening_tx_hash="b-open-1",
            closing_tx_hash="b-close-1",
            close_timestamp=datetime(2025, 1, 1, 12, 5, tzinfo=timezone.utc),
            holding_duration_seconds=4000,
            cost_basis_usd="6",
            realized_pnl_usd="-7",
        ),
        build_trade(
            token_address="TokenD",
            opening_tx_hash="b-open-2",
            closing_tx_hash="b-close-2",
            close_timestamp=datetime(2025, 1, 1, 12, 40, tzinfo=timezone.utc),
            holding_duration_seconds=1200,
            cost_basis_usd="7",
            realized_pnl_usd="1",
        ),
    )
    wallet_a_report = build_trade_report(wallet_a_trades)
    wallet_b_report = build_trade_report(wallet_b_trades)
    wallet_diagnostics = (
        prepare_portfolio_subset_wallet_diagnostics(
            wallet="wallet-a",
            label="Alpha",
            group="Recent",
            trade_report=wallet_a_report,
        ),
        prepare_portfolio_subset_wallet_diagnostics(
            wallet="wallet-b",
            label="Beta",
            group="Recent",
            trade_report=wallet_b_report,
        ),
    )
    wallet_summaries = (
        build_wallet_summary(
            wallet="wallet-a",
            label="Alpha",
            group="Recent",
            trade_report=wallet_a_report,
        ),
        build_wallet_summary(
            wallet="wallet-b",
            label="Beta",
            group="Recent",
            trade_report=wallet_b_report,
        ),
    )
    portfolio_report = build_manifest_portfolio_report(wallet_summaries)
    return wallet_diagnostics, portfolio_report


class PortfolioSubsetDiagnosticsTests(unittest.TestCase):
    def test_behavior_report_aggregates_subset_trade_metrics(self) -> None:
        wallet_diagnostics, portfolio_report = build_fixture_inputs()

        report = build_portfolio_subset_behavior_report(
            wallet_diagnostics,
            portfolio_report=portfolio_report,
        )

        self.assertEqual(report.summary.included_wallet_count, 2)
        self.assertEqual(report.summary.total_matched_trades, 5)
        self.assertEqual(report.summary.aggregate_realized_pnl_usd, Decimal("-13"))
        self.assertEqual(report.summary.winners_count, 2)
        self.assertEqual(report.summary.losers_count, 3)
        self.assertEqual(report.summary.longest_losing_streak_overall, 3)
        self.assertEqual(report.summary.streaks_by_wallet[0].label, "Alpha")
        self.assertEqual(report.summary.streaks_by_wallet[0].longest_losing_streak, 2)
        self.assertEqual(
            [
                (bucket.bucket, bucket.trade_count, bucket.total_pnl_usd)
                for bucket in report.summary.holding_time_buckets
            ],
            [
                ("lt_30s", 1, Decimal("-5")),
                ("30s_to_lt_60s", 1, Decimal("-4")),
                ("1m_to_lt_5m", 0, Decimal("0")),
                ("5m_to_lt_1h", 2, Decimal("3")),
                ("1h_to_lt_24h", 1, Decimal("-7")),
                ("ge_24h", 0, Decimal("0")),
            ],
        )
        self.assertEqual(
            report.summary.loss_concentration_by_wallet[0].label,
            "Alpha",
        )
        self.assertEqual(
            report.summary.loss_concentration_by_token[0].token_address,
            "TokenA",
        )
        self.assertEqual(len(report.trade_rows), 5)

    def test_simulation_report_recomputes_subset_scenarios(self) -> None:
        wallet_diagnostics, _ = build_fixture_inputs()

        report = build_portfolio_subset_simulation_report(wallet_diagnostics)

        self.assertEqual(report.summary.original_trade_count, 5)
        self.assertEqual(report.summary.original_realized_pnl_usd, Decimal("-13"))
        self.assertEqual(
            report.summary.best_improvement_scenario_name,
            "exclude_worst_3_trades",
        )
        self.assertEqual(report.summary.best_improvement_delta_usd, Decimal("16"))
        self.assertEqual(report.summary.best_improvement_new_realized_pnl_usd, Decimal("3"))

    def test_rules_report_ranks_portfolio_candidates_and_includes_subset_caveat(self) -> None:
        wallet_diagnostics, portfolio_report = build_fixture_inputs()
        behavior_report = build_portfolio_subset_behavior_report(
            wallet_diagnostics,
            portfolio_report=portfolio_report,
        )
        simulation_report = build_portfolio_subset_simulation_report(wallet_diagnostics)

        report = build_portfolio_subset_rules_report(
            wallet_diagnostics,
            portfolio_behavior_report=behavior_report,
            portfolio_simulation_report=simulation_report,
        )

        self.assertEqual(
            [item.category for item in report.top_candidate_rules],
            [
                "token/setup blacklist threshold",
                "max cost basis",
                "hold-time floor",
            ],
        )
        self.assertEqual(report.top_candidate_rules[0].affected_wallet_count, 2)
        self.assertEqual(report.top_candidate_rules[1].affected_wallet_count, 2)
        self.assertEqual(report.top_candidate_rules[2].affected_wallet_count, 1)
        self.assertEqual(report.top_leak_patterns[0].affected_wallet_count, 2)
        self.assertTrue(
            any("subset" in item.lower() or "full wallet manifest" in item.lower() for item in report.caution_notes)
        )


if __name__ == "__main__":
    unittest.main()
