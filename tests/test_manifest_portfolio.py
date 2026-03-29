"""Tests for manifest-level portfolio aggregation."""

from __future__ import annotations

import sys
import unittest
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from analytics.manifest_portfolio import (  # noqa: E402
    STATUS_EXCLUDED_NOT_MEANINGFUL,
    STATUS_INCLUDED_COMPLETE,
    STATUS_INCLUDED_SUPPORTED_SUBSET,
    PortfolioWalletSummary,
    UnsupportedCasePatternCount,
    build_manifest_portfolio_report,
)
from analytics.trade_diagnostics import TokenPnlDiagnostic  # noqa: E402
from analytics.wallet_behavior import HoldingTimeBucketDiagnostic  # noqa: E402


def build_wallet_summary(
    *,
    wallet: str,
    label: str,
    status: str,
    included_in_aggregate: bool,
    realized_pnl_usd: str | None,
    win_rate: str | None,
    matched_trade_count: int,
    token_pnl: tuple[tuple[str, str], ...],
    unsupported_patterns: tuple[tuple[str, str, int], ...] = (),
) -> PortfolioWalletSummary:
    pnl_value = Decimal(realized_pnl_usd) if realized_pnl_usd is not None else None
    win_rate_value = Decimal(win_rate) if win_rate is not None else None
    token_diagnostics = tuple(
        TokenPnlDiagnostic(
            token_address=token_address,
            matched_trades=1,
            realized_pnl_usd=Decimal(token_pnl_usd),
        )
        for token_address, token_pnl_usd in token_pnl
    )
    top_losers = tuple(item for item in token_diagnostics if item.realized_pnl_usd < 0)[:5]
    return PortfolioWalletSummary(
        wallet=wallet,
        label=label,
        group="Desk",
        chain="solana",
        status=status,
        included_in_aggregate=included_in_aggregate,
        message=None,
        source_path=f"data/raw/solana/{label}/wallet_snapshot.json",
        analysis_summary_path=f"data/raw/solana/{label}/wallet_analysis_summary.json",
        realized_pnl_usd=pnl_value,
        matched_trade_count=matched_trade_count,
        winners_count=1 if pnl_value is not None and pnl_value > 0 else 0,
        losers_count=1 if pnl_value is not None and pnl_value < 0 else 0,
        win_rate=win_rate_value,
        holding_time_buckets=(
            HoldingTimeBucketDiagnostic(
                bucket="1m_to_lt_5m",
                trade_count=matched_trade_count,
                total_pnl_usd=pnl_value or Decimal("0"),
                avg_pnl_usd=pnl_value,
            ),
        ),
        token_pnl=token_diagnostics,
        top_losing_tokens=top_losers,
        unsupported_transactions_count=0,
        rows_requiring_valuation_after_count=0,
        unsupported_fifo_transactions_count=0,
        skipped_missing_valuation_count=0,
        unsupported_patterns=tuple(
            UnsupportedCasePatternCount(
                pattern_key=pattern_key,
                label=label,
                count=count,
            )
            for pattern_key, label, count in unsupported_patterns
        ),
    )


class ManifestPortfolioAggregationTests(unittest.TestCase):
    def test_build_manifest_portfolio_report_aggregates_rankings_and_concentrations(self) -> None:
        report = build_manifest_portfolio_report(
            (
                build_wallet_summary(
                    wallet="WalletA",
                    label="Alpha",
                    status=STATUS_INCLUDED_COMPLETE,
                    included_in_aggregate=True,
                    realized_pnl_usd="10",
                    win_rate="0.75",
                    matched_trade_count=4,
                    token_pnl=(("TokenWin", "10"), ("TokenSmallLoss", "-1")),
                ),
                build_wallet_summary(
                    wallet="WalletB",
                    label="Beta",
                    status=STATUS_INCLUDED_SUPPORTED_SUBSET,
                    included_in_aggregate=True,
                    realized_pnl_usd="-15",
                    win_rate="0.25",
                    matched_trade_count=6,
                    token_pnl=(("TokenLoss", "-12"), ("TokenShared", "-3")),
                ),
                build_wallet_summary(
                    wallet="WalletC",
                    label="Gamma",
                    status=STATUS_EXCLUDED_NOT_MEANINGFUL,
                    included_in_aggregate=False,
                    realized_pnl_usd=None,
                    win_rate=None,
                    matched_trade_count=0,
                    token_pnl=(),
                ),
            )
        )

        self.assertEqual(report.summary.analyzed_wallet_count, 3)
        self.assertEqual(report.summary.included_wallet_count, 2)
        self.assertEqual(report.summary.included_complete_wallet_count, 1)
        self.assertEqual(report.summary.included_supported_subset_wallet_count, 1)
        self.assertEqual(report.summary.excluded_wallet_count, 1)
        self.assertEqual(report.summary.aggregate_realized_pnl_usd, Decimal("-5"))
        self.assertEqual(report.summary.aggregate_matched_trade_count, 10)
        self.assertEqual(report.summary.best_wallets_by_pnl[0].label, "Alpha")
        self.assertEqual(report.summary.worst_wallets_by_pnl[0].label, "Beta")
        self.assertEqual(report.summary.best_wallets_by_win_rate[0].label, "Alpha")
        self.assertEqual(report.summary.worst_wallets_by_win_rate[0].label, "Beta")
        self.assertEqual(report.summary.loss_concentration_by_wallet[0].label, "Beta")
        self.assertEqual(
            report.summary.loss_concentration_by_wallet[0].loss_contribution_pct,
            Decimal("1"),
        )
        self.assertEqual(report.summary.loss_concentration_by_token[0].token_address, "TokenLoss")
        self.assertEqual(
            report.summary.loss_concentration_by_token[0].loss_contribution_pct,
            Decimal("0.75"),
        )
        self.assertEqual(report.summary.unsupported_patterns_across_wallets, ())

    def test_build_manifest_portfolio_report_is_safe_when_every_wallet_is_excluded(self) -> None:
        report = build_manifest_portfolio_report(
            (
                build_wallet_summary(
                    wallet="WalletX",
                    label="Excluded",
                    status=STATUS_EXCLUDED_NOT_MEANINGFUL,
                    included_in_aggregate=False,
                    realized_pnl_usd=None,
                    win_rate=None,
                    matched_trade_count=0,
                    token_pnl=(),
                ),
            )
        )

        self.assertEqual(report.summary.included_wallet_count, 0)
        self.assertEqual(report.summary.aggregate_realized_pnl_usd, Decimal("0"))
        self.assertEqual(report.summary.best_wallets_by_pnl, ())
        self.assertEqual(report.summary.worst_wallets_by_win_rate, ())
        self.assertEqual(report.summary.loss_concentration_by_wallet, ())
        self.assertEqual(report.summary.loss_concentration_by_token, ())
        self.assertEqual(report.summary.unsupported_patterns_across_wallets, ())

    def test_build_manifest_portfolio_report_groups_unsupported_patterns_across_wallets(self) -> None:
        report = build_manifest_portfolio_report(
            (
                build_wallet_summary(
                    wallet="WalletA",
                    label="Alpha",
                    status=STATUS_EXCLUDED_NOT_MEANINGFUL,
                    included_in_aggregate=False,
                    realized_pnl_usd=None,
                    win_rate=None,
                    matched_trade_count=0,
                    token_pnl=(),
                    unsupported_patterns=(
                        ("multiple_token_deltas", "multiple_token_deltas_or_multi_leg", 5),
                    ),
                ),
                build_wallet_summary(
                    wallet="WalletB",
                    label="Beta",
                    status=STATUS_EXCLUDED_NOT_MEANINGFUL,
                    included_in_aggregate=False,
                    realized_pnl_usd=None,
                    win_rate=None,
                    matched_trade_count=0,
                    token_pnl=(),
                    unsupported_patterns=(
                        ("multiple_token_deltas", "multiple_token_deltas_or_multi_leg", 3),
                        ("failed_transactions", "failed_transactions", 2),
                    ),
                ),
            )
        )

        self.assertEqual(
            report.summary.unsupported_patterns_across_wallets[0].pattern_key,
            "multiple_token_deltas",
        )
        self.assertEqual(report.summary.unsupported_patterns_across_wallets[0].total_count, 8)
        self.assertEqual(report.summary.unsupported_patterns_across_wallets[0].affected_wallets, 2)


if __name__ == "__main__":
    unittest.main()
