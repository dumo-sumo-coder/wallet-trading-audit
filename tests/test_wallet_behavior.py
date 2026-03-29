"""Tests for wallet behavior diagnostics derived from matched trades."""

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
    return MatchedTradeDiagnostic(
        token_address=token_address,
        opening_tx_hash=opening_tx_hash,
        closing_tx_hash=closing_tx_hash,
        open_timestamp=close_timestamp - timedelta(seconds=holding_duration_seconds),
        close_timestamp=close_timestamp,
        holding_duration_seconds=holding_duration_seconds,
        quantity_matched=Decimal("1"),
        cost_basis_usd=Decimal(cost_basis_usd),
        proceeds_usd=Decimal(cost_basis_usd) + Decimal(realized_pnl_usd),
        realized_pnl_usd=Decimal(realized_pnl_usd),
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


class WalletBehaviorTests(unittest.TestCase):
    def test_build_wallet_behavior_report_aggregates_holding_time_and_daily_metrics(self) -> None:
        report = build_wallet_behavior_report(build_mixed_trades())

        self.assertEqual(report.summary.total_matched_trades, 5)
        self.assertEqual(
            [
                (bucket.bucket, bucket.trade_count, bucket.total_pnl_usd, bucket.avg_pnl_usd)
                for bucket in report.summary.holding_time_buckets
            ],
            [
                ("lt_30s", 1, Decimal("-5"), Decimal("-5")),
                ("30s_to_lt_60s", 1, Decimal("-3"), Decimal("-3")),
                ("1m_to_lt_5m", 1, Decimal("4"), Decimal("4")),
                ("5m_to_lt_1h", 0, Decimal("0"), None),
                ("1h_to_lt_24h", 1, Decimal("7"), Decimal("7")),
                ("ge_24h", 1, Decimal("-2"), Decimal("-2")),
            ],
        )
        self.assertEqual(
            [
                (day.day, day.trade_count, day.realized_pnl_usd, day.win_rate)
                for day in report.summary.pnl_by_day
            ],
            [
                ("2025-01-01", 3, Decimal("-4"), Decimal("0.3333333333333333333333333333")),
                ("2025-01-02", 2, Decimal("5"), Decimal("0.5")),
            ],
        )
        self.assertEqual(
            [
                (item.bucket, item.trade_count, item.pnl_contribution_usd)
                for item in report.summary.fast_rotation_diagnostics
            ],
            [
                ("under_30s", 1, Decimal("-5")),
                ("under_60s", 2, Decimal("-8")),
                ("under_5m", 3, Decimal("-4")),
            ],
        )

    def test_build_wallet_behavior_report_computes_streak_and_notional_diagnostics(self) -> None:
        report = build_wallet_behavior_report(build_mixed_trades())
        streaks = report.summary.streak_diagnostics
        notional = report.summary.notional_diagnostics

        self.assertEqual(streaks.longest_losing_streak, 2)
        self.assertEqual(streaks.longest_winning_streak, 2)
        self.assertEqual(streaks.pnl_after_prior_loss_usd, Decimal("1"))
        self.assertEqual(streaks.pnl_after_prior_win_usd, Decimal("5"))
        self.assertEqual(streaks.avg_pnl_after_prior_loss_usd, Decimal("0.5"))
        self.assertEqual(streaks.avg_pnl_after_prior_win_usd, Decimal("2.5"))

        self.assertEqual(notional.average_cost_basis_usd, Decimal("12.1"))
        self.assertEqual(notional.median_cost_basis_usd, Decimal("8"))
        self.assertEqual(
            [
                (bucket.bucket, bucket.trade_count, bucket.total_pnl_usd)
                for bucket in notional.cost_basis_buckets
            ],
            [
                ("lt_1_usd", 1, Decimal("-5")),
                ("1_to_lt_5_usd", 1, Decimal("-3")),
                ("5_to_lt_10_usd", 1, Decimal("4")),
                ("10_to_lt_25_usd", 1, Decimal("7")),
                ("25_to_lt_100_usd", 1, Decimal("-2")),
                ("ge_100_usd", 0, Decimal("0")),
            ],
        )

    def test_build_wallet_behavior_report_computes_concentration_diagnostics(self) -> None:
        report = build_wallet_behavior_report(build_mixed_trades())
        concentration = report.summary.concentration_diagnostics

        self.assertEqual(concentration.top_5_losing_tokens_contribution_pct, Decimal("1"))
        self.assertEqual(concentration.top_5_winning_tokens_contribution_pct, Decimal("1"))
        self.assertEqual(concentration.worst_20_percent_trades_count, 1)
        self.assertEqual(concentration.worst_20_percent_trades_pnl_usd, Decimal("-5"))
        self.assertEqual(concentration.worst_20_percent_share_of_total_pnl, Decimal("-5"))
        self.assertEqual(
            [
                (item.token_address, item.realized_pnl_usd, item.contribution_pct)
                for item in concentration.top_losing_tokens
            ],
            [
                ("TokenA", Decimal("-7"), Decimal("0.7")),
                ("TokenB", Decimal("-3"), Decimal("0.3")),
            ],
        )
        self.assertEqual(
            [
                (item.token_address, item.realized_pnl_usd, item.contribution_pct)
                for item in concentration.top_winning_tokens
            ],
            [
                ("TokenD", Decimal("7"), Decimal("0.6363636363636363636363636364")),
                ("TokenC", Decimal("4"), Decimal("0.3636363636363636363636363636")),
            ],
        )

    def test_build_wallet_behavior_report_is_safe_for_small_trade_sets(self) -> None:
        empty_report = build_wallet_behavior_report(())
        single_trade_report = build_wallet_behavior_report(
            (
                build_trade(
                    token_address="Solo",
                    opening_tx_hash="solo-open",
                    closing_tx_hash="solo-close",
                    close_timestamp=datetime(2025, 1, 3, 12, 0, tzinfo=timezone.utc),
                    holding_duration_seconds=25,
                    cost_basis_usd="4",
                    realized_pnl_usd="-1",
                ),
            )
        )

        self.assertEqual(empty_report.summary.total_matched_trades, 0)
        self.assertEqual(empty_report.trade_rows, ())
        self.assertEqual(empty_report.summary.pnl_by_day, ())
        self.assertEqual(empty_report.summary.concentration_diagnostics.top_losing_tokens, ())
        self.assertIsNone(
            empty_report.summary.concentration_diagnostics.worst_20_percent_share_of_total_pnl
        )

        self.assertEqual(single_trade_report.summary.total_matched_trades, 1)
        self.assertEqual(
            single_trade_report.summary.streak_diagnostics.longest_losing_streak,
            1,
        )
        self.assertEqual(
            single_trade_report.summary.fast_rotation_diagnostics[0].trade_count,
            1,
        )
        self.assertEqual(
            single_trade_report.summary.concentration_diagnostics.top_5_losing_tokens_contribution_pct,
            Decimal("1"),
        )
        self.assertEqual(
            single_trade_report.summary.concentration_diagnostics.worst_20_percent_share_of_total_pnl,
            Decimal("1"),
        )


if __name__ == "__main__":
    unittest.main()
