"""Fixture-backed tests for trade performance analysis."""

from __future__ import annotations

import csv
import sys
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
FIXTURE = ROOT / "tests" / "fixtures" / "normalized_transactions_sample.csv"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from analytics.performance import (  # noqa: E402
    analyze_fifo_pipeline_performance,
    build_closed_trade_performance_rows,
)
from normalize.schema import CANONICAL_TRANSACTION_SCHEMA_FIELDS, NormalizedTransaction  # noqa: E402
from pnl.pipeline import run_fifo_pipeline  # noqa: E402


def load_fixture_transactions() -> list[NormalizedTransaction]:
    with FIXTURE.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != CANONICAL_TRANSACTION_SCHEMA_FIELDS:
            raise AssertionError("Fixture columns do not match the canonical schema")
        return [NormalizedTransaction.from_row(row) for row in reader]


class TradePerformanceTests(unittest.TestCase):
    def test_build_closed_trade_performance_rows_computes_trade_level_metrics(self) -> None:
        fifo_pipeline_result = run_fifo_pipeline(load_fixture_transactions())

        closed_trades = build_closed_trade_performance_rows(
            fifo_pipeline_result.fifo_result.trade_matches
        )

        self.assertEqual(len(closed_trades), 4)
        first_trade = closed_trades[0]
        self.assertEqual(first_trade.entry_tx_hash, "sol-buy-001")
        self.assertEqual(first_trade.exit_tx_hash, "sol-sell-001")
        self.assertEqual(
            first_trade.entry_timestamp,
            datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(
            first_trade.exit_timestamp,
            datetime(2026, 1, 1, 14, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(first_trade.holding_time_seconds, 7200)
        self.assertEqual(first_trade.realized_pnl_usd, Decimal("60.0"))
        self.assertEqual(first_trade.cost_basis_usd, Decimal("80"))
        self.assertEqual(first_trade.return_pct, Decimal("75.00"))
        self.assertIsNone(first_trade.max_unrealized_pnl_usd)
        self.assertIsNone(first_trade.max_unrealized_return_pct)
        self.assertIsNone(first_trade.pnl_capture_ratio)

    def test_analyze_fifo_pipeline_performance_summarizes_fixture_dataset(self) -> None:
        fifo_pipeline_result = run_fifo_pipeline(load_fixture_transactions())

        analysis = analyze_fifo_pipeline_performance(fifo_pipeline_result)

        self.assertEqual(len(analysis.closed_trades), 4)
        self.assertEqual(analysis.summary.total_trades, 4)
        self.assertEqual(analysis.summary.win_rate, Decimal("1"))
        self.assertEqual(
            analysis.summary.average_return_pct,
            Decimal("52.95138888888888888888888888"),
        )
        self.assertEqual(
            analysis.summary.median_return_pct,
            Decimal("54.16666666666666666666666665"),
        )
        self.assertEqual(analysis.summary.total_realized_pnl_usd, Decimal("208.0"))
        self.assertEqual(analysis.summary.largest_win_usd, Decimal("75.0"))
        self.assertIsNone(analysis.summary.largest_loss_usd)
        self.assertEqual(
            analysis.summary.average_holding_time_winners_seconds,
            Decimal("13050"),
        )
        self.assertIsNone(analysis.summary.average_holding_time_losers_seconds)
        self.assertEqual(
            [
                (bucket.label, bucket.trade_count)
                for bucket in analysis.summary.pnl_distribution_buckets
            ],
            [
                ("loss_lt_0_usd", 0),
                ("flat_0_usd", 0),
                ("win_0_to_50_usd", 2),
                ("win_50_to_100_usd", 2),
                ("win_ge_100_usd", 0),
            ],
        )
        self.assertEqual(analysis.summary.max_consecutive_wins, 4)
        self.assertEqual(analysis.summary.max_consecutive_losses, 0)


if __name__ == "__main__":
    unittest.main()
