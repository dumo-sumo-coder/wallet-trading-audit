"""Tests for trade-level FIFO diagnostics."""

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

from analytics.trade_diagnostics import (  # noqa: E402
    build_trade_diagnostic_report,
    summarize_trade_diagnostic_report,
)
from normalize.schema import CANONICAL_TRANSACTION_SCHEMA_FIELDS, NormalizedTransaction  # noqa: E402
from pnl.fifo_engine import FifoEngineResult  # noqa: E402
from pnl.pipeline import run_fifo_pipeline  # noqa: E402

SOLANA_TOKEN = "9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E"
BNB_TOKEN = "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d"


def load_fixture_transactions() -> list[NormalizedTransaction]:
    with FIXTURE.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != CANONICAL_TRANSACTION_SCHEMA_FIELDS:
            raise AssertionError("Fixture columns do not match the canonical schema")
        return [NormalizedTransaction.from_row(row) for row in reader]


class TradeDiagnosticTests(unittest.TestCase):
    def test_build_trade_diagnostic_report_includes_trade_rows_and_fee_references(self) -> None:
        fifo_pipeline_result = run_fifo_pipeline(load_fixture_transactions())

        report = build_trade_diagnostic_report(fifo_pipeline_result.fifo_result)

        self.assertEqual(len(report.matched_trades), 4)
        first_trade = report.matched_trades[0]
        self.assertEqual(first_trade.token_address, SOLANA_TOKEN)
        self.assertEqual(first_trade.opening_tx_hash, "sol-buy-001")
        self.assertEqual(first_trade.closing_tx_hash, "sol-sell-001")
        self.assertEqual(
            first_trade.open_timestamp,
            datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(
            first_trade.close_timestamp,
            datetime(2026, 1, 1, 14, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(first_trade.holding_duration_seconds, 7200)
        self.assertEqual(first_trade.quantity_matched, Decimal("40"))
        self.assertEqual(first_trade.cost_basis_usd, Decimal("80"))
        self.assertEqual(first_trade.proceeds_usd, Decimal("140"))
        self.assertEqual(first_trade.realized_pnl_usd, Decimal("60"))
        self.assertEqual(first_trade.opening_fee_native, Decimal("0.000005"))
        self.assertEqual(first_trade.opening_fee_usd, Decimal("0.0010"))
        self.assertEqual(first_trade.closing_fee_native, Decimal("0.000005"))
        self.assertEqual(first_trade.closing_fee_usd, Decimal("0.0011"))

    def test_summarize_trade_diagnostic_report_builds_aggregate_metrics(self) -> None:
        fifo_pipeline_result = run_fifo_pipeline(load_fixture_transactions())

        report = build_trade_diagnostic_report(fifo_pipeline_result.fifo_result)
        summary = report.summary

        self.assertEqual(summary.total_matched_trades, 4)
        self.assertEqual(summary.winners_count, 4)
        self.assertEqual(summary.losers_count, 0)
        self.assertEqual(summary.avg_winner_usd, Decimal("52"))
        self.assertIsNone(summary.avg_loser_usd)
        self.assertEqual(summary.largest_win_usd, Decimal("75"))
        self.assertIsNone(summary.largest_loss_usd)
        self.assertEqual(
            [
                (item.token_address, item.matched_trades, item.realized_pnl_usd)
                for item in summary.pnl_by_token
            ],
            [
                (BNB_TOKEN, 2, Decimal("73")),
                (SOLANA_TOKEN, 2, Decimal("135")),
            ],
        )

    def test_summarize_trade_diagnostic_report_is_safe_when_no_matches_exist(self) -> None:
        summary = summarize_trade_diagnostic_report(())
        report = build_trade_diagnostic_report(
            FifoEngineResult(
                trade_matches=(),
                open_lots=(),
                ignored_transfers=(),
                recorded_fees=(),
            )
        )

        self.assertEqual(summary.total_matched_trades, 0)
        self.assertEqual(summary.winners_count, 0)
        self.assertEqual(summary.losers_count, 0)
        self.assertIsNone(summary.avg_winner_usd)
        self.assertIsNone(summary.avg_loser_usd)
        self.assertIsNone(summary.largest_win_usd)
        self.assertIsNone(summary.largest_loss_usd)
        self.assertEqual(summary.pnl_by_token, ())
        self.assertEqual(report.matched_trades, ())
        self.assertEqual(report.summary.total_matched_trades, 0)


if __name__ == "__main__":
    unittest.main()
