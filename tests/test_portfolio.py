"""Fixture-backed tests for portfolio-level capital analysis."""

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

from analytics.portfolio import (  # noqa: E402
    PositionClassification,
    PositionLifecycle,
    analyze_normalized_portfolio,
)
from normalize.schema import CANONICAL_TRANSACTION_SCHEMA_FIELDS, NormalizedTransaction  # noqa: E402

SOLANA_TOKEN = "9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E"
BNB_TOKEN = "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d"


def load_fixture_transactions() -> list[NormalizedTransaction]:
    with FIXTURE.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != CANONICAL_TRANSACTION_SCHEMA_FIELDS:
            raise AssertionError("Fixture columns do not match the canonical schema")
        return [NormalizedTransaction.from_row(row) for row in reader]


class PortfolioAnalysisTests(unittest.TestCase):
    def test_analyze_normalized_portfolio_summarizes_fixture_positions(self) -> None:
        analysis = analyze_normalized_portfolio(load_fixture_transactions())

        self.assertEqual(len(analysis.positions), 2)
        position_by_token = {
            position.token_address: position for position in analysis.positions
        }

        solana_position = position_by_token[SOLANA_TOKEN]
        self.assertEqual(solana_position.total_tokens_acquired, Decimal("130"))
        self.assertEqual(solana_position.total_tokens_sold, Decimal("100"))
        self.assertEqual(solana_position.remaining_balance, Decimal("30"))
        self.assertEqual(solana_position.remaining_cost_basis_usd, Decimal("98"))
        self.assertEqual(solana_position.capital_deployed_usd, Decimal("278"))
        self.assertEqual(solana_position.capital_returned_usd, Decimal("315"))
        self.assertEqual(solana_position.lifecycle, PositionLifecycle.PARTIALLY_OPEN)
        self.assertEqual(solana_position.classification, PositionClassification.OPEN)

        bnb_position = position_by_token[BNB_TOKEN]
        self.assertEqual(bnb_position.total_tokens_acquired, Decimal("200"))
        self.assertEqual(bnb_position.total_tokens_sold, Decimal("200"))
        self.assertEqual(bnb_position.remaining_balance, Decimal("0"))
        self.assertEqual(bnb_position.remaining_cost_basis_usd, Decimal("0"))
        self.assertEqual(bnb_position.capital_deployed_usd, Decimal("240"))
        self.assertEqual(bnb_position.capital_returned_usd, Decimal("313"))
        self.assertEqual(bnb_position.lifecycle, PositionLifecycle.FULLY_CLOSED)
        self.assertEqual(bnb_position.classification, PositionClassification.CLOSED)

        self.assertEqual(analysis.summary.total_positions, 2)
        self.assertEqual(analysis.summary.fully_closed_positions, 1)
        self.assertEqual(analysis.summary.partially_open_positions, 1)
        self.assertEqual(analysis.summary.fully_open_positions, 0)
        self.assertEqual(analysis.summary.dead_positions, 0)
        self.assertEqual(analysis.summary.total_capital_deployed_usd, Decimal("518"))
        self.assertEqual(analysis.summary.total_capital_returned_usd, Decimal("628"))
        self.assertEqual(analysis.summary.net_capital_still_in_market_usd, Decimal("98"))
        self.assertEqual(analysis.summary.total_pnl_usd, Decimal("208"))
        self.assertEqual(
            analysis.summary.capital_efficiency_ratio,
            Decimal("1.212355212355212355212355212"),
        )
        self.assertEqual(
            analysis.summary.percent_capital_stuck_in_open_positions,
            Decimal("0.1891891891891891891891891892"),
        )

    def test_dead_position_placeholder_classifies_inactive_fully_open_subset(self) -> None:
        transactions = [
            transaction
            for transaction in load_fixture_transactions()
            if transaction.tx_hash == "sol-reentry-001"
        ]

        analysis = analyze_normalized_portfolio(
            transactions,
            analysis_as_of=datetime(2026, 3, 15, tzinfo=timezone.utc),
            dead_position_inactivity_days=30,
        )

        self.assertEqual(len(analysis.positions), 1)
        position = analysis.positions[0]
        self.assertEqual(position.total_tokens_acquired, Decimal("30"))
        self.assertEqual(position.total_tokens_sold, Decimal("0"))
        self.assertEqual(position.remaining_balance, Decimal("30"))
        self.assertEqual(position.remaining_cost_basis_usd, Decimal("78"))
        self.assertEqual(position.lifecycle, PositionLifecycle.FULLY_OPEN)
        self.assertEqual(position.classification, PositionClassification.DEAD_POSITION)
        self.assertEqual(analysis.summary.fully_open_positions, 1)
        self.assertEqual(analysis.summary.dead_positions, 1)
        self.assertEqual(analysis.summary.total_capital_deployed_usd, Decimal("78"))
        self.assertEqual(analysis.summary.total_capital_returned_usd, Decimal("0"))
        self.assertEqual(analysis.summary.net_capital_still_in_market_usd, Decimal("78"))
        self.assertEqual(analysis.summary.total_pnl_usd, Decimal("0"))
        self.assertEqual(analysis.summary.capital_efficiency_ratio, Decimal("0"))
        self.assertEqual(
            analysis.summary.percent_capital_stuck_in_open_positions,
            Decimal("1"),
        )


if __name__ == "__main__":
    unittest.main()
