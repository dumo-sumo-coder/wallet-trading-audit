"""End-to-end tests for the normalized-to-FIFO pipeline."""

from __future__ import annotations

import csv
import sys
import unittest
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
FIXTURE = ROOT / "tests" / "fixtures" / "normalized_transactions_sample.csv"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from normalize.schema import CANONICAL_TRANSACTION_SCHEMA_FIELDS, NormalizedTransaction  # noqa: E402
from pnl.pipeline import run_fifo_pipeline  # noqa: E402

SOLANA_TOKEN = "9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E"


def load_fixture_transactions() -> list[NormalizedTransaction]:
    with FIXTURE.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != CANONICAL_TRANSACTION_SCHEMA_FIELDS:
            raise AssertionError("Fixture columns do not match the canonical schema")
        return [NormalizedTransaction.from_row(row) for row in reader]


class FifoPipelineTests(unittest.TestCase):
    def test_sample_dataset_produces_expected_realized_pnl(self) -> None:
        result = run_fifo_pipeline(load_fixture_transactions())

        self.assertEqual(result.realized_pnl_usd, Decimal("208"))
        self.assertEqual(len(result.fifo_result.trade_matches), 4)
        self.assertEqual(
            [trade_match.realized_pnl_usd for trade_match in result.fifo_result.trade_matches],
            [Decimal("60"), Decimal("75"), Decimal("32"), Decimal("41")],
        )

    def test_sample_dataset_closes_expected_fifo_lots(self) -> None:
        result = run_fifo_pipeline(load_fixture_transactions())

        self.assertEqual(
            [
                (
                    trade_match.entry_tx_hash,
                    trade_match.exit_tx_hash,
                    trade_match.quantity,
                )
                for trade_match in result.fifo_result.trade_matches
            ],
            [
                ("sol-buy-001", "sol-sell-001", Decimal("40")),
                ("sol-buy-001", "sol-exit-001", Decimal("50")),
                ("0xbuy001", "0xpartial001", Decimal("80")),
                ("0xbuy001", "0xexit001", Decimal("120")),
            ],
        )
        self.assertEqual(len(result.remaining_positions), 1)
        remaining_position = result.remaining_positions[0]
        self.assertEqual(remaining_position.token_address, SOLANA_TOKEN)
        self.assertEqual(remaining_position.quantity_open, Decimal("40"))
        self.assertEqual(remaining_position.cost_basis_usd, Decimal("98"))
        self.assertEqual(len(result.fifo_result.open_lots), 2)
        self.assertEqual(
            [lot.source_tx_hash for lot in result.fifo_result.open_lots],
            ["sol-buy-001", "sol-reentry-001"],
        )
        self.assertEqual(
            [lot.quantity_open for lot in result.fifo_result.open_lots],
            [Decimal("10"), Decimal("30")],
        )


if __name__ == "__main__":
    unittest.main()
