"""End-to-end tests for raw Solana fixtures through normalization and FIFO."""

from __future__ import annotations

import sys
import unittest
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "raw_solana"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from normalize.schema import EventType  # noqa: E402
from pnl.solana_pipeline import (  # noqa: E402
    run_solana_fixture_fifo_pipeline,
    summarize_solana_fixture_pipeline,
)

BUY_TX_HASH = (
    "5uyM8JpVQCBq9x3AjC8nH9fYH5x7c3qvKc6PjH9Tn7rR7hTj6yV7iM6m2T9g7Z6dQwL7jVn4pQy3mK8sR1n4bUy"
)
SELL_TX_HASH = (
    "3eQFvN6wL8tH7bQ2pK9sR6cW1xY5mJ4uT8nP2qV7zA3sD5fG6hJ8kL2mN4pR6tU8wY1qC3eF5gH7jK9mP2sV4w"
)


def fixture_paths() -> list[Path]:
    return [
        FIXTURE_DIR / "solana_transaction_response_buy_example.json",
        FIXTURE_DIR / "solana_transaction_response_sell_example.json",
        FIXTURE_DIR / "solana_transaction_response_transfer_in_example.json",
        FIXTURE_DIR / "solana_transaction_response_example.json",
        FIXTURE_DIR / "solana_wallet_snapshot.json",
    ]


class SolanaFixturePipelineTests(unittest.TestCase):
    def test_supported_raw_fixtures_flow_into_fifo(self) -> None:
        result = run_solana_fixture_fifo_pipeline(
            fixture_paths(),
            usd_value_overrides_by_tx_hash={
                BUY_TX_HASH: Decimal("100"),
                SELL_TX_HASH: Decimal("150"),
            },
        )

        self.assertEqual(result.total_raw_transactions_reviewed, 5)
        self.assertEqual(len(result.normalized_transactions), 3)
        self.assertEqual(len(result.unsupported_transactions), 2)
        self.assertEqual(
            [transaction.event_type for transaction in result.normalized_transactions],
            [EventType.SWAP, EventType.SWAP, EventType.TRANSFER],
        )
        self.assertEqual(result.fifo_pipeline_result.realized_pnl_usd, Decimal("50"))
        self.assertEqual(len(result.fifo_pipeline_result.fifo_result.trade_matches), 1)
        trade_match = result.fifo_pipeline_result.fifo_result.trade_matches[0]
        self.assertEqual(trade_match.entry_tx_hash, BUY_TX_HASH)
        self.assertEqual(trade_match.exit_tx_hash, SELL_TX_HASH)
        self.assertEqual(trade_match.quantity, Decimal("25"))
        self.assertEqual(trade_match.cost_basis_usd, Decimal("100"))
        self.assertEqual(trade_match.proceeds_usd, Decimal("150"))
        self.assertEqual(trade_match.realized_pnl_usd, Decimal("50"))
        self.assertEqual(len(result.fifo_pipeline_result.remaining_positions), 0)
        self.assertEqual(len(result.fifo_pipeline_result.fifo_result.open_lots), 0)
        self.assertEqual(len(result.fifo_pipeline_result.fifo_result.ignored_transfers), 1)

    def test_unsupported_fixtures_fail_explicitly_and_predictably(self) -> None:
        result = run_solana_fixture_fifo_pipeline(
            fixture_paths(),
            usd_value_overrides_by_tx_hash={
                BUY_TX_HASH: Decimal("100"),
                SELL_TX_HASH: Decimal("150"),
            },
        )

        self.assertEqual(
            [item.fixture_name for item in result.unsupported_transactions],
            [
                "solana_transaction_response_example.json",
                "solana_wallet_snapshot.json",
            ],
        )
        for unsupported in result.unsupported_transactions:
            self.assertIn(
                "Unsupported Solana normalization case",
                unsupported.reason,
            )

    def test_fees_remain_explicit_and_separate(self) -> None:
        result = run_solana_fixture_fifo_pipeline(
            fixture_paths(),
            usd_value_overrides_by_tx_hash={
                BUY_TX_HASH: Decimal("100"),
                SELL_TX_HASH: Decimal("150"),
            },
        )

        recorded_fees = result.fifo_pipeline_result.fifo_result.recorded_fees
        self.assertEqual(len(recorded_fees), 3)
        self.assertEqual(
            [fee.event_type for fee in recorded_fees],
            [EventType.SWAP, EventType.SWAP, EventType.TRANSFER],
        )
        self.assertEqual(
            [fee.fee_native for fee in recorded_fees],
            [Decimal("0.000005"), Decimal("0.000005"), Decimal("0.000005")],
        )
        self.assertEqual(
            result.fifo_pipeline_result.realized_pnl_usd,
            Decimal("50"),
        )

    def test_summary_helper_reports_counts_reasons_and_realized_pnl(self) -> None:
        result = run_solana_fixture_fifo_pipeline(
            fixture_paths(),
            usd_value_overrides_by_tx_hash={
                BUY_TX_HASH: Decimal("100"),
                SELL_TX_HASH: Decimal("150"),
            },
        )

        summary = summarize_solana_fixture_pipeline(result)

        self.assertEqual(summary.total_raw_transactions_reviewed, 5)
        self.assertEqual(summary.normalized_transactions_count, 3)
        self.assertEqual(summary.unsupported_transactions_count, 2)
        self.assertEqual(summary.realized_pnl_usd, Decimal("50"))
        self.assertEqual(
            summary.unsupported_reasons,
            (
                "solana_transaction_response_example.json: Unsupported Solana normalization case: native SOL moved without a single non-zero wallet token delta. TODO: add fixture-driven rules for rent, account creation, and other non-trade balance changes.",
                "solana_wallet_snapshot.json: Unsupported Solana normalization case: native SOL moved without a single non-zero wallet token delta. TODO: add fixture-driven rules for rent, account creation, and other non-trade balance changes.",
            ),
        )


if __name__ == "__main__":
    unittest.main()
