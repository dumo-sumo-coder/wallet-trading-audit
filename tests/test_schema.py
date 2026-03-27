"""Scaffold verification for the canonical schema."""

from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from normalize.schema import (  # noqa: E402
    CANONICAL_TRANSACTION_SCHEMA_FIELDS,
    Chain,
    EventType,
    NormalizedTransaction,
)


class NormalizedTransactionSchemaTests(unittest.TestCase):
    def test_schema_field_contract_is_stable(self) -> None:
        expected = (
            "chain",
            "wallet",
            "tx_hash",
            "block_time",
            "token_in_address",
            "token_out_address",
            "amount_in",
            "amount_out",
            "usd_value",
            "fee_native",
            "fee_usd",
            "event_type",
            "source",
        )
        self.assertEqual(CANONICAL_TRANSACTION_SCHEMA_FIELDS, expected)

    def test_to_row_returns_all_required_columns(self) -> None:
        normalized = NormalizedTransaction(
            chain=Chain.SOLANA,
            wallet="wallet-1",
            tx_hash="tx-123",
            block_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            token_in_address="So11111111111111111111111111111111111111112",
            token_out_address="Es9vMFrzaCERmJfr6Woj7q4Tt6kRXKuX3sX5Yucs5cjB",
            amount_in=Decimal("1.5"),
            amount_out=Decimal("50"),
            usd_value=None,
            fee_native=Decimal("0.000005"),
            fee_usd=None,
            event_type=EventType.SWAP,
            source="example-dex",
        )

        row = normalized.to_row()

        self.assertEqual(tuple(row.keys()), CANONICAL_TRANSACTION_SCHEMA_FIELDS)
        self.assertEqual(row["chain"], "solana")
        self.assertEqual(row["event_type"], "swap")


if __name__ == "__main__":
    unittest.main()
