"""Fixture-level verification for sample normalized transactions."""

from __future__ import annotations

import csv
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
FIXTURE = ROOT / "tests" / "fixtures" / "normalized_transactions_sample.csv"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from normalize.schema import (  # noqa: E402
    CANONICAL_TRANSACTION_SCHEMA_FIELDS,
    Chain,
    EventType,
    NormalizedTransaction,
)

SOLANA_TOKEN = "9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E"
BNB_TOKEN = "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d"


def load_fixture_transactions() -> list[NormalizedTransaction]:
    """Load the sample CSV fixture as typed normalized transactions."""

    with FIXTURE.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != CANONICAL_TRANSACTION_SCHEMA_FIELDS:
            raise AssertionError("Fixture columns do not match the canonical schema")
        return [NormalizedTransaction.from_row(row) for row in reader]


def token_delta(transaction: NormalizedTransaction, token_address: str) -> int:
    """Return wallet-relative quantity change for one token address."""

    delta = 0
    if transaction.token_in_address == token_address:
        delta += int(transaction.amount_in)
    if transaction.token_out_address == token_address:
        delta -= int(transaction.amount_out)
    return delta


class SampleDatasetTests(unittest.TestCase):
    def test_fixture_loads_and_covers_both_chains(self) -> None:
        transactions = load_fixture_transactions()

        self.assertEqual(len(transactions), 9)
        self.assertEqual({tx.chain for tx in transactions}, {Chain.SOLANA, Chain.BNB_EVM})
        self.assertEqual(
            {tx.event_type for tx in transactions},
            {EventType.SWAP, EventType.TRANSFER, EventType.FEE},
        )

    def test_solana_fixture_reaches_full_exit_then_reentry(self) -> None:
        transactions = [
            tx for tx in load_fixture_transactions() if tx.chain == Chain.SOLANA
        ]

        balances: list[int] = []
        running_balance = 0
        for transaction in transactions:
            running_balance += token_delta(transaction, SOLANA_TOKEN)
            if transaction.event_type != EventType.FEE:
                balances.append(running_balance)

        self.assertEqual(balances, [100, 60, 50, 0, 30])

    def test_bnb_fixture_forms_a_complete_roundtrip(self) -> None:
        transactions = [
            tx for tx in load_fixture_transactions() if tx.chain == Chain.BNB_EVM
        ]

        balances: list[int] = []
        running_balance = 0
        for transaction in transactions:
            running_balance += token_delta(transaction, BNB_TOKEN)
            balances.append(running_balance)

        self.assertEqual(balances, [200, 120, 0])

    def test_fixture_contains_a_standalone_fee_row(self) -> None:
        fee_rows = [
            tx
            for tx in load_fixture_transactions()
            if tx.event_type == EventType.FEE
        ]

        self.assertEqual(len(fee_rows), 1)
        fee_row = fee_rows[0]
        self.assertIsNone(fee_row.token_in_address)
        self.assertIsNone(fee_row.token_out_address)
        self.assertGreater(fee_row.fee_native, 0)
        self.assertIsNone(fee_row.fee_usd)


if __name__ == "__main__":
    unittest.main()
