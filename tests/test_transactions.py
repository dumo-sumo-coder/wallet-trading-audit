"""Tests for conservative raw transaction normalization."""

from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from normalize.schema import Chain, EventType, NormalizedTransaction  # noqa: E402
from normalize.transactions import (  # noqa: E402
    normalize_evm_tx,
    normalize_solana_tx,
    normalize_transaction,
)


def solana_raw_row() -> dict[str, object]:
    return {
        "chain": Chain.SOLANA,
        "wallet": "wallet-solana-1",
        "tx_hash": "solana-signature-1",
        "block_time": datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
        "token_in_address": "So11111111111111111111111111111111111111112",
        "token_out_address": "Es9vMFrzaCERmJfr6Woj7q4Tt6kRXKuX3sX5Yucs5cjB",
        "amount_in": Decimal("1.5"),
        "amount_out": Decimal("50"),
        "usd_value": Decimal("150"),
        "fee_native": Decimal("0.000005"),
        "fee_usd": None,
        "event_type": EventType.SWAP,
        "source": "jupiter",
    }


def bnb_evm_raw_row() -> dict[str, object]:
    return {
        "chain": "bnb_evm",
        "wallet": "0x1111222233334444555566667777888899990000",
        "tx_hash": "0xabc123",
        "block_time": datetime(2026, 1, 2, 5, 30, tzinfo=timezone.utc),
        "token_in_address": "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
        "token_out_address": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
        "amount_in": Decimal("15"),
        "amount_out": Decimal("0.04"),
        "usd_value": Decimal("15.50"),
        "fee_native": Decimal("0.0004"),
        "fee_usd": None,
        "event_type": "swap",
        "source": "pancakeswap_v3",
    }


class NormalizeTransactionTests(unittest.TestCase):
    def test_normalize_transaction_accepts_canonical_like_rows(self) -> None:
        normalized = normalize_transaction(solana_raw_row())

        self.assertIsInstance(normalized, NormalizedTransaction)
        self.assertEqual(normalized.chain, Chain.SOLANA)
        self.assertEqual(
            normalized.block_time,
            datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(normalized.amount_in, Decimal("1.5"))
        self.assertEqual(normalized.amount_out, Decimal("50"))
        self.assertEqual(normalized.event_type, EventType.SWAP)

    def test_normalize_transaction_routes_to_solana_adapter(self) -> None:
        with patch(
            "normalize.transactions.normalize_solana_tx",
            return_value=normalize_solana_tx(solana_raw_row()),
        ) as mock_normalize_solana_tx:
            normalized = normalize_transaction({"chain": "solana"})

        mock_normalize_solana_tx.assert_called_once_with({"chain": "solana"})
        self.assertEqual(normalized.chain, Chain.SOLANA)

    def test_normalize_transaction_routes_to_bnb_evm_adapter(self) -> None:
        with patch(
            "normalize.transactions.normalize_evm_tx",
            return_value=normalize_evm_tx(bnb_evm_raw_row()),
        ) as mock_normalize_evm_tx:
            normalized = normalize_transaction({"chain": "bnb_evm"})

        mock_normalize_evm_tx.assert_called_once_with({"chain": "bnb_evm"})
        self.assertEqual(normalized.chain, Chain.BNB_EVM)

    def test_normalize_transaction_rejects_unsupported_chains(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unsupported chain: ethereum"):
            normalize_transaction({"chain": "ethereum"})

    def test_normalize_transaction_rejects_missing_required_fields(self) -> None:
        raw = solana_raw_row()
        raw.pop("amount_in")

        with self.assertRaisesRegex(ValueError, "amount_in"):
            normalize_transaction(raw)


if __name__ == "__main__":
    unittest.main()
