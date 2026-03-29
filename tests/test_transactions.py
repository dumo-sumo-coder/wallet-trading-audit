"""Tests for conservative raw transaction normalization."""

from __future__ import annotations

import copy
import sys
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
RAW_SOLANA_FIXTURE_DIR = ROOT / "tests" / "fixtures" / "raw_solana"
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


def load_json_fixture(name: str) -> dict[str, object]:
    import json

    fixture_path = RAW_SOLANA_FIXTURE_DIR / name
    return json.loads(fixture_path.read_text(encoding="utf-8"))


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

    def test_normalize_solana_tx_normalizes_obvious_buy_fixture(self) -> None:
        raw = load_json_fixture("solana_transaction_response_buy_example.json")

        normalized = normalize_transaction({"chain": "solana", **raw})

        self.assertEqual(normalized.chain, Chain.SOLANA)
        self.assertEqual(normalized.event_type, EventType.SWAP)
        self.assertEqual(
            normalized.tx_hash,
            "5uyM8JpVQCBq9x3AjC8nH9fYH5x7c3qvKc6PjH9Tn7rR7hTj6yV7iM6m2T9g7Z6dQwL7jVn4pQy3mK8sR1n4bUy",
        )
        self.assertEqual(
            normalized.block_time,
            datetime(2025, 11, 21, 19, 13, 35, tzinfo=timezone.utc),
        )
        self.assertEqual(
            normalized.token_in_address,
            "9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E",
        )
        self.assertEqual(
            normalized.token_out_address,
            "So11111111111111111111111111111111111111112",
        )
        self.assertEqual(normalized.amount_in, Decimal("25"))
        self.assertEqual(normalized.amount_out, Decimal("1"))
        self.assertEqual(normalized.fee_native, Decimal("0.000005"))
        self.assertIsNone(normalized.usd_value)

    def test_normalize_solana_tx_normalizes_obvious_sell_fixture(self) -> None:
        raw = load_json_fixture("solana_transaction_response_sell_example.json")

        adapted = normalize_solana_tx(raw)

        self.assertEqual(adapted["event_type"], "swap")
        self.assertEqual(
            adapted["token_in_address"],
            "So11111111111111111111111111111111111111112",
        )
        self.assertEqual(
            adapted["token_out_address"],
            "9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E",
        )
        self.assertEqual(Decimal(str(adapted["amount_in"])), Decimal("1.5"))
        self.assertEqual(Decimal(str(adapted["amount_out"])), Decimal("25"))
        self.assertEqual(Decimal(str(adapted["fee_native"])), Decimal("0.000005"))

    def test_normalize_solana_tx_normalizes_transfer_like_fixture(self) -> None:
        raw = load_json_fixture("solana_transaction_response_transfer_in_example.json")

        adapted = normalize_solana_tx(raw)

        self.assertEqual(adapted["event_type"], "transfer")
        self.assertEqual(
            adapted["token_in_address"],
            "9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E",
        )
        self.assertIsNone(adapted["token_out_address"])
        self.assertEqual(Decimal(str(adapted["amount_in"])), Decimal("25"))
        self.assertEqual(Decimal(str(adapted["amount_out"])), Decimal("0"))
        self.assertEqual(Decimal(str(adapted["fee_native"])), Decimal("0.000005"))

    def test_normalize_solana_tx_normalizes_native_sol_outflow_fixture(self) -> None:
        raw = load_json_fixture("solana_transaction_response_example.json")

        normalized = normalize_transaction({"chain": "solana", **raw})

        self.assertEqual(normalized.event_type, EventType.TRANSFER)
        self.assertIsNone(normalized.token_in_address)
        self.assertEqual(
            normalized.token_out_address,
            "So11111111111111111111111111111111111111112",
        )
        self.assertEqual(normalized.amount_in, Decimal("0"))
        self.assertEqual(normalized.amount_out, Decimal("0.00203928"))
        self.assertEqual(normalized.fee_native, Decimal("0.000005"))
        self.assertEqual(
            normalized.tx_hash,
            "56rgv1Fqg7MHLoct3ESNx8DHeC2c5TFYyK1As35paWn2KYuRuXPFZK3XDEwkkZemVfmnKrkj17mMzz4N1d8kMYC4",
        )
        self.assertEqual(
            normalized.block_time,
            datetime(2025, 11, 21, 18, 56, 55, tzinfo=timezone.utc),
        )

    def test_normalize_solana_tx_normalizes_snapshot_envelope_native_outflow(self) -> None:
        raw = load_json_fixture("solana_wallet_snapshot.json")

        adapted = normalize_solana_tx(raw)

        self.assertEqual(adapted["event_type"], "transfer")
        self.assertIsNone(adapted["token_in_address"])
        self.assertEqual(
            adapted["token_out_address"],
            "So11111111111111111111111111111111111111112",
        )
        self.assertEqual(Decimal(str(adapted["amount_out"])), Decimal("0.00203928"))
        self.assertEqual(Decimal(str(adapted["fee_native"])), Decimal("0.000005"))

    def test_normalize_solana_tx_rejects_ambiguous_multi_token_fixture(self) -> None:
        raw = load_json_fixture("solana_transaction_response_buy_example.json")
        ambiguous = copy.deepcopy(raw)
        ambiguous["result"]["meta"]["postTokenBalances"].append(
            {
                "accountIndex": 2,
                "mint": "Es9vMFrzaCERmJfr6Woj7q4Tt6kRXKuX3sX5Yucs5cjB",
                "owner": "47eFuHR9ste9kopiJ9eRxcwahmE62JovbKe5r7AjANut",
                "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                "uiTokenAmount": {
                    "amount": "1000000",
                    "decimals": 6,
                    "uiAmount": 1.0,
                    "uiAmountString": "1",
                },
            }
        )

        with self.assertRaisesRegex(
            ValueError,
            "Unsupported Solana normalization case: multiple wallet token balance deltas detected",
        ):
            normalize_solana_tx(ambiguous)

    def test_normalize_solana_tx_supports_exact_two_token_zero_native_swap(self) -> None:
        raw = copy.deepcopy(load_json_fixture("solana_transaction_response_buy_example.json"))
        result = raw["result"]
        meta = result["meta"]
        wallet = "47eFuHR9ste9kopiJ9eRxcwahmE62JovbKe5r7AjANut"

        meta["fee"] = 0
        transaction = result["transaction"]
        message = transaction["message"]
        account_keys = message["accountKeys"]
        wallet_index = account_keys.index(wallet)
        pre_balances = meta["preBalances"]
        post_balances = meta["postBalances"]
        post_balances[wallet_index] = pre_balances[wallet_index]

        meta["preTokenBalances"] = [
            {
                "accountIndex": 2,
                "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "owner": wallet,
                "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                "uiTokenAmount": {
                    "amount": "5000000",
                    "decimals": 6,
                    "uiAmount": 5.0,
                    "uiAmountString": "5",
                },
            }
        ]

        adapted = normalize_solana_tx(raw)

        self.assertEqual(adapted["event_type"], "swap")
        self.assertEqual(
            adapted["token_in_address"],
            "9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E",
        )
        self.assertEqual(
            adapted["token_out_address"],
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        )
        self.assertEqual(Decimal(str(adapted["amount_in"])), Decimal("25"))
        self.assertEqual(Decimal(str(adapted["amount_out"])), Decimal("5"))
        self.assertEqual(Decimal(str(adapted["usd_value"])), Decimal("5"))
        self.assertEqual(Decimal(str(adapted["fee_native"])), Decimal("0"))

    def test_normalize_solana_tx_supports_exact_two_token_swap_when_wallet_did_not_pay_fee(
        self,
    ) -> None:
        raw = copy.deepcopy(load_json_fixture("solana_transaction_response_buy_example.json"))
        result = raw["result"]
        meta = result["meta"]
        wallet = "47eFuHR9ste9kopiJ9eRxcwahmE62JovbKe5r7AjANut"

        transaction = result["transaction"]
        message = transaction["message"]
        account_keys = message["accountKeys"]
        wallet_index = account_keys.index(wallet)
        self.assertEqual(wallet_index, 0)
        account_keys[0], account_keys[1] = account_keys[1], account_keys[0]
        meta["fee"] = 5000
        pre_balances = meta["preBalances"]
        post_balances = meta["postBalances"]
        pre_balances[0], pre_balances[1] = pre_balances[1], pre_balances[0]
        post_balances[0], post_balances[1] = post_balances[1], post_balances[0]
        wallet_index = account_keys.index(wallet)
        post_balances[wallet_index] = pre_balances[wallet_index]

        meta["preTokenBalances"] = [
            {
                "accountIndex": 2,
                "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "owner": wallet,
                "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                "uiTokenAmount": {
                    "amount": "5000000",
                    "decimals": 6,
                    "uiAmount": 5.0,
                    "uiAmountString": "5",
                },
            }
        ]

        adapted = normalize_solana_tx(raw)

        self.assertEqual(adapted["event_type"], "swap")
        self.assertEqual(
            adapted["token_in_address"],
            "9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E",
        )
        self.assertEqual(
            adapted["token_out_address"],
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        )
        self.assertEqual(Decimal(str(adapted["usd_value"])), Decimal("5"))
        self.assertEqual(Decimal(str(adapted["fee_native"])), Decimal("0.000005"))


if __name__ == "__main__":
    unittest.main()
