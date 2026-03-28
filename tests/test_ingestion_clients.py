"""Tests for minimal raw ingestion clients."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ingestion.evm_client import EvmWalletClient  # noqa: E402
from ingestion.solana_client import SolanaRpcClient  # noqa: E402


class SolanaRpcClientTests(unittest.TestCase):
    def test_fetch_recent_transaction_history_preserves_raw_rpc_responses(self) -> None:
        client = SolanaRpcClient(rpc_url="https://example.solana.invalid")
        signatures_response = {
            "jsonrpc": "2.0",
            "result": [
                {"signature": "sig-1", "slot": 1},
                {"signature": "sig-2", "slot": 2},
            ],
            "id": 1,
        }
        transaction_one = {"jsonrpc": "2.0", "result": {"slot": 1}, "id": 1}
        transaction_two = {"jsonrpc": "2.0", "result": {"slot": 2}, "id": 1}

        with patch.object(
            client,
            "_rpc_request",
            side_effect=[signatures_response, transaction_one, transaction_two],
        ) as mock_rpc_request:
            snapshot = client.fetch_recent_transaction_history(
                "TestSolanaWallet11111111111111111111111111111",
                limit=2,
            )

        self.assertEqual(snapshot["signatures_response"], signatures_response)
        self.assertEqual(
            snapshot["transaction_responses"],
            [transaction_one, transaction_two],
        )
        self.assertEqual(mock_rpc_request.call_count, 3)

    def test_save_recent_transaction_history_writes_under_solana_raw_directory(self) -> None:
        client = SolanaRpcClient(rpc_url="https://example.solana.invalid")
        snapshot = {
            "wallet": "TestSolanaWallet11111111111111111111111111111",
            "fetched_at_utc": "2026-03-28T12:00:00+00:00",
            "signatures_response": {"result": []},
            "transaction_responses": [],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            repository_root = Path(temp_dir)
            with patch.object(
                client,
                "fetch_recent_transaction_history",
                return_value=snapshot,
            ):
                output_path = client.save_recent_transaction_history(
                    snapshot["wallet"],
                    repository_root=repository_root,
                )

            self.assertEqual(output_path.parent, repository_root / "data" / "raw" / "solana")
            saved_snapshot = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(saved_snapshot, snapshot)


class EvmWalletClientTests(unittest.TestCase):
    def test_fetch_recent_transaction_history_preserves_raw_api_responses(self) -> None:
        client = EvmWalletClient(
            api_key="test-key",
            api_url="https://example.etherscan.invalid/v2/api",
        )
        normal_transactions = {"status": "1", "message": "OK", "result": [{"hash": "0x1"}]}
        internal_transactions = {"status": "1", "message": "OK", "result": []}
        erc20_transfers = {"status": "1", "message": "OK", "result": [{"hash": "0x2"}]}

        with patch.object(
            client,
            "_api_get",
            side_effect=[normal_transactions, internal_transactions, erc20_transfers],
        ) as mock_api_get:
            snapshot = client.fetch_recent_transaction_history(
                "0x1111222233334444555566667777888899990000",
                page=1,
                offset=5,
            )

        self.assertEqual(
            snapshot["responses"],
            {
                "normal_transactions": normal_transactions,
                "internal_transactions": internal_transactions,
                "erc20_transfers": erc20_transfers,
            },
        )
        self.assertEqual(mock_api_get.call_count, 3)

    def test_save_recent_transaction_history_writes_under_evm_raw_directory(self) -> None:
        client = EvmWalletClient(
            api_key="test-key",
            api_url="https://example.etherscan.invalid/v2/api",
        )
        snapshot = {
            "wallet": "0x1111222233334444555566667777888899990000",
            "fetched_at_utc": "2026-03-28T12:00:00+00:00",
            "responses": {
                "normal_transactions": {"status": "1", "message": "OK", "result": []},
                "internal_transactions": {"status": "1", "message": "OK", "result": []},
                "erc20_transfers": {"status": "1", "message": "OK", "result": []},
            },
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            repository_root = Path(temp_dir)
            with patch.object(
                client,
                "fetch_recent_transaction_history",
                return_value=snapshot,
            ):
                output_path = client.save_recent_transaction_history(
                    snapshot["wallet"],
                    repository_root=repository_root,
                )

            self.assertEqual(output_path.parent, repository_root / "data" / "raw" / "evm")
            saved_snapshot = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(saved_snapshot, snapshot)


if __name__ == "__main__":
    unittest.main()
