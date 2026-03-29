"""Tests for minimal raw ingestion clients."""

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
from urllib.error import HTTPError
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ingestion.evm_client import EvmWalletClient  # noqa: E402
from ingestion.solana_client import (  # noqa: E402
    SolanaRpcClient,
    extract_solana_rpc_diagnostics,
)


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
        self.assertEqual(snapshot["source"]["rpc_url"], "https://example.solana.invalid")
        self.assertEqual(snapshot["capture"]["response_bodies_preserved"], True)
        self.assertEqual(snapshot["capture"]["http_headers_preserved"], False)
        self.assertEqual(snapshot["transaction_request"]["method"], "getTransaction")
        self.assertEqual(
            snapshot["transaction_request"]["max_supported_transaction_version"],
            0,
        )
        self.assertEqual(mock_rpc_request.call_count, 3)

    def test_fetch_recent_transaction_history_redacts_rpc_query_params_in_saved_metadata(self) -> None:
        client = SolanaRpcClient(rpc_url="https://example.solana.invalid/?api-key=secret-value")
        signatures_response = {"jsonrpc": "2.0", "result": [], "id": 1}

        with patch.object(
            client,
            "_rpc_request",
            return_value=signatures_response,
        ):
            snapshot = client.fetch_recent_transaction_history(
                "TestSolanaWallet11111111111111111111111111111",
                limit=1,
            )

        self.assertEqual(snapshot["source"]["rpc_url"], "https://example.solana.invalid/?redacted")
        self.assertNotIn("secret-value", json.dumps(snapshot))

    def test_rpc_error_diagnostics_include_sanitized_http_details(self) -> None:
        client = SolanaRpcClient(rpc_url="https://example.solana.invalid/?api-key=secret-value")
        http_error = HTTPError(
            client.rpc_url,
            401,
            "Unauthorized",
            hdrs=None,
            fp=io.BytesIO(
                b'{"error":"bad api key for https://example.solana.invalid/?api-key=secret-value"}'
            ),
        )

        with patch("ingestion.solana_client.urlopen", side_effect=http_error):
            with self.assertRaises(Exception) as context:
                client._rpc_request(method="getVersion", params=[])

        diagnostics = extract_solana_rpc_diagnostics(context.exception)
        self.assertEqual(diagnostics["failure_category"], "http_error")
        self.assertEqual(diagnostics["provider_status"], "401")
        self.assertEqual(diagnostics["rpc_method"], "getVersion")
        self.assertEqual(diagnostics["exception_class"], "HTTPError")
        self.assertIn("?redacted", diagnostics["response_snippet"] or "")
        self.assertNotIn("secret-value", diagnostics["response_snippet"] or "")

    def test_save_recent_transaction_history_writes_under_solana_raw_directory(self) -> None:
        client = SolanaRpcClient(rpc_url="https://example.solana.invalid")
        snapshot = {
            "wallet": "TestSolanaWallet11111111111111111111111111111",
            "fetched_at_utc": "2026-03-28T12:00:00+00:00",
            "capture": {
                "normalization_applied": False,
                "response_body_format": "json",
                "response_bodies_preserved": True,
                "http_headers_preserved": False,
                "signature_order": "newest_first",
                "retrieval_pattern": "getSignaturesForAddress_then_getTransaction",
            },
            "request": {
                "method": "getSignaturesForAddress",
                "limit": 20,
                "before": None,
                "commitment": "confirmed",
            },
            "transaction_request": {
                "method": "getTransaction",
                "commitment": "confirmed",
                "encoding": "json",
                "max_supported_transaction_version": 0,
            },
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
        self.assertEqual(snapshot["source"]["provider"], "etherscan_v2_multichain")
        self.assertEqual(snapshot["capture"]["response_bodies_preserved"], True)
        self.assertEqual(snapshot["capture"]["http_headers_preserved"], False)
        self.assertIn("paid-tier access", snapshot["capture"]["provider_access_note"])
        self.assertEqual(
            snapshot["request"]["actions"],
            ["txlist", "txlistinternal", "tokentx"],
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
            "capture": {
                "normalization_applied": False,
                "response_body_format": "json",
                "response_bodies_preserved": True,
                "http_headers_preserved": False,
                "provider_access_note": "Assumes paid-tier access.",
            },
            "request": {
                "provider_family": "etherscan_v2",
                "module": "account",
                "actions": ["txlist", "txlistinternal", "tokentx"],
                "chain_id": "56",
                "startblock": "0",
                "endblock": "9999999999",
                "page": 1,
                "offset": 20,
                "sort": "desc",
            },
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

    def test_bscscan_env_var_is_not_implicitly_treated_as_etherscan_v2_access(self) -> None:
        with patch.dict(os.environ, {"BSCSCAN_API_KEY": "bsc-only-key"}, clear=True):
            client = EvmWalletClient()

        with self.assertRaisesRegex(ValueError, "ETHERSCAN_API_KEY"):
            client.fetch_recent_transaction_history(
                "0x1111222233334444555566667777888899990000",
            )


if __name__ == "__main__":
    unittest.main()
