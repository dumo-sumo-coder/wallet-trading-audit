"""Tests for the single-wallet Solana fetch script."""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "test_single_wallet.py"

SPEC = importlib.util.spec_from_file_location("test_single_wallet_script_module", SCRIPT_PATH)
if SPEC is None or SPEC.loader is None:  # pragma: no cover - defensive import guard
    raise RuntimeError("Unable to load scripts/test_single_wallet.py for tests")
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

from ingestion.solana_client import (  # noqa: E402
    SOLANA_PROVIDER_NAME,
    SolanaRpcRequestDiagnostics,
    SolanaRpcRequestError,
)


class _FakeSolanaClient:
    def fetch_recent_transaction_history(
        self,
        wallet: str,
        *,
        limit: int = 20,
    ) -> dict[str, object]:
        return {
            "wallet": wallet,
            "fetched_at_utc": "2026-03-29T01:15:00+00:00",
            "source": {"provider": "solana_json_rpc", "rpc_url": "https://example.invalid"},
            "transaction_responses": [{"signature": "sig-1"} for _ in range(limit)],
        }


class _FailingSolanaClient:
    def __init__(self) -> None:
        self.rpc_url_for_output = "https://example.solana.invalid/?redacted"

    def fetch_recent_transaction_history(
        self,
        wallet: str,
        *,
        limit: int = 20,
    ) -> dict[str, object]:
        raise SolanaRpcRequestError(
            "Solana RPC request failed with HTTP 401 for method getSignaturesForAddress",
            diagnostics=SolanaRpcRequestDiagnostics(
                provider=SOLANA_PROVIDER_NAME,
                rpc_url=self.rpc_url_for_output,
                rpc_method="getSignaturesForAddress",
                failure_category="http_error",
                provider_status="401",
                response_snippet='{"error":"bad api key"}',
                exception_class="HTTPError",
            ),
        )


class SingleWalletScriptTests(unittest.TestCase):
    def test_validate_test_wallet_accepts_target_wallet(self) -> None:
        chain = MODULE.validate_test_wallet(MODULE.DEFAULT_TEST_WALLET)

        self.assertEqual(chain, MODULE.SOLANA_WALLET_KIND)

    def test_validate_test_wallet_rejects_non_solana_shapes(self) -> None:
        with self.assertRaisesRegex(ValueError, "non-empty"):
            MODULE.validate_test_wallet(" ")

        with self.assertRaisesRegex(ValueError, "does not look like a Solana wallet"):
            MODULE.validate_test_wallet("0x1111222233334444")

    def test_ensure_helius_api_key_present_checks_presence_only(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "HELIUS_API_KEY"):
                MODULE.ensure_helius_api_key_present()

        with patch.dict(os.environ, {"HELIUS_API_KEY": "loaded-in-shell"}, clear=True):
            MODULE.ensure_helius_api_key_present()

    def test_run_single_wallet_test_supports_dry_run_without_network(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict(os.environ, {"HELIUS_API_KEY": "loaded-in-shell"}, clear=True):
                result = MODULE.run_single_wallet_test(
                    repository_root=Path(temp_dir),
                    dry_run=True,
                )

        self.assertEqual(result.status, "dry_run")
        self.assertEqual(result.tx_count, 0)
        self.assertIsNone(result.snapshot_path)
        self.assertIsNone(result.metadata_path)
        self.assertEqual(result.diagnostics["provider"], SOLANA_PROVIDER_NAME)

    def test_run_single_wallet_test_saves_snapshot_and_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repository_root = Path(temp_dir)
            with patch.dict(os.environ, {"HELIUS_API_KEY": "loaded-in-shell"}, clear=True):
                result = MODULE.run_single_wallet_test(
                    repository_root=repository_root,
                    tx_limit=3,
                    client=_FakeSolanaClient(),
                )

            self.assertEqual(result.status, "success")
            self.assertEqual(result.tx_count, 3)
            self.assertIsNotNone(result.snapshot_path)
            self.assertIsNotNone(result.metadata_path)

            snapshot = json.loads((repository_root / result.snapshot_path).read_text(encoding="utf-8"))
            metadata = json.loads((repository_root / result.metadata_path).read_text(encoding="utf-8"))

        self.assertEqual(snapshot["wallet"], MODULE.DEFAULT_TEST_WALLET)
        self.assertEqual(len(snapshot["transaction_responses"]), 3)
        self.assertEqual(metadata["wallet"], MODULE.DEFAULT_TEST_WALLET)
        self.assertEqual(metadata["tx_count"], 3)
        self.assertEqual(metadata["status"], "success")

    def test_run_single_wallet_test_writes_provider_failure_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repository_root = Path(temp_dir)
            with patch.dict(os.environ, {"HELIUS_API_KEY": "loaded-in-shell"}, clear=True):
                result = MODULE.run_single_wallet_test(
                    repository_root=repository_root,
                    tx_limit=3,
                    client=_FailingSolanaClient(),
                )

            metadata = json.loads((repository_root / result.metadata_path).read_text(encoding="utf-8"))

        self.assertEqual(result.status, "failure")
        self.assertEqual(metadata["provider"], SOLANA_PROVIDER_NAME)
        self.assertEqual(metadata["rpc_url"], "https://example.solana.invalid/?redacted")
        self.assertEqual(metadata["rpc_method"], "getSignaturesForAddress")
        self.assertEqual(metadata["failure_category"], "http_error")
        self.assertEqual(metadata["provider_status"], "401")
        self.assertEqual(metadata["exception_class"], "HTTPError")
        self.assertIsNone(metadata["snapshot_path"])


if __name__ == "__main__":
    unittest.main()
