"""Tests for the operator single-wallet Solana fetch script."""

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
SCRIPT_PATH = ROOT / "scripts" / "run_single_wallet_fetch.py"

SPEC = importlib.util.spec_from_file_location("run_single_wallet_fetch_script_module", SCRIPT_PATH)
if SPEC is None or SPEC.loader is None:  # pragma: no cover - defensive import guard
    raise RuntimeError("Unable to load scripts/run_single_wallet_fetch.py for tests")
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

from ingestion.solana_client import (  # noqa: E402
    SolanaRpcRequestDiagnostics,
    SolanaRpcRequestError,
)


class _SuccessfulClient:
    rpc_url_for_output = "https://mainnet.helius-rpc.com/?redacted"

    def _rpc_request(self, *, method: str, params: list[object]) -> dict[str, object]:
        return {"jsonrpc": "2.0", "result": {"solana-core": "2.1.0"}, "id": 1}

    def fetch_recent_transaction_history(
        self,
        wallet: str,
        *,
        limit: int = 20,
    ) -> dict[str, object]:
        return {
            "wallet": wallet,
            "fetched_at_utc": "2026-03-29T01:15:00+00:00",
            "source": {
                "provider": "solana_json_rpc",
                "rpc_url": self.rpc_url_for_output,
            },
            "transaction_responses": [{"signature": f"sig-{index}"} for index in range(limit)],
        }


class _ConnectivityFailingClient:
    rpc_url_for_output = "https://mainnet.helius-rpc.com/?redacted"

    def _rpc_request(self, *, method: str, params: list[object]) -> dict[str, object]:
        raise SolanaRpcRequestError(
            "Solana RPC TLS verification failed for method getVersion",
            diagnostics=SolanaRpcRequestDiagnostics(
                provider="solana_json_rpc",
                rpc_url=self.rpc_url_for_output,
                rpc_method=method,
                failure_category="tls_error",
                provider_status="ssl_cert_verify_failed",
                response_snippet="certificate verify failed",
                exception_class="SSLCertVerificationError",
            ),
        )


class RunSingleWalletFetchScriptTests(unittest.TestCase):
    def test_parse_args_supports_wallet_limit_and_preflight(self) -> None:
        args = MODULE.parse_args(
            [
                "--wallet",
                MODULE.DEFAULT_TEST_WALLET,
                "--tx-limit",
                "12",
                "--verbose",
                "--preflight-only",
            ]
        )

        self.assertEqual(args.wallet, MODULE.DEFAULT_TEST_WALLET)
        self.assertEqual(args.tx_limit, 12)
        self.assertEqual(args.verbose, True)
        self.assertEqual(args.preflight_only, True)

    def test_run_workflow_rejects_missing_env(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "HELIUS_API_KEY"):
                MODULE.run_single_wallet_fetch_workflow(
                    wallet=MODULE.DEFAULT_TEST_WALLET,
                    tx_limit=5,
                    preflight_only=True,
                )

    def test_run_workflow_supports_preflight_only_without_network(self) -> None:
        with patch.dict(os.environ, {"HELIUS_API_KEY": "loaded-in-shell"}, clear=True):
            result = MODULE.run_single_wallet_fetch_workflow(
                wallet=MODULE.DEFAULT_TEST_WALLET,
                tx_limit=5,
                preflight_only=True,
                client=_SuccessfulClient(),
            )

        self.assertEqual(result.preflight_only, True)
        self.assertIsNone(result.connectivity_result)
        self.assertIsNone(result.fetch_result)

    def test_run_workflow_surfaces_connectivity_failure_before_fetch(self) -> None:
        with patch.dict(os.environ, {"HELIUS_API_KEY": "loaded-in-shell"}, clear=True):
            result = MODULE.run_single_wallet_fetch_workflow(
                wallet=MODULE.DEFAULT_TEST_WALLET,
                tx_limit=5,
                preflight_only=False,
                client=_ConnectivityFailingClient(),
            )

        self.assertEqual(result.preflight_only, False)
        self.assertIsNotNone(result.connectivity_result)
        self.assertEqual(result.connectivity_result.status, "failure")
        self.assertEqual(result.connectivity_result.diagnostics["failure_category"], "tls_error")
        self.assertIsNone(result.fetch_result)

    def test_run_workflow_runs_success_path_and_saves_fetch_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repository_root = Path(temp_dir)
            with patch.dict(os.environ, {"HELIUS_API_KEY": "loaded-in-shell"}, clear=True):
                result = MODULE.run_single_wallet_fetch_workflow(
                    wallet=MODULE.DEFAULT_TEST_WALLET,
                    tx_limit=3,
                    preflight_only=False,
                    repository_root=repository_root,
                    client=_SuccessfulClient(),
                )

            self.assertIsNotNone(result.fetch_result)
            fetch_result = result.fetch_result
            assert fetch_result is not None
            metadata = json.loads((repository_root / fetch_result.metadata_path).read_text(encoding="utf-8"))

        self.assertEqual(result.connectivity_result.status, "success")
        self.assertEqual(fetch_result.status, "success")
        self.assertEqual(fetch_result.tx_count, 3)
        self.assertEqual(metadata["status"], "success")
        self.assertEqual(metadata["tx_count"], 3)


if __name__ == "__main__":
    unittest.main()
