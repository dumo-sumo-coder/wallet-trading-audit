"""Tests for the Solana RPC debug script helpers."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "debug_solana_rpc.py"

SPEC = importlib.util.spec_from_file_location("debug_solana_rpc_script_module", SCRIPT_PATH)
if SPEC is None or SPEC.loader is None:  # pragma: no cover - defensive import guard
    raise RuntimeError("Unable to load scripts/debug_solana_rpc.py for tests")
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

from ingestion.solana_client import (  # noqa: E402
    SOLANA_PROVIDER_NAME,
    SolanaRpcRequestDiagnostics,
    SolanaRpcRequestError,
)


class _SuccessfulClient:
    rpc_url_for_output = "https://example.solana.invalid/?redacted"

    def _rpc_request(self, *, method: str, params: list[object]) -> dict[str, object]:
        return {"jsonrpc": "2.0", "result": [{"signature": "sig-1"}, {"signature": "sig-2"}], "id": 1}


class _FailingClient:
    rpc_url_for_output = "https://example.solana.invalid/?redacted"

    def _rpc_request(self, *, method: str, params: list[object]) -> dict[str, object]:
        raise SolanaRpcRequestError(
            "Solana RPC request failed for method getVersion",
            diagnostics=SolanaRpcRequestDiagnostics(
                provider=SOLANA_PROVIDER_NAME,
                rpc_url=self.rpc_url_for_output,
                rpc_method=method,
                failure_category="url_error",
                provider_status=None,
                response_snippet="connection refused",
                exception_class="URLError",
            ),
        )


class DebugSolanaRpcScriptTests(unittest.TestCase):
    def test_attempt_rpc_call_reports_success(self) -> None:
        result = MODULE.attempt_rpc_call(
            _SuccessfulClient(),
            method="getSignaturesForAddress",
            params=[],
        )

        self.assertEqual(result.status, "success")
        self.assertEqual(result.result_count, 2)
        self.assertEqual(result.diagnostics["provider"], SOLANA_PROVIDER_NAME)

    def test_attempt_rpc_call_reports_sanitized_failure(self) -> None:
        result = MODULE.attempt_rpc_call(
            _FailingClient(),
            method="getVersion",
            params=[],
        )

        self.assertEqual(result.status, "failure")
        self.assertIsNone(result.result_count)
        self.assertEqual(result.diagnostics["failure_category"], "url_error")
        self.assertEqual(result.diagnostics["exception_class"], "URLError")
        self.assertEqual(result.diagnostics["rpc_url"], "https://example.solana.invalid/?redacted")


if __name__ == "__main__":
    unittest.main()
