"""Fixture-backed tests for raw Solana payload review helpers."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "raw_solana"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ingestion.solana_review import (  # noqa: E402
    export_representative_transaction_payloads,
    inspect_solana_snapshot,
    inspect_solana_transaction_response,
    load_json_mapping,
)


class SolanaReviewFixtureTests(unittest.TestCase):
    def test_snapshot_fixture_loads(self) -> None:
        snapshot = load_json_mapping(FIXTURE_DIR / "solana_wallet_snapshot.json")

        self.assertEqual(
            snapshot["wallet"],
            "47eFuHR9ste9kopiJ9eRxcwahmE62JovbKe5r7AjANut",
        )
        self.assertIn("signatures_response", snapshot)
        self.assertEqual(len(snapshot["transaction_responses"]), 1)

    def test_transaction_response_fixture_loads(self) -> None:
        payload = load_json_mapping(
            FIXTURE_DIR / "solana_transaction_response_example.json"
        )

        self.assertEqual(payload["jsonrpc"], "2.0")
        self.assertIn("result", payload)
        self.assertEqual(payload["result"]["version"], 0)

    def test_inspect_solana_snapshot_reports_key_presence(self) -> None:
        snapshot = load_json_mapping(FIXTURE_DIR / "solana_wallet_snapshot.json")

        inspection = inspect_solana_snapshot(snapshot)

        self.assertEqual(
            inspection["snapshot_keys"],
            [
                "capture",
                "fetched_at_utc",
                "request",
                "signatures_response",
                "source",
                "transaction_request",
                "transaction_responses",
                "wallet",
            ],
        )
        self.assertEqual(inspection["transaction_response_count"], 1)
        payload_summary = inspection["payloads"][0]
        self.assertEqual(
            payload_summary["signature"],
            "56rgv1Fqg7MHLoct3ESNx8DHeC2c5TFYyK1As35paWn2KYuRuXPFZK3XDEwkkZemVfmnKrkj17mMzz4N1d8kMYC4",
        )
        self.assertTrue(payload_summary["field_presence"]["result.blockTime"])
        self.assertTrue(payload_summary["field_presence"]["result.meta.fee"])
        self.assertTrue(
            payload_summary["field_presence"][
                "result.transaction.message.accountKeys"
            ]
        )
        self.assertEqual(payload_summary["field_counts"]["instructions"], 1)
        self.assertIn(
            "TODO: wallet-side token_in/token_out mapping still needs fixture-driven rules for account ownership and balance deltas.",
            payload_summary["notes"],
        )

    def test_inspect_single_payload_reports_response_and_result_keys(self) -> None:
        payload = load_json_mapping(
            FIXTURE_DIR / "solana_transaction_response_example.json"
        )

        inspection = inspect_solana_transaction_response(payload)

        self.assertEqual(inspection["response_keys"], ["id", "jsonrpc", "result"])
        self.assertIn("meta", inspection["result_keys"])
        self.assertIn("transaction", inspection["result_keys"])
        self.assertEqual(inspection["field_counts"]["pre_token_balances"], 0)
        self.assertEqual(inspection["field_counts"]["post_token_balances"], 1)

    def test_export_representative_transaction_payloads_writes_fixture_like_files(self) -> None:
        snapshot = load_json_mapping(FIXTURE_DIR / "solana_wallet_snapshot.json")
        expected_payload = load_json_mapping(
            FIXTURE_DIR / "solana_transaction_response_example.json"
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            output_paths = export_representative_transaction_payloads(
                snapshot,
                Path(temp_dir),
                limit=1,
            )

            self.assertEqual(len(output_paths), 1)
            written_payload = json.loads(output_paths[0].read_text(encoding="utf-8"))
            self.assertEqual(written_payload, expected_payload)


if __name__ == "__main__":
    unittest.main()
