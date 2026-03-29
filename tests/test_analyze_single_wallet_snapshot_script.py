"""Tests for the single-wallet Solana snapshot analysis script."""

from __future__ import annotations

import copy
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "analyze_single_wallet_snapshot.py"
RAW_SOLANA_FIXTURE_DIR = ROOT / "tests" / "fixtures" / "raw_solana"

SPEC = importlib.util.spec_from_file_location(
    "analyze_single_wallet_snapshot_script_module",
    SCRIPT_PATH,
)
if SPEC is None or SPEC.loader is None:  # pragma: no cover - defensive import guard
    raise RuntimeError("Unable to load scripts/analyze_single_wallet_snapshot.py for tests")
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def load_json_fixture(name: str) -> dict[str, object]:
    return json.loads((RAW_SOLANA_FIXTURE_DIR / name).read_text(encoding="utf-8"))


def build_snapshot_payload(*transaction_responses: object) -> dict[str, object]:
    snapshot = load_json_fixture("solana_wallet_snapshot.json")
    snapshot["transaction_responses"] = list(transaction_responses)
    signatures_response = snapshot["signatures_response"]
    assert isinstance(signatures_response, dict)
    signatures_response["result"] = [
        {"signature": f"sig-{index}"} for index, _ in enumerate(transaction_responses, start=1)
    ]
    return snapshot


class AnalyzeSingleWalletSnapshotScriptTests(unittest.TestCase):
    def test_find_latest_snapshot_path_uses_latest_timestamped_snapshot(self) -> None:
        snapshot = build_snapshot_payload(load_json_fixture("solana_transaction_response_example.json"))

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            older = temp_path / "wallet_snapshot_20260329T010000Z.json"
            newer = temp_path / "wallet_snapshot_20260329T020000Z.json"
            older.write_text(json.dumps(snapshot), encoding="utf-8")
            newer.write_text(json.dumps(snapshot), encoding="utf-8")

            latest_path = MODULE.find_latest_snapshot_path(temp_path)

        self.assertEqual(latest_path.name, newer.name)

    def test_analyze_snapshot_groups_unsupported_reasons(self) -> None:
        transfer_in = load_json_fixture("solana_transaction_response_transfer_in_example.json")
        ambiguous_buy = copy.deepcopy(load_json_fixture("solana_transaction_response_buy_example.json"))
        ambiguous_buy["result"]["meta"]["postTokenBalances"].append(
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
        snapshot = build_snapshot_payload(transfer_in, ambiguous_buy, copy.deepcopy(ambiguous_buy))

        analysis = MODULE.analyze_snapshot_mapping(
            snapshot,
            snapshot_path=ROOT / "tests" / "fixtures" / "raw_solana" / "synthetic_snapshot.json",
        )

        self.assertEqual(analysis.total_raw_transactions, 3)
        self.assertEqual(analysis.normalized_transactions_count, 1)
        self.assertEqual(analysis.unsupported_transactions_count, 2)
        self.assertEqual(len(analysis.unsupported_reason_counts), 1)
        self.assertEqual(analysis.unsupported_reason_counts[0].count, 2)
        self.assertIn(
            "multiple wallet token balance deltas detected",
            analysis.unsupported_reason_counts[0].reason,
        )

    def test_analyze_snapshot_reports_fifo_not_meaningful_without_usd_value(self) -> None:
        buy = load_json_fixture("solana_transaction_response_buy_example.json")
        sell = load_json_fixture("solana_transaction_response_sell_example.json")
        snapshot = build_snapshot_payload(buy, sell)

        analysis = MODULE.analyze_snapshot_mapping(
            snapshot,
            snapshot_path=ROOT / "tests" / "fixtures" / "raw_solana" / "synthetic_fifo_snapshot.json",
        )

        self.assertEqual(analysis.normalized_transactions_count, 2)
        self.assertEqual(analysis.unsupported_transactions_count, 0)
        self.assertEqual(analysis.fifo_summary.status, "not_meaningful_missing_valuation")
        self.assertEqual(analysis.fifo_summary.skipped_missing_valuation_count, 2)
        self.assertEqual(analysis.fifo_summary.realized_pnl_usd, None)
        self.assertEqual(analysis.fifo_summary.meaningful, False)

    def test_analyze_snapshot_path_writes_json_summary_next_to_snapshot(self) -> None:
        snapshot = build_snapshot_payload(load_json_fixture("solana_transaction_response_example.json"))

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            snapshot_path = temp_path / "wallet_snapshot_20260329T030000Z.json"
            snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")

            analysis = MODULE.analyze_snapshot_path(snapshot_path)
            summary_path = snapshot_path.with_name(f"{snapshot_path.stem}_analysis_summary.json")
            saved_summary = json.loads(summary_path.read_text(encoding="utf-8"))

        self.assertEqual(analysis.summary_path, str(summary_path))
        self.assertEqual(saved_summary["total_raw_transactions"], 1)
        self.assertEqual(saved_summary["normalized_transactions_count"], 1)
        self.assertEqual(saved_summary["unsupported_transactions_count"], 0)


if __name__ == "__main__":
    unittest.main()
