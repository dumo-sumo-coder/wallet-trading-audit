"""Tests for the manual trusted valuation template helper script."""

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "prepare_trusted_valuations_template.py"
RAW_SOLANA_FIXTURE_DIR = ROOT / "tests" / "fixtures" / "raw_solana"

SPEC = importlib.util.spec_from_file_location(
    "prepare_trusted_valuations_template_script_module",
    SCRIPT_PATH,
)
if SPEC is None or SPEC.loader is None:  # pragma: no cover - defensive import guard
    raise RuntimeError("Unable to load scripts/prepare_trusted_valuations_template.py for tests")
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


class PrepareTrustedValuationsTemplateScriptTests(unittest.TestCase):
    def test_prepare_template_generates_pending_rows_with_strict_structure(self) -> None:
        buy = load_json_fixture("solana_transaction_response_buy_example.json")
        sell = load_json_fixture("solana_transaction_response_sell_example.json")
        snapshot = build_snapshot_payload(buy, sell)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            snapshot_path = temp_path / "wallet_snapshot_20260329T030000Z.json"
            snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")

            result = MODULE.prepare_trusted_valuations_template(snapshot_path=snapshot_path)
            template_path = snapshot_path.with_name(
                "wallet_snapshot_20260329T030000Z_trusted_valuations.json"
            )
            saved_template = json.loads(template_path.read_text(encoding="utf-8"))

        self.assertEqual(result.status, "template_created")
        self.assertEqual(result.valuation_rows_count, 2)
        self.assertEqual(result.template_path, str(template_path))
        self.assertEqual(list(saved_template.keys()), ["valuations"])
        self.assertEqual(len(saved_template["valuations"]), 2)
        self.assertEqual(
            list(saved_template["valuations"][0].keys()),
            [
                "tx_hash",
                "wallet",
                "block_time",
                "token_in_address",
                "token_out_address",
                "amount_in",
                "amount_out",
                "usd_value",
                "valuation_source",
                "valuation_status",
            ],
        )
        self.assertEqual(saved_template["valuations"][0]["usd_value"], None)
        self.assertEqual(saved_template["valuations"][0]["valuation_source"], None)
        self.assertEqual(saved_template["valuations"][0]["valuation_status"], "pending")

    def test_prepare_template_skips_creation_when_no_rows_require_valuation(self) -> None:
        transfer = load_json_fixture("solana_transaction_response_transfer_in_example.json")
        snapshot = build_snapshot_payload(transfer)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            snapshot_path = temp_path / "wallet_snapshot_20260329T030000Z.json"
            snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")

            result = MODULE.prepare_trusted_valuations_template(snapshot_path=snapshot_path)
            template_path = snapshot_path.with_name(
                "wallet_snapshot_20260329T030000Z_trusted_valuations.json"
            )
            self.assertEqual(template_path.exists(), False)

        self.assertEqual(result.status, "no_rows_require_valuation")
        self.assertEqual(result.valuation_rows_count, 0)
        self.assertIsNone(result.template_path)


if __name__ == "__main__":
    unittest.main()
