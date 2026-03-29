"""Tests for conservative Solana valuation preparation."""

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

from normalize.schema import EventType  # noqa: E402
from normalize.transactions import normalize_transaction  # noqa: E402
from valuation.solana_valuation import (  # noqa: E402
    VALUATION_STATUS_TRUSTED,
    apply_trusted_usd_values,
    get_rows_requiring_valuation,
    load_trusted_valuation_records,
    summarize_valuation_readiness,
)


def load_json_fixture(name: str) -> dict[str, object]:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def normalize_fixture(name: str) -> object:
    wallet_snapshot = load_json_fixture("solana_wallet_snapshot.json")
    return normalize_transaction(
        {
            "chain": "solana",
            "wallet": wallet_snapshot["wallet"],
            **load_json_fixture(name),
        }
    )


def build_trusted_record(transaction: object, *, usd_value: str) -> dict[str, object]:
    return {
        "tx_hash": transaction.tx_hash,
        "wallet": transaction.wallet,
        "block_time": transaction.block_time.isoformat(),
        "token_in_address": transaction.token_in_address,
        "token_out_address": transaction.token_out_address,
        "amount_in": str(transaction.amount_in),
        "amount_out": str(transaction.amount_out),
        "valuation_source": "manual_review",
        "usd_value": usd_value,
        "valuation_status": VALUATION_STATUS_TRUSTED,
    }


class SolanaValuationTests(unittest.TestCase):
    def test_get_rows_requiring_valuation_identifies_unpriced_swap_rows(self) -> None:
        buy = normalize_fixture("solana_transaction_response_buy_example.json")
        sell = normalize_fixture("solana_transaction_response_sell_example.json")
        transfer = normalize_fixture("solana_transaction_response_transfer_in_example.json")

        rows = get_rows_requiring_valuation([buy, sell, transfer])
        summary = summarize_valuation_readiness([buy, sell, transfer])

        self.assertEqual(len(rows), 2)
        self.assertEqual(summary.swap_transactions, 2)
        self.assertEqual(summary.rows_requiring_valuation_count, 2)
        self.assertEqual(summary.swap_rows_already_valued_count, 0)
        self.assertEqual(summary.valuation_complete, False)

    def test_apply_trusted_usd_values_updates_matching_rows(self) -> None:
        buy = normalize_fixture("solana_transaction_response_buy_example.json")
        sell = normalize_fixture("solana_transaction_response_sell_example.json")
        records = load_trusted_valuation_records_from_objects(
            [
                build_trusted_record(buy, usd_value="100"),
                build_trusted_record(sell, usd_value="150"),
            ]
        )

        result = apply_trusted_usd_values([buy, sell], records)
        summary = summarize_valuation_readiness(result.transactions)

        self.assertEqual(len(result.applied_records), 2)
        self.assertEqual(str(result.transactions[0].usd_value), "100")
        self.assertEqual(str(result.transactions[1].usd_value), "150")
        self.assertEqual(summary.rows_requiring_valuation_count, 0)
        self.assertEqual(summary.valuation_complete, True)

    def test_apply_trusted_usd_values_rejects_mismatched_record(self) -> None:
        buy = normalize_fixture("solana_transaction_response_buy_example.json")
        mismatched_record = build_trusted_record(buy, usd_value="100")
        mismatched_record["amount_out"] = "999"
        records = load_trusted_valuation_records_from_objects([mismatched_record])

        with self.assertRaisesRegex(
            ValueError,
            "Trusted valuation record does not match normalized transaction fields",
        ):
            apply_trusted_usd_values([buy], records)

    def test_load_trusted_valuation_records_rejects_malformed_records(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            malformed_path = Path(temp_dir) / "bad_valuations.json"
            malformed_path.write_text(
                json.dumps(
                    {
                        "valuations": [
                            {
                                "tx_hash": "abc",
                                "wallet": "wallet",
                                "block_time": "2026-03-29T00:00:00+00:00",
                                "token_in_address": "mint-in",
                                "token_out_address": "mint-out",
                                "amount_in": "1",
                                "amount_out": "2",
                                "valuation_source": "manual_review",
                                "usd_value": "10",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "valuation_status"):
                load_trusted_valuation_records(malformed_path)

    def test_load_trusted_valuation_records_ignores_pending_template_rows(self) -> None:
        buy = normalize_fixture("solana_transaction_response_buy_example.json")
        pending_record = build_trusted_record(buy, usd_value="100")
        pending_record["usd_value"] = None
        pending_record["valuation_source"] = None
        pending_record["valuation_status"] = "pending"

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "pending_valuations.json"
            path.write_text(json.dumps({"valuations": [pending_record]}, indent=2), encoding="utf-8")
            loaded = load_trusted_valuation_records(path)

        self.assertEqual(loaded, ())


def load_trusted_valuation_records_from_objects(records: list[dict[str, object]]):
    with tempfile.TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "valuations.json"
        path.write_text(json.dumps({"valuations": records}, indent=2), encoding="utf-8")
        loaded = load_trusted_valuation_records(path)
    return loaded


if __name__ == "__main__":
    unittest.main()
