"""Tests for populating trusted Solana swap valuations via SOL/USD lookup."""

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "populate_trusted_solana_swap_valuations.py"
ANALYZE_SCRIPT_PATH = ROOT / "scripts" / "analyze_single_wallet_snapshot.py"
RAW_SOLANA_FIXTURE_DIR = ROOT / "tests" / "fixtures" / "raw_solana"

POPULATE_SPEC = importlib.util.spec_from_file_location(
    "populate_trusted_solana_swap_valuations_script_module",
    SCRIPT_PATH,
)
if POPULATE_SPEC is None or POPULATE_SPEC.loader is None:  # pragma: no cover
    raise RuntimeError("Unable to load populate_trusted_solana_swap_valuations.py")
POPULATE_MODULE = importlib.util.module_from_spec(POPULATE_SPEC)
sys.modules[POPULATE_SPEC.name] = POPULATE_MODULE
POPULATE_SPEC.loader.exec_module(POPULATE_MODULE)

ANALYZE_SPEC = importlib.util.spec_from_file_location(
    "analyze_single_wallet_snapshot_script_for_populate_tests",
    ANALYZE_SCRIPT_PATH,
)
if ANALYZE_SPEC is None or ANALYZE_SPEC.loader is None:  # pragma: no cover
    raise RuntimeError("Unable to load analyze_single_wallet_snapshot.py")
ANALYZE_MODULE = importlib.util.module_from_spec(ANALYZE_SPEC)
sys.modules[ANALYZE_SPEC.name] = ANALYZE_MODULE
ANALYZE_SPEC.loader.exec_module(ANALYZE_MODULE)

SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from normalize.transactions import normalize_transaction  # noqa: E402
from valuation.sol_usd_lookup import SolUsdLookupResult  # noqa: E402


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


def build_pending_row(*, wallet: str, raw_payload: dict[str, object]) -> dict[str, object]:
    normalized = normalize_transaction({"chain": "solana", "wallet": wallet, **raw_payload})
    return {
        "tx_hash": normalized.tx_hash,
        "wallet": normalized.wallet,
        "block_time": normalized.block_time.isoformat(),
        "token_in_address": normalized.token_in_address,
        "token_out_address": normalized.token_out_address,
        "amount_in": str(normalized.amount_in),
        "amount_out": str(normalized.amount_out),
        "usd_value": None,
        "valuation_source": None,
        "valuation_status": "pending",
    }


def mock_lookup_result(price: str, timestamp: datetime) -> SolUsdLookupResult:
    candle_start = timestamp.replace(second=0, microsecond=0)
    return SolUsdLookupResult(
        source_name="coinbase_exchange_public_candles",
        product_id="SOL-USD",
        reference_price_usd=Decimal(price),
        price_reference_kind="minute_candle_open",
        reference_candle_start=candle_start,
        reference_candle_end=candle_start,
        lookup_timestamp=datetime(2026, 3, 29, 2, 0, tzinfo=UTC),
        request_url="https://api.exchange.coinbase.com/?redacted",
    )


class PopulateTrustedSolanaSwapValuationsScriptTests(unittest.TestCase):
    def test_populate_trusted_valuations_updates_wrapped_sol_rows(self) -> None:
        wallet = load_json_fixture("solana_wallet_snapshot.json")["wallet"]
        buy = load_json_fixture("solana_transaction_response_buy_example.json")
        sell = load_json_fixture("solana_transaction_response_sell_example.json")
        rows = [
            build_pending_row(wallet=wallet, raw_payload=buy),
            build_pending_row(wallet=wallet, raw_payload=sell),
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            trusted_path = Path(temp_dir) / "wallet_snapshot_20260329T011055Z_trusted_valuations.json"
            trusted_path.write_text(json.dumps({"valuations": rows}, indent=2), encoding="utf-8")

            def _lookup(timestamp: datetime):
                return mock_lookup_result("100", timestamp)

            with patch.object(POPULATE_MODULE, "lookup_sol_usd_at_timestamp", side_effect=_lookup):
                result = POPULATE_MODULE.populate_trusted_solana_swap_valuations(
                    trusted_valuation_path=trusted_path
                )

            saved = json.loads(trusted_path.read_text(encoding="utf-8"))

        self.assertEqual(result.rows_processed, 2)
        self.assertEqual(result.wrapped_sol_rows, 2)
        self.assertEqual(result.trusted_rows_populated, 2)
        self.assertEqual(saved["valuations"][0]["valuation_status"], "trusted")
        self.assertEqual(saved["valuations"][1]["valuation_status"], "trusted")
        self.assertEqual(Decimal(saved["valuations"][0]["usd_value"]), Decimal("100"))
        self.assertEqual(Decimal(saved["valuations"][1]["usd_value"]), Decimal("150.0"))
        self.assertIn("coinbase_exchange_public_candles", saved["valuations"][0]["valuation_source"])

    def test_populate_trusted_valuations_does_not_overwrite_existing_without_flag(self) -> None:
        wallet = load_json_fixture("solana_wallet_snapshot.json")["wallet"]
        buy = load_json_fixture("solana_transaction_response_buy_example.json")
        row = build_pending_row(wallet=wallet, raw_payload=buy)
        row["usd_value"] = "12.34"
        row["valuation_source"] = "manual"
        row["valuation_status"] = "trusted"

        with tempfile.TemporaryDirectory() as temp_dir:
            trusted_path = Path(temp_dir) / "wallet_snapshot_20260329T011055Z_trusted_valuations.json"
            trusted_path.write_text(json.dumps({"valuations": [row]}, indent=2), encoding="utf-8")

            with patch.object(
                POPULATE_MODULE,
                "lookup_sol_usd_at_timestamp",
                return_value=mock_lookup_result("100", datetime.fromisoformat(row["block_time"])),
            ):
                result = POPULATE_MODULE.populate_trusted_solana_swap_valuations(
                    trusted_valuation_path=trusted_path
                )

            saved = json.loads(trusted_path.read_text(encoding="utf-8"))

        self.assertEqual(result.trusted_rows_populated, 0)
        self.assertEqual(result.skipped_existing_trusted_rows, 1)
        self.assertEqual(saved["valuations"][0]["usd_value"], "12.34")
        self.assertEqual(saved["valuations"][0]["valuation_source"], "manual")

    def test_analysis_becomes_meaningful_after_trusted_valuations_are_present(self) -> None:
        wallet = load_json_fixture("solana_wallet_snapshot.json")["wallet"]
        buy = load_json_fixture("solana_transaction_response_buy_example.json")
        sell = load_json_fixture("solana_transaction_response_sell_example.json")
        snapshot = build_snapshot_payload(buy, sell)
        rows = [
            build_pending_row(wallet=wallet, raw_payload=buy),
            build_pending_row(wallet=wallet, raw_payload=sell),
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            snapshot_path = temp_path / "wallet_snapshot_20260329T011055Z.json"
            trusted_path = temp_path / "wallet_snapshot_20260329T011055Z_trusted_valuations.json"
            snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
            trusted_path.write_text(json.dumps({"valuations": rows}, indent=2), encoding="utf-8")

            def _lookup(timestamp: datetime):
                return mock_lookup_result("100", timestamp)

            with patch.object(POPULATE_MODULE, "lookup_sol_usd_at_timestamp", side_effect=_lookup):
                POPULATE_MODULE.populate_trusted_solana_swap_valuations(
                    trusted_valuation_path=trusted_path
                )

            analysis = ANALYZE_MODULE.analyze_snapshot_path(snapshot_path)

        self.assertEqual(analysis.valuation_summary.rows_requiring_valuation_after_count, 0)
        self.assertEqual(analysis.valuation_summary.local_trusted_valuations_applied_count, 2)
        self.assertEqual(analysis.fifo_summary.meaningful, True)
        self.assertEqual(analysis.fifo_summary.realized_pnl_usd, Decimal("50.0"))
        self.assertEqual(analysis.fifo_summary.trade_matches_count, 1)


if __name__ == "__main__":
    unittest.main()
