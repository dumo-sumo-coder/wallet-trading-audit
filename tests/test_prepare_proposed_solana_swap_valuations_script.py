"""Tests for the review-first proposed Solana swap valuation helper."""

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "prepare_proposed_solana_swap_valuations.py"

SPEC = importlib.util.spec_from_file_location(
    "prepare_proposed_solana_swap_valuations_script_module",
    SCRIPT_PATH,
)
if SPEC is None or SPEC.loader is None:  # pragma: no cover - defensive import guard
    raise RuntimeError("Unable to load scripts/prepare_proposed_solana_swap_valuations.py")
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def pending_row(
    *,
    tx_hash: str,
    token_in_address: str,
    token_out_address: str,
    amount_in: str,
    amount_out: str,
    valuation_status: str = "pending",
) -> dict[str, object]:
    return {
        "tx_hash": tx_hash,
        "wallet": "wallet-1",
        "block_time": "2025-05-02T23:39:33+00:00",
        "token_in_address": token_in_address,
        "token_out_address": token_out_address,
        "amount_in": amount_in,
        "amount_out": amount_out,
        "usd_value": None,
        "valuation_source": None,
        "valuation_status": valuation_status,
    }


class PrepareProposedSolanaSwapValuationsScriptTests(unittest.TestCase):
    def test_detect_wrapped_sol_leg(self) -> None:
        token_in_row = pending_row(
            tx_hash="tx-1",
            token_in_address=MODULE.SOLANA_WRAPPED_SOL_MINT,
            token_out_address="token-out",
            amount_in="0.5",
            amount_out="100",
        )
        token_out_row = pending_row(
            tx_hash="tx-2",
            token_in_address="token-in",
            token_out_address=MODULE.SOLANA_WRAPPED_SOL_MINT,
            amount_in="100",
            amount_out="0.75",
        )
        no_sol_row = pending_row(
            tx_hash="tx-3",
            token_in_address="token-in",
            token_out_address="token-out",
            amount_in="1",
            amount_out="2",
        )

        self.assertEqual(MODULE.detect_wrapped_sol_leg(token_in_row), "token_in")
        self.assertEqual(MODULE.detect_wrapped_sol_leg(token_out_row), "token_out")
        self.assertIsNone(MODULE.detect_wrapped_sol_leg(no_sol_row))

    def test_extract_sol_amount_uses_correct_leg_for_buy_vs_sell(self) -> None:
        buy_like_row = pending_row(
            tx_hash="tx-buy",
            token_in_address="token-in",
            token_out_address=MODULE.SOLANA_WRAPPED_SOL_MINT,
            amount_in="188209.758203",
            amount_out="0.03703928",
        )
        sell_like_row = pending_row(
            tx_hash="tx-sell",
            token_in_address=MODULE.SOLANA_WRAPPED_SOL_MINT,
            token_out_address="token-out",
            amount_in="0.004217634",
            amount_out="188209.758203",
        )

        self.assertEqual(str(MODULE.extract_sol_amount(buy_like_row)), "0.03703928")
        self.assertEqual(str(MODULE.extract_sol_amount(sell_like_row)), "0.004217634")

    def test_prepare_proposals_writes_expected_structure_without_local_price_source(self) -> None:
        trusted_rows = [
            pending_row(
                tx_hash="tx-1",
                token_in_address=MODULE.SOLANA_WRAPPED_SOL_MINT,
                token_out_address="token-out",
                amount_in="0.5",
                amount_out="100",
            ),
            pending_row(
                tx_hash="tx-2",
                token_in_address="token-in",
                token_out_address=MODULE.SOLANA_WRAPPED_SOL_MINT,
                amount_in="200",
                amount_out="0.75",
            ),
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            trusted_path = Path(temp_dir) / "wallet_snapshot_20260329T011055Z_trusted_valuations.json"
            trusted_path.write_text(json.dumps({"valuations": trusted_rows}, indent=2), encoding="utf-8")

            result = MODULE.prepare_proposed_solana_swap_valuations(
                trusted_valuation_path=trusted_path
            )
            proposed_path = Path(temp_dir) / "wallet_snapshot_20260329T011055Z_proposed_valuations.json"
            saved = json.loads(proposed_path.read_text(encoding="utf-8"))

        self.assertEqual(result.rows_processed, 2)
        self.assertEqual(result.rows_with_wrapped_sol_leg, 2)
        self.assertEqual(result.rows_with_proposed_usd_value, 0)
        self.assertEqual(result.rows_missing_external_sol_usd_reference, 2)
        self.assertIn("minimal SOL/USD lookup utility first", result.recommendation)
        self.assertEqual(saved["proposals"][0]["proposed_usd_value"], None)
        self.assertEqual(saved["proposals"][0]["proposed_valuation_source"], None)
        self.assertEqual(saved["proposals"][0]["valuation_status"], "proposed")
        self.assertEqual(saved["proposals"][0]["sol_amount"], "0.5")

    def test_prepare_proposals_ignores_already_trusted_rows(self) -> None:
        trusted_rows = [
            pending_row(
                tx_hash="tx-1",
                token_in_address=MODULE.SOLANA_WRAPPED_SOL_MINT,
                token_out_address="token-out",
                amount_in="0.5",
                amount_out="100",
                valuation_status="trusted",
            ),
            pending_row(
                tx_hash="tx-2",
                token_in_address=MODULE.SOLANA_WRAPPED_SOL_MINT,
                token_out_address="token-out-2",
                amount_in="0.7",
                amount_out="120",
            ),
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            trusted_path = Path(temp_dir) / "wallet_snapshot_20260329T011055Z_trusted_valuations.json"
            trusted_path.write_text(json.dumps({"valuations": trusted_rows}, indent=2), encoding="utf-8")

            result = MODULE.prepare_proposed_solana_swap_valuations(
                trusted_valuation_path=trusted_path
            )
            proposed_path = Path(temp_dir) / "wallet_snapshot_20260329T011055Z_proposed_valuations.json"
            saved = json.loads(proposed_path.read_text(encoding="utf-8"))

        self.assertEqual(result.rows_processed, 2)
        self.assertEqual(result.rows_with_wrapped_sol_leg, 1)
        self.assertEqual(len(saved["proposals"]), 1)
        self.assertEqual(saved["proposals"][0]["tx_hash"], "tx-2")

    def test_safe_behavior_when_no_wrapped_sol_rows_exist(self) -> None:
        trusted_rows = [
            pending_row(
                tx_hash="tx-3",
                token_in_address="token-in",
                token_out_address="token-out",
                amount_in="1",
                amount_out="2",
            )
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            trusted_path = Path(temp_dir) / "wallet_snapshot_20260329T011055Z_trusted_valuations.json"
            trusted_path.write_text(json.dumps({"valuations": trusted_rows}, indent=2), encoding="utf-8")

            result = MODULE.prepare_proposed_solana_swap_valuations(
                trusted_valuation_path=trusted_path
            )
            proposed_path = Path(temp_dir) / "wallet_snapshot_20260329T011055Z_proposed_valuations.json"

        self.assertIsNone(result.proposed_valuation_path)
        self.assertEqual(result.rows_processed, 1)
        self.assertEqual(result.rows_with_wrapped_sol_leg, 0)
        self.assertEqual(result.rows_missing_external_sol_usd_reference, 0)
        self.assertEqual(proposed_path.exists(), False)


if __name__ == "__main__":
    unittest.main()
