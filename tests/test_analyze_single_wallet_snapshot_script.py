"""Tests for the single-wallet Solana snapshot analysis script."""

from __future__ import annotations

import copy
import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from decimal import Decimal

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

SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from normalize.transactions import normalize_transaction  # noqa: E402
from valuation.solana_valuation import VALUATION_STATUS_TRUSTED  # noqa: E402


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


def build_trusted_valuation_record(
    *,
    wallet: str,
    raw_payload: dict[str, object],
    usd_value: str,
    valuation_source: str = "manual_review",
) -> dict[str, object]:
    normalized = normalize_transaction({"chain": "solana", "wallet": wallet, **raw_payload})
    return {
        "tx_hash": normalized.tx_hash,
        "wallet": normalized.wallet,
        "block_time": normalized.block_time.isoformat(),
        "token_in_address": normalized.token_in_address,
        "token_out_address": normalized.token_out_address,
        "amount_in": str(normalized.amount_in),
        "amount_out": str(normalized.amount_out),
        "valuation_source": valuation_source,
        "usd_value": usd_value,
        "valuation_status": VALUATION_STATUS_TRUSTED,
    }


class AnalyzeSingleWalletSnapshotScriptTests(unittest.TestCase):
    def test_find_latest_snapshot_path_uses_latest_timestamped_snapshot(self) -> None:
        snapshot = build_snapshot_payload(load_json_fixture("solana_transaction_response_example.json"))

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            older = temp_path / "wallet_snapshot_20260329T010000Z.json"
            newer = temp_path / "wallet_snapshot_20260329T020000Z.json"
            summary = temp_path / "wallet_snapshot_20260329T020000Z_analysis_summary.json"
            proposed = temp_path / "wallet_snapshot_20260329T030000Z_proposed_valuations.json"
            trade_report = temp_path / "wallet_snapshot_20260329T040000Z_trade_report.json"
            behavior_report = temp_path / "wallet_snapshot_20260329T050000Z_behavior_report.json"
            older.write_text(json.dumps(snapshot), encoding="utf-8")
            newer.write_text(json.dumps(snapshot), encoding="utf-8")
            summary.write_text(json.dumps({"summary": True}), encoding="utf-8")
            proposed.write_text(json.dumps({"valuations": []}), encoding="utf-8")
            trade_report.write_text(json.dumps({"matched_trades": []}), encoding="utf-8")
            behavior_report.write_text(json.dumps({"trade_rows": []}), encoding="utf-8")

            latest_path = MODULE.find_latest_snapshot_path(temp_path)

        self.assertEqual(latest_path.name, newer.name)

    def test_find_latest_fetch_metadata_path_prefers_latest_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            older = temp_path / "wallet_fetch_metadata_20260329T010000Z.json"
            newer = temp_path / "wallet_fetch_metadata_20260329T020000Z.json"
            derived_report = temp_path / "wallet_fetch_metadata_20260329T030000Z_trade_report.json"
            older.write_text(json.dumps({"page_snapshot_paths": []}), encoding="utf-8")
            newer.write_text(json.dumps({"page_snapshot_paths": []}), encoding="utf-8")
            derived_report.write_text(json.dumps({"matched_trades": []}), encoding="utf-8")

            latest_path = MODULE.find_latest_fetch_metadata_path(temp_path)

        self.assertEqual(latest_path, newer)

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

    def test_analyze_fetch_metadata_combines_pages_deduplicates_and_orders_transactions(self) -> None:
        wallet = load_json_fixture("solana_wallet_snapshot.json")["wallet"]
        buy = load_json_fixture("solana_transaction_response_buy_example.json")
        sell = load_json_fixture("solana_transaction_response_sell_example.json")

        older_page = build_snapshot_payload(buy, sell)
        newer_page = build_snapshot_payload(sell)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            older_page_path = temp_path / "fetch_20260329T040000Z" / "wallet_snapshot_page_002.json"
            newer_page_path = temp_path / "fetch_20260329T040000Z" / "wallet_snapshot_page_001.json"
            older_page_path.parent.mkdir(parents=True, exist_ok=True)
            older_page_path.write_text(json.dumps(older_page), encoding="utf-8")
            newer_page_path.write_text(json.dumps(newer_page), encoding="utf-8")
            fetch_metadata_path = temp_path / "wallet_fetch_metadata_20260329T040000Z.json"
            fetch_metadata_path.write_text(
                json.dumps(
                    {
                        "wallet": wallet,
                        "page_snapshot_paths": [
                            newer_page_path.relative_to(ROOT).as_posix()
                            if newer_page_path.is_relative_to(ROOT)
                            else str(newer_page_path),
                            older_page_path.relative_to(ROOT).as_posix()
                            if older_page_path.is_relative_to(ROOT)
                            else str(older_page_path),
                        ],
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            valuation_path = temp_path / "wallet_fetch_metadata_20260329T040000Z_trusted_valuations.json"
            valuation_path.write_text(
                json.dumps(
                    {
                        "valuations": [
                            build_trusted_valuation_record(
                                wallet=wallet,
                                raw_payload=buy,
                                usd_value="100",
                            ),
                            build_trusted_valuation_record(
                                wallet=wallet,
                                raw_payload=sell,
                                usd_value="150",
                            ),
                        ]
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )

            analysis = MODULE.analyze_fetch_metadata_path(fetch_metadata_path)

            self.assertEqual(analysis.trade_diagnostics.report_summary.total_matched_trades, 1)
            self.assertEqual(analysis.trade_diagnostics.report_summary.winners_count, 1)
            self.assertEqual(analysis.trade_diagnostics.report_summary.losers_count, 0)
            trade_report_json_path = ROOT / analysis.trade_diagnostics.trade_report_json_path
            trade_report_csv_path = ROOT / analysis.trade_diagnostics.trade_report_csv_path
            self.assertTrue(trade_report_json_path.exists())
            self.assertTrue(trade_report_csv_path.exists())
            trade_report_payload = json.loads(trade_report_json_path.read_text(encoding="utf-8"))
            self.assertEqual(len(trade_report_payload["matched_trades"]), 1)
            self.assertEqual(trade_report_payload["summary"]["largest_win_usd"], "50")
            with trade_report_csv_path.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["realized_pnl_usd"], "50")
            self.assertEqual(analysis.behavior_diagnostics.report_summary.total_matched_trades, 1)
            self.assertEqual(
                analysis.behavior_diagnostics.report_summary.streak_diagnostics.longest_winning_streak,
                1,
            )
            behavior_report_json_path = ROOT / analysis.behavior_diagnostics.behavior_report_json_path
            behavior_report_csv_path = ROOT / analysis.behavior_diagnostics.behavior_report_csv_path
            self.assertTrue(behavior_report_json_path.exists())
            self.assertTrue(behavior_report_csv_path.exists())
            behavior_report_payload = json.loads(
                behavior_report_json_path.read_text(encoding="utf-8")
            )
            self.assertEqual(behavior_report_payload["summary"]["total_matched_trades"], 1)
            self.assertEqual(
                behavior_report_payload["summary"]["notional_diagnostics"]["average_cost_basis_usd"],
                "100",
            )
            with behavior_report_csv_path.open("r", encoding="utf-8", newline="") as handle:
                behavior_rows = list(csv.DictReader(handle))
            self.assertEqual(len(behavior_rows), 1)
            self.assertEqual(behavior_rows[0]["outcome"], "win")
            self.assertEqual(analysis.simulation_diagnostics.report_summary.original_trade_count, 1)
            simulation_report_json_path = ROOT / analysis.simulation_diagnostics.simulation_report_json_path
            simulation_report_csv_path = ROOT / analysis.simulation_diagnostics.simulation_report_csv_path
            self.assertTrue(simulation_report_json_path.exists())
            self.assertTrue(simulation_report_csv_path.exists())
            simulation_report_payload = json.loads(
                simulation_report_json_path.read_text(encoding="utf-8")
            )
            self.assertEqual(simulation_report_payload["summary"]["original_trade_count"], 1)
            self.assertGreater(
                len(simulation_report_payload["summary"]["scenario_results"]),
                0,
            )
            with simulation_report_csv_path.open("r", encoding="utf-8", newline="") as handle:
                simulation_rows = list(csv.DictReader(handle))
            self.assertGreater(len(simulation_rows), 0)
            rules_report_json_path = ROOT / analysis.rules_diagnostics.rules_report_json_path
            rules_report_markdown_path = ROOT / analysis.rules_diagnostics.rules_report_markdown_path
            self.assertTrue(rules_report_json_path.exists())
            self.assertTrue(rules_report_markdown_path.exists())
            rules_report_payload = json.loads(
                rules_report_json_path.read_text(encoding="utf-8")
            )
            self.assertEqual(rules_report_payload["original_realized_pnl_usd"], "50")
            self.assertEqual(rules_report_payload["top_candidate_rules"], [])
            self.assertIn(
                "Wallet Rules Coaching Report",
                rules_report_markdown_path.read_text(encoding="utf-8"),
            )

        self.assertEqual(analysis.total_raw_transactions, 2)
        self.assertEqual(analysis.normalized_transactions_count, 2)
        self.assertEqual(analysis.unsupported_transactions_count, 0)
        self.assertEqual(analysis.valuation_summary.local_trusted_valuations_applied_count, 2)
        self.assertEqual(analysis.fifo_summary.trade_matches_count, 1)
        self.assertEqual(analysis.fifo_summary.realized_pnl_usd, Decimal("50"))
        self.assertEqual(analysis.fifo_summary.unsupported_fifo_transactions_count, 0)

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
        self.assertEqual(analysis.valuation_summary.rows_requiring_valuation_before_count, 2)
        self.assertEqual(analysis.valuation_summary.local_trusted_valuations_applied_count, 0)
        self.assertEqual(analysis.valuation_summary.rows_requiring_valuation_after_count, 2)
        self.assertEqual(analysis.fifo_summary.status, "not_meaningful_missing_valuation")
        self.assertEqual(analysis.fifo_summary.skipped_missing_valuation_count, 2)
        self.assertEqual(analysis.fifo_summary.realized_pnl_usd, None)
        self.assertEqual(analysis.fifo_summary.meaningful, False)
        self.assertEqual(analysis.trade_diagnostics.report_summary.total_matched_trades, 0)
        self.assertEqual(analysis.behavior_diagnostics.report_summary.total_matched_trades, 0)
        self.assertEqual(analysis.simulation_diagnostics.report_summary.original_trade_count, 0)
        self.assertEqual(analysis.rules_diagnostics.report_summary.top_candidate_rules, ())
        self.assertTrue(
            any(
                "one wallet" in item or "overfit" in item
                for item in analysis.rules_diagnostics.report_summary.caution_notes
            )
        )

    def test_analyze_snapshot_applies_local_trusted_valuations_and_enables_fifo(self) -> None:
        buy = load_json_fixture("solana_transaction_response_buy_example.json")
        sell = load_json_fixture("solana_transaction_response_sell_example.json")
        snapshot = build_snapshot_payload(buy, sell)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            snapshot_path = temp_path / "wallet_snapshot_20260329T030000Z.json"
            valuation_path = temp_path / "wallet_snapshot_20260329T030000Z_trusted_valuations.json"
            snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
            valuation_path.write_text(
                json.dumps(
                    {
                        "valuations": [
                            build_trusted_valuation_record(
                                wallet=snapshot["wallet"],
                                raw_payload=buy,
                                usd_value="100",
                            ),
                            build_trusted_valuation_record(
                                wallet=snapshot["wallet"],
                                raw_payload=sell,
                                usd_value="150",
                            ),
                        ]
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )

            analysis = MODULE.analyze_snapshot_path(snapshot_path)

            self.assertEqual(analysis.trade_diagnostics.report_summary.total_matched_trades, 1)
            self.assertEqual(analysis.trade_diagnostics.report_summary.winners_count, 1)
            self.assertEqual(analysis.trade_diagnostics.report_summary.losers_count, 0)
            self.assertEqual(analysis.trade_diagnostics.report_summary.largest_win_usd, Decimal("50"))
            self.assertIsNone(analysis.trade_diagnostics.report_summary.largest_loss_usd)
            trade_report_json_path = temp_path / "wallet_snapshot_20260329T030000Z_trade_report.json"
            trade_report_csv_path = temp_path / "wallet_snapshot_20260329T030000Z_trade_report.csv"
            self.assertTrue(trade_report_json_path.exists())
            self.assertTrue(trade_report_csv_path.exists())
            trade_report_payload = json.loads(trade_report_json_path.read_text(encoding="utf-8"))
            self.assertEqual(trade_report_payload["summary"]["total_matched_trades"], 1)
            self.assertEqual(
                trade_report_payload["matched_trades"][0]["opening_tx_hash"],
                "5uyM8JpVQCBq9x3AjC8nH9fYH5x7c3qvKc6PjH9Tn7rR7hTj6yV7iM6m2T9g7Z6dQwL7jVn4pQy3mK8sR1n4bUy",
            )
            self.assertEqual(
                trade_report_payload["matched_trades"][0]["closing_tx_hash"],
                "3eQFvN6wL8tH7bQ2pK9sR6cW1xY5mJ4uT8nP2qV7zA3sD5fG6hJ8kL2mN4pR6tU8wY1qC3eF5gH7jK9mP2sV4w",
            )
            self.assertEqual(analysis.behavior_diagnostics.report_summary.total_matched_trades, 1)
            self.assertEqual(
                analysis.behavior_diagnostics.report_summary.notional_diagnostics.average_cost_basis_usd,
                Decimal("100"),
            )
            self.assertEqual(
                analysis.behavior_diagnostics.report_summary.streak_diagnostics.longest_winning_streak,
                1,
            )
            behavior_report_json_path = temp_path / "wallet_snapshot_20260329T030000Z_behavior_report.json"
            behavior_report_csv_path = temp_path / "wallet_snapshot_20260329T030000Z_behavior_report.csv"
            self.assertTrue(behavior_report_json_path.exists())
            self.assertTrue(behavior_report_csv_path.exists())
            behavior_report_payload = json.loads(
                behavior_report_json_path.read_text(encoding="utf-8")
            )
            self.assertEqual(
                behavior_report_payload["summary"]["streak_diagnostics"]["longest_winning_streak"],
                1,
            )
            self.assertEqual(
                behavior_report_payload["summary"]["holding_time_buckets"][2]["bucket"],
                "1m_to_lt_5m",
            )
            self.assertEqual(
                analysis.simulation_diagnostics.report_summary.original_trade_count,
                1,
            )
            simulation_report_json_path = temp_path / "wallet_snapshot_20260329T030000Z_simulation_report.json"
            simulation_report_csv_path = temp_path / "wallet_snapshot_20260329T030000Z_simulation_report.csv"
            self.assertTrue(simulation_report_json_path.exists())
            self.assertTrue(simulation_report_csv_path.exists())
            simulation_report_payload = json.loads(
                simulation_report_json_path.read_text(encoding="utf-8")
            )
            self.assertEqual(
                simulation_report_payload["summary"]["original_realized_pnl_usd"],
                "50",
            )
            self.assertGreater(
                len(simulation_report_payload["summary"]["scenario_results"]),
                0,
            )
            rules_report_json_path = temp_path / "wallet_snapshot_20260329T030000Z_rules_report.json"
            rules_report_markdown_path = temp_path / "wallet_snapshot_20260329T030000Z_rules_report.md"
            self.assertTrue(rules_report_json_path.exists())
            self.assertTrue(rules_report_markdown_path.exists())
            rules_report_payload = json.loads(
                rules_report_json_path.read_text(encoding="utf-8")
            )
            self.assertEqual(rules_report_payload["original_realized_pnl_usd"], "50")
            self.assertEqual(rules_report_payload["top_candidate_rules"], [])

        self.assertEqual(
            analysis.valuation_summary.local_trusted_valuation_records_count,
            2,
        )
        self.assertEqual(
            analysis.valuation_summary.local_trusted_valuations_applied_count,
            2,
        )
        self.assertEqual(analysis.valuation_summary.rows_requiring_valuation_after_count, 0)
        self.assertEqual(analysis.fifo_summary.status, "computed")
        self.assertEqual(analysis.fifo_summary.unsupported_fifo_transactions_count, 0)
        self.assertEqual(analysis.fifo_summary.meaningful, True)
        self.assertEqual(str(analysis.fifo_summary.realized_pnl_usd), "50")

    def test_analyze_snapshot_ignores_pending_template_rows(self) -> None:
        buy = load_json_fixture("solana_transaction_response_buy_example.json")
        sell = load_json_fixture("solana_transaction_response_sell_example.json")
        snapshot = build_snapshot_payload(buy, sell)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            snapshot_path = temp_path / "wallet_snapshot_20260329T030000Z.json"
            valuation_path = temp_path / "wallet_snapshot_20260329T030000Z_trusted_valuations.json"
            snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
            valuation_path.write_text(
                json.dumps(
                    {
                        "valuations": [
                            {
                                **build_trusted_valuation_record(
                                    wallet=snapshot["wallet"],
                                    raw_payload=buy,
                                    usd_value="100",
                                ),
                                "usd_value": None,
                                "valuation_source": None,
                                "valuation_status": "pending",
                            }
                        ]
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )

            analysis = MODULE.analyze_snapshot_path(snapshot_path)

        self.assertEqual(analysis.valuation_summary.local_trusted_valuation_records_count, 0)
        self.assertEqual(analysis.valuation_summary.local_trusted_valuations_applied_count, 0)
        self.assertEqual(analysis.valuation_summary.rows_requiring_valuation_after_count, 2)
        self.assertEqual(analysis.fifo_summary.unsupported_fifo_transactions_count, 0)
        self.assertEqual(analysis.fifo_summary.meaningful, False)

    def test_analyze_snapshot_skips_wrapped_sol_sell_without_open_inventory(self) -> None:
        missing_entry_sell = copy.deepcopy(
            load_json_fixture("solana_transaction_response_sell_example.json")
        )
        missing_entry_sell["result"]["transaction"]["signatures"][0] = "orphan-sell-001"
        buy = load_json_fixture("solana_transaction_response_buy_example.json")
        sell = load_json_fixture("solana_transaction_response_sell_example.json")
        snapshot = build_snapshot_payload(missing_entry_sell, buy, sell)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            snapshot_path = temp_path / "wallet_snapshot_20260329T030000Z.json"
            valuation_path = temp_path / "wallet_snapshot_20260329T030000Z_trusted_valuations.json"
            snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
            valuation_path.write_text(
                json.dumps(
                    {
                        "valuations": [
                            build_trusted_valuation_record(
                                wallet=snapshot["wallet"],
                                raw_payload=missing_entry_sell,
                                usd_value="150",
                            ),
                            build_trusted_valuation_record(
                                wallet=snapshot["wallet"],
                                raw_payload=buy,
                                usd_value="100",
                            ),
                            build_trusted_valuation_record(
                                wallet=snapshot["wallet"],
                                raw_payload=sell,
                                usd_value="150",
                            ),
                        ]
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )

            analysis = MODULE.analyze_snapshot_path(snapshot_path)

        self.assertEqual(analysis.fifo_summary.status, "computed_supported_subset")
        self.assertEqual(analysis.fifo_summary.unsupported_fifo_transactions_count, 1)
        self.assertEqual(
            analysis.fifo_summary.unsupported_fifo_transactions[0].tx_hash,
            "orphan-sell-001",
        )
        self.assertIn(
            "wrapped-SOL token disposal has no opening inventory",
            analysis.fifo_summary.unsupported_fifo_transactions[0].reason,
        )
        self.assertEqual(analysis.fifo_summary.trade_matches_count, 1)
        self.assertEqual(analysis.fifo_summary.realized_pnl_usd, Decimal("50"))
        self.assertEqual(analysis.fifo_summary.remaining_positions_count, 0)

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
        self.assertEqual(saved_summary["valuation_summary"]["rows_requiring_valuation_before_count"], 0)
        self.assertIn("rules_diagnostics", saved_summary)


if __name__ == "__main__":
    unittest.main()
