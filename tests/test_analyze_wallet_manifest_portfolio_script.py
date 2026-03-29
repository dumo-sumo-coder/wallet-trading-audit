"""Tests for the multi-wallet manifest portfolio analysis script."""

from __future__ import annotations

import copy
import csv
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
SCRIPT_PATH = ROOT / "scripts" / "analyze_wallet_manifest_portfolio.py"
RAW_SOLANA_FIXTURE_DIR = ROOT / "tests" / "fixtures" / "raw_solana"
FIXTURE_SOLANA_WALLET = json.loads(
    (RAW_SOLANA_FIXTURE_DIR / "solana_wallet_snapshot.json").read_text(encoding="utf-8")
)["wallet"]

SPEC = importlib.util.spec_from_file_location(
    "analyze_wallet_manifest_portfolio_script_module",
    SCRIPT_PATH,
)
if SPEC is None or SPEC.loader is None:  # pragma: no cover - defensive import guard
    raise RuntimeError("Unable to load scripts/analyze_wallet_manifest_portfolio.py for tests")
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ingestion.manifest import manifest_entry_wallet_directory, load_wallet_manifest  # noqa: E402
from normalize.transactions import normalize_transaction  # noqa: E402
from analytics.manifest_portfolio import (  # noqa: E402
    STATUS_EXCLUDED_MISSING_LOCAL_DATA,
    STATUS_EXCLUDED_NOT_MEANINGFUL,
    STATUS_EXCLUDED_UNSUPPORTED_CHAIN,
    STATUS_INCLUDED_COMPLETE,
)
from valuation.sol_usd_lookup import SolUsdLookupResult  # noqa: E402
from valuation.solana_valuation import (  # noqa: E402
    VALUATION_STATUS_TRUSTED,
    populate_wrapped_sol_trusted_values as actual_populate_wrapped_sol_trusted_values,
)


def load_json_fixture(name: str) -> dict[str, object]:
    return json.loads((RAW_SOLANA_FIXTURE_DIR / name).read_text(encoding="utf-8"))


def build_snapshot_payload(*transaction_responses: object, wallet: str) -> dict[str, object]:
    snapshot = load_json_fixture("solana_wallet_snapshot.json")
    snapshot["wallet"] = wallet
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
        "valuation_source": "manual_review",
        "usd_value": usd_value,
        "valuation_status": VALUATION_STATUS_TRUSTED,
    }


def write_wallet_snapshot(
    *,
    repository_root: Path,
    manifest_entry,
    snapshot_filename: str,
    snapshot_payload: dict[str, object],
    trusted_valuations: list[dict[str, object]] | None,
) -> Path:
    wallet_directory = manifest_entry_wallet_directory(
        manifest_entry,
        repository_root=repository_root,
    )
    wallet_directory.mkdir(parents=True, exist_ok=True)
    snapshot_path = wallet_directory / snapshot_filename
    snapshot_path.write_text(json.dumps(snapshot_payload, indent=2), encoding="utf-8")
    if trusted_valuations is not None:
        valuation_path = wallet_directory / f"{snapshot_path.stem}_trusted_valuations.json"
        valuation_path.write_text(
            json.dumps({"valuations": trusted_valuations}, indent=2),
            encoding="utf-8",
        )
    return snapshot_path


class _PaginatedTransferClient:
    def fetch_recent_transaction_history(
        self,
        wallet: str,
        *,
        limit: int = 20,
        before: str | None = None,
    ) -> dict[str, object]:
        transfer_in = copy.deepcopy(load_json_fixture("solana_transaction_response_transfer_in_example.json"))
        if before is None:
            responses = [copy.deepcopy(transfer_in), copy.deepcopy(transfer_in)]
            signatures = ["sig-3", "sig-2"]
        elif before == "sig-2":
            responses = [copy.deepcopy(transfer_in)]
            signatures = ["sig-1"]
        else:
            responses = []
            signatures = []

        for signature, response in zip(signatures, responses, strict=False):
            result = response.setdefault("result", {})
            if isinstance(result, dict):
                transaction = result.setdefault("transaction", {})
                if isinstance(transaction, dict):
                    transaction["signatures"] = [signature]

        return {
            "wallet": wallet,
            "fetched_at_utc": "2026-03-29T05:30:00+00:00",
            "source": {
                "provider": "solana_json_rpc",
                "rpc_url": "https://mainnet.helius-rpc.com/?redacted",
            },
            "signatures_response": {
                "result": [{"signature": signature} for signature in signatures],
            },
            "transaction_responses": responses,
        }


class AnalyzeWalletManifestPortfolioScriptTests(unittest.TestCase):
    def test_analyze_wallet_manifest_portfolio_filters_and_aggregates_wallets(self) -> None:
        manifest_text = (
            "wallet,chain,label,group\n"
            f"{FIXTURE_SOLANA_WALLET},solana,Alpha Wallet,Recent\n"
            f"{FIXTURE_SOLANA_WALLET},solana,Beta Wallet,Recent\n"
            f"{FIXTURE_SOLANA_WALLET},solana,Gamma Wallet,Archive\n"
            "0xabc,bnb_evm,BNB Wallet,Recent\n"
        )
        buy = load_json_fixture("solana_transaction_response_buy_example.json")
        sell = load_json_fixture("solana_transaction_response_sell_example.json")

        with tempfile.TemporaryDirectory() as temp_dir:
            repository_root = Path(temp_dir)
            manifest_path = repository_root / "data" / "wallet_manifest.csv"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text(manifest_text, encoding="utf-8")
            entries = load_wallet_manifest(manifest_path)

            alpha_snapshot = build_snapshot_payload(
                copy.deepcopy(buy),
                copy.deepcopy(sell),
                wallet=FIXTURE_SOLANA_WALLET,
            )
            beta_snapshot = build_snapshot_payload(
                copy.deepcopy(buy),
                copy.deepcopy(sell),
                wallet=FIXTURE_SOLANA_WALLET,
            )
            gamma_snapshot = build_snapshot_payload(
                copy.deepcopy(buy),
                copy.deepcopy(sell),
                wallet=FIXTURE_SOLANA_WALLET,
            )

            write_wallet_snapshot(
                repository_root=repository_root,
                manifest_entry=entries[0],
                snapshot_filename="wallet_snapshot_20260329T020000Z.json",
                snapshot_payload=alpha_snapshot,
                trusted_valuations=[
                    build_trusted_valuation_record(
                        wallet=FIXTURE_SOLANA_WALLET,
                        raw_payload=buy,
                        usd_value="100",
                    ),
                    build_trusted_valuation_record(
                        wallet=FIXTURE_SOLANA_WALLET,
                        raw_payload=sell,
                        usd_value="150",
                    ),
                ],
            )
            write_wallet_snapshot(
                repository_root=repository_root,
                manifest_entry=entries[1],
                snapshot_filename="wallet_snapshot_20260329T010000Z.json",
                snapshot_payload=beta_snapshot,
                trusted_valuations=[
                    build_trusted_valuation_record(
                        wallet=FIXTURE_SOLANA_WALLET,
                        raw_payload=buy,
                        usd_value="120",
                    ),
                    build_trusted_valuation_record(
                        wallet=FIXTURE_SOLANA_WALLET,
                        raw_payload=sell,
                        usd_value="90",
                    ),
                ],
            )
            write_wallet_snapshot(
                repository_root=repository_root,
                manifest_entry=entries[2],
                snapshot_filename="wallet_snapshot_20260329T000000Z.json",
                snapshot_payload=gamma_snapshot,
                trusted_valuations=None,
            )

            output_dir = repository_root / "data" / "reports" / "portfolio"
            run = MODULE.analyze_wallet_manifest_portfolio(
                manifest_path,
                repository_root=repository_root,
                output_dir=output_dir,
            )

            self.assertEqual(run.report.summary.analyzed_wallet_count, 4)
            self.assertEqual(run.report.summary.included_wallet_count, 2)
            self.assertEqual(run.report.summary.aggregate_realized_pnl_usd, MODULE.Decimal("20"))
            self.assertEqual(run.report.summary.best_wallets_by_pnl[0].label, "Alpha Wallet")
            self.assertEqual(run.report.summary.worst_wallets_by_pnl[0].label, "Beta Wallet")
            self.assertEqual(
                run.report.summary.best_wallets_by_win_rate[0].label,
                "Alpha Wallet",
            )
            wallet_summaries = {item.label: item for item in run.report.wallet_summaries}
            self.assertEqual(wallet_summaries["Alpha Wallet"].status, STATUS_INCLUDED_COMPLETE)
            self.assertEqual(wallet_summaries["Beta Wallet"].status, STATUS_INCLUDED_COMPLETE)
            self.assertEqual(
                wallet_summaries["Gamma Wallet"].status,
                STATUS_EXCLUDED_NOT_MEANINGFUL,
            )
            self.assertEqual(
                wallet_summaries["BNB Wallet"].status,
                STATUS_EXCLUDED_UNSUPPORTED_CHAIN,
            )

            json_path = repository_root / run.portfolio_summary_json_path
            csv_path = repository_root / run.portfolio_summary_csv_path
            self.assertTrue(json_path.exists())
            self.assertTrue(csv_path.exists())
            payload = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["report"]["summary"]["aggregate_realized_pnl_usd"], "20.0")
            with csv_path.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 4)

    def test_recent_only_and_limit_wallets_pick_newest_local_wallet(self) -> None:
        manifest_text = (
            "wallet,chain,label,group\n"
            f"{FIXTURE_SOLANA_WALLET},solana,Alpha Wallet,Recent\n"
            f"{FIXTURE_SOLANA_WALLET},solana,Beta Wallet,Recent\n"
        )
        buy = load_json_fixture("solana_transaction_response_buy_example.json")
        sell = load_json_fixture("solana_transaction_response_sell_example.json")

        with tempfile.TemporaryDirectory() as temp_dir:
            repository_root = Path(temp_dir)
            manifest_path = repository_root / "data" / "wallet_manifest.csv"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text(manifest_text, encoding="utf-8")
            entries = load_wallet_manifest(manifest_path)

            write_wallet_snapshot(
                repository_root=repository_root,
                manifest_entry=entries[0],
                snapshot_filename="wallet_snapshot_20260329T020000Z.json",
                snapshot_payload=build_snapshot_payload(
                    copy.deepcopy(buy),
                    copy.deepcopy(sell),
                    wallet=FIXTURE_SOLANA_WALLET,
                ),
                trusted_valuations=[
                    build_trusted_valuation_record(
                        wallet=FIXTURE_SOLANA_WALLET,
                        raw_payload=buy,
                        usd_value="100",
                    ),
                    build_trusted_valuation_record(
                        wallet=FIXTURE_SOLANA_WALLET,
                        raw_payload=sell,
                        usd_value="150",
                    ),
                ],
            )
            write_wallet_snapshot(
                repository_root=repository_root,
                manifest_entry=entries[1],
                snapshot_filename="wallet_snapshot_20260329T010000Z.json",
                snapshot_payload=build_snapshot_payload(
                    copy.deepcopy(buy),
                    copy.deepcopy(sell),
                    wallet=FIXTURE_SOLANA_WALLET,
                ),
                trusted_valuations=[
                    build_trusted_valuation_record(
                        wallet=FIXTURE_SOLANA_WALLET,
                        raw_payload=buy,
                        usd_value="100",
                    ),
                    build_trusted_valuation_record(
                        wallet=FIXTURE_SOLANA_WALLET,
                        raw_payload=sell,
                        usd_value="150",
                    ),
                ],
            )

            run = MODULE.analyze_wallet_manifest_portfolio(
                manifest_path,
                repository_root=repository_root,
                output_dir=repository_root / "data" / "reports" / "portfolio",
                recent_only=True,
                limit_wallets=1,
            )

        self.assertEqual(len(run.report.wallet_summaries), 1)
        self.assertEqual(run.report.wallet_summaries[0].label, "Alpha Wallet")

    def test_missing_local_data_is_excluded_explicitly(self) -> None:
        manifest_text = (
            "wallet,chain,label,group\n"
            f"{FIXTURE_SOLANA_WALLET},solana,Alpha Wallet,Recent\n"
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            repository_root = Path(temp_dir)
            manifest_path = repository_root / "data" / "wallet_manifest.csv"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text(manifest_text, encoding="utf-8")

            run = MODULE.analyze_wallet_manifest_portfolio(
                manifest_path,
                repository_root=repository_root,
                output_dir=repository_root / "data" / "reports" / "portfolio",
            )

        self.assertEqual(len(run.report.wallet_summaries), 1)
        self.assertEqual(
            run.report.wallet_summaries[0].status,
            STATUS_EXCLUDED_MISSING_LOCAL_DATA,
        )
        self.assertEqual(run.report.summary.included_wallet_count, 0)

    def test_find_local_analysis_target_falls_back_to_snapshot_when_fetch_metadata_is_single_snapshot_stub(self) -> None:
        manifest_text = (
            "wallet,chain,label,group\n"
            f"{FIXTURE_SOLANA_WALLET},solana,Alpha Wallet,Recent\n"
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            repository_root = Path(temp_dir)
            manifest_path = repository_root / "data" / "wallet_manifest.csv"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text(manifest_text, encoding="utf-8")
            entry = load_wallet_manifest(manifest_path)[0]
            wallet_directory = manifest_entry_wallet_directory(
                entry,
                repository_root=repository_root,
            )
            wallet_directory.mkdir(parents=True, exist_ok=True)
            snapshot_path = wallet_directory / "wallet_snapshot_20260329T050000Z.json"
            snapshot_path.write_text(
                json.dumps(
                    build_snapshot_payload(
                        load_json_fixture("solana_transaction_response_transfer_in_example.json"),
                        wallet=FIXTURE_SOLANA_WALLET,
                    )
                ),
                encoding="utf-8",
            )
            metadata_path = wallet_directory / "wallet_fetch_metadata_20260329T050000Z.json"
            metadata_path.write_text(
                json.dumps(
                    {
                        "wallet": FIXTURE_SOLANA_WALLET,
                        "snapshot_path": str(snapshot_path),
                    }
                ),
                encoding="utf-8",
            )

            target = MODULE._find_local_analysis_target(
                entry,
                repository_root=repository_root,
            )

        self.assertIsNotNone(target)
        self.assertEqual(target.target_type, "snapshot")
        self.assertEqual(target.path.name, snapshot_path.name)

    def test_refetch_existing_refreshes_wallet_with_paginated_history(self) -> None:
        manifest_text = (
            "wallet,chain,label,group\n"
            f"{FIXTURE_SOLANA_WALLET},solana,Alpha Wallet,Recent\n"
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            repository_root = Path(temp_dir)
            manifest_path = repository_root / "data" / "wallet_manifest.csv"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text(manifest_text, encoding="utf-8")
            entry = load_wallet_manifest(manifest_path)[0]

            write_wallet_snapshot(
                repository_root=repository_root,
                manifest_entry=entry,
                snapshot_filename="wallet_snapshot_20260329T010000Z.json",
                snapshot_payload=build_snapshot_payload(
                    load_json_fixture("solana_transaction_response_transfer_in_example.json"),
                    wallet=FIXTURE_SOLANA_WALLET,
                ),
                trusted_valuations=None,
            )

            with patch.object(
                MODULE,
                "SolanaRpcClient",
                return_value=_PaginatedTransferClient(),
            ):
                run = MODULE.analyze_wallet_manifest_portfolio(
                    manifest_path,
                    repository_root=repository_root,
                    output_dir=repository_root / "data" / "reports" / "portfolio",
                    refetch_existing=True,
                    solana_limit=2,
                    solana_max_pages=2,
                )

            wallet_summary = run.report.wallet_summaries[0]
            self.assertIn("wallet_fetch_metadata_", wallet_summary.source_path)

            metadata_path = repository_root / wallet_summary.source_path
            metadata_payload = json.loads(metadata_path.read_text(encoding="utf-8"))
            self.assertEqual(metadata_payload["fetch_mode"], "manifest_portfolio_refetch_existing")
            self.assertEqual(metadata_payload["total_pages_fetched"], 2)
            self.assertEqual(metadata_payload["total_tx_count"], 3)
            self.assertEqual(len(metadata_payload["page_snapshot_paths"]), 2)

    def test_portfolio_analysis_auto_applies_wrapped_sol_valuations(self) -> None:
        manifest_text = (
            "wallet,chain,label,group\n"
            f"{FIXTURE_SOLANA_WALLET},solana,Alpha Wallet,Recent\n"
        )
        buy = load_json_fixture("solana_transaction_response_buy_example.json")
        sell = load_json_fixture("solana_transaction_response_sell_example.json")

        with tempfile.TemporaryDirectory() as temp_dir:
            repository_root = Path(temp_dir)
            manifest_path = repository_root / "data" / "wallet_manifest.csv"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text(manifest_text, encoding="utf-8")
            entry = load_wallet_manifest(manifest_path)[0]

            write_wallet_snapshot(
                repository_root=repository_root,
                manifest_entry=entry,
                snapshot_filename="wallet_snapshot_20260329T060000Z.json",
                snapshot_payload=build_snapshot_payload(
                    copy.deepcopy(buy),
                    copy.deepcopy(sell),
                    wallet=FIXTURE_SOLANA_WALLET,
                ),
                trusted_valuations=None,
            )

            def _lookup(timestamp: datetime) -> SolUsdLookupResult:
                candle_start = timestamp.replace(second=0, microsecond=0)
                return SolUsdLookupResult(
                    source_name="coinbase_exchange_public_candles",
                    product_id="SOL-USD",
                    reference_price_usd=Decimal("100"),
                    price_reference_kind="minute_candle_open",
                    reference_candle_start=candle_start,
                    reference_candle_end=candle_start,
                    lookup_timestamp=datetime(2026, 3, 29, 2, 0, tzinfo=UTC),
                    request_url="https://api.exchange.coinbase.com/?redacted",
                )

            def _populate(records, *, overwrite_existing=False):
                return actual_populate_wrapped_sol_trusted_values(
                    records,
                    overwrite_existing=overwrite_existing,
                    lookup_fn=_lookup,
                )

            with patch.object(MODULE, "populate_wrapped_sol_trusted_values", side_effect=_populate):
                run = MODULE.analyze_wallet_manifest_portfolio(
                    manifest_path,
                    repository_root=repository_root,
                    output_dir=repository_root / "data" / "reports" / "portfolio",
                )

            wallet_summary = run.report.wallet_summaries[0]
            self.assertEqual(wallet_summary.status, STATUS_INCLUDED_COMPLETE)
            self.assertEqual(wallet_summary.realized_pnl_usd, MODULE.Decimal("50.0"))
            self.assertEqual(wallet_summary.matched_trade_count, 1)
            valuation_path = (
                repository_root / "data" / "raw" / "solana" / "Alpha_Wallet"
                / "wallet_snapshot_20260329T060000Z_trusted_valuations.json"
            )
            self.assertTrue(valuation_path.exists())
            saved = json.loads(valuation_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["valuations"][0]["valuation_status"], "trusted")

    def test_portfolio_analysis_groups_unsupported_patterns_by_wallet(self) -> None:
        manifest_text = (
            "wallet,chain,label,group\n"
            f"{FIXTURE_SOLANA_WALLET},solana,Alpha Wallet,Recent\n"
        )
        ambiguous = copy.deepcopy(load_json_fixture("solana_transaction_response_buy_example.json"))
        ambiguous["result"]["meta"]["postTokenBalances"].append(
            {
                "accountIndex": 2,
                "mint": "Es9vMFrzaCERmJfr6Woj7q4Tt6kRXKuX3sX5Yucs5cjB",
                "owner": FIXTURE_SOLANA_WALLET,
                "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                "uiTokenAmount": {
                    "amount": "1000000",
                    "decimals": 6,
                    "uiAmount": 1.0,
                    "uiAmountString": "1",
                },
            }
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            repository_root = Path(temp_dir)
            manifest_path = repository_root / "data" / "wallet_manifest.csv"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text(manifest_text, encoding="utf-8")
            entry = load_wallet_manifest(manifest_path)[0]
            write_wallet_snapshot(
                repository_root=repository_root,
                manifest_entry=entry,
                snapshot_filename="wallet_snapshot_20260329T070000Z.json",
                snapshot_payload=build_snapshot_payload(
                    ambiguous,
                    wallet=FIXTURE_SOLANA_WALLET,
                ),
                trusted_valuations=None,
            )

            run = MODULE.analyze_wallet_manifest_portfolio(
                manifest_path,
                repository_root=repository_root,
                output_dir=repository_root / "data" / "reports" / "portfolio",
            )

        wallet_summary = run.report.wallet_summaries[0]
        self.assertEqual(wallet_summary.unsupported_patterns[0].pattern_key, "multiple_token_deltas")
        self.assertEqual(
            run.report.summary.unsupported_patterns_across_wallets[0].pattern_key,
            "multiple_token_deltas",
        )


if __name__ == "__main__":
    unittest.main()
