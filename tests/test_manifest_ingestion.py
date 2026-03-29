"""Tests for manifest-driven raw ingestion."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ingestion.manifest import (  # noqa: E402
    filter_wallet_manifest_entries,
    fetch_from_wallet_manifest,
    load_wallet_manifest,
    manifest_entry_wallet_directory,
    preflight_wallet_manifest,
)


class _FakeSolanaClient:
    def __init__(self, *, should_fail: bool = False) -> None:
        self.should_fail = should_fail
        self.calls: list[tuple[str, int]] = []

    def fetch_recent_transaction_history(
        self,
        wallet: str,
        *,
        limit: int = 20,
    ) -> dict[str, object]:
        self.calls.append((wallet, limit))
        if self.should_fail:
            raise RuntimeError("Solana RPC rate limited")
        return {
            "wallet": wallet,
            "fetched_at_utc": "2026-03-28T15:30:00+00:00",
            "source": {"provider": "solana_json_rpc"},
            "transaction_responses": [],
        }


class _FakeEvmClient:
    def __init__(self, *, should_fail: bool = False) -> None:
        self.should_fail = should_fail
        self.calls: list[tuple[str, int, int]] = []

    def fetch_recent_transaction_history(
        self,
        wallet: str,
        *,
        page: int = 1,
        offset: int = 20,
    ) -> dict[str, object]:
        self.calls.append((wallet, page, offset))
        if self.should_fail:
            raise RuntimeError("Missing Etherscan API key")
        return {
            "wallet": wallet,
            "fetched_at_utc": "2026-03-28T15:31:00+00:00",
            "source": {"provider": "etherscan_v2_multichain"},
            "responses": {},
        }


class WalletManifestParsingTests(unittest.TestCase):
    def test_load_wallet_manifest_accepts_required_and_optional_columns(self) -> None:
        manifest_text = (
            "wallet,chain,label,group,notes\n"
            "WalletOne,solana,Label One,Trading,Primary wallet\n"
            "0xabc,bnb_evm,Label Two,,\n"
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "wallet_manifest.csv"
            manifest_path.write_text(manifest_text, encoding="utf-8")

            entries = load_wallet_manifest(manifest_path)

        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0].wallet, "WalletOne")
        self.assertEqual(entries[0].group, "Trading")
        self.assertEqual(entries[0].notes, "Primary wallet")
        self.assertEqual(entries[1].chain, "bnb_evm")
        self.assertIsNone(entries[1].group)
        self.assertIsNone(entries[1].notes)

    def test_load_wallet_manifest_rejects_missing_required_columns(self) -> None:
        manifest_text = "wallet,chain,group\nWalletOne,solana,Trading\n"

        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "wallet_manifest.csv"
            manifest_path.write_text(manifest_text, encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "missing required columns: label"):
                load_wallet_manifest(manifest_path)

    def test_load_wallet_manifest_rejects_blank_and_unsupported_rows(self) -> None:
        manifest_text = (
            "wallet,chain,label\n"
            "WalletOne,solana,Label One\n"
            ",solana,Missing Wallet\n"
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "wallet_manifest.csv"
            manifest_path.write_text(manifest_text, encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "blank wallet value"):
                load_wallet_manifest(manifest_path)

        invalid_chain_text = (
            "wallet,chain,label\n"
            "WalletOne,ethereum,Label One\n"
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "wallet_manifest.csv"
            manifest_path.write_text(invalid_chain_text, encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "unsupported chain"):
                load_wallet_manifest(manifest_path)

        blank_row_text = (
            "wallet,chain,label\n"
            "WalletOne,solana,Label One\n"
            ",,\n"
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "wallet_manifest.csv"
            manifest_path.write_text(blank_row_text, encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "is blank"):
                load_wallet_manifest(manifest_path)

    def test_load_wallet_manifest_rejects_malformed_rows(self) -> None:
        manifest_text = (
            "wallet,chain,label\n"
            "WalletOne,solana,Label One,Unexpected Column\n"
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "wallet_manifest.csv"
            manifest_path.write_text(manifest_text, encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "too many columns"):
                load_wallet_manifest(manifest_path)

    def test_filter_wallet_manifest_entries_and_wallet_directory_are_consistent(self) -> None:
        manifest_text = (
            "wallet,chain,label,group\n"
            "WalletOne,solana,Recent Alpha,Trading\n"
            "WalletTwo,solana,Older Beta,Archive\n"
            "0xabc,bnb_evm,BNB Desk,Trading\n"
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            repository_root = Path(temp_dir)
            manifest_path = repository_root / "data" / "wallet_manifest.csv"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text(manifest_text, encoding="utf-8")

            entries = load_wallet_manifest(manifest_path)
            filtered = filter_wallet_manifest_entries(
                entries,
                chain="solana",
                label_filter="alpha",
                group_filter="trad",
                wallets=("WalletOne", "WalletTwo"),
            )
            wallet_directory = manifest_entry_wallet_directory(
                filtered[0],
                repository_root=repository_root,
            )

        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0].wallet, "WalletOne")
        self.assertEqual(
            wallet_directory,
            repository_root / "data" / "raw" / "solana" / "Recent_Alpha",
        )


class WalletManifestFetchTests(unittest.TestCase):
    def test_fetch_from_wallet_manifest_routes_by_chain_and_writes_metadata(self) -> None:
        manifest_text = (
            "wallet,chain,label,group,notes\n"
            "WalletOne,solana,Label One,Desk,Primary\n"
            "0xabc,bnb_evm,Label/Two,Desk,\n"
        )
        fixed_now = datetime(2026, 3, 28, 16, 0, tzinfo=UTC)

        with tempfile.TemporaryDirectory() as temp_dir:
            repository_root = Path(temp_dir)
            manifest_path = repository_root / "data" / "wallet_manifest.csv"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text(manifest_text, encoding="utf-8")

            solana_client = _FakeSolanaClient()
            evm_client = _FakeEvmClient()

            with patch("ingestion.manifest._utc_now", return_value=fixed_now):
                run = fetch_from_wallet_manifest(
                    manifest_path,
                    repository_root=repository_root,
                    solana_client=solana_client,
                    evm_client=evm_client,
                    solana_limit=7,
                    evm_page=2,
                    evm_offset=9,
                )

            self.assertEqual(solana_client.calls, [("WalletOne", 7)])
            self.assertEqual(evm_client.calls, [("0xabc", 2, 9)])
            self.assertEqual(run.success_count, 2)
            self.assertEqual(run.failure_count, 0)

            solana_record = run.records[0]
            evm_record = run.records[1]
            self.assertEqual(solana_record.status, "success")
            self.assertEqual(evm_record.status, "success")
            self.assertEqual(
                solana_record.snapshot_path,
                "data/raw/solana/Label_One/raw_snapshot_line2_WalletOne_20260328T153000Z.json",
            )
            self.assertEqual(
                evm_record.snapshot_path,
                "data/raw/bnb_evm/Label_Two/raw_snapshot_line3_0xabc_20260328T153100Z.json",
            )

            solana_metadata = json.loads(
                (repository_root / solana_record.metadata_path).read_text(encoding="utf-8")
            )
            self.assertEqual(solana_metadata["wallet"], "WalletOne")
            self.assertEqual(solana_metadata["provider"], "solana_json_rpc")
            self.assertEqual(solana_metadata["status"], "success")
            self.assertEqual(
                solana_metadata["snapshot_path"],
                "data/raw/solana/Label_One/raw_snapshot_line2_WalletOne_20260328T153000Z.json",
            )

    def test_fetch_from_wallet_manifest_records_failure_metadata_and_continues(self) -> None:
        manifest_text = (
            "wallet,chain,label,group\n"
            "WalletOne,solana,Label One,Desk\n"
            "0xabc,bnb_evm,Label Two,Desk\n"
        )
        fixed_now = datetime(2026, 3, 28, 17, 45, tzinfo=UTC)

        with tempfile.TemporaryDirectory() as temp_dir:
            repository_root = Path(temp_dir)
            manifest_path = repository_root / "data" / "wallet_manifest.csv"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text(manifest_text, encoding="utf-8")

            with patch("ingestion.manifest._utc_now", return_value=fixed_now):
                run = fetch_from_wallet_manifest(
                    manifest_path,
                    repository_root=repository_root,
                    solana_client=_FakeSolanaClient(should_fail=True),
                    evm_client=_FakeEvmClient(),
                )

            self.assertEqual(run.success_count, 1)
            self.assertEqual(run.failure_count, 1)

            failed_record = run.records[0]
            success_record = run.records[1]
            self.assertEqual(failed_record.status, "failure")
            self.assertIn("rate limited", failed_record.error_message or "")
            self.assertEqual(failed_record.provider, "solana_json_rpc")
            self.assertIsNone(failed_record.snapshot_path)

            failed_metadata = json.loads(
                (repository_root / failed_record.metadata_path).read_text(encoding="utf-8")
            )
            self.assertEqual(failed_metadata["status"], "failure")
            self.assertEqual(failed_metadata["provider"], "solana_json_rpc")
            self.assertIsNone(failed_metadata["snapshot_path"])
            self.assertIn("rate limited", failed_metadata["error_message"])
            self.assertEqual(success_record.status, "success")

    def test_fetch_from_wallet_manifest_lazily_skips_solana_client_when_not_needed(self) -> None:
        manifest_text = "wallet,chain,label\n0xabc,bnb_evm,Label Two\n"

        with tempfile.TemporaryDirectory() as temp_dir:
            repository_root = Path(temp_dir)
            manifest_path = repository_root / "data" / "wallet_manifest.csv"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text(manifest_text, encoding="utf-8")

            run = fetch_from_wallet_manifest(
                manifest_path,
                repository_root=repository_root,
                evm_client=_FakeEvmClient(),
            )

        self.assertEqual(run.success_count, 1)
        self.assertEqual(run.failure_count, 0)

    def test_preflight_wallet_manifest_reports_present_or_missing_secret_status_only(self) -> None:
        manifest_text = "wallet,chain,label\nWalletOne,solana,Label One\n"

        with tempfile.TemporaryDirectory() as temp_dir:
            repository_root = Path(temp_dir)
            manifest_path = repository_root / "data" / "wallet_manifest.csv"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text(manifest_text, encoding="utf-8")

            with patch.dict(os.environ, {}, clear=True):
                missing = preflight_wallet_manifest(
                    manifest_path,
                    repository_root=repository_root,
                )

            with patch.dict(os.environ, {"HELIUS_API_KEY": "super-secret"}, clear=True):
                present = preflight_wallet_manifest(
                    manifest_path,
                    repository_root=repository_root,
                )

        self.assertEqual(missing.helius_api_key_status, "missing")
        self.assertEqual(present.helius_api_key_status, "present")
        self.assertFalse(missing.is_ready)
        self.assertTrue(present.is_ready)
        self.assertNotIn("super-secret", "\n".join(missing.errors))

    def test_preflight_wallet_manifest_requires_at_least_one_solana_wallet(self) -> None:
        manifest_text = "wallet,chain,label\n0xabc,bnb_evm,Label Two\n"

        with tempfile.TemporaryDirectory() as temp_dir:
            repository_root = Path(temp_dir)
            manifest_path = repository_root / "data" / "wallet_manifest.csv"
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text(manifest_text, encoding="utf-8")

            result = preflight_wallet_manifest(
                manifest_path,
                repository_root=repository_root,
            )

        self.assertFalse(result.is_ready)
        self.assertEqual(result.solana_wallet_count, 0)
        self.assertIn("No Solana wallets", result.errors[0])


if __name__ == "__main__":
    unittest.main()
