"""Tests for the wallet-list import helper."""

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "import_wallet_list.py"

SPEC = importlib.util.spec_from_file_location(
    "import_wallet_list_script_module",
    SCRIPT_PATH,
)
if SPEC is None or SPEC.loader is None:  # pragma: no cover - defensive import guard
    raise RuntimeError("Unable to load scripts/import_wallet_list.py for tests")
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class ImportWalletListScriptTests(unittest.TestCase):
    def test_classify_wallet_conservatively_uses_safe_format_rules(self) -> None:
        self.assertEqual(
            MODULE.classify_wallet_conservatively(
                "0x155e11000d8155416f2d5d9bc8380031772c88c6"
            ),
            "bnb_evm",
        )
        self.assertEqual(
            MODULE.classify_wallet_conservatively(
                "3rv3sVUvkoLzCWpGGc6DSA9ZJDWBdjCQLB26jLeGchMY"
            ),
            "solana",
        )
        self.assertEqual(
            MODULE.classify_wallet_conservatively("not-a-wallet"),
            "unknown",
        )

    def test_import_wallet_list_deduplicates_preserves_order_and_skips_existing_wallets(self) -> None:
        manifest_text = (
            "wallet,chain,label,group,notes\n"
            "3rv3sVUvkoLzCWpGGc6DSA9ZJDWBdjCQLB26jLeGchMY,solana,Axiom 11,Axiom,\n"
        )
        wallet_lines = (
            "3rv3sVUvkoLzCWpGGc6DSA9ZJDWBdjCQLB26jLeGchMY",
            "CcNvTgSzJpN1mUcpnidothDewazN56ZUaAPEZX4o8kPn",
            "CcNvTgSzJpN1mUcpnidothDewazN56ZUaAPEZX4o8kPn",
            "0x155e11000d8155416f2d5d9bc8380031772c88c6",
            "bad-wallet",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "wallet_manifest.csv"
            manifest_path.write_text(manifest_text, encoding="utf-8")

            summary = MODULE.import_wallet_list(wallet_lines, manifest_path=manifest_path)
            updated_manifest = manifest_path.read_text(encoding="utf-8")

        self.assertEqual(summary.total_wallets_provided, 5)
        self.assertEqual(summary.unique_wallets_provided, 4)
        self.assertEqual(summary.added_wallets_count, 2)
        self.assertEqual(summary.existing_wallets_count, 1)
        self.assertEqual(summary.solana_count, 2)
        self.assertEqual(summary.bnb_evm_count, 1)
        self.assertEqual(summary.unknown_count, 1)
        self.assertEqual(summary.unknown_wallets, ("bad-wallet",))
        self.assertEqual(summary.added_wallets[0].label, "imported_solana_01")
        self.assertEqual(summary.added_wallets[1].label, "imported_bnb_evm_01")
        self.assertIn("Imported from wallet list; import_order=02", updated_manifest)
        self.assertIn("Imported from wallet list; import_order=03", updated_manifest)
        self.assertNotIn("bad-wallet,unknown", updated_manifest)

    def test_import_wallet_list_preserves_existing_import_sequence(self) -> None:
        manifest_text = (
            "wallet,chain,label,group,notes\n"
            "WalletOne,solana,imported_solana_03,Imported,\n"
            "0xabc0000000000000000000000000000000000000,bnb_evm,imported_bnb_evm_02,Imported,\n"
        )
        wallet_lines = (
            "CcNvTgSzJpN1mUcpnidothDewazN56ZUaAPEZX4o8kPn",
            "0x155e11000d8155416f2d5d9bc8380031772c88c6",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "wallet_manifest.csv"
            manifest_path.write_text(manifest_text, encoding="utf-8")

            summary = MODULE.import_wallet_list(wallet_lines, manifest_path=manifest_path)

        self.assertEqual(summary.added_wallets[0].label, "imported_solana_04")
        self.assertEqual(summary.added_wallets[1].label, "imported_bnb_evm_03")

    def test_main_can_print_json_summary(self) -> None:
        manifest_text = "wallet,chain,label,group,notes\n"

        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "wallet_manifest.csv"
            manifest_path.write_text(manifest_text, encoding="utf-8")

            with tempfile.TemporaryDirectory() as temp_output:
                wallet_file = Path(temp_output) / "wallets.txt"
                wallet_file.write_text(
                    "CcNvTgSzJpN1mUcpnidothDewazN56ZUaAPEZX4o8kPn\n",
                    encoding="utf-8",
                )

                stdout = StringIO()
                with redirect_stdout(stdout):
                    exit_code = MODULE.main(
                        [
                            "--manifest-path",
                            str(manifest_path),
                            "--wallet-list-file",
                            str(wallet_file),
                            "--json",
                        ]
                    )

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["added_wallets_count"], 1)
        self.assertEqual(payload["solana_count"], 1)


if __name__ == "__main__":
    unittest.main()
