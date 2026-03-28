"""Preflight check for manifest-driven local ingestion."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from config import get_manual_env_load_instructions  # noqa: E402
from ingestion.manifest import (  # noqa: E402
    DEFAULT_WALLET_MANIFEST_PATH,
    preflight_wallet_manifest,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Check wallet manifest and env readiness without making any live "
            "provider calls."
        ),
    )
    parser.add_argument(
        "--manifest-path",
        default=str(ROOT / DEFAULT_WALLET_MANIFEST_PATH),
        help="Wallet manifest CSV to read.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        result = preflight_wallet_manifest(
            Path(args.manifest_path),
            repository_root=ROOT,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        print(
            "If you prefer manual terminal loading, run "
            f"'{get_manual_env_load_instructions()}' before this script.",
            file=sys.stderr,
        )
        return 1

    print(f"Manifest: {result.manifest_path}")
    print(
        "Wallets: "
        f"total={result.total_wallet_count} "
        f"solana={result.solana_wallet_count} "
        f"bnb_evm={result.bnb_evm_wallet_count}"
    )
    print(f"HELIUS_API_KEY: {result.helius_api_key_status}")
    print(f"ETHERSCAN_API_KEY: {result.etherscan_api_key_status}")

    if not result.is_ready:
        for error in result.errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1

    print("Ready for manifest-driven Solana ingestion. No live provider calls were made.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
