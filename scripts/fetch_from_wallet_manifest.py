"""CLI helper to fetch raw wallet snapshots from a manifest CSV."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from config import get_manual_env_load_instructions  # noqa: E402
from ingestion.evm_client import EvmWalletClient  # noqa: E402
from ingestion.manifest import (  # noqa: E402
    DEFAULT_WALLET_MANIFEST_PATH,
    fetch_from_wallet_manifest,
    preflight_wallet_manifest,
)
from ingestion.solana_client import SolanaRpcClient  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch raw wallet transaction snapshots from data/wallet_manifest.csv "
            "and save provider-native JSON plus metadata sidecars."
        ),
    )
    parser.add_argument(
        "--manifest-path",
        default=str(ROOT / DEFAULT_WALLET_MANIFEST_PATH),
        help="Wallet manifest CSV to read.",
    )
    parser.add_argument(
        "--solana-limit",
        type=int,
        default=20,
        help="Recent Solana signatures to request per wallet.",
    )
    parser.add_argument(
        "--evm-page",
        type=int,
        default=1,
        help="EVM API page number to request per wallet.",
    )
    parser.add_argument(
        "--evm-offset",
        type=int,
        default=20,
        help="EVM records per request.",
    )
    parser.add_argument(
        "--rpc-url",
        default=None,
        help=(
            "Optional Solana RPC URL override. If omitted, use SOLANA_RPC_URL or "
            "HELIUS_API_KEY from the environment."
        ),
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help=(
            "Optional Etherscan V2 API key override. If omitted, use "
            "ETHERSCAN_API_KEY from the environment."
        ),
    )
    parser.add_argument(
        "--preflight",
        action="store_true",
        help=(
            "Validate the wallet manifest and required env presence for Solana "
            "without making any live provider calls."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest_path = Path(args.manifest_path)

    try:
        if args.preflight:
            preflight = preflight_wallet_manifest(
                manifest_path,
                repository_root=ROOT,
            )
            print(f"Manifest: {preflight.manifest_path}")
            print(
                "Wallets: "
                f"total={preflight.total_wallet_count} "
                f"solana={preflight.solana_wallet_count} "
                f"bnb_evm={preflight.bnb_evm_wallet_count}"
            )
            print(f"HELIUS_API_KEY: {preflight.helius_api_key_status}")
            print(f"ETHERSCAN_API_KEY: {preflight.etherscan_api_key_status}")
            if not preflight.is_ready:
                for error in preflight.errors:
                    print(f"ERROR: {error}", file=sys.stderr)
                return 1
            print("Preflight ready. No live provider calls were made.")
            return 0

        run = fetch_from_wallet_manifest(
            manifest_path,
            repository_root=ROOT,
            solana_client=(
                SolanaRpcClient(rpc_url=args.rpc_url) if args.rpc_url is not None else None
            ),
            evm_client=EvmWalletClient(api_key=args.api_key) if args.api_key is not None else None,
            solana_limit=args.solana_limit,
            evm_page=args.evm_page,
            evm_offset=args.evm_offset,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        print(
            "If you prefer manual terminal loading, run "
            f"'{get_manual_env_load_instructions()}' before this script.",
            file=sys.stderr,
        )
        return 1

    for record in run.records:
        if record.status == "success":
            print(
                f"SUCCESS {record.chain} {record.label} "
                f"snapshot={record.snapshot_path} metadata={record.metadata_path}"
            )
        else:
            print(
                f"FAILURE {record.chain} {record.label} "
                f"metadata={record.metadata_path} error={record.error_message}"
            )

    print(
        f"Processed {len(run.records)} wallets from {run.manifest_path}: "
        f"{run.success_count} succeeded, {run.failure_count} failed"
    )
    return 0 if run.failure_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
