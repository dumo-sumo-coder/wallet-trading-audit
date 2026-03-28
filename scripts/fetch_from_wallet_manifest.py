"""CLI helper to fetch raw wallet snapshots from a manifest CSV."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ingestion.evm_client import EvmWalletClient  # noqa: E402
from ingestion.manifest import (  # noqa: E402
    DEFAULT_WALLET_MANIFEST_PATH,
    fetch_from_wallet_manifest,
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
        default=SolanaRpcClient().rpc_url,
        help="Solana RPC URL to use.",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="Optional Etherscan V2 API key for BNB/EVM fetches.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run = fetch_from_wallet_manifest(
        Path(args.manifest_path),
        repository_root=ROOT,
        solana_client=SolanaRpcClient(rpc_url=args.rpc_url),
        evm_client=EvmWalletClient(api_key=args.api_key),
        solana_limit=args.solana_limit,
        evm_page=args.evm_page,
        evm_offset=args.evm_offset,
    )

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
