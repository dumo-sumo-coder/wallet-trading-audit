"""CLI helper to fetch and save raw wallet transaction snapshots."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ingestion.evm_client import EvmWalletClient  # noqa: E402
from ingestion.solana_client import SolanaRpcClient  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch raw wallet transactions and save provider-native JSON snapshots.",
    )
    parser.add_argument(
        "--chain",
        choices=("solana", "evm"),
        required=True,
        help="Wallet chain family to fetch from.",
    )
    parser.add_argument(
        "--wallet",
        required=True,
        help="Wallet address to fetch.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Recent Solana signatures to request.",
    )
    parser.add_argument(
        "--page",
        type=int,
        default=1,
        help="EVM API page number.",
    )
    parser.add_argument(
        "--offset",
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
    if args.chain == "solana":
        snapshot_path = SolanaRpcClient(
            rpc_url=args.rpc_url,
        ).save_recent_transaction_history(
            args.wallet,
            repository_root=ROOT,
            limit=args.limit,
        )
    else:
        snapshot_path = EvmWalletClient(
            api_key=args.api_key,
        ).save_recent_transaction_history(
            args.wallet,
            repository_root=ROOT,
            page=args.page,
            offset=args.offset,
        )

    print(snapshot_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
