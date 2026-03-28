"""CLI helper to fetch and save raw wallet transaction snapshots."""

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
from ingestion.solana_client import SolanaRpcClient  # noqa: E402
from ingestion.solana_review import (  # noqa: E402
    export_representative_transaction_payloads,
    load_json_mapping,
)


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
        "--copy-solana-payload-fixtures",
        action="store_true",
        help=(
            "For Solana snapshots, copy one or more raw getTransaction response "
            "bodies into tests/fixtures/raw_solana/."
        ),
    )
    parser.add_argument(
        "--solana-fixture-count",
        type=int,
        default=2,
        help="How many Solana transaction response bodies to copy when fixture export is enabled.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    exported_fixture_paths: tuple[Path, ...] = ()
    try:
        if args.chain == "solana":
            snapshot_path = SolanaRpcClient(
                rpc_url=args.rpc_url,
            ).save_recent_transaction_history(
                args.wallet,
                repository_root=ROOT,
                limit=args.limit,
            )
            if args.copy_solana_payload_fixtures:
                snapshot = load_json_mapping(snapshot_path)
                exported_fixture_paths = export_representative_transaction_payloads(
                    snapshot,
                    ROOT / "tests" / "fixtures" / "raw_solana",
                    limit=args.solana_fixture_count,
                )
        else:
            if args.copy_solana_payload_fixtures:
                raise ValueError(
                    "--copy-solana-payload-fixtures is only supported with --chain solana"
                )
            snapshot_path = EvmWalletClient(
                api_key=args.api_key,
            ).save_recent_transaction_history(
                args.wallet,
                repository_root=ROOT,
                page=args.page,
                offset=args.offset,
            )
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        print(
            "If you prefer manual terminal loading, run "
            f"'{get_manual_env_load_instructions()}' before this script.",
            file=sys.stderr,
        )
        return 1

    print(snapshot_path)
    for fixture_path in exported_fixture_paths:
        print(fixture_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
