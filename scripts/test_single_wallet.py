"""Controlled end-to-end test fetch for one Solana wallet."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from config import (  # noqa: E402
    build_missing_env_message,
    get_env_var_status,
    get_manual_env_load_instructions,
)
from ingestion.solana_client import (  # noqa: E402
    SOLANA_PROVIDER_NAME,
    SolanaRpcClient,
    extract_solana_rpc_diagnostics,
)

DEFAULT_TEST_WALLET = "5xKwYXp27dbDxV3UBnk8NWTeEH97sdaufRBA2qdd8X5B"
DEFAULT_TX_LIMIT = 50
SOLANA_WALLET_KIND = "solana"
SOLANA_BASE58_ALPHABET = frozenset("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")


@dataclass(frozen=True, slots=True)
class SingleWalletTestResult:
    wallet: str
    chain: str
    status: str
    tx_count: int
    output_directory: str
    snapshot_path: str | None
    metadata_path: str | None
    diagnostics: dict[str, str | None]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch one controlled Solana wallet snapshot for a first live test.",
    )
    parser.add_argument(
        "--tx-limit",
        type=int,
        default=DEFAULT_TX_LIMIT,
        help="Maximum number of recent Solana signatures to fetch for the test wallet.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate env and wallet classification without making a live provider call.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print sanitized provider diagnostics for this single-wallet test.",
    )
    return parser.parse_args()


def ensure_helius_api_key_present() -> None:
    if get_env_var_status("HELIUS_API_KEY") != "present":
        raise ValueError(
            build_missing_env_message(
                "HELIUS_API_KEY",
                purpose_text=(
                    "This single-wallet Solana test requires HELIUS_API_KEY to be present."
                ),
            )
        )


def validate_test_wallet(wallet: str) -> str:
    wallet_text = wallet.strip()
    if not wallet_text:
        raise ValueError("Test wallet must be a non-empty string.")
    if wallet_text.startswith("0x"):
        raise ValueError("Test wallet does not look like a Solana wallet.")
    if not 32 <= len(wallet_text) <= 44:
        raise ValueError("Test wallet length is not valid for a Solana address.")
    if any(character not in SOLANA_BASE58_ALPHABET for character in wallet_text):
        raise ValueError("Test wallet contains non-base58 characters and is not a Solana wallet.")
    return SOLANA_WALLET_KIND


def run_single_wallet_test(
    *,
    wallet: str = DEFAULT_TEST_WALLET,
    tx_limit: int = DEFAULT_TX_LIMIT,
    dry_run: bool = False,
    repository_root: Path = ROOT,
    client: SolanaRpcClient | None = None,
) -> SingleWalletTestResult:
    ensure_helius_api_key_present()
    chain = validate_test_wallet(wallet)
    if tx_limit <= 0:
        raise ValueError("tx_limit must be positive.")

    output_directory = repository_root / "data" / "raw" / "solana" / "test_wallet"
    output_directory.mkdir(parents=True, exist_ok=True)

    if dry_run:
        return SingleWalletTestResult(
            wallet=wallet,
            chain=chain,
            status="dry_run",
            tx_count=0,
            output_directory=_relative_path_text(output_directory, repository_root),
            snapshot_path=None,
            metadata_path=None,
            diagnostics={
                "provider": SOLANA_PROVIDER_NAME,
                "rpc_url": None,
                "rpc_method": None,
                "failure_category": None,
                "provider_status": None,
                "response_snippet": None,
                "exception_class": None,
            },
        )

    fetcher = client or SolanaRpcClient()
    current_time = _utc_now()
    fetched_at = current_time.isoformat()
    timestamp_token = current_time.strftime("%Y%m%dT%H%M%SZ")
    snapshot_path: Path | None = None
    status = "failure"
    tx_count = 0
    error_message: str | None = None
    diagnostics: dict[str, str | None] = {
        "provider": SOLANA_PROVIDER_NAME,
        "rpc_url": getattr(fetcher, "rpc_url_for_output", None),
        "rpc_method": None,
        "failure_category": None,
        "provider_status": None,
        "response_snippet": None,
        "exception_class": None,
    }

    try:
        snapshot = fetcher.fetch_recent_transaction_history(
            wallet,
            limit=tx_limit,
        )
        fetched_at = _extract_fetched_at(snapshot, default=fetched_at)
        tx_count = _count_transaction_responses(snapshot)
        snapshot_path = output_directory / f"wallet_snapshot_{timestamp_token}.json"
        snapshot_path.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
        status = "success"
    except Exception as exc:
        error_message = str(exc)
        diagnostics = extract_solana_rpc_diagnostics(exc)

    metadata_path = output_directory / f"wallet_test_metadata_{timestamp_token}.json"
    metadata_payload = {
        "wallet": wallet,
        "chain": chain,
        "tested_at": fetched_at,
        "tx_limit": tx_limit,
        "tx_count": tx_count,
        "status": status,
        "error_message": error_message,
        "provider": diagnostics["provider"],
        "rpc_url": diagnostics["rpc_url"],
        "rpc_method": diagnostics["rpc_method"],
        "failure_category": diagnostics["failure_category"],
        "provider_status": diagnostics["provider_status"],
        "response_snippet": diagnostics["response_snippet"],
        "exception_class": diagnostics["exception_class"],
        "snapshot_path": (
            _relative_path_text(snapshot_path, repository_root) if snapshot_path is not None else None
        ),
    }
    metadata_path.write_text(json.dumps(metadata_payload, indent=2), encoding="utf-8")

    return SingleWalletTestResult(
        wallet=wallet,
        chain=chain,
        status=status,
        tx_count=tx_count,
        output_directory=_relative_path_text(output_directory, repository_root),
        snapshot_path=(
            _relative_path_text(snapshot_path, repository_root) if snapshot_path is not None else None
        ),
        metadata_path=_relative_path_text(metadata_path, repository_root),
        diagnostics=diagnostics,
    )


def main() -> int:
    args = parse_args()
    try:
        result = run_single_wallet_test(
            tx_limit=args.tx_limit,
            dry_run=args.dry_run,
        )
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        print(
            "If you prefer manual terminal loading, run "
            f"'{get_manual_env_load_instructions()}' before this script.",
            file=sys.stderr,
        )
        return 1

    print("HELIUS_API_KEY: present")
    print(f"Wallet validation: success (chain={result.chain})")
    print(f"Wallet tested: {result.wallet}")
    print(f"Result: {result.status}")
    print(f"Transactions fetched: {result.tx_count}")
    print(f"Output directory: {result.output_directory}")
    if result.snapshot_path is not None:
        print(f"Snapshot path: {result.snapshot_path}")
    if result.metadata_path is not None:
        print(f"Metadata path: {result.metadata_path}")
    if args.verbose:
        _print_diagnostics(result.diagnostics)
    if result.status == "dry_run":
        print("Dry run only. No live provider call was made.")
        return 0
    return 0 if result.status == "success" else 1


def _count_transaction_responses(snapshot: dict[str, object]) -> int:
    responses = snapshot.get("transaction_responses")
    if not isinstance(responses, list):
        raise ValueError("Solana snapshot is missing transaction_responses.")
    return len(responses)


def _extract_fetched_at(snapshot: dict[str, object], *, default: str) -> str:
    fetched_at = snapshot.get("fetched_at_utc")
    if not isinstance(fetched_at, str) or not fetched_at.strip():
        return default
    return fetched_at


def _relative_path_text(path: Path, repository_root: Path) -> str:
    try:
        return path.relative_to(repository_root).as_posix()
    except ValueError:
        return str(path)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _print_diagnostics(diagnostics: dict[str, str | None]) -> None:
    print("Provider diagnostics:")
    print(f"  provider={diagnostics.get('provider')}")
    print(f"  rpc_url={diagnostics.get('rpc_url')}")
    print(f"  rpc_method={diagnostics.get('rpc_method')}")
    print(f"  failure_category={diagnostics.get('failure_category')}")
    print(f"  provider_status={diagnostics.get('provider_status')}")
    print(f"  exception_class={diagnostics.get('exception_class')}")
    print(f"  response_snippet={diagnostics.get('response_snippet')}")


if __name__ == "__main__":
    raise SystemExit(main())
