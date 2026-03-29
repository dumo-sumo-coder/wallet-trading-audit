"""Operator flow for one controlled Solana wallet fetch."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
SRC = ROOT / "src"
for search_path in (SCRIPT_DIR, SRC):
    if str(search_path) not in sys.path:
        sys.path.insert(0, str(search_path))

from debug_solana_rpc import RpcDebugCallResult, attempt_rpc_call  # noqa: E402
from ingestion.solana_client import SolanaRpcClient  # noqa: E402
from test_single_wallet import (  # noqa: E402
    DEFAULT_TEST_WALLET,
    DEFAULT_TX_LIMIT,
    SingleWalletTestResult,
    ensure_helius_api_key_present,
    run_single_wallet_test,
    validate_test_wallet,
)


@dataclass(frozen=True, slots=True)
class SingleWalletFetchWorkflowResult:
    wallet: str
    chain: str
    provider: str
    rpc_url: str
    preflight_only: bool
    connectivity_result: RpcDebugCallResult | None
    fetch_result: SingleWalletTestResult | None


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a controlled live Solana fetch for one wallet with safe diagnostics.",
    )
    parser.add_argument(
        "--wallet",
        default=DEFAULT_TEST_WALLET,
        help="Solana wallet to test. Defaults to the current target wallet.",
    )
    parser.add_argument(
        "--tx-limit",
        type=int,
        default=DEFAULT_TX_LIMIT,
        help="Maximum number of recent Solana signatures to fetch.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print sanitized provider diagnostics.",
    )
    parser.add_argument(
        "--preflight-only",
        action="store_true",
        help="Validate env and wallet shape without making a live provider call.",
    )
    return parser.parse_args(argv)


def run_single_wallet_fetch_workflow(
    *,
    wallet: str,
    tx_limit: int,
    preflight_only: bool,
    repository_root: Path = ROOT,
    client: SolanaRpcClient | None = None,
) -> SingleWalletFetchWorkflowResult:
    ensure_helius_api_key_present()
    chain = validate_test_wallet(wallet)
    if tx_limit <= 0:
        raise ValueError("tx_limit must be positive.")

    fetcher = client or SolanaRpcClient()
    provider = "solana_json_rpc"
    rpc_url = getattr(fetcher, "rpc_url_for_output", "[configured_rpc_url]")

    if preflight_only:
        return SingleWalletFetchWorkflowResult(
            wallet=wallet,
            chain=chain,
            provider=provider,
            rpc_url=rpc_url,
            preflight_only=True,
            connectivity_result=None,
            fetch_result=None,
        )

    connectivity_result = attempt_rpc_call(
        fetcher,
        method="getVersion",
        params=[],
    )
    if connectivity_result.status != "success":
        return SingleWalletFetchWorkflowResult(
            wallet=wallet,
            chain=chain,
            provider=provider,
            rpc_url=rpc_url,
            preflight_only=False,
            connectivity_result=connectivity_result,
            fetch_result=None,
        )

    fetch_result = run_single_wallet_test(
        wallet=wallet,
        tx_limit=tx_limit,
        repository_root=repository_root,
        client=fetcher,
    )
    return SingleWalletFetchWorkflowResult(
        wallet=wallet,
        chain=chain,
        provider=provider,
        rpc_url=rpc_url,
        preflight_only=False,
        connectivity_result=connectivity_result,
        fetch_result=fetch_result,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = run_single_wallet_fetch_workflow(
            wallet=args.wallet,
            tx_limit=args.tx_limit,
            preflight_only=args.preflight_only,
        )
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print("HELIUS_API_KEY: present")
    print(f"Wallet validation: success (chain={result.chain})")
    print(f"Wallet tested: {result.wallet}")
    print(f"Provider: {result.provider}")
    print(f"RPC endpoint: {result.rpc_url}")

    if result.preflight_only:
        print("Result: preflight_only")
        print("Provider connectivity check skipped by --preflight-only.")
        return 0

    if result.connectivity_result is not None:
        print(f"Connectivity check ({result.connectivity_result.rpc_method}): {result.connectivity_result.status}")
        if args.verbose or result.connectivity_result.status != "success":
            _print_diagnostics(result.connectivity_result.diagnostics)

    if result.fetch_result is None:
        print("Result: failure")
        print("Fetch was not attempted because the provider connectivity check failed.")
        return 1

    fetch_result = result.fetch_result
    print(f"Result: {fetch_result.status}")
    print(f"Transactions fetched: {fetch_result.tx_count}")
    print(f"Output directory: {fetch_result.output_directory}")
    if fetch_result.snapshot_path is not None:
        print(f"Snapshot path: {fetch_result.snapshot_path}")
    if fetch_result.metadata_path is not None:
        print(f"Metadata path: {fetch_result.metadata_path}")
    if args.verbose or fetch_result.status != "success":
        _print_diagnostics(fetch_result.diagnostics)
    return 0 if fetch_result.status == "success" else 1


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
