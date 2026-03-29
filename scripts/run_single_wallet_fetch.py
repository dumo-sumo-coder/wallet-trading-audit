"""Operator flow for one controlled Solana wallet fetch."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
SRC = ROOT / "src"
for search_path in (SCRIPT_DIR, SRC):
    if str(search_path) not in sys.path:
        sys.path.insert(0, str(search_path))

from debug_solana_rpc import RpcDebugCallResult, attempt_rpc_call  # noqa: E402
from ingestion.solana_client import SolanaRpcClient, extract_solana_rpc_diagnostics  # noqa: E402
from test_single_wallet import (  # noqa: E402
    DEFAULT_TEST_WALLET,
    DEFAULT_TX_LIMIT,
    ensure_helius_api_key_present,
    validate_test_wallet,
)


@dataclass(frozen=True, slots=True)
class SingleWalletHistoryFetchResult:
    wallet: str
    chain: str
    status: str
    total_tx_count: int
    total_pages_fetched: int
    output_directory: str
    metadata_path: str | None
    page_snapshot_paths: tuple[str, ...]
    diagnostics: dict[str, str | None]


@dataclass(frozen=True, slots=True)
class SingleWalletFetchWorkflowResult:
    wallet: str
    chain: str
    provider: str
    rpc_url: str
    preflight_only: bool
    connectivity_result: RpcDebugCallResult | None
    fetch_result: SingleWalletHistoryFetchResult | None


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
        help="Maximum number of recent Solana signatures to fetch per page.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=1,
        help="Maximum number of paginated Solana history pages to fetch.",
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
    max_pages: int,
    preflight_only: bool,
    repository_root: Path = ROOT,
    client: SolanaRpcClient | None = None,
) -> SingleWalletFetchWorkflowResult:
    ensure_helius_api_key_present()
    chain = validate_test_wallet(wallet)
    if tx_limit <= 0:
        raise ValueError("tx_limit must be positive.")
    if max_pages <= 0:
        raise ValueError("max_pages must be positive.")

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

    fetch_result = fetch_single_wallet_history_pages(
        wallet=wallet,
        tx_limit=tx_limit,
        max_pages=max_pages,
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


def fetch_single_wallet_history_pages(
    *,
    wallet: str,
    tx_limit: int,
    max_pages: int,
    repository_root: Path = ROOT,
    client: SolanaRpcClient | None = None,
) -> SingleWalletHistoryFetchResult:
    ensure_helius_api_key_present()
    chain = validate_test_wallet(wallet)
    if tx_limit <= 0:
        raise ValueError("tx_limit must be positive.")
    if max_pages <= 0:
        raise ValueError("max_pages must be positive.")

    fetcher = client or SolanaRpcClient()
    current_time = _utc_now()
    fetched_at = current_time.isoformat()
    timestamp_token = current_time.strftime("%Y%m%dT%H%M%SZ")
    output_root = repository_root / "data" / "raw" / "solana" / "test_wallet"
    output_root.mkdir(parents=True, exist_ok=True)
    page_directory = output_root / f"fetch_{timestamp_token}"
    page_directory.mkdir(parents=True, exist_ok=True)

    status = "failure"
    total_tx_count = 0
    page_snapshot_paths: list[str] = []
    page_records: list[dict[str, object]] = []
    error_message: str | None = None
    before: str | None = None
    diagnostics: dict[str, str | None] = {
        "provider": "solana_json_rpc",
        "rpc_url": getattr(fetcher, "rpc_url_for_output", None),
        "rpc_method": None,
        "failure_category": None,
        "provider_status": None,
        "response_snippet": None,
        "exception_class": None,
    }

    try:
        for page_number in range(1, max_pages + 1):
            snapshot = fetcher.fetch_recent_transaction_history(
                wallet,
                limit=tx_limit,
                before=before,
            )
            fetched_at = _extract_fetched_at(snapshot, default=fetched_at)
            tx_count = _count_transaction_responses(snapshot)
            if tx_count == 0:
                break

            snapshot_path = page_directory / f"wallet_snapshot_page_{page_number:03d}.json"
            snapshot_path.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
            snapshot_relative_path = _relative_path_text(snapshot_path, repository_root)
            page_snapshot_paths.append(snapshot_relative_path)
            total_tx_count += tx_count
            page_records.append(
                {
                    "page_number": page_number,
                    "before": before,
                    "tx_count": tx_count,
                    "snapshot_path": snapshot_relative_path,
                    "first_tx_hash": _extract_first_tx_hash(snapshot),
                    "last_tx_hash": _extract_last_tx_hash(snapshot),
                }
            )

            if tx_count < tx_limit:
                break

            before = _extract_last_signature(snapshot)
            if before is None:
                break
    except Exception as exc:
        error_message = str(exc)
        diagnostics = _extract_sanitized_diagnostics(exc, fetcher)
        status = "partial_failure" if page_records else "failure"
    else:
        status = "success"

    metadata_path = output_root / f"wallet_fetch_metadata_{timestamp_token}.json"
    metadata_payload = {
        "wallet": wallet,
        "chain": chain,
        "tested_at": fetched_at,
        "tx_limit_per_page": tx_limit,
        "max_pages_requested": max_pages,
        "total_pages_fetched": len(page_records),
        "total_tx_count": total_tx_count,
        "status": status,
        "error_message": error_message,
        "provider": diagnostics["provider"],
        "rpc_url": diagnostics["rpc_url"],
        "rpc_method": diagnostics["rpc_method"],
        "failure_category": diagnostics["failure_category"],
        "provider_status": diagnostics["provider_status"],
        "response_snippet": diagnostics["response_snippet"],
        "exception_class": diagnostics["exception_class"],
        "fetch_directory": _relative_path_text(page_directory, repository_root),
        "page_snapshot_paths": page_snapshot_paths,
        "pages": page_records,
    }
    metadata_path.write_text(json.dumps(metadata_payload, indent=2), encoding="utf-8")

    return SingleWalletHistoryFetchResult(
        wallet=wallet,
        chain=chain,
        status=status,
        total_tx_count=total_tx_count,
        total_pages_fetched=len(page_records),
        output_directory=_relative_path_text(page_directory, repository_root),
        metadata_path=_relative_path_text(metadata_path, repository_root),
        page_snapshot_paths=tuple(page_snapshot_paths),
        diagnostics=diagnostics,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = run_single_wallet_fetch_workflow(
            wallet=args.wallet,
            tx_limit=args.tx_limit,
            max_pages=args.max_pages,
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
        print(
            f"Connectivity check ({result.connectivity_result.rpc_method}): "
            f"{result.connectivity_result.status}"
        )
        if args.verbose or result.connectivity_result.status != "success":
            _print_diagnostics(result.connectivity_result.diagnostics)

    if result.fetch_result is None:
        print("Result: failure")
        print("Fetch was not attempted because the provider connectivity check failed.")
        return 1

    fetch_result = result.fetch_result
    print(f"Result: {fetch_result.status}")
    print(f"Transactions fetched: {fetch_result.total_tx_count}")
    print(f"Pages fetched: {fetch_result.total_pages_fetched}")
    print(f"Output directory: {fetch_result.output_directory}")
    if fetch_result.metadata_path is not None:
        print(f"Metadata path: {fetch_result.metadata_path}")
    for page_snapshot_path in fetch_result.page_snapshot_paths:
        print(f"Page snapshot: {page_snapshot_path}")
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


def _count_transaction_responses(snapshot: Mapping[str, object]) -> int:
    responses = snapshot.get("transaction_responses")
    if not isinstance(responses, list):
        raise ValueError("Solana snapshot is missing transaction_responses.")
    return len(responses)


def _extract_fetched_at(snapshot: Mapping[str, object], *, default: str) -> str:
    fetched_at = snapshot.get("fetched_at_utc")
    if not isinstance(fetched_at, str) or not fetched_at.strip():
        return default
    return fetched_at


def _extract_last_signature(snapshot: Mapping[str, object]) -> str | None:
    signatures_response = snapshot.get("signatures_response")
    if not isinstance(signatures_response, Mapping):
        return None
    result = signatures_response.get("result")
    if not isinstance(result, list) or not result:
        return None
    last_row = result[-1]
    if not isinstance(last_row, Mapping):
        return None
    signature = last_row.get("signature")
    if not isinstance(signature, str) or not signature.strip():
        return None
    return signature


def _extract_first_tx_hash(snapshot: Mapping[str, object]) -> str | None:
    responses = snapshot.get("transaction_responses")
    if not isinstance(responses, list) or not responses:
        return None
    return _extract_tx_hash_from_payload(responses[0])


def _extract_last_tx_hash(snapshot: Mapping[str, object]) -> str | None:
    responses = snapshot.get("transaction_responses")
    if not isinstance(responses, list) or not responses:
        return None
    return _extract_tx_hash_from_payload(responses[-1])


def _extract_tx_hash_from_payload(payload: object) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    result = payload.get("result")
    if not isinstance(result, Mapping):
        return None
    transaction = result.get("transaction")
    if not isinstance(transaction, Mapping):
        return None
    signatures = transaction.get("signatures")
    if not isinstance(signatures, list) or not signatures:
        return None
    first_signature = signatures[0]
    if not isinstance(first_signature, str) or not first_signature.strip():
        return None
    return first_signature


def _extract_sanitized_diagnostics(
    exc: Exception,
    fetcher: SolanaRpcClient | object,
) -> dict[str, str | None]:
    diagnostics = extract_solana_rpc_diagnostics(exc)
    if diagnostics["rpc_url"] is None:
        diagnostics["rpc_url"] = getattr(fetcher, "rpc_url_for_output", None)
    return diagnostics


def _relative_path_text(path: Path, repository_root: Path) -> str:
    try:
        return path.relative_to(repository_root).as_posix()
    except ValueError:
        return str(path)


def _utc_now() -> datetime:
    return datetime.now(UTC)


if __name__ == "__main__":
    raise SystemExit(main())
