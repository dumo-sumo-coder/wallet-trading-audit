"""Minimal Solana RPC debug helper for the test wallet."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
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
DEFAULT_SIGNATURE_LIMIT = 5


@dataclass(frozen=True, slots=True)
class RpcDebugCallResult:
    rpc_method: str
    status: str
    result_count: int | None
    diagnostics: dict[str, str | None]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Debug the Solana RPC path with safe, sanitized diagnostics.",
    )
    parser.add_argument(
        "--signature-limit",
        type=int,
        default=DEFAULT_SIGNATURE_LIMIT,
        help="How many signatures to request during the RPC debug call.",
    )
    return parser.parse_args()


def ensure_helius_api_key_present() -> None:
    if get_env_var_status("HELIUS_API_KEY") != "present":
        raise ValueError(
            build_missing_env_message(
                "HELIUS_API_KEY",
                purpose_text="This Solana RPC debug flow requires HELIUS_API_KEY to be present.",
            )
        )


def attempt_rpc_call(
    client: SolanaRpcClient,
    *,
    method: str,
    params: list[object],
) -> RpcDebugCallResult:
    try:
        response = client._rpc_request(method=method, params=params)
    except Exception as exc:
        return RpcDebugCallResult(
            rpc_method=method,
            status="failure",
            result_count=None,
            diagnostics=extract_solana_rpc_diagnostics(exc),
        )

    result_count: int | None = None
    result = response.get("result")
    if isinstance(result, list):
        result_count = len(result)

    return RpcDebugCallResult(
        rpc_method=method,
        status="success",
        result_count=result_count,
        diagnostics={
            "provider": SOLANA_PROVIDER_NAME,
            "rpc_url": client.rpc_url_for_output,
            "rpc_method": method,
            "failure_category": None,
            "provider_status": "ok",
            "response_snippet": None,
            "exception_class": None,
        },
    )


def main() -> int:
    args = parse_args()
    try:
        ensure_helius_api_key_present()
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        print(
            "If you prefer manual terminal loading, run "
            f"'{get_manual_env_load_instructions()}' before this script.",
            file=sys.stderr,
        )
        return 1

    client = SolanaRpcClient()
    print("HELIUS_API_KEY: present")
    print(f"Provider: {SOLANA_PROVIDER_NAME}")
    print(f"RPC endpoint: {client.rpc_url_for_output}")

    version_result = attempt_rpc_call(
        client,
        method="getVersion",
        params=[],
    )
    _print_call_result(version_result)

    signatures_result = attempt_rpc_call(
        client,
        method="getSignaturesForAddress",
        params=[
            DEFAULT_TEST_WALLET,
            {
                "commitment": "confirmed",
                "limit": args.signature_limit,
            },
        ],
    )
    _print_call_result(signatures_result)

    return 0 if version_result.status == "success" and signatures_result.status == "success" else 1


def _print_call_result(result: RpcDebugCallResult) -> None:
    print(f"{result.rpc_method}: {result.status}")
    if result.result_count is not None:
        print(f"  result_count={result.result_count}")
    print(f"  provider={result.diagnostics.get('provider')}")
    print(f"  rpc_url={result.diagnostics.get('rpc_url')}")
    print(f"  failure_category={result.diagnostics.get('failure_category')}")
    print(f"  provider_status={result.diagnostics.get('provider_status')}")
    print(f"  exception_class={result.diagnostics.get('exception_class')}")
    print(f"  response_snippet={result.diagnostics.get('response_snippet')}")


if __name__ == "__main__":
    raise SystemExit(main())
