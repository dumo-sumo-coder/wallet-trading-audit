"""Minimal Solana raw-ingestion client.

This client intentionally stores JSON-RPC response bodies only. It does not
normalize transaction fields and it does not preserve HTTP headers.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DEFAULT_SOLANA_RPC_URL = "https://api.mainnet-beta.solana.com"


class SolanaRpcClient:
    """Fetch recent raw Solana wallet transaction data from public RPC.

    The current implementation uses:
    - `getSignaturesForAddress` to list recent signatures, newest first
    - `getTransaction` once per returned signature with `encoding="json"`

    It preserves the JSON-RPC response bodies verbatim after JSON parsing, but
    does not store raw bytes or HTTP response headers.
    """

    # TODO: Add full pagination support with `before`/`until` traversal when
    # recent snapshots are no longer sufficient for audit needs.
    # TODO: Respect `Retry-After` and RPC-specific rate-limit behavior from
    # the public endpoint instead of surfacing HTTP failures directly.
    # TODO: Revisit whether `confirmed` should remain the default commitment for
    # audit snapshots, or whether long-horizon backfills should use `finalized`.
    # TODO: Review whether `maxSupportedTransactionVersion=0` remains
    # sufficient if newer transaction versions appear in target wallets.

    def __init__(
        self,
        rpc_url: str = DEFAULT_SOLANA_RPC_URL,
        *,
        timeout_seconds: int = 30,
    ) -> None:
        self.rpc_url = rpc_url
        self.timeout_seconds = timeout_seconds

    def fetch_recent_transaction_history(
        self,
        wallet: str,
        *,
        limit: int = 20,
        before: str | None = None,
    ) -> dict[str, object]:
        """Fetch recent Solana transaction payloads for one wallet.

        This preserves provider-native JSON-RPC responses instead of mapping
        fields into the project's normalized schema.
        """

        wallet_text = wallet.strip()
        if not wallet_text:
            raise ValueError("wallet must be a non-empty string")
        if limit <= 0:
            raise ValueError("limit must be positive")

        signature_params: dict[str, object] = {
            "commitment": "confirmed",
            "limit": limit,
        }
        if before is not None:
            signature_params["before"] = before
        transaction_request = {
            "method": "getTransaction",
            "commitment": "confirmed",
            "encoding": "json",
            "max_supported_transaction_version": 0,
        }

        signatures_response = self._rpc_request(
            method="getSignaturesForAddress",
            params=[wallet_text, signature_params],
        )
        signature_rows = self._extract_signature_rows(signatures_response)

        transaction_responses: list[dict[str, Any]] = []
        for signature_row in signature_rows:
            signature = signature_row.get("signature")
            if not isinstance(signature, str) or not signature.strip():
                raise ValueError("Solana signature response is missing a valid signature")
            transaction_responses.append(
                self._rpc_request(
                    method="getTransaction",
                    params=[
                        signature,
                        {
                            "commitment": transaction_request["commitment"],
                            "encoding": transaction_request["encoding"],
                            "maxSupportedTransactionVersion": transaction_request[
                                "max_supported_transaction_version"
                            ],
                        },
                    ],
                )
            )

        fetched_at = datetime.now(UTC).isoformat()
        return {
            "wallet": wallet_text,
            "fetched_at_utc": fetched_at,
            "source": {
                "provider": "solana_json_rpc",
                "rpc_url": self.rpc_url,
            },
            "capture": {
                "normalization_applied": False,
                "response_body_format": "json",
                "response_bodies_preserved": True,
                "http_headers_preserved": False,
                "signature_order": "newest_first",
                "retrieval_pattern": "getSignaturesForAddress_then_getTransaction",
            },
            "request": {
                "method": "getSignaturesForAddress",
                "limit": limit,
                "before": before,
                "commitment": "confirmed",
            },
            "transaction_request": transaction_request,
            "signatures_response": signatures_response,
            "transaction_responses": transaction_responses,
        }

    def save_recent_transaction_history(
        self,
        wallet: str,
        repository_root: Path,
        *,
        limit: int = 20,
        before: str | None = None,
    ) -> Path:
        """Fetch and save a raw Solana snapshot under data/raw/solana/."""

        snapshot = self.fetch_recent_transaction_history(
            wallet,
            limit=limit,
            before=before,
        )
        storage_dir = repository_root / "data" / "raw" / "solana"
        storage_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        filename = f"{_safe_path_component(wallet)}_{timestamp}.json"
        snapshot_path = storage_dir / filename
        snapshot_path.write_text(
            json.dumps(snapshot, indent=2),
            encoding="utf-8",
        )
        return snapshot_path

    def _rpc_request(
        self,
        *,
        method: str,
        params: list[object],
    ) -> dict[str, Any]:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }
        request = Request(
            self.rpc_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "User-Agent": "wallet-trading-audit/0.1",
            },
            method="POST",
        )

        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                raw_body = response.read().decode("utf-8")
        except HTTPError as exc:
            raise RuntimeError(
                f"Solana RPC request failed with HTTP {exc.code} for method {method}"
            ) from exc
        except URLError as exc:
            raise RuntimeError(f"Solana RPC request failed for method {method}") from exc

        try:
            parsed = json.loads(raw_body)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Solana RPC returned invalid JSON for {method}") from exc
        if not isinstance(parsed, dict):
            raise ValueError(f"Solana RPC returned a non-object payload for {method}")
        if "error" in parsed:
            raise RuntimeError(f"Solana RPC returned an error for {method}: {parsed['error']}")
        return parsed

    def _extract_signature_rows(
        self,
        response: dict[str, Any],
    ) -> list[dict[str, Any]]:
        result = response.get("result")
        if not isinstance(result, list):
            raise ValueError("Solana getSignaturesForAddress response missing list result")
        signature_rows: list[dict[str, Any]] = []
        for row in result:
            if not isinstance(row, dict):
                raise ValueError("Solana signature row must be an object")
            signature_rows.append(row)
        return signature_rows


def _safe_path_component(value: str) -> str:
    cleaned = "".join(
        character if character.isalnum() or character in {"-", "_"} else "_"
        for character in value.strip()
    )
    return cleaned or "wallet"
