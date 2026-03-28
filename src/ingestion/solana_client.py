"""Minimal Solana raw-ingestion client."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DEFAULT_SOLANA_RPC_URL = "https://api.mainnet-beta.solana.com"


class SolanaRpcClient:
    """Fetch recent raw Solana wallet transaction data from public RPC."""

    # TODO: Add full pagination support with `before`/`until` traversal when
    # recent snapshots are no longer sufficient for audit needs.
    # TODO: Respect `Retry-After` and RPC-specific rate-limit behavior from
    # the public endpoint instead of surfacing HTTP failures directly.
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
                            "commitment": "confirmed",
                            "encoding": "json",
                            "maxSupportedTransactionVersion": 0,
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
            "request": {
                "method": "getSignaturesForAddress",
                "limit": limit,
                "before": before,
                "commitment": "confirmed",
            },
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

        parsed = json.loads(raw_body)
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
