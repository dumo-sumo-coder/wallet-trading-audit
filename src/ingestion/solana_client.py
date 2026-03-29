"""Minimal Solana raw-ingestion client.

This client intentionally stores JSON-RPC response bodies only. It does not
normalize transaction fields and it does not preserve HTTP headers.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from config import get_solana_rpc_url, sanitize_text_for_output, sanitize_url_for_output

SOLANA_PROVIDER_NAME = "solana_json_rpc"


@dataclass(frozen=True, slots=True)
class SolanaRpcRequestDiagnostics:
    provider: str
    rpc_url: str
    rpc_method: str
    failure_category: str
    provider_status: str | None
    response_snippet: str | None
    exception_class: str


class SolanaRpcRequestError(RuntimeError):
    """Sanitized Solana RPC failure with structured diagnostics."""

    def __init__(
        self,
        message: str,
        *,
        diagnostics: SolanaRpcRequestDiagnostics,
    ) -> None:
        super().__init__(message)
        self.diagnostics = diagnostics


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
        rpc_url: str | None = None,
        *,
        timeout_seconds: int = 30,
    ) -> None:
        self.rpc_url = rpc_url or get_solana_rpc_url(required=True)
        self.rpc_url_for_output = sanitize_url_for_output(self.rpc_url)
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
                "provider": SOLANA_PROVIDER_NAME,
                "rpc_url": self.rpc_url_for_output,
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
            response_snippet = _read_http_error_snippet(
                exc,
                rpc_url=self.rpc_url,
                rpc_url_for_output=self.rpc_url_for_output,
            )
            raise SolanaRpcRequestError(
                f"Solana RPC request failed with HTTP {exc.code} for method {method}",
                diagnostics=SolanaRpcRequestDiagnostics(
                    provider=SOLANA_PROVIDER_NAME,
                    rpc_url=self.rpc_url_for_output,
                    rpc_method=method,
                    failure_category="http_error",
                    provider_status=str(exc.code),
                    response_snippet=response_snippet,
                    exception_class=exc.__class__.__name__,
                ),
            ) from exc
        except URLError as exc:
            raise SolanaRpcRequestError(
                f"Solana RPC request failed for method {method}",
                diagnostics=SolanaRpcRequestDiagnostics(
                    provider=SOLANA_PROVIDER_NAME,
                    rpc_url=self.rpc_url_for_output,
                    rpc_method=method,
                    failure_category="url_error",
                    provider_status=None,
                    response_snippet=_sanitize_snippet(
                        str(exc.reason),
                        rpc_url=self.rpc_url,
                        rpc_url_for_output=self.rpc_url_for_output,
                    ),
                    exception_class=exc.__class__.__name__,
                ),
            ) from exc

        try:
            parsed = json.loads(raw_body)
        except json.JSONDecodeError as exc:
            raise SolanaRpcRequestError(
                f"Solana RPC returned invalid JSON for {method}",
                diagnostics=SolanaRpcRequestDiagnostics(
                    provider=SOLANA_PROVIDER_NAME,
                    rpc_url=self.rpc_url_for_output,
                    rpc_method=method,
                    failure_category="invalid_json",
                    provider_status=None,
                    response_snippet=_sanitize_snippet(
                        raw_body,
                        rpc_url=self.rpc_url,
                        rpc_url_for_output=self.rpc_url_for_output,
                    ),
                    exception_class=exc.__class__.__name__,
                ),
            ) from exc
        if not isinstance(parsed, dict):
            raise SolanaRpcRequestError(
                f"Solana RPC returned a non-object payload for {method}",
                diagnostics=SolanaRpcRequestDiagnostics(
                    provider=SOLANA_PROVIDER_NAME,
                    rpc_url=self.rpc_url_for_output,
                    rpc_method=method,
                    failure_category="invalid_payload",
                    provider_status=None,
                    response_snippet=_sanitize_snippet(
                        json.dumps(parsed),
                        rpc_url=self.rpc_url,
                        rpc_url_for_output=self.rpc_url_for_output,
                    ),
                    exception_class=type(parsed).__name__,
                ),
            )
        if "error" in parsed:
            error_payload = parsed["error"]
            provider_status: str | None = None
            if isinstance(error_payload, dict) and "code" in error_payload:
                provider_status = str(error_payload["code"])
            raise SolanaRpcRequestError(
                f"Solana RPC returned an error for {method}",
                diagnostics=SolanaRpcRequestDiagnostics(
                    provider=SOLANA_PROVIDER_NAME,
                    rpc_url=self.rpc_url_for_output,
                    rpc_method=method,
                    failure_category="rpc_error",
                    provider_status=provider_status,
                    response_snippet=_sanitize_snippet(
                        json.dumps(error_payload),
                        rpc_url=self.rpc_url,
                        rpc_url_for_output=self.rpc_url_for_output,
                    ),
                    exception_class="JsonRpcError",
                ),
            )
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


def extract_solana_rpc_diagnostics(exc: Exception) -> dict[str, str | None]:
    if isinstance(exc, SolanaRpcRequestError):
        diagnostics = exc.diagnostics
        return {
            "provider": diagnostics.provider,
            "rpc_url": diagnostics.rpc_url,
            "rpc_method": diagnostics.rpc_method,
            "failure_category": diagnostics.failure_category,
            "provider_status": diagnostics.provider_status,
            "response_snippet": diagnostics.response_snippet,
            "exception_class": diagnostics.exception_class,
        }
    return {
        "provider": SOLANA_PROVIDER_NAME,
        "rpc_url": None,
        "rpc_method": None,
        "failure_category": "unexpected_error",
        "provider_status": None,
        "response_snippet": sanitize_text_for_output(str(exc)) or None,
        "exception_class": exc.__class__.__name__,
    }


def _read_http_error_snippet(
    exc: HTTPError,
    *,
    rpc_url: str,
    rpc_url_for_output: str,
) -> str | None:
    try:
        raw_body = exc.read().decode("utf-8", errors="replace")
    except Exception:  # pragma: no cover - defensive fallback
        return None
    return _sanitize_snippet(
        raw_body,
        rpc_url=rpc_url,
        rpc_url_for_output=rpc_url_for_output,
    )


def _sanitize_snippet(
    value: str,
    *,
    rpc_url: str,
    rpc_url_for_output: str,
    limit: int = 240,
) -> str | None:
    sanitized = sanitize_text_for_output(value.replace(rpc_url, rpc_url_for_output))
    if not sanitized:
        return None
    if len(sanitized) > limit:
        return f"{sanitized[:limit].rstrip()}..."
    return sanitized
