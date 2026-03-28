"""Minimal BNB/EVM raw-ingestion client.

This client intentionally stores JSON API response bodies only. It does not
normalize transaction fields and it does not preserve HTTP headers.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from config import get_etherscan_api_key

DEFAULT_ETHERSCAN_API_URL = "https://api.etherscan.io/v2/api"
DEFAULT_BNB_CHAIN_ID = "56"


class EvmWalletClient:
    """Fetch recent raw BNB/EVM wallet transaction data from Etherscan V2.

    The default configuration targets BNB Smart Chain mainnet through
    `chain_id="56"` on `api.etherscan.io/v2/api`.

    Important:
    - This is an Etherscan V2 client, not a dedicated BscScan client.
    - For the default BNB mainnet configuration, Etherscan currently documents
      chain `56` as paid-tier access rather than free-tier access.
    - Callers may override `api_url`, `chain_id`, and `api_key` if they have a
      different Etherscan-compatible source and have verified compatibility.
    """

    # TODO: Add pagination across additional pages when wallet history exceeds
    # a single `page`/`offset` slice.
    # TODO: Handle provider rate limiting and retry semantics explicitly
    # rather than surfacing HTTP failures directly.
    # TODO: Revisit whether BNB mainnet should prefer a dedicated primary source
    # such as BscScan or direct chain RPC/log capture instead of Etherscan V2.
    # TODO: Decide whether raw ingestion also needs NFT, token1155, receipt,
    # or log endpoints before calling this a complete wallet snapshot.

    def __init__(
        self,
        *,
        api_key: str | None = None,
        api_url: str = DEFAULT_ETHERSCAN_API_URL,
        chain_id: str = DEFAULT_BNB_CHAIN_ID,
        timeout_seconds: int = 30,
    ) -> None:
        self.api_key = api_key or get_etherscan_api_key(required=False)
        self.api_url = api_url
        self.chain_id = chain_id
        self.timeout_seconds = timeout_seconds

    def fetch_recent_transaction_history(
        self,
        wallet: str,
        *,
        page: int = 1,
        offset: int = 20,
        sort: str = "desc",
    ) -> dict[str, object]:
        """Fetch recent BNB/EVM transaction payloads for one wallet."""

        wallet_text = wallet.strip()
        if not wallet_text:
            raise ValueError("wallet must be a non-empty string")
        if page <= 0:
            raise ValueError("page must be positive")
        if offset <= 0:
            raise ValueError("offset must be positive")
        if sort not in {"asc", "desc"}:
            raise ValueError("sort must be 'asc' or 'desc'")
        if not self.api_key:
            raise ValueError(
                "EvmWalletClient requires an explicit Etherscan V2 API key via "
                "`api_key` or `ETHERSCAN_API_KEY`. `BSCSCAN_API_KEY` is not "
                "implicitly assumed to work with api.etherscan.io/v2/api."
            )

        base_params = {
            "address": wallet_text,
            "page": str(page),
            "offset": str(offset),
            "sort": sort,
            "startblock": "0",
            "endblock": "9999999999",
        }

        normal_transactions = self._api_get(
            action="txlist",
            **base_params,
        )
        internal_transactions = self._api_get(
            action="txlistinternal",
            **base_params,
        )
        erc20_transfers = self._api_get(
            action="tokentx",
            **base_params,
        )

        fetched_at = datetime.now(UTC).isoformat()
        return {
            "wallet": wallet_text,
            "fetched_at_utc": fetched_at,
            "source": {
                "provider": "etherscan_v2_multichain",
                "api_url": self.api_url,
                "chain_id": self.chain_id,
            },
            "capture": {
                "normalization_applied": False,
                "response_body_format": "json",
                "response_bodies_preserved": True,
                "http_headers_preserved": False,
                "provider_access_note": _provider_access_note(
                    api_url=self.api_url,
                    chain_id=self.chain_id,
                ),
            },
            "request": {
                "provider_family": "etherscan_v2",
                "module": "account",
                "actions": ["txlist", "txlistinternal", "tokentx"],
                "chain_id": self.chain_id,
                "startblock": "0",
                "endblock": "9999999999",
                "page": page,
                "offset": offset,
                "sort": sort,
            },
            "responses": {
                "normal_transactions": normal_transactions,
                "internal_transactions": internal_transactions,
                "erc20_transfers": erc20_transfers,
            },
        }

    def save_recent_transaction_history(
        self,
        wallet: str,
        repository_root: Path,
        *,
        page: int = 1,
        offset: int = 20,
        sort: str = "desc",
    ) -> Path:
        """Fetch and save a raw BNB/EVM snapshot under data/raw/evm/."""

        snapshot = self.fetch_recent_transaction_history(
            wallet,
            page=page,
            offset=offset,
            sort=sort,
        )
        storage_dir = repository_root / "data" / "raw" / "evm"
        storage_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        filename = f"{_safe_path_component(wallet)}_{timestamp}.json"
        snapshot_path = storage_dir / filename
        snapshot_path.write_text(
            json.dumps(snapshot, indent=2),
            encoding="utf-8",
        )
        return snapshot_path

    def _api_get(self, *, action: str, **params: str) -> dict[str, Any]:
        query = urlencode(
            {
                "chainid": self.chain_id,
                "module": "account",
                "action": action,
                "apikey": self.api_key,
                **params,
            }
        )
        request = Request(
            f"{self.api_url}?{query}",
            headers={"User-Agent": "wallet-trading-audit/0.1"},
            method="GET",
        )

        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                raw_body = response.read().decode("utf-8")
        except HTTPError as exc:
            raise RuntimeError(
                f"EVM API request failed with HTTP {exc.code} for action {action}"
            ) from exc
        except URLError as exc:
            raise RuntimeError(f"EVM API request failed for action {action}") from exc

        try:
            parsed = json.loads(raw_body)
        except json.JSONDecodeError as exc:
            raise ValueError(f"EVM API returned invalid JSON for action {action}") from exc
        if not isinstance(parsed, dict):
            raise ValueError(f"EVM API returned a non-object payload for action {action}")

        status = parsed.get("status")
        message = parsed.get("message")
        result = parsed.get("result")
        if status == "0" and message == "NOTOK":
            raise RuntimeError(f"EVM API returned an error for action {action}: {result}")
        return parsed


def _safe_path_component(value: str) -> str:
    cleaned = "".join(
        character if character.isalnum() or character in {"-", "_"} else "_"
        for character in value.strip()
    )
    return cleaned or "wallet"


def _provider_access_note(*, api_url: str, chain_id: str) -> str | None:
    if api_url == DEFAULT_ETHERSCAN_API_URL and chain_id == DEFAULT_BNB_CHAIN_ID:
        return (
            "Assumes access to Etherscan V2 support for BNB Smart Chain mainnet "
            "(chain_id 56), which Etherscan currently documents as paid-tier access."
        )
    return None
