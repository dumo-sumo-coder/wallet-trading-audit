"""Centralized environment configuration for local ingestion tooling."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DOTENV_PATH = REPOSITORY_ROOT / ".env"
DEFAULT_ENV = "dev"
DEFAULT_PUBLIC_SOLANA_RPC_URL = "https://api.mainnet-beta.solana.com"
HELIUS_MAINNET_RPC_TEMPLATE = "https://mainnet.helius-rpc.com/?api-key={api_key}"

load_dotenv(DOTENV_PATH, override=False)


def get_env() -> str:
    """Return the current application environment."""

    value = _get_optional_env_var("ENV")
    if value is None:
        return DEFAULT_ENV
    return value


def get_helius_api_key(*, required: bool = False) -> str | None:
    """Return the configured Helius API key."""

    return _get_env_var(
        "HELIUS_API_KEY",
        required=required,
        required_message=(
            "HELIUS_API_KEY is required for Solana ingestion when SOLANA_RPC_URL "
            "is not set. Copy .env.example to .env and add one of those values."
        ),
    )


def get_solana_rpc_url(*, required: bool = False) -> str:
    """Return the Solana RPC URL from explicit config or a Helius key."""

    configured_url = _get_optional_env_var("SOLANA_RPC_URL")
    if configured_url is not None:
        return configured_url

    helius_api_key = get_helius_api_key(required=False)
    if helius_api_key is not None:
        return HELIUS_MAINNET_RPC_TEMPLATE.format(api_key=helius_api_key)

    if required:
        raise ValueError(
            "Solana ingestion requires HELIUS_API_KEY or SOLANA_RPC_URL. "
            "Copy .env.example to .env and configure one of them."
        )
    return DEFAULT_PUBLIC_SOLANA_RPC_URL


def get_etherscan_api_key(*, required: bool = False) -> str | None:
    """Return the configured Etherscan V2 API key."""

    return _get_env_var(
        "ETHERSCAN_API_KEY",
        required=required,
        required_message=(
            "ETHERSCAN_API_KEY is required for the current BNB/EVM ingestion flow. "
            "Copy .env.example to .env and set ETHERSCAN_API_KEY."
        ),
    )


def get_evm_rpc_url() -> str | None:
    """Return an optional direct EVM RPC URL placeholder for future use."""

    return _get_optional_env_var("EVM_RPC_URL")


def _get_env_var(
    name: str,
    *,
    required: bool,
    required_message: str,
) -> str | None:
    value = _get_optional_env_var(name)
    if value is None and required:
        raise ValueError(required_message)
    return value


def _get_optional_env_var(name: str) -> str | None:
    raw_value = os.getenv(name)
    if raw_value is None:
        return None
    value = raw_value.strip()
    if not value:
        raise ValueError(f"{name} is set but blank. Update .env with a non-empty value.")
    return value
