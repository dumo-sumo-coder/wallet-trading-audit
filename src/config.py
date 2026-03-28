"""Centralized environment configuration for local ingestion tooling."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from dotenv import load_dotenv

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DOTENV_PATH = REPOSITORY_ROOT / ".env"
DEFAULT_ENV = "dev"
DEFAULT_PUBLIC_SOLANA_RPC_URL = "https://api.mainnet-beta.solana.com"
HELIUS_MAINNET_RPC_TEMPLATE = "https://mainnet.helius-rpc.com/?api-key={api_key}"

load_dotenv(DOTENV_PATH, override=False)


def get_manual_env_load_instructions() -> str:
    """Return the recommended manual .env load command."""

    return "set -a; source .env; set +a"


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
        required_message=_missing_env_message(
            "HELIUS_API_KEY",
            purpose_text=(
                "Solana ingestion requires HELIUS_API_KEY unless you pass an explicit "
                "SOLANA_RPC_URL override."
            ),
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
        raise ValueError(_missing_solana_config_message())
    return DEFAULT_PUBLIC_SOLANA_RPC_URL


def get_etherscan_api_key(*, required: bool = False) -> str | None:
    """Return the configured Etherscan V2 API key."""

    return _get_env_var(
        "ETHERSCAN_API_KEY",
        required=required,
        required_message=_missing_env_message(
            "ETHERSCAN_API_KEY",
            purpose_text="The current BNB/EVM ingestion flow requires ETHERSCAN_API_KEY.",
        ),
    )


def get_evm_rpc_url() -> str | None:
    """Return an optional direct EVM RPC URL placeholder for future use."""

    return _get_optional_env_var("EVM_RPC_URL")


def get_env_var_status(name: str) -> str:
    """Return a secret-safe status string for one environment variable."""

    return "present" if _get_optional_env_var(name) is not None else "missing"


def build_missing_env_message(name: str, *, purpose_text: str) -> str:
    """Build a consistent missing-env error without exposing secret values."""

    return _missing_env_message(name, purpose_text=purpose_text)


def sanitize_url_for_output(value: str) -> str:
    """Redact query strings and fragments before printing or persisting a URL."""

    parsed = urlsplit(value)
    if not parsed.scheme or not parsed.netloc:
        return "[configured_rpc_url]"
    sanitized_query = "redacted" if parsed.query else ""
    sanitized_fragment = "redacted" if parsed.fragment else ""
    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            sanitized_query,
            sanitized_fragment,
        )
    )


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
        return None
    return value


def _missing_env_message(name: str, *, purpose_text: str) -> str:
    return (
        f"{name} is missing or blank. {purpose_text} Create a local .env from "
        ".env.example, or if you prefer manual terminal loading, run "
        f"'{get_manual_env_load_instructions()}' in this shell before the command."
    )


def _missing_solana_config_message() -> str:
    return (
        "Solana ingestion requires HELIUS_API_KEY or SOLANA_RPC_URL. Create a local "
        ".env from .env.example, or if you prefer manual terminal loading, run "
        f"'{get_manual_env_load_instructions()}' in this shell before the command."
    )
