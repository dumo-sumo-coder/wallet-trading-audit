"""Tests for centralized environment configuration."""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from config import (  # noqa: E402
    get_env,
    get_etherscan_api_key,
    get_helius_api_key,
    get_solana_rpc_url,
)
from ingestion.evm_client import EvmWalletClient  # noqa: E402
from ingestion.solana_client import SolanaRpcClient  # noqa: E402


class ConfigTests(unittest.TestCase):
    def test_get_env_defaults_to_dev(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(get_env(), "dev")

    def test_get_env_returns_configured_value(self) -> None:
        with patch.dict(os.environ, {"ENV": "prod"}, clear=True):
            self.assertEqual(get_env(), "prod")

    def test_get_helius_api_key_raises_clear_error_when_required(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "HELIUS_API_KEY"):
                get_helius_api_key(required=True)

    def test_get_solana_rpc_url_prefers_explicit_override(self) -> None:
        with patch.dict(
            os.environ,
            {
                "HELIUS_API_KEY": "helius-key",
                "SOLANA_RPC_URL": "https://custom.solana.example/rpc",
            },
            clear=True,
        ):
            self.assertEqual(get_solana_rpc_url(required=True), "https://custom.solana.example/rpc")

    def test_get_solana_rpc_url_derives_helius_endpoint(self) -> None:
        with patch.dict(os.environ, {"HELIUS_API_KEY": "helius-key"}, clear=True):
            self.assertEqual(
                get_solana_rpc_url(required=True),
                "https://mainnet.helius-rpc.com/?api-key=helius-key",
            )

    def test_solona_client_fails_fast_when_env_config_is_missing(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "SOLANA_RPC_URL"):
                SolanaRpcClient()

    def test_solana_client_uses_env_derived_rpc_url(self) -> None:
        with patch.dict(os.environ, {"HELIUS_API_KEY": "helius-key"}, clear=True):
            client = SolanaRpcClient()

        self.assertEqual(client.rpc_url, "https://mainnet.helius-rpc.com/?api-key=helius-key")

    def test_evm_client_uses_centralized_etherscan_api_key(self) -> None:
        with patch.dict(os.environ, {"ETHERSCAN_API_KEY": "etherscan-key"}, clear=True):
            client = EvmWalletClient()

        self.assertEqual(client.api_key, "etherscan-key")

    def test_blank_secret_values_raise_clear_errors(self) -> None:
        with patch.dict(os.environ, {"HELIUS_API_KEY": "   "}, clear=True):
            with self.assertRaisesRegex(ValueError, "set but blank"):
                get_helius_api_key(required=False)

        with patch.dict(os.environ, {"ETHERSCAN_API_KEY": ""}, clear=True):
            with self.assertRaisesRegex(ValueError, "set but blank"):
                get_etherscan_api_key(required=False)


if __name__ == "__main__":
    unittest.main()
