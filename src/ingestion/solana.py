"""Solana ingestion placeholders."""

from __future__ import annotations

from typing import Any, Iterable, Mapping

from .base import IngestionWindow, RawTransactionSource
from normalize.schema import Chain


class SolanaWalletSource(RawTransactionSource):
    """Placeholder for Solana wallet history ingestion.

    TODO:
    - Choose the authoritative Solana source for raw transaction history.
    - Preserve raw transaction, balance-diff, and instruction context exactly as
      delivered by that source.
    - Define deterministic fixture coverage before implementing field mapping.
    """

    chain = Chain.SOLANA

    def fetch_wallet_transactions(
        self,
        wallet: str,
        window: IngestionWindow | None = None,
    ) -> Iterable[Mapping[str, Any]]:
        raise NotImplementedError(
            "Solana ingestion is not implemented yet. TODO: choose a real data "
            "source and map only verified raw payload structures."
        )
