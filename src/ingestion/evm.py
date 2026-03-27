"""BNB EVM ingestion placeholders."""

from __future__ import annotations

from typing import Any, Iterable, Mapping

from .base import IngestionWindow, RawTransactionSource
from normalize.schema import Chain


class BnbEvmWalletSource(RawTransactionSource):
    """Placeholder for BNB EVM wallet history ingestion.

    TODO:
    - Choose the authoritative BNB EVM source for transactions, receipts, and
      log or trace context.
    - Preserve raw payloads before decoding protocol-specific behavior.
    - Lock down exact provider fields only after sample payloads are saved.
    """

    chain = Chain.BNB_EVM

    def fetch_wallet_transactions(
        self,
        wallet: str,
        window: IngestionWindow | None = None,
    ) -> Iterable[Mapping[str, Any]]:
        raise NotImplementedError(
            "BNB EVM ingestion is not implemented yet. TODO: choose a real data "
            "source and map only verified raw payload structures."
        )
