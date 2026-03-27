"""Interfaces for converting raw provider payloads into canonical records."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Mapping

from .schema import Chain, NormalizedTransaction


@dataclass(frozen=True, slots=True)
class RawTransactionRecord:
    """Opaque raw record envelope captured before normalization."""

    chain: Chain
    wallet: str
    payload: Mapping[str, Any]
    observed_at: datetime
    source_name: str | None = None


class TransactionNormalizer(ABC):
    """Contract for deterministic normalization into the canonical schema."""

    @abstractmethod
    def normalize(
        self,
        record: RawTransactionRecord,
    ) -> Iterable[NormalizedTransaction]:
        """Yield canonical records for one raw provider payload.

        TODO:
        - Define exact field mappings only after representative Solana and BNB
          EVM raw payload fixtures are captured under `data/raw/`.
        - Add deterministic intra-transaction ordering if one raw transaction
          expands into multiple canonical rows.
        """
