"""Base interfaces for raw transaction ingestion."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from normalize.schema import Chain


@dataclass(frozen=True, slots=True)
class IngestionWindow:
    """Optional time window for wallet transaction capture."""

    start_time: datetime | None = None
    end_time: datetime | None = None


class RawTransactionSource(ABC):
    """Contract for fetching wallet history and storing verbatim raw payloads.

    Implementations should preserve provider-native records under `data/raw/`
    before any normalization occurs.
    """

    chain: Chain

    @abstractmethod
    def fetch_wallet_transactions(
        self,
        wallet: str,
        window: IngestionWindow | None = None,
    ) -> Iterable[Mapping[str, Any]]:
        """Yield provider-native raw transaction payloads for a wallet."""

    def raw_storage_path(self, repository_root: Path) -> Path:
        """Return the chain-specific raw storage directory within this repository."""

        return repository_root / "data" / "raw" / self.chain.value
