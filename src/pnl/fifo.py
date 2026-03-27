"""FIFO trade reconstruction placeholders."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Sequence

from normalize.schema import NormalizedTransaction


@dataclass(frozen=True, slots=True)
class InventoryLot:
    """Open inventory lot tracked by contract or mint address."""

    wallet: str
    token_address: str
    acquired_at: datetime
    quantity_open: Decimal
    unit_cost_usd: Decimal | None
    source_tx_hash: str


@dataclass(frozen=True, slots=True)
class TradeMatch:
    """Closed FIFO match between one entry lot and one exit event."""

    wallet: str
    token_address: str
    entry_tx_hash: str
    exit_tx_hash: str
    entry_time: datetime
    exit_time: datetime
    quantity: Decimal
    cost_basis_usd: Decimal | None
    proceeds_usd: Decimal | None
    realized_pnl_usd: Decimal | None


class FifoTradeReconstructor:
    """Planned FIFO engine for deterministic trade reconstruction.

    Expected high-level behavior:
    - sort normalized transactions chronologically
    - open lots on net token acquisitions
    - close lots on disposals using first-in, first-out matching
    - produce explicit trade matches for audit review

    TODO:
    - Decide how to break ties when multiple normalized rows share the same
      `block_time` and `tx_hash`.
    - Define how fees are allocated across partially closed lots.
    - Add support for transfers that move inventory without realizing PnL.
    """

    def reconstruct(
        self,
        transactions: Sequence[NormalizedTransaction],
    ) -> list[TradeMatch]:
        """Convert normalized transactions into FIFO trade matches."""

        raise NotImplementedError(
            "FIFO trade reconstruction is intentionally not implemented yet."
        )
