"""Deterministic pipeline from normalized transactions into FIFO results."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Sequence

from normalize.schema import NormalizedTransaction

from .fifo import InventoryLot
from .fifo_engine import FifoEngine, FifoEngineResult

ZERO = Decimal("0")


@dataclass(frozen=True, slots=True)
class RemainingPosition:
    """Aggregated remaining inventory for one wallet and token."""

    wallet: str
    token_address: str
    quantity_open: Decimal
    cost_basis_usd: Decimal | None


@dataclass(frozen=True, slots=True)
class FifoPipelineResult:
    """Pipeline output for normalized-to-FIFO reconstruction."""

    realized_pnl_usd: Decimal
    remaining_positions: tuple[RemainingPosition, ...]
    fifo_result: FifoEngineResult


def run_fifo_pipeline(
    transactions: Sequence[NormalizedTransaction],
) -> FifoPipelineResult:
    """Run the first-pass FIFO engine on normalized transactions."""

    fifo_result = FifoEngine().reconstruct(transactions)
    realized_pnl_usd = sum(
        (
            trade_match.realized_pnl_usd or ZERO
            for trade_match in fifo_result.trade_matches
        ),
        ZERO,
    )
    remaining_positions = _aggregate_remaining_positions(fifo_result.open_lots)

    return FifoPipelineResult(
        realized_pnl_usd=realized_pnl_usd,
        remaining_positions=remaining_positions,
        fifo_result=fifo_result,
    )


def _aggregate_remaining_positions(
    open_lots: Sequence[InventoryLot],
) -> tuple[RemainingPosition, ...]:
    grouped_positions: dict[tuple[str, str], RemainingPosition] = {}

    for lot in sorted(
        open_lots,
        key=lambda item: (item.wallet, item.token_address, item.acquired_at, item.source_tx_hash),
    ):
        position_key = (lot.wallet, lot.token_address)
        lot_cost_basis = _lot_cost_basis_usd(lot)
        existing_position = grouped_positions.get(position_key)
        if existing_position is None:
            grouped_positions[position_key] = RemainingPosition(
                wallet=lot.wallet,
                token_address=lot.token_address,
                quantity_open=lot.quantity_open,
                cost_basis_usd=lot_cost_basis,
            )
            continue

        grouped_positions[position_key] = RemainingPosition(
            wallet=existing_position.wallet,
            token_address=existing_position.token_address,
            quantity_open=existing_position.quantity_open + lot.quantity_open,
            cost_basis_usd=_sum_cost_basis(
                existing_position.cost_basis_usd,
                lot_cost_basis,
            ),
        )

    return tuple(grouped_positions.values())


def _lot_cost_basis_usd(lot: InventoryLot) -> Decimal | None:
    if lot.unit_cost_usd is None:
        return None
    return lot.unit_cost_usd * lot.quantity_open


def _sum_cost_basis(
    left: Decimal | None,
    right: Decimal | None,
) -> Decimal | None:
    if left is None or right is None:
        return None
    return left + right
