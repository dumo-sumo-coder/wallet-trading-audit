"""Minimal FIFO engine for simple realized PnL reconstruction."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import DefaultDict, Sequence

from normalize.schema import EventType, NormalizedTransaction

from .fifo import InventoryLot, TradeMatch

ZERO = Decimal("0")


@dataclass(frozen=True, slots=True)
class RecordedFee:
    """Explicit fee record kept separate from realized trade proceeds."""

    wallet: str
    tx_hash: str
    block_time: datetime
    event_type: EventType
    fee_native: Decimal
    fee_usd: Decimal | None
    source: str | None


@dataclass(frozen=True, slots=True)
class FifoEngineResult:
    """Deterministic FIFO reconstruction outputs."""

    trade_matches: tuple[TradeMatch, ...]
    open_lots: tuple[InventoryLot, ...]
    ignored_transfers: tuple[NormalizedTransaction, ...]
    recorded_fees: tuple[RecordedFee, ...]


@dataclass(slots=True)
class _OpenLot:
    wallet: str
    token_address: str
    acquired_at: datetime
    quantity_open: Decimal
    unit_cost_usd: Decimal
    source_tx_hash: str


class FifoEngine:
    """First-pass FIFO engine for simple completed trades.

    Supported behavior:
    - open one inventory lot for an acquisition row with trusted `usd_value`
    - close existing lots in FIFO order for disposals with trusted `usd_value`
    - keep transfer rows out of realized PnL by default
    - keep fees explicit and separate from trade proceeds

    TODO:
    - Model transfer rows as non-realizing inventory moves once wallet-to-wallet
      linkage rules are defined.
    - Support token-to-token rotations that both close one asset and open the
      acquired asset in the same normalized row.
    - Add a dedicated rounding policy if downstream reports require quantized
      currency outputs instead of exact `Decimal` math.
    """

    def reconstruct(
        self,
        transactions: Sequence[NormalizedTransaction],
    ) -> FifoEngineResult:
        ordered_transactions = [
            transaction
            for _, transaction in sorted(
                enumerate(transactions),
                key=lambda item: (
                    item[1].block_time,
                    item[1].tx_hash,
                    item[0],
                ),
            )
        ]

        lots_by_wallet_token: DefaultDict[tuple[str, str], list[_OpenLot]] = defaultdict(list)
        trade_matches: list[TradeMatch] = []
        ignored_transfers: list[NormalizedTransaction] = []
        recorded_fees: list[RecordedFee] = []

        for transaction in ordered_transactions:
            self._record_fee(transaction, recorded_fees)

            if transaction.event_type == EventType.FEE:
                continue

            if transaction.event_type == EventType.TRANSFER:
                ignored_transfers.append(transaction)
                continue

            if transaction.event_type != EventType.SWAP:
                raise ValueError(
                    f"Unsupported event_type for FIFO engine: {transaction.event_type.value}"
                )

            swap_action = self._classify_swap(
                transaction,
                lots_by_wallet_token,
            )
            if swap_action == "close":
                trade_matches.extend(
                    self._close_lots(transaction, lots_by_wallet_token)
                )
                continue

            self._open_lot(transaction, lots_by_wallet_token)

        open_lots = tuple(
            InventoryLot(
                wallet=lot.wallet,
                token_address=lot.token_address,
                acquired_at=lot.acquired_at,
                quantity_open=lot.quantity_open,
                unit_cost_usd=lot.unit_cost_usd,
                source_tx_hash=lot.source_tx_hash,
            )
            for _, lots in sorted(lots_by_wallet_token.items())
            for lot in lots
            if lot.quantity_open > ZERO
        )

        return FifoEngineResult(
            trade_matches=tuple(trade_matches),
            open_lots=open_lots,
            ignored_transfers=tuple(ignored_transfers),
            recorded_fees=tuple(recorded_fees),
        )

    def _record_fee(
        self,
        transaction: NormalizedTransaction,
        recorded_fees: list[RecordedFee],
    ) -> None:
        if transaction.fee_native == ZERO and transaction.fee_usd is None:
            return

        recorded_fees.append(
            RecordedFee(
                wallet=transaction.wallet,
                tx_hash=transaction.tx_hash,
                block_time=transaction.block_time,
                event_type=transaction.event_type,
                fee_native=transaction.fee_native,
                fee_usd=transaction.fee_usd,
                source=transaction.source,
            )
        )

    def _classify_swap(
        self,
        transaction: NormalizedTransaction,
        lots_by_wallet_token: DefaultDict[tuple[str, str], list[_OpenLot]],
    ) -> str:
        has_inflow = transaction.token_in_address is not None and transaction.amount_in > ZERO
        has_outflow = (
            transaction.token_out_address is not None and transaction.amount_out > ZERO
        )

        if has_outflow and not has_inflow:
            return "close"
        if has_inflow and not has_outflow:
            return "open"
        if not has_inflow and not has_outflow:
            raise ValueError(
                "Swap rows must carry a positive amount_in or amount_out: "
                f"{transaction.tx_hash}"
            )

        inventory_key = (transaction.wallet, transaction.token_out_address)
        if any(lot.quantity_open > ZERO for lot in lots_by_wallet_token[inventory_key]):
            return "close"

        return "open"

    def _open_lot(
        self,
        transaction: NormalizedTransaction,
        lots_by_wallet_token: DefaultDict[tuple[str, str], list[_OpenLot]],
    ) -> None:
        if transaction.token_in_address is None or transaction.amount_in == ZERO:
            raise ValueError(
                f"Cannot open FIFO inventory without token_in_address and amount_in: "
                f"{transaction.tx_hash}"
            )
        if transaction.usd_value is None:
            raise ValueError(
                "Simple FIFO engine requires usd_value for acquisition cost basis: "
                f"{transaction.tx_hash}"
            )

        inventory_key = (transaction.wallet, transaction.token_in_address)
        lots_by_wallet_token[inventory_key].append(
            _OpenLot(
                wallet=transaction.wallet,
                token_address=transaction.token_in_address,
                acquired_at=transaction.block_time,
                quantity_open=transaction.amount_in,
                unit_cost_usd=transaction.usd_value / transaction.amount_in,
                source_tx_hash=transaction.tx_hash,
            )
        )

    def _close_lots(
        self,
        transaction: NormalizedTransaction,
        lots_by_wallet_token: DefaultDict[tuple[str, str], list[_OpenLot]],
    ) -> list[TradeMatch]:
        if transaction.token_out_address is None or transaction.amount_out == ZERO:
            raise ValueError(
                f"Cannot close FIFO inventory without token_out_address and amount_out: "
                f"{transaction.tx_hash}"
            )
        if transaction.usd_value is None:
            raise ValueError(
                "Simple FIFO engine requires usd_value for disposal proceeds: "
                f"{transaction.tx_hash}"
            )

        inventory_key = (transaction.wallet, transaction.token_out_address)
        open_lots = lots_by_wallet_token[inventory_key]
        remaining_quantity = transaction.amount_out
        unit_proceeds_usd = transaction.usd_value / transaction.amount_out
        matches: list[TradeMatch] = []

        while remaining_quantity > ZERO:
            if not open_lots:
                raise ValueError(
                    "Insufficient inventory for FIFO disposal: "
                    f"{transaction.wallet} {transaction.token_out_address} {transaction.tx_hash}"
                )

            lot = open_lots[0]
            matched_quantity = min(lot.quantity_open, remaining_quantity)
            cost_basis_usd = lot.unit_cost_usd * matched_quantity
            proceeds_usd = unit_proceeds_usd * matched_quantity

            matches.append(
                TradeMatch(
                    wallet=transaction.wallet,
                    token_address=transaction.token_out_address,
                    entry_tx_hash=lot.source_tx_hash,
                    exit_tx_hash=transaction.tx_hash,
                    entry_time=lot.acquired_at,
                    exit_time=transaction.block_time,
                    quantity=matched_quantity,
                    cost_basis_usd=cost_basis_usd,
                    proceeds_usd=proceeds_usd,
                    realized_pnl_usd=proceeds_usd - cost_basis_usd,
                )
            )

            lot.quantity_open -= matched_quantity
            remaining_quantity -= matched_quantity

            if lot.quantity_open == ZERO:
                open_lots.pop(0)

        return matches
