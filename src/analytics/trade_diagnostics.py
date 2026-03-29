"""Trade-level diagnostics built from FIFO trade matches."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Sequence

from pnl.fifo_engine import FifoEngineResult, RecordedFee

ZERO = Decimal("0")


@dataclass(frozen=True, slots=True)
class MatchedTradeDiagnostic:
    token_address: str
    opening_tx_hash: str
    closing_tx_hash: str
    open_timestamp: datetime
    close_timestamp: datetime
    holding_duration_seconds: int
    quantity_matched: Decimal
    cost_basis_usd: Decimal | None
    proceeds_usd: Decimal | None
    realized_pnl_usd: Decimal | None
    opening_fee_native: Decimal | None
    opening_fee_usd: Decimal | None
    closing_fee_native: Decimal | None
    closing_fee_usd: Decimal | None


@dataclass(frozen=True, slots=True)
class TokenPnlDiagnostic:
    token_address: str
    matched_trades: int
    realized_pnl_usd: Decimal


@dataclass(frozen=True, slots=True)
class TradeDiagnosticSummary:
    total_matched_trades: int
    winners_count: int
    losers_count: int
    avg_winner_usd: Decimal | None
    avg_loser_usd: Decimal | None
    largest_win_usd: Decimal | None
    largest_loss_usd: Decimal | None
    pnl_by_token: tuple[TokenPnlDiagnostic, ...]


@dataclass(frozen=True, slots=True)
class TradeDiagnosticReport:
    matched_trades: tuple[MatchedTradeDiagnostic, ...]
    summary: TradeDiagnosticSummary


@dataclass(frozen=True, slots=True)
class _FeeTotals:
    fee_native: Decimal
    fee_usd: Decimal | None


def build_trade_diagnostic_report(
    fifo_result: FifoEngineResult,
) -> TradeDiagnosticReport:
    fee_totals_by_tx_hash = _aggregate_fees_by_tx_hash(fifo_result.recorded_fees)
    matched_trades: list[MatchedTradeDiagnostic] = []

    for trade_match in fifo_result.trade_matches:
        opening_fee = fee_totals_by_tx_hash.get(trade_match.entry_tx_hash)
        closing_fee = fee_totals_by_tx_hash.get(trade_match.exit_tx_hash)
        holding_duration_seconds = int(
            (trade_match.exit_time - trade_match.entry_time).total_seconds()
        )
        if holding_duration_seconds < 0:
            raise ValueError("Trade match exit_time cannot be earlier than entry_time")

        matched_trades.append(
            MatchedTradeDiagnostic(
                token_address=trade_match.token_address,
                opening_tx_hash=trade_match.entry_tx_hash,
                closing_tx_hash=trade_match.exit_tx_hash,
                open_timestamp=trade_match.entry_time,
                close_timestamp=trade_match.exit_time,
                holding_duration_seconds=holding_duration_seconds,
                quantity_matched=trade_match.quantity,
                cost_basis_usd=trade_match.cost_basis_usd,
                proceeds_usd=trade_match.proceeds_usd,
                realized_pnl_usd=trade_match.realized_pnl_usd,
                opening_fee_native=opening_fee.fee_native if opening_fee is not None else None,
                opening_fee_usd=opening_fee.fee_usd if opening_fee is not None else None,
                closing_fee_native=closing_fee.fee_native if closing_fee is not None else None,
                closing_fee_usd=closing_fee.fee_usd if closing_fee is not None else None,
            )
        )

    matched_trade_rows = tuple(matched_trades)
    return TradeDiagnosticReport(
        matched_trades=matched_trade_rows,
        summary=summarize_trade_diagnostic_report(matched_trade_rows),
    )


def summarize_trade_diagnostic_report(
    matched_trades: Sequence[MatchedTradeDiagnostic],
) -> TradeDiagnosticSummary:
    if not matched_trades:
        return TradeDiagnosticSummary(
            total_matched_trades=0,
            winners_count=0,
            losers_count=0,
            avg_winner_usd=None,
            avg_loser_usd=None,
            largest_win_usd=None,
            largest_loss_usd=None,
            pnl_by_token=(),
        )

    realized_pnls = [
        trade.realized_pnl_usd
        for trade in matched_trades
        if trade.realized_pnl_usd is not None
    ]
    winning_pnls = [pnl for pnl in realized_pnls if pnl > ZERO]
    losing_pnls = [pnl for pnl in realized_pnls if pnl < ZERO]

    return TradeDiagnosticSummary(
        total_matched_trades=len(matched_trades),
        winners_count=len(winning_pnls),
        losers_count=len(losing_pnls),
        avg_winner_usd=_mean_decimal(winning_pnls),
        avg_loser_usd=_mean_decimal(losing_pnls),
        largest_win_usd=max(winning_pnls) if winning_pnls else None,
        largest_loss_usd=min(losing_pnls) if losing_pnls else None,
        pnl_by_token=_summarize_pnl_by_token(matched_trades),
    )


def _aggregate_fees_by_tx_hash(
    recorded_fees: Sequence[RecordedFee],
) -> dict[str, _FeeTotals]:
    fee_native_by_tx_hash: dict[str, Decimal] = defaultdict(lambda: ZERO)
    fee_usd_by_tx_hash: dict[str, Decimal | None] = {}

    for fee in recorded_fees:
        fee_native_by_tx_hash[fee.tx_hash] += fee.fee_native
        existing_fee_usd = fee_usd_by_tx_hash.get(fee.tx_hash)
        if fee.fee_usd is None or existing_fee_usd is None:
            fee_usd_by_tx_hash.setdefault(fee.tx_hash, fee.fee_usd)
            if existing_fee_usd is not None and fee.fee_usd is not None:
                fee_usd_by_tx_hash[fee.tx_hash] = existing_fee_usd + fee.fee_usd
            continue
        fee_usd_by_tx_hash[fee.tx_hash] = existing_fee_usd + fee.fee_usd

    return {
        tx_hash: _FeeTotals(
            fee_native=fee_native,
            fee_usd=fee_usd_by_tx_hash.get(tx_hash),
        )
        for tx_hash, fee_native in fee_native_by_tx_hash.items()
    }


def _summarize_pnl_by_token(
    matched_trades: Sequence[MatchedTradeDiagnostic],
) -> tuple[TokenPnlDiagnostic, ...]:
    grouped: dict[str, list[Decimal]] = defaultdict(list)
    for trade in matched_trades:
        if trade.realized_pnl_usd is None:
            continue
        grouped[trade.token_address].append(trade.realized_pnl_usd)

    return tuple(
        TokenPnlDiagnostic(
            token_address=token_address,
            matched_trades=len(realized_pnls),
            realized_pnl_usd=sum(realized_pnls, ZERO),
        )
        for token_address, realized_pnls in sorted(
            grouped.items(),
            key=lambda item: (sum(item[1], ZERO), item[0]),
        )
    )


def _mean_decimal(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, ZERO) / Decimal(len(values))
