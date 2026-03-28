"""Portfolio-level analysis that combines closed-trade and open-position views."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Sequence

from normalize.schema import EventType, NormalizedTransaction
from pnl.pipeline import FifoPipelineResult, run_fifo_pipeline

ZERO = Decimal("0")


class PositionLifecycle(str, Enum):
    """Inventory lifecycle derived from total acquired, sold, and remaining."""

    FULLY_CLOSED = "fully_closed"
    PARTIALLY_OPEN = "partially_open"
    FULLY_OPEN = "fully_open"


class PositionClassification(str, Enum):
    """Portfolio classification for current position status."""

    CLOSED = "closed"
    OPEN = "open"
    DEAD_POSITION = "dead_position"


@dataclass(frozen=True, slots=True)
class PortfolioTokenPosition:
    """Portfolio view for one wallet/token stream."""

    wallet: str
    token_address: str
    total_tokens_acquired: Decimal
    total_tokens_sold: Decimal
    remaining_balance: Decimal
    remaining_cost_basis_usd: Decimal
    capital_deployed_usd: Decimal
    capital_returned_usd: Decimal
    lifecycle: PositionLifecycle
    classification: PositionClassification
    last_activity_timestamp: datetime | None


@dataclass(frozen=True, slots=True)
class PortfolioSummary:
    """Aggregate capital and position metrics across the portfolio."""

    total_positions: int
    fully_closed_positions: int
    partially_open_positions: int
    fully_open_positions: int
    dead_positions: int
    total_capital_deployed_usd: Decimal
    total_capital_returned_usd: Decimal
    net_capital_still_in_market_usd: Decimal
    total_pnl_usd: Decimal
    capital_efficiency_ratio: Decimal | None
    percent_capital_stuck_in_open_positions: Decimal | None


@dataclass(frozen=True, slots=True)
class PortfolioAnalysis:
    """Detailed portfolio view derived from normalized transactions and FIFO."""

    positions: tuple[PortfolioTokenPosition, ...]
    fully_closed_positions: tuple[PortfolioTokenPosition, ...]
    partially_open_positions: tuple[PortfolioTokenPosition, ...]
    fully_open_positions: tuple[PortfolioTokenPosition, ...]
    dead_positions: tuple[PortfolioTokenPosition, ...]
    summary: PortfolioSummary


def analyze_portfolio(
    transactions: Sequence[NormalizedTransaction],
    pipeline_result: FifoPipelineResult,
    *,
    analysis_as_of: datetime | None = None,
    dead_position_inactivity_days: int = 30,
) -> PortfolioAnalysis:
    """Build portfolio-level capital analysis from normalized rows and FIFO output."""

    positions = _build_portfolio_positions(
        transactions,
        pipeline_result=pipeline_result,
        analysis_as_of=analysis_as_of,
        dead_position_inactivity_days=dead_position_inactivity_days,
    )
    fully_closed_positions = tuple(
        position for position in positions if position.lifecycle == PositionLifecycle.FULLY_CLOSED
    )
    partially_open_positions = tuple(
        position for position in positions if position.lifecycle == PositionLifecycle.PARTIALLY_OPEN
    )
    fully_open_positions = tuple(
        position for position in positions if position.lifecycle == PositionLifecycle.FULLY_OPEN
    )
    dead_positions = tuple(
        position
        for position in positions
        if position.classification == PositionClassification.DEAD_POSITION
    )

    total_capital_deployed_usd = sum(
        (position.capital_deployed_usd for position in positions),
        ZERO,
    )
    total_capital_returned_usd = sum(
        (position.capital_returned_usd for position in positions),
        ZERO,
    )
    net_capital_still_in_market_usd = sum(
        (position.remaining_cost_basis_usd for position in positions),
        ZERO,
    )

    summary = PortfolioSummary(
        total_positions=len(positions),
        fully_closed_positions=len(fully_closed_positions),
        partially_open_positions=len(partially_open_positions),
        fully_open_positions=len(fully_open_positions),
        dead_positions=len(dead_positions),
        total_capital_deployed_usd=total_capital_deployed_usd,
        total_capital_returned_usd=total_capital_returned_usd,
        net_capital_still_in_market_usd=net_capital_still_in_market_usd,
        total_pnl_usd=pipeline_result.realized_pnl_usd,
        capital_efficiency_ratio=(
            total_capital_returned_usd / total_capital_deployed_usd
            if total_capital_deployed_usd > ZERO
            else None
        ),
        percent_capital_stuck_in_open_positions=(
            net_capital_still_in_market_usd / total_capital_deployed_usd
            if total_capital_deployed_usd > ZERO
            else None
        ),
    )

    return PortfolioAnalysis(
        positions=positions,
        fully_closed_positions=fully_closed_positions,
        partially_open_positions=partially_open_positions,
        fully_open_positions=fully_open_positions,
        dead_positions=dead_positions,
        summary=summary,
    )


def analyze_normalized_portfolio(
    transactions: Sequence[NormalizedTransaction],
    *,
    analysis_as_of: datetime | None = None,
    dead_position_inactivity_days: int = 30,
) -> PortfolioAnalysis:
    """Convenience wrapper that runs the existing FIFO pipeline first."""

    return analyze_portfolio(
        transactions,
        pipeline_result=run_fifo_pipeline(transactions),
        analysis_as_of=analysis_as_of,
        dead_position_inactivity_days=dead_position_inactivity_days,
    )


def _build_portfolio_positions(
    transactions: Sequence[NormalizedTransaction],
    *,
    pipeline_result: FifoPipelineResult,
    analysis_as_of: datetime | None,
    dead_position_inactivity_days: int,
) -> tuple[PortfolioTokenPosition, ...]:
    remaining_cost_basis_by_key = {
        (position.wallet, position.token_address): position.cost_basis_usd or ZERO
        for position in pipeline_result.remaining_positions
    }
    tracked_keys = _collect_tracked_position_keys(transactions, pipeline_result)
    positions: list[PortfolioTokenPosition] = []

    for wallet, token_address in sorted(tracked_keys):
        token_transactions = [
            transaction
            for transaction in transactions
            if transaction.wallet == wallet
            and (
                transaction.token_in_address == token_address
                or transaction.token_out_address == token_address
            )
        ]
        total_tokens_acquired = sum(
            (
                transaction.amount_in
                for transaction in token_transactions
                if transaction.token_in_address == token_address
            ),
            ZERO,
        )
        total_tokens_sold = sum(
            (
                transaction.amount_out
                for transaction in token_transactions
                if transaction.token_out_address == token_address
            ),
            ZERO,
        )
        remaining_balance = total_tokens_acquired - total_tokens_sold
        last_activity_timestamp = max(
            (transaction.block_time for transaction in token_transactions),
            default=None,
        )
        capital_deployed_usd = sum(
            (
                transaction.usd_value or ZERO
                for transaction in token_transactions
                if transaction.event_type == EventType.SWAP
                and transaction.token_in_address == token_address
                and transaction.amount_in > ZERO
            ),
            ZERO,
        )
        capital_returned_usd = sum(
            (
                transaction.usd_value or ZERO
                for transaction in token_transactions
                if transaction.event_type == EventType.SWAP
                and transaction.token_out_address == token_address
                and transaction.amount_out > ZERO
            ),
            ZERO,
        )
        lifecycle = _classify_position_lifecycle(
            total_tokens_acquired=total_tokens_acquired,
            total_tokens_sold=total_tokens_sold,
            remaining_balance=remaining_balance,
        )
        classification = _classify_position(
            token_address=token_address,
            lifecycle=lifecycle,
            token_transactions=token_transactions,
            last_activity_timestamp=last_activity_timestamp,
            analysis_as_of=analysis_as_of,
            dead_position_inactivity_days=dead_position_inactivity_days,
        )

        positions.append(
            PortfolioTokenPosition(
                wallet=wallet,
                token_address=token_address,
                total_tokens_acquired=total_tokens_acquired,
                total_tokens_sold=total_tokens_sold,
                remaining_balance=remaining_balance,
                remaining_cost_basis_usd=remaining_cost_basis_by_key.get(
                    (wallet, token_address),
                    ZERO,
                ),
                capital_deployed_usd=capital_deployed_usd,
                capital_returned_usd=capital_returned_usd,
                lifecycle=lifecycle,
                classification=classification,
                last_activity_timestamp=last_activity_timestamp,
            )
        )

    return tuple(positions)


def _collect_tracked_position_keys(
    transactions: Sequence[NormalizedTransaction],
    pipeline_result: FifoPipelineResult,
) -> set[tuple[str, str]]:
    tracked_keys = {
        (position.wallet, position.token_address)
        for position in pipeline_result.remaining_positions
    }
    tracked_keys.update(
        (trade_match.wallet, trade_match.token_address)
        for trade_match in pipeline_result.fifo_result.trade_matches
    )
    tracked_keys.update(
        (transaction.wallet, transaction.token_in_address)
        for transaction in transactions
        if transaction.event_type == EventType.TRANSFER
        and transaction.token_in_address is not None
        and transaction.amount_in > ZERO
    )
    tracked_keys.update(
        (transaction.wallet, transaction.token_out_address)
        for transaction in transactions
        if transaction.event_type == EventType.TRANSFER
        and transaction.token_out_address is not None
        and transaction.amount_out > ZERO
    )
    return tracked_keys


def _classify_position_lifecycle(
    *,
    total_tokens_acquired: Decimal,
    total_tokens_sold: Decimal,
    remaining_balance: Decimal,
) -> PositionLifecycle:
    if remaining_balance == ZERO:
        return PositionLifecycle.FULLY_CLOSED
    if total_tokens_sold == ZERO:
        return PositionLifecycle.FULLY_OPEN
    return PositionLifecycle.PARTIALLY_OPEN


def _classify_position(
    *,
    token_address: str,
    lifecycle: PositionLifecycle,
    token_transactions: Sequence[NormalizedTransaction],
    last_activity_timestamp: datetime | None,
    analysis_as_of: datetime | None,
    dead_position_inactivity_days: int,
) -> PositionClassification:
    if lifecycle == PositionLifecycle.FULLY_CLOSED:
        return PositionClassification.CLOSED

    has_swap_sell = any(
        transaction.event_type == EventType.SWAP
        and transaction.token_out_address == token_address
        and transaction.amount_out > ZERO
        for transaction in token_transactions
    )
    if (
        lifecycle == PositionLifecycle.FULLY_OPEN
        and not has_swap_sell
        and last_activity_timestamp is not None
        and analysis_as_of is not None
        and analysis_as_of - last_activity_timestamp
        >= timedelta(days=dead_position_inactivity_days)
    ):
        return PositionClassification.DEAD_POSITION

    return PositionClassification.OPEN
