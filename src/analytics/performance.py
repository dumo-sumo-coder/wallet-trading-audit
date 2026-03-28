"""Trade performance analysis built on closed FIFO trade matches."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Sequence

from pnl.fifo import TradeMatch
from pnl.fifo_engine import FifoEngineResult
from pnl.pipeline import FifoPipelineResult

ZERO = Decimal("0")
ONE_HUNDRED = Decimal("100")


@dataclass(frozen=True, slots=True)
class ClosedTradePerformance:
    """Per-trade performance view for one closed FIFO match."""

    wallet: str
    token_address: str
    entry_tx_hash: str
    exit_tx_hash: str
    entry_timestamp: object
    exit_timestamp: object
    holding_time_seconds: int
    realized_pnl_usd: Decimal
    cost_basis_usd: Decimal
    return_pct: Decimal
    max_unrealized_pnl_usd: Decimal | None
    max_unrealized_return_pct: Decimal | None
    pnl_capture_ratio: Decimal | None


@dataclass(frozen=True, slots=True)
class PnlDistributionBucket:
    """Coarse realized-PnL bucket for behavior analysis."""

    label: str
    trade_count: int


@dataclass(frozen=True, slots=True)
class TradePerformanceSummary:
    """Aggregate and behavioral metrics across closed trades."""

    total_trades: int
    win_rate: Decimal | None
    average_return_pct: Decimal | None
    median_return_pct: Decimal | None
    total_realized_pnl_usd: Decimal
    largest_win_usd: Decimal | None
    largest_loss_usd: Decimal | None
    average_holding_time_winners_seconds: Decimal | None
    average_holding_time_losers_seconds: Decimal | None
    pnl_distribution_buckets: tuple[PnlDistributionBucket, ...]
    max_consecutive_wins: int
    max_consecutive_losses: int


@dataclass(frozen=True, slots=True)
class TradePerformanceAnalysis:
    """Full performance output derived from FIFO trade matches."""

    closed_trades: tuple[ClosedTradePerformance, ...]
    summary: TradePerformanceSummary


def analyze_fifo_trade_performance(
    fifo_result: FifoEngineResult,
) -> TradePerformanceAnalysis:
    """Build per-trade and aggregate performance stats from FIFO output."""

    closed_trades = build_closed_trade_performance_rows(fifo_result.trade_matches)
    return TradePerformanceAnalysis(
        closed_trades=closed_trades,
        summary=summarize_closed_trade_performance(closed_trades),
    )


def analyze_fifo_pipeline_performance(
    pipeline_result: FifoPipelineResult,
) -> TradePerformanceAnalysis:
    """Convenience wrapper for existing normalized-to-FIFO pipeline results."""

    return analyze_fifo_trade_performance(pipeline_result.fifo_result)


def build_closed_trade_performance_rows(
    trade_matches: Sequence[TradeMatch],
) -> tuple[ClosedTradePerformance, ...]:
    """Convert FIFO trade matches into per-trade performance records."""

    performance_rows: list[ClosedTradePerformance] = []
    for trade_match in trade_matches:
        if trade_match.realized_pnl_usd is None:
            raise ValueError(
                "Trade performance analysis requires realized_pnl_usd for each closed trade"
            )
        if trade_match.cost_basis_usd is None:
            raise ValueError(
                "Trade performance analysis requires cost_basis_usd for each closed trade"
            )
        if trade_match.cost_basis_usd <= ZERO:
            raise ValueError(
                "Trade performance analysis requires positive cost_basis_usd for each closed trade"
            )

        holding_time_seconds = int(
            (trade_match.exit_time - trade_match.entry_time).total_seconds()
        )
        if holding_time_seconds < 0:
            raise ValueError("Closed trade exit_time cannot be earlier than entry_time")

        performance_rows.append(
            ClosedTradePerformance(
                wallet=trade_match.wallet,
                token_address=trade_match.token_address,
                entry_tx_hash=trade_match.entry_tx_hash,
                exit_tx_hash=trade_match.exit_tx_hash,
                entry_timestamp=trade_match.entry_time,
                exit_timestamp=trade_match.exit_time,
                holding_time_seconds=holding_time_seconds,
                realized_pnl_usd=trade_match.realized_pnl_usd,
                cost_basis_usd=trade_match.cost_basis_usd,
                return_pct=(trade_match.realized_pnl_usd / trade_match.cost_basis_usd)
                * ONE_HUNDRED,
                max_unrealized_pnl_usd=None,
                max_unrealized_return_pct=None,
                pnl_capture_ratio=None,
            )
        )

    return tuple(performance_rows)


def summarize_closed_trade_performance(
    closed_trades: Sequence[ClosedTradePerformance],
) -> TradePerformanceSummary:
    """Aggregate first-pass performance and behavior metrics."""

    if not closed_trades:
        return TradePerformanceSummary(
            total_trades=0,
            win_rate=None,
            average_return_pct=None,
            median_return_pct=None,
            total_realized_pnl_usd=ZERO,
            largest_win_usd=None,
            largest_loss_usd=None,
            average_holding_time_winners_seconds=None,
            average_holding_time_losers_seconds=None,
            pnl_distribution_buckets=_build_pnl_distribution_buckets(()),
            max_consecutive_wins=0,
            max_consecutive_losses=0,
        )

    ordered_trades = tuple(
        sorted(
            closed_trades,
            key=lambda item: (
                item.exit_timestamp,
                item.exit_tx_hash,
                item.entry_tx_hash,
            ),
        )
    )
    winning_trades = tuple(
        trade for trade in ordered_trades if trade.realized_pnl_usd > ZERO
    )
    losing_trades = tuple(
        trade for trade in ordered_trades if trade.realized_pnl_usd < ZERO
    )
    returns = tuple(trade.return_pct for trade in ordered_trades)
    realized_pnls = tuple(trade.realized_pnl_usd for trade in ordered_trades)
    max_consecutive_wins, max_consecutive_losses = _compute_consecutive_outcome_streaks(
        ordered_trades
    )

    return TradePerformanceSummary(
        total_trades=len(ordered_trades),
        win_rate=Decimal(len(winning_trades)) / Decimal(len(ordered_trades)),
        average_return_pct=_mean_decimal(returns),
        median_return_pct=_median_decimal(returns),
        total_realized_pnl_usd=sum(realized_pnls, ZERO),
        largest_win_usd=max(winning_trades, key=lambda item: item.realized_pnl_usd).realized_pnl_usd
        if winning_trades
        else None,
        largest_loss_usd=min(losing_trades, key=lambda item: item.realized_pnl_usd).realized_pnl_usd
        if losing_trades
        else None,
        average_holding_time_winners_seconds=_mean_decimal(
            tuple(Decimal(trade.holding_time_seconds) for trade in winning_trades)
        ),
        average_holding_time_losers_seconds=_mean_decimal(
            tuple(Decimal(trade.holding_time_seconds) for trade in losing_trades)
        ),
        pnl_distribution_buckets=_build_pnl_distribution_buckets(realized_pnls),
        max_consecutive_wins=max_consecutive_wins,
        max_consecutive_losses=max_consecutive_losses,
    )


def _build_pnl_distribution_buckets(
    realized_pnls: Sequence[Decimal],
) -> tuple[PnlDistributionBucket, ...]:
    counts = {
        "loss_lt_0_usd": 0,
        "flat_0_usd": 0,
        "win_0_to_50_usd": 0,
        "win_50_to_100_usd": 0,
        "win_ge_100_usd": 0,
    }

    for pnl in realized_pnls:
        if pnl < ZERO:
            counts["loss_lt_0_usd"] += 1
        elif pnl == ZERO:
            counts["flat_0_usd"] += 1
        elif pnl < Decimal("50"):
            counts["win_0_to_50_usd"] += 1
        elif pnl < Decimal("100"):
            counts["win_50_to_100_usd"] += 1
        else:
            counts["win_ge_100_usd"] += 1

    return tuple(
        PnlDistributionBucket(label=label, trade_count=trade_count)
        for label, trade_count in counts.items()
    )


def _compute_consecutive_outcome_streaks(
    closed_trades: Sequence[ClosedTradePerformance],
) -> tuple[int, int]:
    max_wins = 0
    max_losses = 0
    current_wins = 0
    current_losses = 0

    for trade in closed_trades:
        if trade.realized_pnl_usd > ZERO:
            current_wins += 1
            current_losses = 0
            max_wins = max(max_wins, current_wins)
            continue
        if trade.realized_pnl_usd < ZERO:
            current_losses += 1
            current_wins = 0
            max_losses = max(max_losses, current_losses)
            continue

        current_wins = 0
        current_losses = 0

    return max_wins, max_losses


def _mean_decimal(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, ZERO) / Decimal(len(values))


def _median_decimal(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None

    sorted_values = tuple(sorted(values))
    midpoint = len(sorted_values) // 2
    if len(sorted_values) % 2 == 1:
        return sorted_values[midpoint]
    return (sorted_values[midpoint - 1] + sorted_values[midpoint]) / Decimal("2")
