"""Wallet behavior diagnostics derived from matched trade outputs."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from math import ceil
from typing import Sequence

from .trade_diagnostics import MatchedTradeDiagnostic

ZERO = Decimal("0")

HOLDING_TIME_BUCKETS: tuple[tuple[str, int | None], ...] = (
    ("lt_30s", 30),
    ("30s_to_lt_60s", 60),
    ("1m_to_lt_5m", 300),
    ("5m_to_lt_1h", 3600),
    ("1h_to_lt_24h", 86400),
    ("ge_24h", None),
)

FAST_ROTATION_BUCKETS: tuple[tuple[str, int], ...] = (
    ("under_30s", 30),
    ("under_60s", 60),
    ("under_5m", 300),
)

COST_BASIS_BUCKETS: tuple[tuple[str, Decimal | None], ...] = (
    ("lt_1_usd", Decimal("1")),
    ("1_to_lt_5_usd", Decimal("5")),
    ("5_to_lt_10_usd", Decimal("10")),
    ("10_to_lt_25_usd", Decimal("25")),
    ("25_to_lt_100_usd", Decimal("100")),
    ("ge_100_usd", None),
)


@dataclass(frozen=True, slots=True)
class BehaviorTradeRow:
    token_address: str
    opening_tx_hash: str
    closing_tx_hash: str
    open_timestamp: object
    close_timestamp: object
    close_day: str
    holding_duration_seconds: int
    holding_time_bucket: str
    quantity_matched: Decimal
    cost_basis_usd: Decimal | None
    cost_basis_bucket: str
    proceeds_usd: Decimal | None
    realized_pnl_usd: Decimal | None
    outcome: str
    prior_trade_outcome: str | None


@dataclass(frozen=True, slots=True)
class HoldingTimeBucketDiagnostic:
    bucket: str
    trade_count: int
    total_pnl_usd: Decimal
    avg_pnl_usd: Decimal | None


@dataclass(frozen=True, slots=True)
class DailyBehaviorDiagnostic:
    day: str
    trade_count: int
    realized_pnl_usd: Decimal
    win_rate: Decimal | None


@dataclass(frozen=True, slots=True)
class FastRotationDiagnostic:
    bucket: str
    trade_count: int
    pnl_contribution_usd: Decimal


@dataclass(frozen=True, slots=True)
class CostBasisBucketDiagnostic:
    bucket: str
    trade_count: int
    total_pnl_usd: Decimal
    avg_pnl_usd: Decimal | None


@dataclass(frozen=True, slots=True)
class StreakDiagnostics:
    longest_losing_streak: int
    longest_winning_streak: int
    pnl_after_prior_loss_usd: Decimal | None
    pnl_after_prior_win_usd: Decimal | None
    avg_pnl_after_prior_loss_usd: Decimal | None
    avg_pnl_after_prior_win_usd: Decimal | None


@dataclass(frozen=True, slots=True)
class NotionalDiagnostics:
    average_cost_basis_usd: Decimal | None
    median_cost_basis_usd: Decimal | None
    cost_basis_buckets: tuple[CostBasisBucketDiagnostic, ...]


@dataclass(frozen=True, slots=True)
class TokenConcentrationDiagnostic:
    token_address: str
    matched_trades: int
    realized_pnl_usd: Decimal
    contribution_pct: Decimal | None


@dataclass(frozen=True, slots=True)
class ConcentrationDiagnostics:
    top_losing_tokens: tuple[TokenConcentrationDiagnostic, ...]
    top_winning_tokens: tuple[TokenConcentrationDiagnostic, ...]
    top_5_losing_tokens_contribution_pct: Decimal | None
    top_5_winning_tokens_contribution_pct: Decimal | None
    worst_20_percent_trades_count: int
    worst_20_percent_trades_pnl_usd: Decimal | None
    worst_20_percent_share_of_total_pnl: Decimal | None


@dataclass(frozen=True, slots=True)
class WalletBehaviorSummary:
    total_matched_trades: int
    holding_time_buckets: tuple[HoldingTimeBucketDiagnostic, ...]
    pnl_by_day: tuple[DailyBehaviorDiagnostic, ...]
    streak_diagnostics: StreakDiagnostics
    notional_diagnostics: NotionalDiagnostics
    fast_rotation_diagnostics: tuple[FastRotationDiagnostic, ...]
    concentration_diagnostics: ConcentrationDiagnostics


@dataclass(frozen=True, slots=True)
class WalletBehaviorReport:
    trade_rows: tuple[BehaviorTradeRow, ...]
    summary: WalletBehaviorSummary


def build_wallet_behavior_report(
    matched_trades: Sequence[MatchedTradeDiagnostic],
) -> WalletBehaviorReport:
    ordered_trades = sorted(
        matched_trades,
        key=lambda item: (
            item.close_timestamp,
            item.open_timestamp,
            item.closing_tx_hash,
            item.opening_tx_hash,
            item.token_address,
        ),
    )

    prior_outcome: str | None = None
    trade_rows: list[BehaviorTradeRow] = []
    for trade in ordered_trades:
        outcome = _classify_trade_outcome(trade.realized_pnl_usd)
        trade_rows.append(
            BehaviorTradeRow(
                token_address=trade.token_address,
                opening_tx_hash=trade.opening_tx_hash,
                closing_tx_hash=trade.closing_tx_hash,
                open_timestamp=trade.open_timestamp,
                close_timestamp=trade.close_timestamp,
                close_day=trade.close_timestamp.date().isoformat(),
                holding_duration_seconds=trade.holding_duration_seconds,
                holding_time_bucket=_holding_time_bucket(trade.holding_duration_seconds),
                quantity_matched=trade.quantity_matched,
                cost_basis_usd=trade.cost_basis_usd,
                cost_basis_bucket=_cost_basis_bucket(trade.cost_basis_usd),
                proceeds_usd=trade.proceeds_usd,
                realized_pnl_usd=trade.realized_pnl_usd,
                outcome=outcome,
                prior_trade_outcome=prior_outcome,
            )
        )
        prior_outcome = outcome if outcome in {"win", "loss"} else None

    trade_rows_tuple = tuple(trade_rows)
    return WalletBehaviorReport(
        trade_rows=trade_rows_tuple,
        summary=summarize_wallet_behavior(trade_rows_tuple),
    )


def summarize_wallet_behavior(
    trade_rows: Sequence[BehaviorTradeRow],
) -> WalletBehaviorSummary:
    return WalletBehaviorSummary(
        total_matched_trades=len(trade_rows),
        holding_time_buckets=_summarize_holding_time_buckets(trade_rows),
        pnl_by_day=_summarize_pnl_by_day(trade_rows),
        streak_diagnostics=_summarize_streaks(trade_rows),
        notional_diagnostics=_summarize_notional(trade_rows),
        fast_rotation_diagnostics=_summarize_fast_rotation(trade_rows),
        concentration_diagnostics=_summarize_concentration(trade_rows),
    )


def _summarize_holding_time_buckets(
    trade_rows: Sequence[BehaviorTradeRow],
) -> tuple[HoldingTimeBucketDiagnostic, ...]:
    grouped: dict[str, list[Decimal]] = {label: [] for label, _ in HOLDING_TIME_BUCKETS}
    counts: dict[str, int] = {label: 0 for label, _ in HOLDING_TIME_BUCKETS}

    for trade in trade_rows:
        counts[trade.holding_time_bucket] += 1
        if trade.realized_pnl_usd is not None:
            grouped[trade.holding_time_bucket].append(trade.realized_pnl_usd)

    return tuple(
        HoldingTimeBucketDiagnostic(
            bucket=label,
            trade_count=counts[label],
            total_pnl_usd=sum(grouped[label], ZERO),
            avg_pnl_usd=_mean_decimal(grouped[label]),
        )
        for label, _ in HOLDING_TIME_BUCKETS
    )


def _summarize_pnl_by_day(
    trade_rows: Sequence[BehaviorTradeRow],
) -> tuple[DailyBehaviorDiagnostic, ...]:
    grouped: dict[str, list[BehaviorTradeRow]] = defaultdict(list)
    for trade in trade_rows:
        grouped[trade.close_day].append(trade)

    diagnostics: list[DailyBehaviorDiagnostic] = []
    for day, trades in sorted(grouped.items()):
        realized_pnls = [
            trade.realized_pnl_usd for trade in trades if trade.realized_pnl_usd is not None
        ]
        winning_pnls = [pnl for pnl in realized_pnls if pnl > ZERO]
        win_rate = (
            Decimal(len(winning_pnls)) / Decimal(len(realized_pnls))
            if realized_pnls
            else None
        )
        diagnostics.append(
            DailyBehaviorDiagnostic(
                day=day,
                trade_count=len(trades),
                realized_pnl_usd=sum(realized_pnls, ZERO),
                win_rate=win_rate,
            )
        )

    return tuple(diagnostics)


def _summarize_streaks(
    trade_rows: Sequence[BehaviorTradeRow],
) -> StreakDiagnostics:
    longest_losing_streak = 0
    longest_winning_streak = 0
    current_losing_streak = 0
    current_winning_streak = 0
    pnl_after_prior_loss: list[Decimal] = []
    pnl_after_prior_win: list[Decimal] = []
    previous_outcome: str | None = None

    for trade in trade_rows:
        if trade.realized_pnl_usd is not None:
            if previous_outcome == "loss":
                pnl_after_prior_loss.append(trade.realized_pnl_usd)
            elif previous_outcome == "win":
                pnl_after_prior_win.append(trade.realized_pnl_usd)

        if trade.outcome == "loss":
            current_losing_streak += 1
            current_winning_streak = 0
        elif trade.outcome == "win":
            current_winning_streak += 1
            current_losing_streak = 0
        else:
            current_losing_streak = 0
            current_winning_streak = 0

        longest_losing_streak = max(longest_losing_streak, current_losing_streak)
        longest_winning_streak = max(longest_winning_streak, current_winning_streak)
        previous_outcome = trade.outcome if trade.outcome in {"win", "loss"} else None

    return StreakDiagnostics(
        longest_losing_streak=longest_losing_streak,
        longest_winning_streak=longest_winning_streak,
        pnl_after_prior_loss_usd=_sum_or_none(pnl_after_prior_loss),
        pnl_after_prior_win_usd=_sum_or_none(pnl_after_prior_win),
        avg_pnl_after_prior_loss_usd=_mean_decimal(pnl_after_prior_loss),
        avg_pnl_after_prior_win_usd=_mean_decimal(pnl_after_prior_win),
    )


def _summarize_notional(
    trade_rows: Sequence[BehaviorTradeRow],
) -> NotionalDiagnostics:
    cost_basis_values = [
        trade.cost_basis_usd for trade in trade_rows if trade.cost_basis_usd is not None
    ]
    grouped_pnls: dict[str, list[Decimal]] = {
        label: [] for label, _ in COST_BASIS_BUCKETS
    }
    counts: dict[str, int] = {label: 0 for label, _ in COST_BASIS_BUCKETS}
    unknown_bucket_pnls: list[Decimal] = []
    unknown_bucket_count = 0

    for trade in trade_rows:
        if trade.cost_basis_usd is None:
            unknown_bucket_count += 1
            if trade.realized_pnl_usd is not None:
                unknown_bucket_pnls.append(trade.realized_pnl_usd)
            continue

        counts[trade.cost_basis_bucket] += 1
        if trade.realized_pnl_usd is not None:
            grouped_pnls[trade.cost_basis_bucket].append(trade.realized_pnl_usd)

    bucket_diagnostics = [
        CostBasisBucketDiagnostic(
            bucket=label,
            trade_count=counts[label],
            total_pnl_usd=sum(grouped_pnls[label], ZERO),
            avg_pnl_usd=_mean_decimal(grouped_pnls[label]),
        )
        for label, _ in COST_BASIS_BUCKETS
    ]
    if unknown_bucket_count > 0:
        bucket_diagnostics.append(
            CostBasisBucketDiagnostic(
                bucket="unknown_cost_basis",
                trade_count=unknown_bucket_count,
                total_pnl_usd=sum(unknown_bucket_pnls, ZERO),
                avg_pnl_usd=_mean_decimal(unknown_bucket_pnls),
            )
        )

    return NotionalDiagnostics(
        average_cost_basis_usd=_mean_decimal(cost_basis_values),
        median_cost_basis_usd=_median_decimal(cost_basis_values),
        cost_basis_buckets=tuple(bucket_diagnostics),
    )


def _summarize_fast_rotation(
    trade_rows: Sequence[BehaviorTradeRow],
) -> tuple[FastRotationDiagnostic, ...]:
    diagnostics: list[FastRotationDiagnostic] = []
    for label, upper_bound_seconds in FAST_ROTATION_BUCKETS:
        matching_trades = [
            trade
            for trade in trade_rows
            if trade.holding_duration_seconds < upper_bound_seconds
        ]
        diagnostics.append(
            FastRotationDiagnostic(
                bucket=label,
                trade_count=len(matching_trades),
                pnl_contribution_usd=sum(
                    (
                        trade.realized_pnl_usd
                        for trade in matching_trades
                        if trade.realized_pnl_usd is not None
                    ),
                    ZERO,
                ),
            )
        )
    return tuple(diagnostics)


def _summarize_concentration(
    trade_rows: Sequence[BehaviorTradeRow],
) -> ConcentrationDiagnostics:
    pnl_by_token: dict[str, list[Decimal]] = defaultdict(list)
    for trade in trade_rows:
        if trade.realized_pnl_usd is None:
            continue
        pnl_by_token[trade.token_address].append(trade.realized_pnl_usd)

    token_rows = [
        (
            token_address,
            len(realized_pnls),
            sum(realized_pnls, ZERO),
        )
        for token_address, realized_pnls in pnl_by_token.items()
    ]
    losing_token_rows = sorted(
        [row for row in token_rows if row[2] < ZERO],
        key=lambda item: (item[2], item[0]),
    )
    winning_token_rows = sorted(
        [row for row in token_rows if row[2] > ZERO],
        key=lambda item: (-item[2], item[0]),
    )

    total_losses_magnitude = sum((-row[2] for row in losing_token_rows), ZERO)
    total_wins = sum((row[2] for row in winning_token_rows), ZERO)
    top_losing_tokens = tuple(
        TokenConcentrationDiagnostic(
            token_address=token_address,
            matched_trades=matched_trades,
            realized_pnl_usd=realized_pnl_usd,
            contribution_pct=(
                (-realized_pnl_usd) / total_losses_magnitude
                if total_losses_magnitude != ZERO
                else None
            ),
        )
        for token_address, matched_trades, realized_pnl_usd in losing_token_rows[:5]
    )
    top_winning_tokens = tuple(
        TokenConcentrationDiagnostic(
            token_address=token_address,
            matched_trades=matched_trades,
            realized_pnl_usd=realized_pnl_usd,
            contribution_pct=(
                realized_pnl_usd / total_wins if total_wins != ZERO else None
            ),
        )
        for token_address, matched_trades, realized_pnl_usd in winning_token_rows[:5]
    )

    realized_pnls = [
        trade.realized_pnl_usd for trade in trade_rows if trade.realized_pnl_usd is not None
    ]
    worst_trade_count = max(1, ceil(len(realized_pnls) * Decimal("0.2"))) if realized_pnls else 0
    worst_trades_pnl = (
        sum(sorted(realized_pnls)[:worst_trade_count], ZERO) if realized_pnls else None
    )
    total_realized_pnl = sum(realized_pnls, ZERO)

    return ConcentrationDiagnostics(
        top_losing_tokens=top_losing_tokens,
        top_winning_tokens=top_winning_tokens,
        top_5_losing_tokens_contribution_pct=(
            sum(
                (
                    item.contribution_pct
                    for item in top_losing_tokens
                    if item.contribution_pct is not None
                ),
                ZERO,
            )
            if top_losing_tokens
            else None
        ),
        top_5_winning_tokens_contribution_pct=(
            sum(
                (
                    item.contribution_pct
                    for item in top_winning_tokens
                    if item.contribution_pct is not None
                ),
                ZERO,
            )
            if top_winning_tokens
            else None
        ),
        worst_20_percent_trades_count=worst_trade_count,
        worst_20_percent_trades_pnl_usd=worst_trades_pnl,
        worst_20_percent_share_of_total_pnl=(
            worst_trades_pnl / total_realized_pnl
            if worst_trades_pnl is not None and total_realized_pnl != ZERO
            else None
        ),
    )


def _holding_time_bucket(holding_duration_seconds: int) -> str:
    for label, upper_bound_seconds in HOLDING_TIME_BUCKETS:
        if upper_bound_seconds is None or holding_duration_seconds < upper_bound_seconds:
            return label
    raise AssertionError("Unreachable holding-time bucket classification")


def _cost_basis_bucket(cost_basis_usd: Decimal | None) -> str:
    if cost_basis_usd is None:
        return "unknown_cost_basis"
    for label, upper_bound in COST_BASIS_BUCKETS:
        if upper_bound is None or cost_basis_usd < upper_bound:
            return label
    raise AssertionError("Unreachable cost-basis bucket classification")


def _classify_trade_outcome(realized_pnl_usd: Decimal | None) -> str:
    if realized_pnl_usd is None:
        return "unknown"
    if realized_pnl_usd > ZERO:
        return "win"
    if realized_pnl_usd < ZERO:
        return "loss"
    return "flat"


def _mean_decimal(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, ZERO) / Decimal(len(values))


def _median_decimal(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    sorted_values = sorted(values)
    midpoint = len(sorted_values) // 2
    if len(sorted_values) % 2 == 1:
        return sorted_values[midpoint]
    return (sorted_values[midpoint - 1] + sorted_values[midpoint]) / Decimal("2")


def _sum_or_none(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, ZERO)
