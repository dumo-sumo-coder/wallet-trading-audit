"""Aggregate per-wallet analysis outputs into a portfolio audit summary."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Sequence

from .trade_diagnostics import TokenPnlDiagnostic
from .wallet_behavior import HoldingTimeBucketDiagnostic

ZERO = Decimal("0")

STATUS_INCLUDED_COMPLETE = "included_complete"
STATUS_INCLUDED_SUPPORTED_SUBSET = "included_supported_subset"
STATUS_EXCLUDED_NOT_MEANINGFUL = "excluded_not_meaningful"
STATUS_EXCLUDED_UNSUPPORTED_CHAIN = "excluded_unsupported_chain"
STATUS_EXCLUDED_MISSING_LOCAL_DATA = "excluded_missing_local_data"
STATUS_EXCLUDED_ANALYSIS_FAILED = "excluded_analysis_failed"


@dataclass(frozen=True, slots=True)
class PortfolioWalletSummary:
    wallet: str
    label: str
    group: str | None
    chain: str
    status: str
    included_in_aggregate: bool
    message: str | None
    source_path: str | None
    analysis_summary_path: str | None
    realized_pnl_usd: Decimal | None
    matched_trade_count: int
    winners_count: int
    losers_count: int
    win_rate: Decimal | None
    holding_time_buckets: tuple[HoldingTimeBucketDiagnostic, ...]
    token_pnl: tuple[TokenPnlDiagnostic, ...]
    top_losing_tokens: tuple[TokenPnlDiagnostic, ...]
    unsupported_transactions_count: int | None
    rows_requiring_valuation_after_count: int | None
    unsupported_fifo_transactions_count: int | None
    skipped_missing_valuation_count: int | None
    unsupported_patterns: tuple["UnsupportedCasePatternCount", ...]


@dataclass(frozen=True, slots=True)
class UnsupportedCasePatternCount:
    pattern_key: str
    label: str
    count: int


@dataclass(frozen=True, slots=True)
class PortfolioUnsupportedPatternSummary:
    pattern_key: str
    label: str
    total_count: int
    affected_wallets: int


@dataclass(frozen=True, slots=True)
class PortfolioWalletRanking:
    label: str
    wallet: str
    group: str | None
    chain: str
    realized_pnl_usd: Decimal | None
    win_rate: Decimal | None
    matched_trade_count: int
    status: str


@dataclass(frozen=True, slots=True)
class PortfolioWalletLossConcentration:
    label: str
    wallet: str
    group: str | None
    realized_pnl_usd: Decimal
    loss_contribution_pct: Decimal | None
    matched_trade_count: int


@dataclass(frozen=True, slots=True)
class PortfolioTokenLossConcentration:
    token_address: str
    realized_pnl_usd: Decimal
    loss_contribution_pct: Decimal | None
    contributing_wallets: int


@dataclass(frozen=True, slots=True)
class ManifestPortfolioAggregateSummary:
    analyzed_wallet_count: int
    included_wallet_count: int
    included_complete_wallet_count: int
    included_supported_subset_wallet_count: int
    excluded_wallet_count: int
    aggregate_realized_pnl_usd: Decimal
    aggregate_matched_trade_count: int
    best_wallets_by_pnl: tuple[PortfolioWalletRanking, ...]
    worst_wallets_by_pnl: tuple[PortfolioWalletRanking, ...]
    best_wallets_by_win_rate: tuple[PortfolioWalletRanking, ...]
    worst_wallets_by_win_rate: tuple[PortfolioWalletRanking, ...]
    loss_concentration_by_wallet: tuple[PortfolioWalletLossConcentration, ...]
    loss_concentration_by_token: tuple[PortfolioTokenLossConcentration, ...]
    unsupported_patterns_across_wallets: tuple[PortfolioUnsupportedPatternSummary, ...]


@dataclass(frozen=True, slots=True)
class ManifestPortfolioReport:
    wallet_summaries: tuple[PortfolioWalletSummary, ...]
    summary: ManifestPortfolioAggregateSummary


def build_manifest_portfolio_report(
    wallet_summaries: Sequence[PortfolioWalletSummary],
) -> ManifestPortfolioReport:
    wallet_summaries_tuple = tuple(wallet_summaries)
    included_wallets = [
        item
        for item in wallet_summaries_tuple
        if item.included_in_aggregate and item.realized_pnl_usd is not None
    ]

    aggregate_realized_pnl_usd = sum(
        (item.realized_pnl_usd for item in included_wallets if item.realized_pnl_usd is not None),
        ZERO,
    )
    aggregate_matched_trade_count = sum(item.matched_trade_count for item in included_wallets)

    return ManifestPortfolioReport(
        wallet_summaries=wallet_summaries_tuple,
        summary=ManifestPortfolioAggregateSummary(
            analyzed_wallet_count=len(wallet_summaries_tuple),
            included_wallet_count=len(included_wallets),
            included_complete_wallet_count=sum(
                1 for item in included_wallets if item.status == STATUS_INCLUDED_COMPLETE
            ),
            included_supported_subset_wallet_count=sum(
                1
                for item in included_wallets
                if item.status == STATUS_INCLUDED_SUPPORTED_SUBSET
            ),
            excluded_wallet_count=len(wallet_summaries_tuple) - len(included_wallets),
            aggregate_realized_pnl_usd=aggregate_realized_pnl_usd,
            aggregate_matched_trade_count=aggregate_matched_trade_count,
            best_wallets_by_pnl=_rank_wallets_by_pnl(included_wallets, reverse=True),
            worst_wallets_by_pnl=_rank_wallets_by_pnl(included_wallets, reverse=False),
            best_wallets_by_win_rate=_rank_wallets_by_win_rate(included_wallets, reverse=True),
            worst_wallets_by_win_rate=_rank_wallets_by_win_rate(included_wallets, reverse=False),
            loss_concentration_by_wallet=_summarize_wallet_loss_concentration(included_wallets),
            loss_concentration_by_token=_summarize_token_loss_concentration(included_wallets),
            unsupported_patterns_across_wallets=_summarize_unsupported_patterns(
                wallet_summaries_tuple
            ),
        ),
    )


def _rank_wallets_by_pnl(
    wallet_summaries: Sequence[PortfolioWalletSummary],
    *,
    reverse: bool,
) -> tuple[PortfolioWalletRanking, ...]:
    ranked_wallets = sorted(
        wallet_summaries,
        key=lambda item: (
            item.realized_pnl_usd if item.realized_pnl_usd is not None else ZERO,
            item.win_rate if item.win_rate is not None else ZERO,
            item.matched_trade_count,
            item.label,
            item.wallet,
        ),
        reverse=reverse,
    )
    return tuple(_to_wallet_ranking(item) for item in ranked_wallets[:5])


def _rank_wallets_by_win_rate(
    wallet_summaries: Sequence[PortfolioWalletSummary],
    *,
    reverse: bool,
) -> tuple[PortfolioWalletRanking, ...]:
    ranked_wallets = sorted(
        [
            item
            for item in wallet_summaries
            if item.win_rate is not None and item.matched_trade_count > 0
        ],
        key=lambda item: (
            item.win_rate if item.win_rate is not None else ZERO,
            item.realized_pnl_usd if item.realized_pnl_usd is not None else ZERO,
            item.matched_trade_count,
            item.label,
            item.wallet,
        ),
        reverse=reverse,
    )
    return tuple(_to_wallet_ranking(item) for item in ranked_wallets[:5])


def _summarize_wallet_loss_concentration(
    wallet_summaries: Sequence[PortfolioWalletSummary],
) -> tuple[PortfolioWalletLossConcentration, ...]:
    losing_wallets = [
        item
        for item in wallet_summaries
        if item.realized_pnl_usd is not None and item.realized_pnl_usd < ZERO
    ]
    total_losses = sum((-item.realized_pnl_usd for item in losing_wallets), ZERO)
    ranked_wallets = sorted(
        losing_wallets,
        key=lambda item: (
            item.realized_pnl_usd if item.realized_pnl_usd is not None else ZERO,
            item.label,
            item.wallet,
        ),
    )
    return tuple(
        PortfolioWalletLossConcentration(
            label=item.label,
            wallet=item.wallet,
            group=item.group,
            realized_pnl_usd=item.realized_pnl_usd or ZERO,
            loss_contribution_pct=(
                (-(item.realized_pnl_usd or ZERO) / total_losses)
                if total_losses > ZERO
                else None
            ),
            matched_trade_count=item.matched_trade_count,
        )
        for item in ranked_wallets[:10]
    )


def _summarize_token_loss_concentration(
    wallet_summaries: Sequence[PortfolioWalletSummary],
) -> tuple[PortfolioTokenLossConcentration, ...]:
    token_losses: dict[str, Decimal] = defaultdict(lambda: ZERO)
    contributing_wallets_by_token: dict[str, set[str]] = defaultdict(set)

    for wallet in wallet_summaries:
        for token in wallet.token_pnl:
            if token.realized_pnl_usd >= ZERO:
                continue
            token_losses[token.token_address] += token.realized_pnl_usd
            contributing_wallets_by_token[token.token_address].add(wallet.wallet)

    total_losses = sum((-loss for loss in token_losses.values()), ZERO)
    ranked_tokens = sorted(token_losses.items(), key=lambda item: (item[1], item[0]))
    return tuple(
        PortfolioTokenLossConcentration(
            token_address=token_address,
            realized_pnl_usd=realized_pnl_usd,
            loss_contribution_pct=(
                (-realized_pnl_usd / total_losses) if total_losses > ZERO else None
            ),
            contributing_wallets=len(contributing_wallets_by_token[token_address]),
        )
        for token_address, realized_pnl_usd in ranked_tokens[:10]
    )


def _to_wallet_ranking(
    wallet_summary: PortfolioWalletSummary,
) -> PortfolioWalletRanking:
    return PortfolioWalletRanking(
        label=wallet_summary.label,
        wallet=wallet_summary.wallet,
        group=wallet_summary.group,
        chain=wallet_summary.chain,
        realized_pnl_usd=wallet_summary.realized_pnl_usd,
        win_rate=wallet_summary.win_rate,
        matched_trade_count=wallet_summary.matched_trade_count,
        status=wallet_summary.status,
    )


def _summarize_unsupported_patterns(
    wallet_summaries: Sequence[PortfolioWalletSummary],
) -> tuple[PortfolioUnsupportedPatternSummary, ...]:
    totals_by_pattern: dict[str, Decimal] = defaultdict(lambda: ZERO)
    labels_by_pattern: dict[str, str] = {}
    wallets_by_pattern: dict[str, set[str]] = defaultdict(set)

    for wallet_summary in wallet_summaries:
        for pattern in wallet_summary.unsupported_patterns:
            totals_by_pattern[pattern.pattern_key] += Decimal(pattern.count)
            labels_by_pattern[pattern.pattern_key] = pattern.label
            wallets_by_pattern[pattern.pattern_key].add(wallet_summary.wallet)

    ranked_patterns = sorted(
        totals_by_pattern.items(),
        key=lambda item: (-int(item[1]), labels_by_pattern[item[0]], item[0]),
    )
    return tuple(
        PortfolioUnsupportedPatternSummary(
            pattern_key=pattern_key,
            label=labels_by_pattern[pattern_key],
            total_count=int(total_count),
            affected_wallets=len(wallets_by_pattern[pattern_key]),
        )
        for pattern_key, total_count in ranked_patterns
    )
