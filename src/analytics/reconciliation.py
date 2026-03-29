"""Capital-flow reconciliation for valued wallet trade activity."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Sequence

from normalize.schema import EventType, NormalizedTransaction
from normalize.transactions import SOLANA_USDC_MINT, SOLANA_WRAPPED_SOL_MINT
from valuation.solana_valuation import SolanaValuationRecord

from .trade_diagnostics import MatchedTradeDiagnostic

ZERO = Decimal("0")
FLOW_DIRECTION_DEPLOYED = "deployed"
FLOW_DIRECTION_RETURNED = "returned"


@dataclass(frozen=True, slots=True)
class ReconciliationBlockedNotional:
    measured_usd: Decimal
    measured_row_count: int
    unknown_row_count: int


@dataclass(frozen=True, slots=True)
class WalletReconciliationSummary:
    capital_flow_transaction_count: int
    unclassified_valued_swap_transaction_count: int
    total_capital_deployed_usd: Decimal
    total_capital_returned_usd: Decimal
    net_capital_flow_usd: Decimal
    matched_realized_pnl_usd: Decimal | None
    matched_cost_basis_usd: Decimal
    matched_proceeds_usd: Decimal
    unmatched_deployed_notional_usd: Decimal
    unmatched_returned_notional_usd: Decimal
    unmatched_notional_usd: Decimal
    unsupported_transaction_count: int
    unsupported_notional: ReconciliationBlockedNotional
    valuation_blocked_row_count: int
    valuation_blocked_notional: ReconciliationBlockedNotional
    open_positions_count: int
    skipped_fifo_rows_count: int
    reconciliation_gap_usd: Decimal | None


@dataclass(frozen=True, slots=True)
class PortfolioWalletReconciliationSummary:
    wallet: str
    label: str
    group: str | None
    status: str
    included_in_aggregate: bool
    matched_realized_pnl_usd: Decimal | None
    net_capital_flow_usd: Decimal
    reconciliation_gap_usd: Decimal | None
    unmatched_notional_usd: Decimal
    unsupported_transaction_count: int
    unsupported_notional: ReconciliationBlockedNotional
    valuation_blocked_row_count: int
    valuation_blocked_notional: ReconciliationBlockedNotional
    open_positions_count: int
    skipped_fifo_rows_count: int


@dataclass(frozen=True, slots=True)
class PortfolioReconciliationSummary:
    analyzed_wallet_count: int
    included_wallet_count: int
    matched_realized_pnl_usd: Decimal
    net_capital_flow_usd: Decimal
    reconciliation_gap_usd: Decimal
    unmatched_notional_usd: Decimal
    unsupported_transaction_count: int
    unsupported_notional: ReconciliationBlockedNotional
    valuation_blocked_row_count: int
    valuation_blocked_notional: ReconciliationBlockedNotional
    open_positions_count: int
    skipped_fifo_rows_count: int
    wallet_rankings_by_gap: tuple[PortfolioWalletReconciliationSummary, ...]


@dataclass(frozen=True, slots=True)
class PortfolioReconciliationReport:
    wallet_summaries: tuple[PortfolioWalletReconciliationSummary, ...]
    summary: PortfolioReconciliationSummary


def build_wallet_reconciliation_summary(
    normalized_transactions: Sequence[NormalizedTransaction],
    *,
    matched_trades: Sequence[MatchedTradeDiagnostic],
    matched_realized_pnl_usd: Decimal | None,
    unsupported_transaction_count: int,
    valuation_blocked_rows: Sequence[SolanaValuationRecord],
    open_positions_count: int,
    skipped_fifo_rows_count: int,
) -> WalletReconciliationSummary:
    capital_flow_count = 0
    unclassified_valued_swap_count = 0
    total_capital_deployed_usd = ZERO
    total_capital_returned_usd = ZERO

    for transaction in normalized_transactions:
        flow_direction = _classify_capital_flow_direction(transaction)
        if flow_direction is None:
            if transaction.event_type == EventType.SWAP and transaction.usd_value is not None:
                unclassified_valued_swap_count += 1
            continue

        capital_flow_count += 1
        if flow_direction == FLOW_DIRECTION_DEPLOYED:
            total_capital_deployed_usd += transaction.usd_value or ZERO
        else:
            total_capital_returned_usd += transaction.usd_value or ZERO

    matched_cost_basis_usd = sum(
        (trade.cost_basis_usd or ZERO for trade in matched_trades),
        ZERO,
    )
    matched_proceeds_usd = sum(
        (trade.proceeds_usd or ZERO for trade in matched_trades),
        ZERO,
    )

    unsupported_notional = ReconciliationBlockedNotional(
        measured_usd=ZERO,
        measured_row_count=0,
        unknown_row_count=unsupported_transaction_count,
    )
    valuation_blocked_notional = _measure_valuation_blocked_notional(
        valuation_blocked_rows
    )
    net_capital_flow_usd = total_capital_returned_usd - total_capital_deployed_usd
    unmatched_deployed_notional_usd = max(total_capital_deployed_usd - matched_cost_basis_usd, ZERO)
    unmatched_returned_notional_usd = max(total_capital_returned_usd - matched_proceeds_usd, ZERO)
    unmatched_notional_usd = unmatched_deployed_notional_usd + unmatched_returned_notional_usd

    return WalletReconciliationSummary(
        capital_flow_transaction_count=capital_flow_count,
        unclassified_valued_swap_transaction_count=unclassified_valued_swap_count,
        total_capital_deployed_usd=total_capital_deployed_usd,
        total_capital_returned_usd=total_capital_returned_usd,
        net_capital_flow_usd=net_capital_flow_usd,
        matched_realized_pnl_usd=matched_realized_pnl_usd,
        matched_cost_basis_usd=matched_cost_basis_usd,
        matched_proceeds_usd=matched_proceeds_usd,
        unmatched_deployed_notional_usd=unmatched_deployed_notional_usd,
        unmatched_returned_notional_usd=unmatched_returned_notional_usd,
        unmatched_notional_usd=unmatched_notional_usd,
        unsupported_transaction_count=unsupported_transaction_count,
        unsupported_notional=unsupported_notional,
        valuation_blocked_row_count=len(valuation_blocked_rows),
        valuation_blocked_notional=valuation_blocked_notional,
        open_positions_count=open_positions_count,
        skipped_fifo_rows_count=skipped_fifo_rows_count,
        reconciliation_gap_usd=(
            net_capital_flow_usd - matched_realized_pnl_usd
            if matched_realized_pnl_usd is not None
            else None
        ),
    )


def build_portfolio_reconciliation_report(
    wallet_summaries: Sequence[PortfolioWalletReconciliationSummary],
) -> PortfolioReconciliationReport:
    wallet_summaries_tuple = tuple(wallet_summaries)
    included_wallets = [
        item
        for item in wallet_summaries_tuple
        if item.included_in_aggregate and item.matched_realized_pnl_usd is not None
    ]

    matched_realized_pnl_usd = sum(
        (item.matched_realized_pnl_usd or ZERO for item in included_wallets),
        ZERO,
    )
    net_capital_flow_usd = sum(
        (item.net_capital_flow_usd for item in included_wallets),
        ZERO,
    )
    unsupported_notional = ReconciliationBlockedNotional(
        measured_usd=sum(
            (item.unsupported_notional.measured_usd for item in included_wallets),
            ZERO,
        ),
        measured_row_count=sum(
            item.unsupported_notional.measured_row_count for item in included_wallets
        ),
        unknown_row_count=sum(
            item.unsupported_notional.unknown_row_count for item in included_wallets
        ),
    )
    valuation_blocked_notional = ReconciliationBlockedNotional(
        measured_usd=sum(
            (item.valuation_blocked_notional.measured_usd for item in included_wallets),
            ZERO,
        ),
        measured_row_count=sum(
            item.valuation_blocked_notional.measured_row_count for item in included_wallets
        ),
        unknown_row_count=sum(
            item.valuation_blocked_notional.unknown_row_count for item in included_wallets
        ),
    )

    ranked_wallets = tuple(
        sorted(
            included_wallets,
            key=lambda item: (
                item.reconciliation_gap_usd if item.reconciliation_gap_usd is not None else ZERO,
                item.label,
                item.wallet,
            ),
        )
    )

    return PortfolioReconciliationReport(
        wallet_summaries=wallet_summaries_tuple,
        summary=PortfolioReconciliationSummary(
            analyzed_wallet_count=len(wallet_summaries_tuple),
            included_wallet_count=len(included_wallets),
            matched_realized_pnl_usd=matched_realized_pnl_usd,
            net_capital_flow_usd=net_capital_flow_usd,
            reconciliation_gap_usd=net_capital_flow_usd - matched_realized_pnl_usd,
            unmatched_notional_usd=sum(
                (item.unmatched_notional_usd for item in included_wallets),
                ZERO,
            ),
            unsupported_transaction_count=sum(
                item.unsupported_transaction_count for item in included_wallets
            ),
            unsupported_notional=unsupported_notional,
            valuation_blocked_row_count=sum(
                item.valuation_blocked_row_count for item in included_wallets
            ),
            valuation_blocked_notional=valuation_blocked_notional,
            open_positions_count=sum(item.open_positions_count for item in included_wallets),
            skipped_fifo_rows_count=sum(item.skipped_fifo_rows_count for item in included_wallets),
            wallet_rankings_by_gap=ranked_wallets[:10],
        ),
    )


def _classify_capital_flow_direction(
    transaction: NormalizedTransaction,
) -> str | None:
    if transaction.event_type != EventType.SWAP or transaction.usd_value is None:
        return None
    token_in_address = transaction.token_in_address
    token_out_address = transaction.token_out_address
    if token_in_address is None or token_out_address is None:
        return None

    token_in_is_quote = _is_quote_asset(token_in_address)
    token_out_is_quote = _is_quote_asset(token_out_address)
    if token_in_is_quote == token_out_is_quote:
        return None
    if token_out_is_quote:
        return FLOW_DIRECTION_DEPLOYED
    return FLOW_DIRECTION_RETURNED


def _measure_valuation_blocked_notional(
    valuation_blocked_rows: Sequence[SolanaValuationRecord],
) -> ReconciliationBlockedNotional:
    measured_usd = ZERO
    measured_row_count = 0
    unknown_row_count = 0

    for record in valuation_blocked_rows:
        usd_amount = _measure_usd_amount_from_known_stable_leg(record)
        if usd_amount is None:
            unknown_row_count += 1
            continue
        measured_usd += usd_amount
        measured_row_count += 1

    return ReconciliationBlockedNotional(
        measured_usd=measured_usd,
        measured_row_count=measured_row_count,
        unknown_row_count=unknown_row_count,
    )


def _measure_usd_amount_from_known_stable_leg(
    record: SolanaValuationRecord,
) -> Decimal | None:
    if record.token_in_address == SOLANA_USDC_MINT:
        return record.amount_in
    if record.token_out_address == SOLANA_USDC_MINT:
        return record.amount_out
    return None


def _is_quote_asset(token_address: str) -> bool:
    return token_address in {SOLANA_WRAPPED_SOL_MINT, SOLANA_USDC_MINT}
