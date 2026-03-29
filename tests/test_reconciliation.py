"""Tests for wallet and portfolio capital-flow reconciliation."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from analytics.reconciliation import (
    PortfolioWalletReconciliationSummary,
    ReconciliationBlockedNotional,
    build_portfolio_reconciliation_report,
    build_wallet_reconciliation_summary,
)
from analytics.trade_diagnostics import MatchedTradeDiagnostic
from normalize.schema import Chain, EventType, NormalizedTransaction
from normalize.transactions import SOLANA_USDC_MINT, SOLANA_WRAPPED_SOL_MINT
from valuation.solana_valuation import (
    VALUATION_STATUS_NEEDS_VALUATION,
    SolanaValuationRecord,
)


def _swap(
    *,
    tx_hash: str,
    block_time: datetime,
    token_in_address: str,
    token_out_address: str,
    amount_in: str,
    amount_out: str,
    usd_value: str | None,
) -> NormalizedTransaction:
    return NormalizedTransaction(
        chain=Chain.SOLANA,
        wallet="wallet-1",
        tx_hash=tx_hash,
        block_time=block_time,
        token_in_address=token_in_address,
        token_out_address=token_out_address,
        amount_in=Decimal(amount_in),
        amount_out=Decimal(amount_out),
        usd_value=None if usd_value is None else Decimal(usd_value),
        fee_native=Decimal("0.0001"),
        fee_usd=None,
        event_type=EventType.SWAP,
        source="dex",
    )


def _matched_trade(
    *,
    token_address: str,
    open_tx: str,
    close_tx: str,
    open_time: datetime,
    close_time: datetime,
    quantity: str,
    cost_basis_usd: str,
    proceeds_usd: str,
    realized_pnl_usd: str,
) -> MatchedTradeDiagnostic:
    return MatchedTradeDiagnostic(
        token_address=token_address,
        opening_tx_hash=open_tx,
        closing_tx_hash=close_tx,
        open_timestamp=open_time,
        close_timestamp=close_time,
        holding_duration_seconds=int((close_time - open_time).total_seconds()),
        quantity_matched=Decimal(quantity),
        cost_basis_usd=Decimal(cost_basis_usd),
        proceeds_usd=Decimal(proceeds_usd),
        realized_pnl_usd=Decimal(realized_pnl_usd),
        opening_fee_native=Decimal("0.0001"),
        opening_fee_usd=None,
        closing_fee_native=Decimal("0.0001"),
        closing_fee_usd=None,
    )


class WalletReconciliationTests(unittest.TestCase):
    def test_wallet_reconciliation_matches_clean_round_trip(self) -> None:
        open_time = datetime(2025, 5, 1, 12, 0, tzinfo=UTC)
        close_time = datetime(2025, 5, 1, 13, 0, tzinfo=UTC)
        transactions = (
            _swap(
                tx_hash="buy",
                block_time=open_time,
                token_in_address="token-a",
                token_out_address=SOLANA_USDC_MINT,
                amount_in="1000",
                amount_out="100",
                usd_value="100",
            ),
            _swap(
                tx_hash="sell",
                block_time=close_time,
                token_in_address=SOLANA_USDC_MINT,
                token_out_address="token-a",
                amount_in="80",
                amount_out="1000",
                usd_value="80",
            ),
        )
        matched_trades = (
            _matched_trade(
                token_address="token-a",
                open_tx="buy",
                close_tx="sell",
                open_time=open_time,
                close_time=close_time,
                quantity="1000",
                cost_basis_usd="100",
                proceeds_usd="80",
                realized_pnl_usd="-20",
            ),
        )

        summary = build_wallet_reconciliation_summary(
            transactions,
            matched_trades=matched_trades,
            matched_realized_pnl_usd=Decimal("-20"),
            unsupported_transaction_count=0,
            valuation_blocked_rows=(),
            open_positions_count=0,
            skipped_fifo_rows_count=0,
        )

        self.assertEqual(summary.capital_flow_transaction_count, 2)
        self.assertEqual(summary.total_capital_deployed_usd, Decimal("100"))
        self.assertEqual(summary.total_capital_returned_usd, Decimal("80"))
        self.assertEqual(summary.net_capital_flow_usd, Decimal("-20"))
        self.assertEqual(summary.matched_realized_pnl_usd, Decimal("-20"))
        self.assertEqual(summary.unmatched_notional_usd, Decimal("0"))
        self.assertEqual(summary.reconciliation_gap_usd, Decimal("0"))

    def test_wallet_reconciliation_surfaces_open_position_gap(self) -> None:
        open_time = datetime(2025, 5, 1, 12, 0, tzinfo=UTC)
        transactions = (
            _swap(
                tx_hash="buy",
                block_time=open_time,
                token_in_address="token-a",
                token_out_address=SOLANA_WRAPPED_SOL_MINT,
                amount_in="1000",
                amount_out="2",
                usd_value="100",
            ),
        )

        summary = build_wallet_reconciliation_summary(
            transactions,
            matched_trades=(),
            matched_realized_pnl_usd=Decimal("0"),
            unsupported_transaction_count=3,
            valuation_blocked_rows=(
                SolanaValuationRecord(
                    tx_hash="pending-usdc",
                    wallet="wallet-1",
                    block_time=open_time,
                    token_in_address="token-b",
                    token_out_address=SOLANA_USDC_MINT,
                    amount_in=Decimal("50"),
                    amount_out=Decimal("12.5"),
                    valuation_source=None,
                    usd_value=None,
                    valuation_status=VALUATION_STATUS_NEEDS_VALUATION,
                ),
                SolanaValuationRecord(
                    tx_hash="pending-sol",
                    wallet="wallet-1",
                    block_time=open_time,
                    token_in_address="token-c",
                    token_out_address=SOLANA_WRAPPED_SOL_MINT,
                    amount_in=Decimal("25"),
                    amount_out=Decimal("1"),
                    valuation_source=None,
                    usd_value=None,
                    valuation_status=VALUATION_STATUS_NEEDS_VALUATION,
                ),
            ),
            open_positions_count=1,
            skipped_fifo_rows_count=2,
        )

        self.assertEqual(summary.total_capital_deployed_usd, Decimal("100"))
        self.assertEqual(summary.total_capital_returned_usd, Decimal("0"))
        self.assertEqual(summary.net_capital_flow_usd, Decimal("-100"))
        self.assertEqual(summary.unmatched_notional_usd, Decimal("100"))
        self.assertEqual(summary.reconciliation_gap_usd, Decimal("-100"))
        self.assertEqual(summary.unsupported_transaction_count, 3)
        self.assertEqual(summary.unsupported_notional.measured_usd, Decimal("0"))
        self.assertEqual(summary.unsupported_notional.unknown_row_count, 3)
        self.assertEqual(summary.valuation_blocked_row_count, 2)
        self.assertEqual(summary.valuation_blocked_notional.measured_usd, Decimal("12.5"))
        self.assertEqual(summary.valuation_blocked_notional.measured_row_count, 1)
        self.assertEqual(summary.valuation_blocked_notional.unknown_row_count, 1)
        self.assertEqual(summary.open_positions_count, 1)
        self.assertEqual(summary.skipped_fifo_rows_count, 2)


class PortfolioReconciliationTests(unittest.TestCase):
    def test_portfolio_reconciliation_ranks_largest_negative_gap(self) -> None:
        report = build_portfolio_reconciliation_report(
            (
                PortfolioWalletReconciliationSummary(
                    wallet="wallet-a",
                    label="Alpha",
                    group="grp",
                    status="included_supported_subset",
                    included_in_aggregate=True,
                    matched_realized_pnl_usd=Decimal("-20"),
                    net_capital_flow_usd=Decimal("-120"),
                    reconciliation_gap_usd=Decimal("-100"),
                    unmatched_notional_usd=Decimal("100"),
                    unsupported_transaction_count=2,
                    unsupported_notional=ReconciliationBlockedNotional(
                        measured_usd=Decimal("0"),
                        measured_row_count=0,
                        unknown_row_count=2,
                    ),
                    valuation_blocked_row_count=0,
                    valuation_blocked_notional=ReconciliationBlockedNotional(
                        measured_usd=Decimal("0"),
                        measured_row_count=0,
                        unknown_row_count=0,
                    ),
                    open_positions_count=1,
                    skipped_fifo_rows_count=1,
                ),
                PortfolioWalletReconciliationSummary(
                    wallet="wallet-b",
                    label="Beta",
                    group=None,
                    status="included_complete",
                    included_in_aggregate=True,
                    matched_realized_pnl_usd=Decimal("5"),
                    net_capital_flow_usd=Decimal("-15"),
                    reconciliation_gap_usd=Decimal("-20"),
                    unmatched_notional_usd=Decimal("20"),
                    unsupported_transaction_count=0,
                    unsupported_notional=ReconciliationBlockedNotional(
                        measured_usd=Decimal("0"),
                        measured_row_count=0,
                        unknown_row_count=0,
                    ),
                    valuation_blocked_row_count=1,
                    valuation_blocked_notional=ReconciliationBlockedNotional(
                        measured_usd=Decimal("3"),
                        measured_row_count=1,
                        unknown_row_count=0,
                    ),
                    open_positions_count=0,
                    skipped_fifo_rows_count=0,
                ),
                PortfolioWalletReconciliationSummary(
                    wallet="wallet-c",
                    label="Gamma",
                    group=None,
                    status="excluded_not_meaningful",
                    included_in_aggregate=False,
                    matched_realized_pnl_usd=None,
                    net_capital_flow_usd=Decimal("-999"),
                    reconciliation_gap_usd=None,
                    unmatched_notional_usd=Decimal("999"),
                    unsupported_transaction_count=5,
                    unsupported_notional=ReconciliationBlockedNotional(
                        measured_usd=Decimal("0"),
                        measured_row_count=0,
                        unknown_row_count=5,
                    ),
                    valuation_blocked_row_count=2,
                    valuation_blocked_notional=ReconciliationBlockedNotional(
                        measured_usd=Decimal("0"),
                        measured_row_count=0,
                        unknown_row_count=2,
                    ),
                    open_positions_count=5,
                    skipped_fifo_rows_count=3,
                ),
            )
        )

        self.assertEqual(report.summary.analyzed_wallet_count, 3)
        self.assertEqual(report.summary.included_wallet_count, 2)
        self.assertEqual(report.summary.matched_realized_pnl_usd, Decimal("-15"))
        self.assertEqual(report.summary.net_capital_flow_usd, Decimal("-135"))
        self.assertEqual(report.summary.reconciliation_gap_usd, Decimal("-120"))
        self.assertEqual(report.summary.unmatched_notional_usd, Decimal("120"))
        self.assertEqual(report.summary.unsupported_transaction_count, 2)
        self.assertEqual(report.summary.valuation_blocked_row_count, 1)
        self.assertEqual(report.summary.wallet_rankings_by_gap[0].label, "Alpha")
        self.assertEqual(
            report.summary.wallet_rankings_by_gap[0].reconciliation_gap_usd,
            Decimal("-100"),
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
