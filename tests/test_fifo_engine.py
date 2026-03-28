"""Tests for the first-pass FIFO trade reconstruction engine."""

from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from normalize.schema import Chain, EventType, NormalizedTransaction  # noqa: E402
from pnl.fifo_engine import FifoEngine  # noqa: E402

WALLET = "wallet-1"
TRADED_TOKEN = "TokenMint1111111111111111111111111111111111"
QUOTE_TOKEN = "UsdMint111111111111111111111111111111111111"


def normalized_swap(
    *,
    tx_hash: str,
    block_time: datetime,
    token_in_address: str,
    token_out_address: str,
    amount_in: str,
    amount_out: str,
    usd_value: str,
    fee_native: str = "0",
    fee_usd: str | None = None,
) -> NormalizedTransaction:
    return NormalizedTransaction(
        chain=Chain.SOLANA,
        wallet=WALLET,
        tx_hash=tx_hash,
        block_time=block_time,
        token_in_address=token_in_address,
        token_out_address=token_out_address,
        amount_in=Decimal(amount_in),
        amount_out=Decimal(amount_out),
        usd_value=Decimal(usd_value),
        fee_native=Decimal(fee_native),
        fee_usd=None if fee_usd is None else Decimal(fee_usd),
        event_type=EventType.SWAP,
        source="test-dex",
    )


def normalized_transfer(
    *,
    tx_hash: str,
    block_time: datetime,
    amount_out: str,
) -> NormalizedTransaction:
    return NormalizedTransaction(
        chain=Chain.SOLANA,
        wallet=WALLET,
        tx_hash=tx_hash,
        block_time=block_time,
        token_in_address=None,
        token_out_address=TRADED_TOKEN,
        amount_in=Decimal("0"),
        amount_out=Decimal(amount_out),
        usd_value=None,
        fee_native=Decimal("0"),
        fee_usd=None,
        event_type=EventType.TRANSFER,
        source="wallet-transfer",
    )


def normalized_fee_row(
    *,
    tx_hash: str,
    block_time: datetime,
    fee_native: str,
    fee_usd: str | None = None,
) -> NormalizedTransaction:
    return NormalizedTransaction(
        chain=Chain.SOLANA,
        wallet=WALLET,
        tx_hash=tx_hash,
        block_time=block_time,
        token_in_address=None,
        token_out_address=None,
        amount_in=Decimal("0"),
        amount_out=Decimal("0"),
        usd_value=None,
        fee_native=Decimal(fee_native),
        fee_usd=None if fee_usd is None else Decimal(fee_usd),
        event_type=EventType.FEE,
        source="network",
    )


class FifoEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = FifoEngine()
        self.start = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    def test_single_buy_then_full_sell(self) -> None:
        transactions = [
            normalized_swap(
                tx_hash="buy-1",
                block_time=self.start,
                token_in_address=TRADED_TOKEN,
                token_out_address=QUOTE_TOKEN,
                amount_in="10",
                amount_out="100",
                usd_value="100",
            ),
            normalized_swap(
                tx_hash="sell-1",
                block_time=self.start.replace(hour=13),
                token_in_address=QUOTE_TOKEN,
                token_out_address=TRADED_TOKEN,
                amount_in="150",
                amount_out="10",
                usd_value="150",
            ),
        ]

        result = self.engine.reconstruct(transactions)

        self.assertEqual(len(result.trade_matches), 1)
        trade_match = result.trade_matches[0]
        self.assertEqual(trade_match.quantity, Decimal("10"))
        self.assertEqual(trade_match.cost_basis_usd, Decimal("100"))
        self.assertEqual(trade_match.proceeds_usd, Decimal("150"))
        self.assertEqual(trade_match.realized_pnl_usd, Decimal("50"))
        self.assertEqual(trade_match.entry_time, self.start)
        self.assertEqual(trade_match.exit_time, self.start.replace(hour=13))
        self.assertEqual(result.open_lots, ())

    def test_single_buy_then_partial_sell_then_full_sell(self) -> None:
        transactions = [
            normalized_swap(
                tx_hash="buy-1",
                block_time=self.start,
                token_in_address=TRADED_TOKEN,
                token_out_address=QUOTE_TOKEN,
                amount_in="10",
                amount_out="100",
                usd_value="100",
            ),
            normalized_swap(
                tx_hash="sell-1",
                block_time=self.start.replace(hour=13),
                token_in_address=QUOTE_TOKEN,
                token_out_address=TRADED_TOKEN,
                amount_in="60",
                amount_out="4",
                usd_value="60",
            ),
        ]

        partial_result = self.engine.reconstruct(transactions)

        self.assertEqual(len(partial_result.trade_matches), 1)
        self.assertEqual(partial_result.trade_matches[0].realized_pnl_usd, Decimal("20"))
        self.assertEqual(len(partial_result.open_lots), 1)
        self.assertEqual(partial_result.open_lots[0].quantity_open, Decimal("6"))

        transactions.append(
            normalized_swap(
                tx_hash="sell-2",
                block_time=self.start.replace(hour=14),
                token_in_address=QUOTE_TOKEN,
                token_out_address=TRADED_TOKEN,
                amount_in="72",
                amount_out="6",
                usd_value="72",
            )
        )

        final_result = self.engine.reconstruct(transactions)

        self.assertEqual(len(final_result.trade_matches), 2)
        self.assertEqual(
            [match.realized_pnl_usd for match in final_result.trade_matches],
            [Decimal("20"), Decimal("12")],
        )
        self.assertEqual(final_result.open_lots, ())

    def test_multiple_buys_then_one_sell_uses_fifo_order(self) -> None:
        transactions = [
            normalized_swap(
                tx_hash="buy-1",
                block_time=self.start,
                token_in_address=TRADED_TOKEN,
                token_out_address=QUOTE_TOKEN,
                amount_in="5",
                amount_out="50",
                usd_value="50",
            ),
            normalized_swap(
                tx_hash="buy-2",
                block_time=self.start.replace(hour=12, minute=30),
                token_in_address=TRADED_TOKEN,
                token_out_address=QUOTE_TOKEN,
                amount_in="5",
                amount_out="60",
                usd_value="60",
            ),
            normalized_swap(
                tx_hash="sell-1",
                block_time=self.start.replace(hour=13),
                token_in_address=QUOTE_TOKEN,
                token_out_address=TRADED_TOKEN,
                amount_in="120",
                amount_out="8",
                usd_value="120",
            ),
        ]

        result = self.engine.reconstruct(transactions)

        self.assertEqual(len(result.trade_matches), 2)
        first_match, second_match = result.trade_matches
        self.assertEqual(first_match.entry_tx_hash, "buy-1")
        self.assertEqual(first_match.quantity, Decimal("5"))
        self.assertEqual(first_match.cost_basis_usd, Decimal("50"))
        self.assertEqual(first_match.proceeds_usd, Decimal("75"))
        self.assertEqual(first_match.realized_pnl_usd, Decimal("25"))
        self.assertEqual(second_match.entry_tx_hash, "buy-2")
        self.assertEqual(second_match.quantity, Decimal("3"))
        self.assertEqual(second_match.cost_basis_usd, Decimal("36"))
        self.assertEqual(second_match.proceeds_usd, Decimal("45"))
        self.assertEqual(second_match.realized_pnl_usd, Decimal("9"))
        self.assertEqual(len(result.open_lots), 1)
        self.assertEqual(result.open_lots[0].source_tx_hash, "buy-2")
        self.assertEqual(result.open_lots[0].quantity_open, Decimal("2"))
        self.assertEqual(result.open_lots[0].unit_cost_usd, Decimal("12"))

    def test_transfer_row_is_ignored_for_realized_pnl(self) -> None:
        transactions = [
            normalized_transfer(
                tx_hash="transfer-1",
                block_time=self.start,
                amount_out="3",
            )
        ]

        result = self.engine.reconstruct(transactions)

        self.assertEqual(result.trade_matches, ())
        self.assertEqual(result.open_lots, ())
        self.assertEqual(result.ignored_transfers, tuple(transactions))

    def test_fee_handling_stays_explicit_and_separate(self) -> None:
        transactions = [
            normalized_swap(
                tx_hash="buy-1",
                block_time=self.start,
                token_in_address=TRADED_TOKEN,
                token_out_address=QUOTE_TOKEN,
                amount_in="10",
                amount_out="100",
                usd_value="100",
                fee_native="0.01",
                fee_usd="1",
            ),
            normalized_fee_row(
                tx_hash="fee-1",
                block_time=self.start.replace(hour=12, minute=30),
                fee_native="0.02",
                fee_usd="2",
            ),
            normalized_swap(
                tx_hash="sell-1",
                block_time=self.start.replace(hour=13),
                token_in_address=QUOTE_TOKEN,
                token_out_address=TRADED_TOKEN,
                amount_in="150",
                amount_out="10",
                usd_value="150",
                fee_native="0.01",
                fee_usd="1",
            ),
        ]

        result = self.engine.reconstruct(transactions)

        self.assertEqual(len(result.trade_matches), 1)
        self.assertEqual(result.trade_matches[0].realized_pnl_usd, Decimal("50"))
        self.assertEqual(len(result.recorded_fees), 3)
        self.assertEqual(
            [fee.tx_hash for fee in result.recorded_fees],
            ["buy-1", "fee-1", "sell-1"],
        )
        self.assertEqual(
            [fee.fee_usd for fee in result.recorded_fees],
            [Decimal("1"), Decimal("2"), Decimal("1")],
        )


if __name__ == "__main__":
    unittest.main()
