"""Canonical transaction schema shared across Solana and BNB EVM analysis."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum

CANONICAL_TRANSACTION_SCHEMA_FIELDS = (
    "chain",
    "wallet",
    "tx_hash",
    "block_time",
    "token_in_address",
    "token_out_address",
    "amount_in",
    "amount_out",
    "usd_value",
    "fee_native",
    "fee_usd",
    "event_type",
    "source",
)


class Chain(str, Enum):
    """Supported chains for the first implementation phase."""

    SOLANA = "solana"
    BNB_EVM = "bnb_evm"


class EventType(str, Enum):
    """Normalized transaction event categories."""

    SWAP = "swap"
    TRANSFER = "transfer"
    FEE = "fee"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class NormalizedTransaction:
    """Audit-friendly normalized transaction row.

    Notes:
    - Use contract or mint addresses only. Token symbols are out of scope.
    - One-sided events should keep the missing token leg as `None` and the
      corresponding amount as `Decimal("0")`.
    - `usd_value` and `fee_usd` stay `None` until reliable pricing is available.
    """

    chain: Chain
    wallet: str
    tx_hash: str
    block_time: datetime
    token_in_address: str | None
    token_out_address: str | None
    amount_in: Decimal
    amount_out: Decimal
    usd_value: Decimal | None
    fee_native: Decimal
    fee_usd: Decimal | None
    event_type: EventType
    source: str | None

    def __post_init__(self) -> None:
        if not self.wallet.strip():
            raise ValueError("wallet must be a non-empty string")
        if not self.tx_hash.strip():
            raise ValueError("tx_hash must be a non-empty string")
        if self.block_time.tzinfo is None or self.block_time.utcoffset() is None:
            raise ValueError("block_time must be timezone-aware")
        if self.amount_in < 0:
            raise ValueError("amount_in cannot be negative")
        if self.amount_out < 0:
            raise ValueError("amount_out cannot be negative")
        if self.fee_native < 0:
            raise ValueError("fee_native cannot be negative")
        if self.usd_value is not None and self.usd_value < 0:
            raise ValueError("usd_value cannot be negative")
        if self.fee_usd is not None and self.fee_usd < 0:
            raise ValueError("fee_usd cannot be negative")

    def to_row(self) -> dict[str, object]:
        """Return a flat, serialization-friendly mapping."""

        return {
            "chain": self.chain.value,
            "wallet": self.wallet,
            "tx_hash": self.tx_hash,
            "block_time": self.block_time.isoformat(),
            "token_in_address": self.token_in_address,
            "token_out_address": self.token_out_address,
            "amount_in": str(self.amount_in),
            "amount_out": str(self.amount_out),
            "usd_value": None if self.usd_value is None else str(self.usd_value),
            "fee_native": str(self.fee_native),
            "fee_usd": None if self.fee_usd is None else str(self.fee_usd),
            "event_type": self.event_type.value,
            "source": self.source,
        }
