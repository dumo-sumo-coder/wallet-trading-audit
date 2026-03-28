"""Canonical transaction schema shared across Solana and BNB EVM analysis."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Mapping


@dataclass(frozen=True, slots=True)
class SchemaFieldDefinition:
    """Field-level schema contract for normalized transaction rows."""

    name: str
    type_label: str
    nullable: bool
    description: str
    audit_notes: str | None = None


CANONICAL_TRANSACTION_SCHEMA = (
    SchemaFieldDefinition(
        name="chain",
        type_label="enum[solana|bnb_evm]",
        nullable=False,
        description="Chain family for the normalized transaction row.",
    ),
    SchemaFieldDefinition(
        name="wallet",
        type_label="string",
        nullable=False,
        description="Wallet under analysis.",
        audit_notes=(
            "TODO: checksum or canonical formatting rules should be applied only after "
            "real Solana and BNB EVM wallet samples are captured."
        ),
    ),
    SchemaFieldDefinition(
        name="tx_hash",
        type_label="string",
        nullable=False,
        description="Transaction signature or hash as emitted by the chain.",
        audit_notes=(
            "TODO: Solana signatures and EVM hashes may require chain-specific "
            "normalization once raw fixtures exist."
        ),
    ),
    SchemaFieldDefinition(
        name="block_time",
        type_label="datetime",
        nullable=False,
        description="Timezone-aware UTC event timestamp.",
    ),
    SchemaFieldDefinition(
        name="token_in_address",
        type_label="string",
        nullable=True,
        description="Contract or mint address for the asset entering the wallet.",
        audit_notes=(
            "Never store token symbols here. TODO: native-vs-wrapped asset treatment "
            "must be finalized after chain-specific raw payload review."
        ),
    ),
    SchemaFieldDefinition(
        name="token_out_address",
        type_label="string",
        nullable=True,
        description="Contract or mint address for the asset leaving the wallet.",
        audit_notes=(
            "Never store token symbols here. TODO: native-vs-wrapped asset treatment "
            "must be finalized after chain-specific raw payload review."
        ),
    ),
    SchemaFieldDefinition(
        name="amount_in",
        type_label="decimal",
        nullable=False,
        description="Quantity of the asset entering the wallet.",
        audit_notes="Set to Decimal('0') for one-sided outflows.",
    ),
    SchemaFieldDefinition(
        name="amount_out",
        type_label="decimal",
        nullable=False,
        description="Quantity of the asset leaving the wallet.",
        audit_notes="Set to Decimal('0') for one-sided inflows.",
    ),
    SchemaFieldDefinition(
        name="usd_value",
        type_label="decimal",
        nullable=True,
        description="Trusted USD notional for the economic event, if available.",
        audit_notes=(
            "TODO: valuation source and timestamping rules remain undefined until a "
            "price enrichment layer exists."
        ),
    ),
    SchemaFieldDefinition(
        name="fee_native",
        type_label="decimal",
        nullable=False,
        description="Fee paid in the native chain asset for this normalized row.",
    ),
    SchemaFieldDefinition(
        name="fee_usd",
        type_label="decimal",
        nullable=True,
        description="USD value of the fee, if already known with confidence.",
        audit_notes=(
            "TODO: when missing, fee conversion should come from a separate native "
            "asset pricing step rather than guessed inline logic."
        ),
    ),
    SchemaFieldDefinition(
        name="event_type",
        type_label="enum[swap|transfer|fee|unknown]",
        nullable=False,
        description="High-level economic event category.",
    ),
    SchemaFieldDefinition(
        name="source",
        type_label="string",
        nullable=True,
        description="DEX, protocol, or system source when known.",
        audit_notes=(
            "TODO: protocol naming rules should be derived from verified program IDs, "
            "router contracts, or decoder outputs."
        ),
    ),
)

CANONICAL_TRANSACTION_SCHEMA_FIELDS = tuple(
    field.name for field in CANONICAL_TRANSACTION_SCHEMA
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
        for field_name, address in (
            ("token_in_address", self.token_in_address),
            ("token_out_address", self.token_out_address),
        ):
            if address is not None and not address.strip():
                raise ValueError(f"{field_name} cannot be blank when provided")
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
        if self.event_type == EventType.SWAP:
            if self.token_in_address is None or self.token_out_address is None:
                raise ValueError("swap events require both token_in_address and token_out_address")
            if self.amount_in == 0 or self.amount_out == 0:
                raise ValueError("swap events require positive amount_in and amount_out")
        if self.event_type == EventType.TRANSFER and self.amount_in == 0 and self.amount_out == 0:
            raise ValueError("transfer events must move a non-zero token amount")

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

    @classmethod
    def from_row(cls, row: Mapping[str, object]) -> "NormalizedTransaction":
        """Parse a flat row back into a typed normalized transaction.

        This is mainly intended for fixture-driven tests and flat-file exports.
        """

        return cls(
            chain=Chain(_require_text(row, "chain")),
            wallet=_require_text(row, "wallet"),
            tx_hash=_require_text(row, "tx_hash"),
            block_time=datetime.fromisoformat(_require_text(row, "block_time")),
            token_in_address=_optional_text(row.get("token_in_address")),
            token_out_address=_optional_text(row.get("token_out_address")),
            amount_in=_require_decimal(row, "amount_in"),
            amount_out=_require_decimal(row, "amount_out"),
            usd_value=_optional_decimal(row.get("usd_value")),
            fee_native=_require_decimal(row, "fee_native"),
            fee_usd=_optional_decimal(row.get("fee_usd")),
            event_type=EventType(_require_text(row, "event_type")),
            source=_optional_text(row.get("source")),
        )


def _require_text(row: Mapping[str, object], key: str) -> str:
    value = row.get(key)
    if value is None:
        raise ValueError(f"Missing required field: {key}")
    text = str(value).strip()
    if not text:
        raise ValueError(f"Field '{key}' cannot be blank")
    return text


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _require_decimal(row: Mapping[str, object], key: str) -> Decimal:
    value = row.get(key)
    if value is None:
        raise ValueError(f"Missing required decimal field: {key}")
    text = str(value).strip()
    if not text:
        raise ValueError(f"Decimal field '{key}' cannot be blank")
    return Decimal(text)


def _optional_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return Decimal(text)
