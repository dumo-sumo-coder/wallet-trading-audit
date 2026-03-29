"""Conservative valuation preparation for normalized Solana swap rows."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Mapping, Sequence

from normalize.schema import Chain, EventType, NormalizedTransaction

VALUATION_STATUS_NEEDS_VALUATION = "needs_valuation"
VALUATION_STATUS_PENDING = "pending"
VALUATION_STATUS_TRUSTED = "trusted"


@dataclass(frozen=True, slots=True)
class SolanaValuationRecord:
    """Audit-friendly valuation record for one normalized Solana swap row."""

    tx_hash: str
    wallet: str
    block_time: datetime
    token_in_address: str
    token_out_address: str
    amount_in: Decimal
    amount_out: Decimal
    valuation_source: str | None
    usd_value: Decimal | None
    valuation_status: str

    def __post_init__(self) -> None:
        for field_name, value in (
            ("tx_hash", self.tx_hash),
            ("wallet", self.wallet),
            ("token_in_address", self.token_in_address),
            ("token_out_address", self.token_out_address),
            ("valuation_status", self.valuation_status),
        ):
            if not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
        if self.valuation_status not in {
            VALUATION_STATUS_NEEDS_VALUATION,
            VALUATION_STATUS_PENDING,
            VALUATION_STATUS_TRUSTED,
        }:
            raise ValueError(
                "valuation_status must be one of: needs_valuation, pending, trusted"
            )
        if self.block_time.tzinfo is None or self.block_time.utcoffset() is None:
            raise ValueError("block_time must be timezone-aware")
        if self.amount_in <= Decimal("0"):
            raise ValueError("amount_in must be positive")
        if self.amount_out <= Decimal("0"):
            raise ValueError("amount_out must be positive")
        if self.usd_value is not None and self.usd_value < Decimal("0"):
            raise ValueError("usd_value cannot be negative")
        if self.valuation_source is not None and not self.valuation_source.strip():
            raise ValueError("valuation_source cannot be blank when provided")
        if self.valuation_status in {
            VALUATION_STATUS_NEEDS_VALUATION,
            VALUATION_STATUS_PENDING,
        }:
            if self.valuation_source is not None or self.usd_value is not None:
                raise ValueError(
                    f"{self.valuation_status} records cannot carry valuation_source or usd_value"
                )


@dataclass(frozen=True, slots=True)
class SolanaValuationReadinessSummary:
    """Small readiness summary for Solana swap valuation coverage."""

    total_transactions: int
    swap_transactions: int
    swap_rows_already_valued_count: int
    rows_requiring_valuation_count: int
    rows_requiring_valuation: tuple[SolanaValuationRecord, ...]
    valuation_complete: bool


@dataclass(frozen=True, slots=True)
class SolanaValuationApplicationResult:
    """Result of applying trusted USD values to normalized Solana rows."""

    transactions: tuple[NormalizedTransaction, ...]
    applied_records: tuple[SolanaValuationRecord, ...]


def get_rows_requiring_valuation(
    transactions: Sequence[NormalizedTransaction],
) -> tuple[SolanaValuationRecord, ...]:
    """Return Solana swap rows that still need trusted USD valuation."""

    _validate_solana_transactions(transactions)

    return tuple(
        _build_valuation_record(
            transaction,
            valuation_source=None,
            usd_value=None,
            valuation_status=VALUATION_STATUS_NEEDS_VALUATION,
        )
        for transaction in transactions
        if transaction.event_type == EventType.SWAP and transaction.usd_value is None
    )


def apply_trusted_usd_values(
    transactions: Sequence[NormalizedTransaction],
    valuation_records: Sequence[SolanaValuationRecord],
    *,
    overwrite_existing: bool = False,
) -> SolanaValuationApplicationResult:
    """Attach trusted USD values from explicit valuation records."""

    _validate_solana_transactions(transactions)
    record_lookup: dict[str, SolanaValuationRecord] = {}
    for record in valuation_records:
        _validate_trusted_valuation_record(record)
        if record.tx_hash in record_lookup:
            raise ValueError(f"Duplicate trusted valuation record for tx_hash: {record.tx_hash}")
        record_lookup[record.tx_hash] = record

    updated_transactions: list[NormalizedTransaction] = []
    applied_records: list[SolanaValuationRecord] = []
    matched_tx_hashes: set[str] = set()

    for transaction in transactions:
        record = record_lookup.get(transaction.tx_hash)
        if record is None:
            updated_transactions.append(transaction)
            continue

        _validate_record_matches_transaction(record, transaction)
        matched_tx_hashes.add(record.tx_hash)

        if transaction.usd_value is not None:
            if not overwrite_existing:
                raise ValueError(
                    "Refusing to overwrite existing usd_value without explicit permission: "
                    f"{transaction.tx_hash}"
                )
            if transaction.usd_value == record.usd_value:
                updated_transactions.append(transaction)
                applied_records.append(record)
                continue

        row = transaction.to_row()
        row["usd_value"] = str(record.usd_value)
        updated_transactions.append(NormalizedTransaction.from_row(row))
        applied_records.append(record)

    unmatched_tx_hashes = sorted(set(record_lookup) - matched_tx_hashes)
    if unmatched_tx_hashes:
        raise ValueError(
            "Trusted valuation records did not match normalized Solana rows: "
            + ", ".join(unmatched_tx_hashes)
        )

    return SolanaValuationApplicationResult(
        transactions=tuple(updated_transactions),
        applied_records=tuple(applied_records),
    )


def summarize_valuation_readiness(
    transactions: Sequence[NormalizedTransaction],
) -> SolanaValuationReadinessSummary:
    """Summarize how many normalized Solana swap rows still need valuation."""

    _validate_solana_transactions(transactions)
    rows_requiring_valuation = get_rows_requiring_valuation(transactions)
    swap_transactions = [
        transaction for transaction in transactions if transaction.event_type == EventType.SWAP
    ]
    return SolanaValuationReadinessSummary(
        total_transactions=len(transactions),
        swap_transactions=len(swap_transactions),
        swap_rows_already_valued_count=sum(
            1 for transaction in swap_transactions if transaction.usd_value is not None
        ),
        rows_requiring_valuation_count=len(rows_requiring_valuation),
        rows_requiring_valuation=rows_requiring_valuation,
        valuation_complete=len(rows_requiring_valuation) == 0,
    )


def load_trusted_valuation_records(path: Path) -> tuple[SolanaValuationRecord, ...]:
    """Load explicit trusted valuation records from a local JSON file.

    Pending template rows are allowed in the file, but they are ignored until
    they are completed with `valuation_status="trusted"`.
    """

    parsed = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(parsed, Mapping):
        raw_records = parsed.get("valuations")
    else:
        raw_records = parsed

    if not isinstance(raw_records, list):
        raise ValueError(
            "Trusted valuation file must be a JSON list or an object with a 'valuations' list."
        )

    parsed_records = tuple(_parse_valuation_record(item) for item in raw_records)
    return tuple(
        record for record in parsed_records if record.valuation_status == VALUATION_STATUS_TRUSTED
    )


def find_local_trusted_valuation_path(snapshot_path: Path) -> Path | None:
    """Return a sibling trusted valuation file if one exists for the snapshot."""

    candidate = snapshot_path.with_name(f"{snapshot_path.stem}_trusted_valuations.json")
    if candidate.exists():
        return candidate
    return None


def _build_valuation_record(
    transaction: NormalizedTransaction,
    *,
    valuation_source: str | None,
    usd_value: Decimal | None,
    valuation_status: str,
) -> SolanaValuationRecord:
    if transaction.token_in_address is None or transaction.token_out_address is None:
        raise ValueError(
            "Solana valuation records require swap rows with both token addresses: "
            f"{transaction.tx_hash}"
        )
    return SolanaValuationRecord(
        tx_hash=transaction.tx_hash,
        wallet=transaction.wallet,
        block_time=transaction.block_time,
        token_in_address=transaction.token_in_address,
        token_out_address=transaction.token_out_address,
        amount_in=transaction.amount_in,
        amount_out=transaction.amount_out,
        valuation_source=valuation_source,
        usd_value=usd_value,
        valuation_status=valuation_status,
    )


def _validate_solana_transactions(transactions: Sequence[NormalizedTransaction]) -> None:
    for transaction in transactions:
        if transaction.chain != Chain.SOLANA:
            raise ValueError(
                "solana valuation prep only supports normalized Solana transactions"
            )


def _validate_trusted_valuation_record(record: SolanaValuationRecord) -> None:
    if record.valuation_status != VALUATION_STATUS_TRUSTED:
        raise ValueError(
            "Trusted valuation records must declare valuation_status='trusted': "
            f"{record.tx_hash}"
        )
    if record.valuation_source is None or not record.valuation_source.strip():
        raise ValueError(
            "Trusted valuation records require a non-empty valuation_source: "
            f"{record.tx_hash}"
        )
    if record.usd_value is None:
        raise ValueError(
            f"Trusted valuation records require usd_value: {record.tx_hash}"
        )


def _validate_record_matches_transaction(
    record: SolanaValuationRecord,
    transaction: NormalizedTransaction,
) -> None:
    if transaction.event_type != EventType.SWAP:
        raise ValueError(
            "Trusted valuation records can only target swap rows: "
            f"{transaction.tx_hash}"
        )
    mismatches: list[str] = []
    for field_name, record_value, transaction_value in (
        ("wallet", record.wallet, transaction.wallet),
        ("block_time", record.block_time, transaction.block_time),
        ("token_in_address", record.token_in_address, transaction.token_in_address),
        ("token_out_address", record.token_out_address, transaction.token_out_address),
        ("amount_in", record.amount_in, transaction.amount_in),
        ("amount_out", record.amount_out, transaction.amount_out),
    ):
        if record_value != transaction_value:
            mismatches.append(field_name)
    if mismatches:
        raise ValueError(
            "Trusted valuation record does not match normalized transaction fields for "
            f"{transaction.tx_hash}: {', '.join(mismatches)}"
        )


def _parse_valuation_record(value: object) -> SolanaValuationRecord:
    if not isinstance(value, Mapping):
        raise ValueError("Each trusted valuation record must be a JSON object")

    valuation_status = _require_text(value, "valuation_status")
    return SolanaValuationRecord(
        tx_hash=_require_text(value, "tx_hash"),
        wallet=_require_text(value, "wallet"),
        block_time=datetime.fromisoformat(_require_text(value, "block_time")),
        token_in_address=_require_text(value, "token_in_address"),
        token_out_address=_require_text(value, "token_out_address"),
        amount_in=_require_decimal(value, "amount_in"),
        amount_out=_require_decimal(value, "amount_out"),
        valuation_source=(
            _require_text(value, "valuation_source")
            if valuation_status == VALUATION_STATUS_TRUSTED
            else _optional_text(value.get("valuation_source"))
        ),
        usd_value=(
            _require_decimal(value, "usd_value")
            if valuation_status == VALUATION_STATUS_TRUSTED
            else _optional_decimal(value.get("usd_value"))
        ),
        valuation_status=valuation_status,
    )


def _require_text(mapping: Mapping[str, object], key: str) -> str:
    value = mapping.get(key)
    if value is None:
        raise ValueError(f"Missing required valuation field: {key}")
    text = str(value).strip()
    if not text:
        raise ValueError(f"Valuation field '{key}' cannot be blank")
    return text


def _require_decimal(mapping: Mapping[str, object], key: str) -> Decimal:
    value = mapping.get(key)
    if value is None:
        raise ValueError(f"Missing required valuation decimal field: {key}")
    text = str(value).strip()
    if not text:
        raise ValueError(f"Valuation decimal field '{key}' cannot be blank")
    return Decimal(text)


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return Decimal(text)
