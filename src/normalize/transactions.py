"""Conservative adapters for raw transaction-like mappings."""

from __future__ import annotations

from datetime import datetime
from typing import Mapping

from .schema import (
    CANONICAL_TRANSACTION_SCHEMA_FIELDS,
    Chain,
    EventType,
    NormalizedTransaction,
)


def normalize_transaction(raw: Mapping[str, object]) -> NormalizedTransaction:
    """Normalize one raw transaction-like mapping into the canonical schema."""

    chain = _require_chain(raw)
    if chain == Chain.SOLANA:
        adapted = normalize_solana_tx(raw)
    elif chain == Chain.BNB_EVM:
        adapted = normalize_evm_tx(raw)
    else:
        raise ValueError(f"Unsupported chain: {chain.value}")
    return NormalizedTransaction.from_row(adapted)


def normalize_solana_tx(raw: Mapping[str, object]) -> dict[str, object]:
    """Return a flat canonical-like row for a Solana transaction."""

    # TODO: When real Solana raw fixtures exist, map only verified signature,
    # balance-delta, and instruction fields instead of inferring them here.
    return _normalize_flat_transaction(raw, expected_chain=Chain.SOLANA)


def normalize_evm_tx(raw: Mapping[str, object]) -> dict[str, object]:
    """Return a flat canonical-like row for a BNB EVM transaction."""

    # TODO: When real BNB EVM fixtures exist, add explicit mappings from
    # verified transaction, receipt, and log fields instead of guessing event
    # semantics from provider-specific payloads.
    return _normalize_flat_transaction(raw, expected_chain=Chain.BNB_EVM)


def _normalize_flat_transaction(
    raw: Mapping[str, object],
    *,
    expected_chain: Chain,
) -> dict[str, object]:
    _validate_chain(raw, expected_chain=expected_chain)
    _validate_supported_shape(raw, expected_chain=expected_chain)

    adapted: dict[str, object] = {"chain": expected_chain.value}
    for field_name in CANONICAL_TRANSACTION_SCHEMA_FIELDS:
        if field_name == "chain":
            continue
        adapted[field_name] = _normalize_field_value(
            field_name,
            raw.get(field_name),
        )
    return adapted


def _require_chain(raw: Mapping[str, object]) -> Chain:
    if "chain" not in raw:
        raise ValueError("Missing required field: chain")
    return _coerce_chain(raw["chain"])


def _validate_chain(raw: Mapping[str, object], *, expected_chain: Chain) -> None:
    raw_chain = raw.get("chain")
    if raw_chain is None:
        return
    normalized_chain = _coerce_chain(raw_chain)
    if normalized_chain != expected_chain:
        raise ValueError(
            f"Expected chain '{expected_chain.value}' but received '{normalized_chain.value}'"
        )


def _validate_supported_shape(raw: Mapping[str, object], *, expected_chain: Chain) -> None:
    supported_keys = set(CANONICAL_TRANSACTION_SCHEMA_FIELDS) - {"chain"}
    if any(key in raw for key in supported_keys):
        return
    raise ValueError(
        f"{expected_chain.value} normalization currently supports only flat canonical-like "
        "mappings with explicit normalized fields. TODO: add verified provider-payload "
        "parsing after raw fixtures are captured."
    )


def _coerce_chain(value: object) -> Chain:
    if isinstance(value, Chain):
        return value
    text = str(value).strip()
    if not text:
        raise ValueError("Field 'chain' cannot be blank")
    try:
        return Chain(text)
    except ValueError as exc:
        raise ValueError(f"Unsupported chain: {text}") from exc


def _normalize_field_value(field_name: str, value: object) -> object:
    if isinstance(value, datetime) and field_name == "block_time":
        return value.isoformat()
    if isinstance(value, EventType) and field_name == "event_type":
        return value.value
    if isinstance(value, Chain) and field_name == "chain":
        return value.value
    return value
