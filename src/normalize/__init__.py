"""Normalization contracts and canonical schema definitions."""

from .schema import (
    CANONICAL_TRANSACTION_SCHEMA,
    CANONICAL_TRANSACTION_SCHEMA_FIELDS,
    Chain,
    EventType,
    NormalizedTransaction,
    SchemaFieldDefinition,
)
from .transformers import RawTransactionRecord, TransactionNormalizer

__all__ = [
    "CANONICAL_TRANSACTION_SCHEMA",
    "CANONICAL_TRANSACTION_SCHEMA_FIELDS",
    "Chain",
    "EventType",
    "NormalizedTransaction",
    "RawTransactionRecord",
    "SchemaFieldDefinition",
    "TransactionNormalizer",
]
