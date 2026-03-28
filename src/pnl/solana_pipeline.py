"""End-to-end Solana fixture pipeline from raw payloads into FIFO outputs."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Mapping, Sequence

from ingestion.solana_review import load_json_mapping
from normalize.schema import EventType, NormalizedTransaction
from normalize.transactions import normalize_transaction

from .pipeline import FifoPipelineResult, run_fifo_pipeline


@dataclass(frozen=True, slots=True)
class UnsupportedSolanaFixture:
    """Raw Solana fixture that could not be normalized conservatively."""

    fixture_name: str
    reason: str


@dataclass(frozen=True, slots=True)
class SolanaFixturePipelineResult:
    """Deterministic end-to-end result for raw Solana fixture review."""

    total_raw_transactions_reviewed: int
    normalized_transactions: tuple[NormalizedTransaction, ...]
    unsupported_transactions: tuple[UnsupportedSolanaFixture, ...]
    fifo_pipeline_result: FifoPipelineResult


@dataclass(frozen=True, slots=True)
class SolanaFixturePipelineSummary:
    """Compact audit summary for the supported raw Solana subset."""

    total_raw_transactions_reviewed: int
    normalized_transactions_count: int
    unsupported_transactions_count: int
    unsupported_reasons: tuple[str, ...]
    realized_pnl_usd: Decimal


def run_solana_fixture_fifo_pipeline(
    fixture_paths: Sequence[Path],
    *,
    usd_value_overrides_by_tx_hash: Mapping[str, object] | None = None,
) -> SolanaFixturePipelineResult:
    """Load raw Solana fixtures, normalize the supported subset, and run FIFO."""

    normalized_transactions: list[NormalizedTransaction] = []
    unsupported_transactions: list[UnsupportedSolanaFixture] = []
    overrides = usd_value_overrides_by_tx_hash or {}

    for fixture_path in fixture_paths:
        payload = load_json_mapping(fixture_path)
        try:
            normalized = normalize_transaction(
                {
                    "chain": "solana",
                    **payload,
                }
            )
        except ValueError as exc:
            unsupported_transactions.append(
                UnsupportedSolanaFixture(
                    fixture_name=fixture_path.name,
                    reason=str(exc),
                )
            )
            continue

        normalized_transactions.append(
            _apply_usd_value_override(
                normalized,
                usd_value_overrides_by_tx_hash=overrides,
            )
        )

    fifo_pipeline_result = run_fifo_pipeline(normalized_transactions)
    return SolanaFixturePipelineResult(
        total_raw_transactions_reviewed=len(fixture_paths),
        normalized_transactions=tuple(normalized_transactions),
        unsupported_transactions=tuple(unsupported_transactions),
        fifo_pipeline_result=fifo_pipeline_result,
    )


def summarize_solana_fixture_pipeline(
    result: SolanaFixturePipelineResult,
) -> SolanaFixturePipelineSummary:
    """Return a small audit summary for the raw-Solana-to-FIFO run."""

    return SolanaFixturePipelineSummary(
        total_raw_transactions_reviewed=result.total_raw_transactions_reviewed,
        normalized_transactions_count=len(result.normalized_transactions),
        unsupported_transactions_count=len(result.unsupported_transactions),
        unsupported_reasons=tuple(
            f"{item.fixture_name}: {item.reason}"
            for item in result.unsupported_transactions
        ),
        realized_pnl_usd=result.fifo_pipeline_result.realized_pnl_usd,
    )


def _apply_usd_value_override(
    transaction: NormalizedTransaction,
    *,
    usd_value_overrides_by_tx_hash: Mapping[str, object],
) -> NormalizedTransaction:
    if transaction.event_type != EventType.SWAP:
        return transaction

    if transaction.tx_hash not in usd_value_overrides_by_tx_hash:
        raise ValueError(
            "Missing trusted usd_value override for FIFO-ready Solana swap: "
            f"{transaction.tx_hash}"
        )

    row = transaction.to_row()
    row["usd_value"] = usd_value_overrides_by_tx_hash[transaction.tx_hash]
    return NormalizedTransaction.from_row(row)
