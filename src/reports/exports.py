"""Planned flat-file exports for downstream analysis."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from analytics.metrics import metric_names
from normalize.schema import CANONICAL_TRANSACTION_SCHEMA_FIELDS


class ReportStatus(str, Enum):
    """Lifecycle state for report definitions."""

    PLANNED = "planned"


@dataclass(frozen=True, slots=True)
class ReportDefinition:
    """Description of a future export artifact."""

    name: str
    description: str
    columns: tuple[str, ...]
    status: ReportStatus = ReportStatus.PLANNED


REPORT_DEFINITIONS: dict[str, ReportDefinition] = {
    "normalized_transactions": ReportDefinition(
        name="normalized_transactions",
        description="Flat export of canonical normalized transaction rows.",
        columns=CANONICAL_TRANSACTION_SCHEMA_FIELDS,
    ),
    "fifo_roundtrips": ReportDefinition(
        name="fifo_roundtrips",
        description="Closed FIFO trade matches with cost basis and proceeds.",
        columns=(
            "wallet",
            "token_address",
            "entry_tx_hash",
            "exit_tx_hash",
            "entry_time",
            "exit_time",
            "quantity",
            "cost_basis_usd",
            "proceeds_usd",
            "realized_pnl_usd",
        ),
    ),
    "performance_summary": ReportDefinition(
        name="performance_summary",
        description="Scalar performance metrics ready for notebook or CSV export.",
        columns=metric_names(),
    ),
    "behavior_summary": ReportDefinition(
        name="behavior_summary",
        description="Behavioral and sequence-oriented analytics outputs.",
        columns=(
            "wallet",
            "token_address",
            "roundtrip_flag",
            "reentry_penalty_flag",
            "time_to_reentry_minutes",
            "trade_sequence_index",
            "trade_pnl_after_fees",
        ),
    ),
}


def export_report(name: str, destination: Path) -> None:
    """Placeholder for future report serialization logic."""

    raise NotImplementedError(
        f"Report export is not implemented yet for report '{name}' -> {destination}."
    )
