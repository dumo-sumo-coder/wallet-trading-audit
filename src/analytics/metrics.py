"""Definitions and formulas for planned trading performance analytics."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class MetricCategory(str, Enum):
    """Top-level reporting categories."""

    CORE = "core"
    EXECUTION = "execution"
    BEHAVIOR = "behavior"
    ADVANCED = "advanced"


class MetricStatus(str, Enum):
    """Implementation readiness for each metric."""

    DEFINED = "defined"
    TODO_PRICE_DATA = "todo_price_data"
    TODO_HEURISTIC = "todo_heuristic"


@dataclass(frozen=True, slots=True)
class MetricDefinition:
    """Human-readable metric specification."""

    name: str
    category: MetricCategory
    formula: str
    required_inputs: tuple[str, ...]
    description: str
    implementation_status: MetricStatus
    notes: str | None = None


METRIC_DEFINITIONS: dict[str, MetricDefinition] = {
    "net_pnl": MetricDefinition(
        name="net_pnl",
        category=MetricCategory.CORE,
        formula="realized_pnl + unrealized_pnl - total_fees",
        required_inputs=("realized_pnl", "unrealized_pnl", "total_fees"),
        description="Total economic PnL after subtracting all fees.",
        implementation_status=MetricStatus.DEFINED,
    ),
    "realized_pnl": MetricDefinition(
        name="realized_pnl",
        category=MetricCategory.CORE,
        formula=(
            "sum(exit_proceeds_usd - fifo_cost_basis_usd for each closed quantity match)"
        ),
        required_inputs=("fifo_trade_matches",),
        description="PnL realized from closed FIFO-matched quantities before fees.",
        implementation_status=MetricStatus.DEFINED,
        notes="Fee allocation is handled separately so net metrics do not double count.",
    ),
    "unrealized_pnl": MetricDefinition(
        name="unrealized_pnl",
        category=MetricCategory.CORE,
        formula=(
            "sum(mark_to_market_value_usd - remaining_fifo_cost_basis_usd for each open lot)"
        ),
        required_inputs=("open_inventory_lots", "mark_prices"),
        description="Open-position PnL at the chosen valuation timestamp.",
        implementation_status=MetricStatus.TODO_PRICE_DATA,
        notes="Requires a historical or current pricing source for each token address.",
    ),
    "total_fees": MetricDefinition(
        name="total_fees",
        category=MetricCategory.CORE,
        formula="sum(coalesce(fee_usd, fee_native * native_token_price_usd_at_tx_time))",
        required_inputs=("normalized_transactions", "native_token_prices"),
        description="All transaction fees expressed in USD where pricing is available.",
        implementation_status=MetricStatus.TODO_PRICE_DATA,
    ),
    "total_volume": MetricDefinition(
        name="total_volume",
        category=MetricCategory.CORE,
        formula=(
            "sum(abs(coalesce(usd_value, trade_notional_usd_from_prices))) for swap events"
        ),
        required_inputs=("normalized_transactions", "mark_prices"),
        description="Gross traded notional in USD across swap events.",
        implementation_status=MetricStatus.TODO_PRICE_DATA,
    ),
    "win_rate": MetricDefinition(
        name="win_rate",
        category=MetricCategory.EXECUTION,
        formula=(
            "count(closed_trades where trade_pnl_after_fees > 0) / count(closed_trades)"
        ),
        required_inputs=("closed_trade_pnls_after_fees",),
        description="Share of closed roundtrips that finish profitable after fees.",
        implementation_status=MetricStatus.DEFINED,
    ),
    "avg_win": MetricDefinition(
        name="avg_win",
        category=MetricCategory.EXECUTION,
        formula=(
            "sum(trade_pnl_after_fees for winning closed_trades) / "
            "count(winning closed_trades)"
        ),
        required_inputs=("closed_trade_pnls_after_fees",),
        description="Average after-fee PnL of profitable closed trades.",
        implementation_status=MetricStatus.DEFINED,
    ),
    "avg_loss": MetricDefinition(
        name="avg_loss",
        category=MetricCategory.EXECUTION,
        formula=(
            "sum(abs(trade_pnl_after_fees) for losing closed_trades) / "
            "count(losing closed_trades)"
        ),
        required_inputs=("closed_trade_pnls_after_fees",),
        description="Average absolute after-fee loss across losing closed trades.",
        implementation_status=MetricStatus.DEFINED,
    ),
    "profit_factor": MetricDefinition(
        name="profit_factor",
        category=MetricCategory.EXECUTION,
        formula=(
            "sum(trade_pnl_after_fees for winning closed_trades) / "
            "sum(abs(trade_pnl_after_fees) for losing closed_trades)"
        ),
        required_inputs=("closed_trade_pnls_after_fees",),
        description="Gross profits divided by gross losses on closed trades.",
        implementation_status=MetricStatus.DEFINED,
    ),
    "expectancy": MetricDefinition(
        name="expectancy",
        category=MetricCategory.EXECUTION,
        formula="win_rate * avg_win - (1 - win_rate) * avg_loss",
        required_inputs=("win_rate", "avg_win", "avg_loss"),
        description="Expected after-fee PnL per closed trade.",
        implementation_status=MetricStatus.DEFINED,
    ),
    "pnl_by_wallet": MetricDefinition(
        name="pnl_by_wallet",
        category=MetricCategory.BEHAVIOR,
        formula="group sum(net_pnl) by wallet",
        required_inputs=("wallet_level_pnl_rows",),
        description="Wallet-level PnL breakdown for multi-wallet analysis.",
        implementation_status=MetricStatus.DEFINED,
    ),
    "pnl_by_token": MetricDefinition(
        name="pnl_by_token",
        category=MetricCategory.BEHAVIOR,
        formula="group sum(net_pnl) by traded token contract_or_mint_address",
        required_inputs=("token_level_pnl_rows",),
        description="PnL concentration by token address rather than token symbol.",
        implementation_status=MetricStatus.DEFINED,
    ),
    "pnl_by_time_of_day": MetricDefinition(
        name="pnl_by_time_of_day",
        category=MetricCategory.BEHAVIOR,
        formula="group sum(net_pnl) by extract(hour from block_time_utc)",
        required_inputs=("closed_trades",),
        description="PnL broken out by hour of day using UTC timestamps.",
        implementation_status=MetricStatus.DEFINED,
    ),
    "pnl_by_trade_sequence": MetricDefinition(
        name="pnl_by_trade_sequence",
        category=MetricCategory.BEHAVIOR,
        formula="group sum(net_pnl) by chronological roundtrip_index within each wallet",
        required_inputs=("closed_trades",),
        description="Shows whether performance improves or degrades later in a session.",
        implementation_status=MetricStatus.DEFINED,
        notes="Can later be sliced per wallet-token stream if needed.",
    ),
    "reentry_behavior": MetricDefinition(
        name="reentry_behavior",
        category=MetricCategory.BEHAVIOR,
        formula=(
            "for each wallet-token exit, measure time_to_next_entry and "
            "next_roundtrip_pnl; summarize reentry_count, reentry_rate, "
            "median_minutes_to_reentry, and reentry_pnl_delta"
        ),
        required_inputs=("closed_trades", "subsequent_entries"),
        description="Structured summary of how quickly and how often positions are re-entered.",
        implementation_status=MetricStatus.TODO_HEURISTIC,
        notes="TODO: define a canonical reentry window and sequence rules.",
    ),
    "peak_price_after_entry": MetricDefinition(
        name="peak_price_after_entry",
        category=MetricCategory.ADVANCED,
        formula=(
            "max(observed_trade_or_oracle_price_usd between entry_time and "
            "exit_time_or_analysis_cutoff)"
        ),
        required_inputs=("price_series", "entry_exit_windows"),
        description="Highest observed USD price after entry and before exit or cutoff.",
        implementation_status=MetricStatus.TODO_PRICE_DATA,
        notes="TODO until token-level historical price data exists.",
    ),
    "peak_unrealized_pnl": MetricDefinition(
        name="peak_unrealized_pnl",
        category=MetricCategory.ADVANCED,
        formula=(
            "max(position_quantity * observed_price_usd - remaining_fifo_cost_basis_usd "
            "between entry and exit_or_cutoff)"
        ),
        required_inputs=("price_series", "open_position_windows", "fifo_cost_basis"),
        description="Best unrealized PnL seen while the position remained open.",
        implementation_status=MetricStatus.TODO_PRICE_DATA,
    ),
    "capture_ratio": MetricDefinition(
        name="capture_ratio",
        category=MetricCategory.ADVANCED,
        formula=(
            "realized_pnl_after_fees / peak_unrealized_pnl when peak_unrealized_pnl > 0 "
            "else null"
        ),
        required_inputs=("realized_trade_pnls_after_fees", "peak_unrealized_pnl"),
        description="How much of the best available open profit was actually captured.",
        implementation_status=MetricStatus.TODO_PRICE_DATA,
    ),
    "giveback_ratio": MetricDefinition(
        name="giveback_ratio",
        category=MetricCategory.ADVANCED,
        formula=(
            "(peak_unrealized_pnl - realized_pnl_after_fees) / peak_unrealized_pnl "
            "when peak_unrealized_pnl > 0 else null"
        ),
        required_inputs=("realized_trade_pnls_after_fees", "peak_unrealized_pnl"),
        description="Share of peak open profit given back before exit.",
        implementation_status=MetricStatus.TODO_PRICE_DATA,
    ),
    "roundtrip_flag": MetricDefinition(
        name="roundtrip_flag",
        category=MetricCategory.ADVANCED,
        formula=(
            "1 if wallet-token position moves from flat to non-zero and later returns "
            "to flat, else 0"
        ),
        required_inputs=("wallet_token_position_path",),
        description="Binary flag marking a completed open-and-close cycle.",
        implementation_status=MetricStatus.DEFINED,
    ),
    "reentry_penalty_flag": MetricDefinition(
        name="reentry_penalty_flag",
        category=MetricCategory.ADVANCED,
        formula=(
            "1 if the same wallet-token is re-entered within reentry_window after a "
            "profitable roundtrip and the next roundtrip_pnl_after_fees < 0, else 0"
        ),
        required_inputs=("closed_trades", "subsequent_entries"),
        description="Heuristic flag for giving back gains via fast, low-quality re-entry.",
        implementation_status=MetricStatus.TODO_HEURISTIC,
        notes="TODO: tune the reentry window and penalty condition from real data.",
    ),
}


def metric_names() -> tuple[str, ...]:
    """Return metric names in registry order."""

    return tuple(METRIC_DEFINITIONS.keys())
