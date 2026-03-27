"""Scaffold verification for metric definitions."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from analytics.metrics import METRIC_DEFINITIONS, MetricStatus  # noqa: E402


class MetricDefinitionTests(unittest.TestCase):
    def test_all_requested_metrics_exist(self) -> None:
        expected = {
            "net_pnl",
            "realized_pnl",
            "unrealized_pnl",
            "total_fees",
            "total_volume",
            "win_rate",
            "avg_win",
            "avg_loss",
            "profit_factor",
            "expectancy",
            "pnl_by_wallet",
            "pnl_by_token",
            "pnl_by_time_of_day",
            "pnl_by_trade_sequence",
            "reentry_behavior",
            "peak_price_after_entry",
            "peak_unrealized_pnl",
            "capture_ratio",
            "giveback_ratio",
            "roundtrip_flag",
            "reentry_penalty_flag",
        }
        self.assertEqual(set(METRIC_DEFINITIONS), expected)

    def test_price_dependent_metrics_are_marked_as_todo(self) -> None:
        self.assertEqual(
            METRIC_DEFINITIONS["peak_price_after_entry"].implementation_status,
            MetricStatus.TODO_PRICE_DATA,
        )
        self.assertEqual(
            METRIC_DEFINITIONS["peak_unrealized_pnl"].implementation_status,
            MetricStatus.TODO_PRICE_DATA,
        )


if __name__ == "__main__":
    unittest.main()
