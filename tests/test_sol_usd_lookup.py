"""Tests for the explicit SOL/USD lookup utility."""

from __future__ import annotations

import io
import json
import sys
import unittest
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from valuation.sol_usd_lookup import (  # noqa: E402
    COINBASE_PRICE_REFERENCE_KIND,
    COINBASE_SOL_USD_PRODUCT_ID,
    SOL_USD_SOURCE_NAME,
    SolUsdLookupError,
    lookup_sol_usd_at_timestamp,
)


class _MockHttpResponse:
    def __init__(self, body: str) -> None:
        self._body = body.encode("utf-8")

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_MockHttpResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class SolUsdLookupTests(unittest.TestCase):
    def test_lookup_sol_usd_at_timestamp_uses_exact_minute_candle_open(self) -> None:
        timestamp = datetime(2025, 5, 2, 23, 39, 33, tzinfo=UTC)
        candle_start = int(datetime(2025, 5, 2, 23, 39, 0, tzinfo=UTC).timestamp())
        body = json.dumps(
            [
                [candle_start, 145.10, 146.75, 145.55, 146.10, 1234.5],
            ]
        )

        with patch("valuation.sol_usd_lookup.urlopen", return_value=_MockHttpResponse(body)):
            result = lookup_sol_usd_at_timestamp(timestamp)

        self.assertEqual(result.source_name, SOL_USD_SOURCE_NAME)
        self.assertEqual(result.product_id, COINBASE_SOL_USD_PRODUCT_ID)
        self.assertEqual(result.price_reference_kind, COINBASE_PRICE_REFERENCE_KIND)
        self.assertEqual(result.reference_price_usd, Decimal("145.55"))
        self.assertEqual(result.reference_candle_start, datetime(2025, 5, 2, 23, 39, tzinfo=UTC))

    def test_lookup_sol_usd_at_timestamp_fails_when_exact_candle_is_missing(self) -> None:
        timestamp = datetime(2025, 5, 2, 23, 39, 33, tzinfo=UTC)
        wrong_candle_start = int(datetime(2025, 5, 2, 23, 38, 0, tzinfo=UTC).timestamp())
        body = json.dumps(
            [
                [wrong_candle_start, 145.10, 146.75, 145.55, 146.10, 1234.5],
            ]
        )

        with patch("valuation.sol_usd_lookup.urlopen", return_value=_MockHttpResponse(body)):
            with self.assertRaisesRegex(SolUsdLookupError, "No exact 60-second SOL-USD candle"):
                lookup_sol_usd_at_timestamp(timestamp)

    def test_lookup_sol_usd_at_timestamp_surfaces_http_errors_clearly(self) -> None:
        timestamp = datetime(2025, 5, 2, 23, 39, 33, tzinfo=UTC)
        http_error = HTTPError(
            "https://api.exchange.coinbase.com/products/SOL-USD/candles?start=foo",
            503,
            "Service Unavailable",
            hdrs=None,
            fp=io.BytesIO(b'{"message":"temporarily unavailable"}'),
        )

        with patch("valuation.sol_usd_lookup.urlopen", side_effect=http_error):
            with self.assertRaisesRegex(SolUsdLookupError, "HTTP 503"):
                lookup_sol_usd_at_timestamp(timestamp)


if __name__ == "__main__":
    unittest.main()
