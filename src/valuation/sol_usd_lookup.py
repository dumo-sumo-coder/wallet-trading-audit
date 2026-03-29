"""Minimal explicit SOL/USD lookup utility for trusted valuation workflows."""

from __future__ import annotations

import json
import ssl
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from config import get_tls_ca_bundle_path, sanitize_text_for_output, sanitize_url_for_output

COINBASE_EXCHANGE_API_BASE_URL = "https://api.exchange.coinbase.com"
COINBASE_SOL_USD_PRODUCT_ID = "SOL-USD"
COINBASE_CANDLE_GRANULARITY_SECONDS = 60
COINBASE_PRICE_REFERENCE_KIND = "minute_candle_open"
SOL_USD_SOURCE_NAME = "coinbase_exchange_public_candles"


@dataclass(frozen=True, slots=True)
class SolUsdLookupResult:
    """Explicit SOL/USD reference value from a documented public source."""

    source_name: str
    product_id: str
    reference_price_usd: Decimal
    price_reference_kind: str
    reference_candle_start: datetime
    reference_candle_end: datetime
    lookup_timestamp: datetime
    request_url: str


class SolUsdLookupError(RuntimeError):
    """Explicit failure raised when no trusted SOL/USD reference can be returned."""


def lookup_sol_usd_at_timestamp(
    timestamp: datetime,
    *,
    timeout_seconds: int = 30,
    base_url: str = COINBASE_EXCHANGE_API_BASE_URL,
    product_id: str = COINBASE_SOL_USD_PRODUCT_ID,
) -> SolUsdLookupResult:
    """Return a one-minute SOL/USD reference value for the timestamp's minute bucket.

    Source:
    - Coinbase Exchange public market data
    - `GET /products/{product_id}/candles`

    Reference policy:
    - Use the `open` price from the exact 60-second candle whose bucket start is
      the transaction timestamp floored to the minute in UTC.
    - Fail if the exact minute bucket is unavailable.
    """

    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")

    reference_candle_start = timestamp.astimezone(UTC).replace(second=0, microsecond=0)
    reference_candle_end = reference_candle_start + timedelta(
        seconds=COINBASE_CANDLE_GRANULARITY_SECONDS
    )
    query = urlencode(
        {
            "start": _isoformat_utc(reference_candle_start),
            "end": _isoformat_utc(reference_candle_end),
            "granularity": str(COINBASE_CANDLE_GRANULARITY_SECONDS),
        }
    )
    request_url = (
        f"{base_url}/products/{product_id}/candles?{query}"
    )
    request_url_for_output = sanitize_url_for_output(request_url)
    request = Request(
        request_url,
        headers={
            "Accept": "application/json",
            "User-Agent": "wallet-trading-audit/0.1",
        },
        method="GET",
    )

    ssl_context = _build_ssl_context()
    try:
        with urlopen(request, timeout=timeout_seconds, context=ssl_context) as response:
            raw_body = response.read().decode("utf-8")
    except HTTPError as exc:
        response_snippet = _read_http_error_snippet(exc)
        raise SolUsdLookupError(
            "SOL/USD lookup failed with HTTP "
            f"{exc.code} from {SOL_USD_SOURCE_NAME} at {request_url_for_output}. "
            f"Response: {response_snippet or 'none'}"
        ) from exc
    except URLError as exc:
        raise SolUsdLookupError(
            "SOL/USD lookup failed from "
            f"{SOL_USD_SOURCE_NAME} at {request_url_for_output}: "
            f"{sanitize_text_for_output(str(exc.reason)) or exc.__class__.__name__}"
        ) from exc

    try:
        parsed = json.loads(raw_body, parse_float=Decimal)
    except json.JSONDecodeError as exc:
        raise SolUsdLookupError(
            f"SOL/USD lookup returned invalid JSON from {request_url_for_output}"
        ) from exc
    if not isinstance(parsed, list):
        raise SolUsdLookupError(
            f"SOL/USD lookup returned a non-list payload from {request_url_for_output}"
        )

    reference_unix_time = int(reference_candle_start.timestamp())
    exact_candle = _find_exact_candle(parsed, reference_unix_time)
    if exact_candle is None:
        raise SolUsdLookupError(
            "No exact 60-second SOL-USD candle was returned for "
            f"{reference_candle_start.isoformat()} from {SOL_USD_SOURCE_NAME}"
        )

    reference_price_usd = _extract_open_price(exact_candle)
    return SolUsdLookupResult(
        source_name=SOL_USD_SOURCE_NAME,
        product_id=product_id,
        reference_price_usd=reference_price_usd,
        price_reference_kind=COINBASE_PRICE_REFERENCE_KIND,
        reference_candle_start=reference_candle_start,
        reference_candle_end=reference_candle_end,
        lookup_timestamp=datetime.now(UTC),
        request_url=request_url_for_output,
    )


def _find_exact_candle(
    candles: list[Any],
    reference_unix_time: int,
) -> list[Any] | None:
    for candle in candles:
        if not isinstance(candle, list) or len(candle) < 5:
            continue
        candle_time = candle[0]
        if isinstance(candle_time, Decimal):
            candle_time_value = int(candle_time)
        else:
            candle_time_value = int(candle_time)
        if candle_time_value == reference_unix_time:
            return candle
    return None


def _extract_open_price(candle: list[Any]) -> Decimal:
    open_value = candle[3]
    if isinstance(open_value, Decimal):
        return open_value
    return Decimal(str(open_value))


def _build_ssl_context() -> ssl.SSLContext:
    ca_bundle_path = get_tls_ca_bundle_path()
    if ca_bundle_path is None:
        return ssl.create_default_context()
    return ssl.create_default_context(cafile=ca_bundle_path)


def _read_http_error_snippet(exc: HTTPError) -> str | None:
    try:
        raw_body = exc.read().decode("utf-8", errors="replace")
    except Exception:  # pragma: no cover - defensive fallback
        return None
    sanitized = sanitize_text_for_output(raw_body)
    if not sanitized:
        return None
    if len(sanitized) > 240:
        return f"{sanitized[:240].rstrip()}..."
    return sanitized


def _isoformat_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
