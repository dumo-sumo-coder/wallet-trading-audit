"""Conservative adapters for raw transaction-like mappings."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Mapping

from .schema import (
    CANONICAL_TRANSACTION_SCHEMA_FIELDS,
    Chain,
    EventType,
    NormalizedTransaction,
)

LAMPORTS_PER_SOL = Decimal("1000000000")
SOLANA_WRAPPED_SOL_MINT = "So11111111111111111111111111111111111111112"
SOLANA_USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
ZERO = Decimal("0")


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

    if _looks_like_flat_normalized_row(raw):
        return _normalize_flat_transaction(raw, expected_chain=Chain.SOLANA)
    return _normalize_solana_provider_payload(raw)


def normalize_evm_tx(raw: Mapping[str, object]) -> dict[str, object]:
    """Return a flat canonical-like row for a BNB EVM transaction."""

    # TODO: When real BNB EVM fixtures exist, add explicit mappings from
    # verified transaction, receipt, and log fields instead of guessing event
    # semantics from provider-specific payloads.
    return _normalize_flat_transaction(raw, expected_chain=Chain.BNB_EVM)


def _normalize_solana_provider_payload(raw: Mapping[str, object]) -> dict[str, object]:
    payload, wallet_override = _extract_solana_transaction_payload(raw)
    result = _require_mapping(payload, "result")
    meta = _require_mapping(result, "meta")

    if meta.get("err") is not None:
        raise ValueError(
            "Unsupported Solana normalization case: failed transactions are out of "
            "scope for first-pass normalization."
        )

    wallet = _resolve_solana_wallet(payload, wallet_override=wallet_override)
    tx_hash = _extract_solana_signature(payload)
    block_time = _extract_solana_block_time(result)
    fee_lamports = Decimal(_require_int(meta, "fee"))
    wallet_native_delta_lamports = _extract_wallet_native_sol_delta_lamports(
        result,
        wallet=wallet,
    )
    economic_native_delta_lamports = wallet_native_delta_lamports
    if _wallet_paid_solana_fee(result, wallet=wallet):
        economic_native_delta_lamports += fee_lamports
    token_deltas = _extract_wallet_token_balance_deltas(meta, wallet=wallet)
    non_zero_token_deltas = {
        mint: amount_delta
        for mint, amount_delta in token_deltas.items()
        if amount_delta != ZERO
    }

    fee_native = _lamports_to_sol(fee_lamports)
    ambiguous_reason = _detect_ambiguous_solana_case(
        non_zero_token_deltas=non_zero_token_deltas,
        economic_native_delta_lamports=economic_native_delta_lamports,
    )
    if ambiguous_reason is not None:
        raise ValueError(
            f"Unsupported Solana normalization case: {ambiguous_reason}"
        )

    if _is_exact_two_token_zero_native_swap(
        non_zero_token_deltas=non_zero_token_deltas,
        economic_native_delta_lamports=economic_native_delta_lamports,
    ):
        (
            token_in_address,
            amount_in,
            token_out_address,
            amount_out,
        ) = _extract_two_token_zero_native_swap_legs(non_zero_token_deltas)
        return _build_solana_row(
            wallet=wallet,
            tx_hash=tx_hash,
            block_time=block_time,
            token_in_address=token_in_address,
            token_out_address=token_out_address,
            amount_in=amount_in,
            amount_out=amount_out,
            usd_value=_derive_explicit_swap_usd_value(
                token_in_address=token_in_address,
                amount_in=amount_in,
                token_out_address=token_out_address,
                amount_out=amount_out,
            ),
            fee_native=fee_native,
            event_type=EventType.SWAP,
        )

    if not non_zero_token_deltas:
        if economic_native_delta_lamports == ZERO:
            return _build_solana_row(
                wallet=wallet,
                tx_hash=tx_hash,
                block_time=block_time,
                token_in_address=None,
                token_out_address=None,
                amount_in=ZERO,
                amount_out=ZERO,
                fee_native=fee_native,
                event_type=EventType.FEE,
            )
        if economic_native_delta_lamports > ZERO:
            return _build_solana_row(
                wallet=wallet,
                tx_hash=tx_hash,
                block_time=block_time,
                token_in_address=SOLANA_WRAPPED_SOL_MINT,
                token_out_address=None,
                amount_in=_lamports_to_sol(economic_native_delta_lamports),
                amount_out=ZERO,
                fee_native=fee_native,
                event_type=EventType.TRANSFER,
            )
        return _build_solana_row(
            wallet=wallet,
            tx_hash=tx_hash,
            block_time=block_time,
            token_in_address=None,
            token_out_address=SOLANA_WRAPPED_SOL_MINT,
            amount_in=ZERO,
            amount_out=_lamports_to_sol(-economic_native_delta_lamports),
            fee_native=fee_native,
            event_type=EventType.TRANSFER,
        )

    mint, token_delta = next(iter(non_zero_token_deltas.items()))
    if token_delta > ZERO:
        if economic_native_delta_lamports == ZERO:
            return _build_solana_row(
                wallet=wallet,
                tx_hash=tx_hash,
                block_time=block_time,
                token_in_address=mint,
                token_out_address=None,
                amount_in=token_delta,
                amount_out=ZERO,
                fee_native=fee_native,
                event_type=EventType.TRANSFER,
            )
        if economic_native_delta_lamports < ZERO:
            # TODO: native-vs-wrapped treatment is still provisional. We reuse the
            # repo's existing wrapped-SOL mint convention until raw fixture review
            # establishes a better canonical representation for native SOL.
            return _build_solana_row(
                wallet=wallet,
                tx_hash=tx_hash,
                block_time=block_time,
                token_in_address=mint,
                token_out_address=SOLANA_WRAPPED_SOL_MINT,
                amount_in=token_delta,
                amount_out=_lamports_to_sol(-economic_native_delta_lamports),
                usd_value=_derive_explicit_swap_usd_value(
                    token_in_address=mint,
                    amount_in=token_delta,
                    token_out_address=SOLANA_WRAPPED_SOL_MINT,
                    amount_out=_lamports_to_sol(-economic_native_delta_lamports),
                ),
                fee_native=fee_native,
                event_type=EventType.SWAP,
            )
        raise AssertionError("Unexpected ambiguous Solana inflow case reached")

    token_out_amount = -token_delta
    if economic_native_delta_lamports == ZERO:
        return _build_solana_row(
            wallet=wallet,
            tx_hash=tx_hash,
            block_time=block_time,
            token_in_address=None,
            token_out_address=mint,
            amount_in=ZERO,
            amount_out=token_out_amount,
            fee_native=fee_native,
            event_type=EventType.TRANSFER,
        )
    if economic_native_delta_lamports <= ZERO:
        raise AssertionError("Unexpected ambiguous Solana outflow case reached")
    return _build_solana_row(
        wallet=wallet,
        tx_hash=tx_hash,
        block_time=block_time,
        token_in_address=SOLANA_WRAPPED_SOL_MINT,
        token_out_address=mint,
        amount_in=_lamports_to_sol(economic_native_delta_lamports),
        amount_out=token_out_amount,
        usd_value=_derive_explicit_swap_usd_value(
            token_in_address=SOLANA_WRAPPED_SOL_MINT,
            amount_in=_lamports_to_sol(economic_native_delta_lamports),
            token_out_address=mint,
            amount_out=token_out_amount,
        ),
        fee_native=fee_native,
        event_type=EventType.SWAP,
    )


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


def _looks_like_flat_normalized_row(raw: Mapping[str, object]) -> bool:
    if any(
        key in raw
        for key in ("jsonrpc", "result", "transaction_responses", "signatures_response")
    ):
        return False
    return any(
        key in raw
        for key in ("tx_hash", "block_time", "amount_in", "amount_out", "event_type")
    )


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


def _extract_solana_transaction_payload(
    raw: Mapping[str, object],
) -> tuple[Mapping[str, object], str | None]:
    wallet_override = _optional_text(raw.get("wallet"))
    transaction_responses = raw.get("transaction_responses")
    if transaction_responses is not None:
        if not isinstance(transaction_responses, list):
            raise ValueError("Solana snapshot field 'transaction_responses' must be a list")
        if len(transaction_responses) != 1:
            raise ValueError(
                "Solana snapshot normalization currently requires exactly one "
                "transaction response. TODO: iterate snapshots row-by-row before "
                "normalizing multi-transaction captures."
            )
        payload = transaction_responses[0]
        if not isinstance(payload, Mapping):
            raise ValueError("Each Solana transaction response must be an object")
        return payload, wallet_override
    if "result" in raw:
        return raw, wallet_override
    raise ValueError(
        "solana normalization currently supports only raw getTransaction "
        "response bodies or single-response Solana snapshot envelopes."
    )


def _resolve_solana_wallet(
    payload: Mapping[str, object],
    *,
    wallet_override: str | None,
) -> str:
    if wallet_override is not None:
        return wallet_override

    result = _require_mapping(payload, "result")
    meta = _require_mapping(result, "meta")
    owners: set[str] = set()
    for balances_key in ("preTokenBalances", "postTokenBalances"):
        rows = meta.get(balances_key)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            owner = _optional_text(row.get("owner"))
            if owner is not None:
                owners.add(owner)

    if len(owners) == 1:
        return next(iter(owners))
    if not owners:
        raise ValueError(
            "Unsupported Solana normalization case: could not resolve a single "
            "wallet owner from token balances. TODO: pass an explicit wallet or "
            "add fixture-driven wallet resolution rules."
        )
    raise ValueError(
        "Unsupported Solana normalization case: multiple wallet owners appear in "
        "token balances. TODO: add fixture-driven account ownership rules before "
        "normalizing this payload."
    )


def _extract_solana_signature(payload: Mapping[str, object]) -> str:
    result = _require_mapping(payload, "result")
    transaction = _require_mapping(result, "transaction")
    signatures = transaction.get("signatures")
    if not isinstance(signatures, list) or not signatures:
        raise ValueError(
            "Unsupported Solana normalization case: transaction.signatures is missing"
        )
    first_signature = _optional_text(signatures[0])
    if first_signature is None:
        raise ValueError(
            "Unsupported Solana normalization case: transaction signature is blank"
        )
    return first_signature


def _extract_solana_block_time(result: Mapping[str, object]) -> str:
    block_time = result.get("blockTime")
    if not isinstance(block_time, (int, float)):
        raise ValueError(
            "Unsupported Solana normalization case: blockTime is missing or invalid"
        )
    return datetime.fromtimestamp(block_time, tz=timezone.utc).isoformat()


def _extract_wallet_native_sol_delta_lamports(
    result: Mapping[str, object],
    *,
    wallet: str,
) -> Decimal:
    transaction = _require_mapping(result, "transaction")
    message = _require_mapping(transaction, "message")
    account_keys = message.get("accountKeys")
    if not isinstance(account_keys, list):
        raise ValueError(
            "Unsupported Solana normalization case: accountKeys is missing"
        )
    normalized_account_keys = [_require_text_value(key, "accountKeys entry") for key in account_keys]
    try:
        wallet_index = normalized_account_keys.index(wallet)
    except ValueError as exc:
        raise ValueError(
            "Unsupported Solana normalization case: resolved wallet is not present "
            "in accountKeys."
        ) from exc

    meta = _require_mapping(result, "meta")
    pre_balances = meta.get("preBalances")
    post_balances = meta.get("postBalances")
    if not isinstance(pre_balances, list) or not isinstance(post_balances, list):
        raise ValueError(
            "Unsupported Solana normalization case: preBalances/postBalances are missing"
        )
    if wallet_index >= len(pre_balances) or wallet_index >= len(post_balances):
        raise ValueError(
            "Unsupported Solana normalization case: wallet balance index is out of range"
        )

    pre_balance = Decimal(_require_int_like_value(pre_balances[wallet_index], "preBalances entry"))
    post_balance = Decimal(_require_int_like_value(post_balances[wallet_index], "postBalances entry"))
    return post_balance - pre_balance


def _wallet_paid_solana_fee(
    result: Mapping[str, object],
    *,
    wallet: str,
) -> bool:
    transaction = _require_mapping(result, "transaction")
    message = _require_mapping(transaction, "message")
    account_keys = message.get("accountKeys")
    if not isinstance(account_keys, list) or not account_keys:
        raise ValueError(
            "Unsupported Solana normalization case: accountKeys is missing"
        )
    fee_payer = _require_text_value(account_keys[0], "accountKeys entry")
    return fee_payer == wallet


def _extract_wallet_token_balance_deltas(
    meta: Mapping[str, object],
    *,
    wallet: str,
) -> dict[str, Decimal]:
    pre_balances = _extract_wallet_token_amounts(
        meta.get("preTokenBalances"),
        wallet=wallet,
    )
    post_balances = _extract_wallet_token_amounts(
        meta.get("postTokenBalances"),
        wallet=wallet,
    )
    token_deltas: dict[str, Decimal] = {}
    for mint in sorted(set(pre_balances) | set(post_balances)):
        token_deltas[mint] = post_balances.get(mint, ZERO) - pre_balances.get(mint, ZERO)
    return token_deltas


def _detect_ambiguous_solana_case(
    *,
    non_zero_token_deltas: Mapping[str, Decimal],
    economic_native_delta_lamports: Decimal,
) -> str | None:
    if len(non_zero_token_deltas) > 1:
        if _is_exact_two_token_zero_native_swap(
            non_zero_token_deltas=non_zero_token_deltas,
            economic_native_delta_lamports=economic_native_delta_lamports,
        ):
            return None
        return (
            "multiple wallet token balance deltas detected. TODO: add "
            "fixture-driven handling for multi-leg or protocol-specific flows "
            "before classifying this transaction."
        )

    if not non_zero_token_deltas:
        return None

    token_delta = next(iter(non_zero_token_deltas.values()))
    if token_delta > ZERO and economic_native_delta_lamports > ZERO:
        return (
            "token inflow with net SOL inflow is ambiguous. TODO: add "
            "fixture-driven rules before classifying rewards, refunds, or "
            "protocol-specific flows."
        )
    if token_delta < ZERO and economic_native_delta_lamports < ZERO:
        return (
            "token outflow with net SOL outflow is ambiguous. TODO: add "
            "fixture-driven rules before classifying liquidity adds, burns, or "
            "protocol-specific flows."
        )
    return None


def _extract_wallet_token_amounts(
    raw_balances: object,
    *,
    wallet: str,
) -> dict[str, Decimal]:
    if raw_balances is None:
        return {}
    if not isinstance(raw_balances, list):
        raise ValueError(
            "Unsupported Solana normalization case: token balance rows must be lists"
        )

    token_amounts: dict[str, Decimal] = {}
    for row in raw_balances:
        if not isinstance(row, Mapping):
            raise ValueError(
                "Unsupported Solana normalization case: token balance row must be an object"
            )
        owner = _optional_text(row.get("owner"))
        if owner != wallet:
            continue
        mint = _require_text_value(row.get("mint"), "token balance mint")
        token_amounts[mint] = token_amounts.get(mint, ZERO) + _extract_ui_token_amount(row)
    return token_amounts


def _is_exact_two_token_zero_native_swap(
    *,
    non_zero_token_deltas: Mapping[str, Decimal],
    economic_native_delta_lamports: Decimal,
) -> bool:
    if economic_native_delta_lamports != ZERO or len(non_zero_token_deltas) != 2:
        return False
    positive_mints = [mint for mint, delta in non_zero_token_deltas.items() if delta > ZERO]
    negative_mints = [mint for mint, delta in non_zero_token_deltas.items() if delta < ZERO]
    return len(positive_mints) == 1 and len(negative_mints) == 1


def _extract_two_token_zero_native_swap_legs(
    non_zero_token_deltas: Mapping[str, Decimal],
) -> tuple[str, Decimal, str, Decimal]:
    if not _is_exact_two_token_zero_native_swap(
        non_zero_token_deltas=non_zero_token_deltas,
        economic_native_delta_lamports=ZERO,
    ):
        raise ValueError("Expected exact two-token zero-native swap pattern")

    token_in_address = next(
        mint for mint, delta in non_zero_token_deltas.items() if delta > ZERO
    )
    token_out_address = next(
        mint for mint, delta in non_zero_token_deltas.items() if delta < ZERO
    )
    amount_in = non_zero_token_deltas[token_in_address]
    amount_out = -non_zero_token_deltas[token_out_address]
    return token_in_address, amount_in, token_out_address, amount_out


def _derive_explicit_swap_usd_value(
    *,
    token_in_address: str | None,
    amount_in: Decimal,
    token_out_address: str | None,
    amount_out: Decimal,
) -> Decimal | None:
    if token_in_address == SOLANA_USDC_MINT:
        return amount_in
    if token_out_address == SOLANA_USDC_MINT:
        return amount_out
    return None


def _extract_ui_token_amount(row: Mapping[str, object]) -> Decimal:
    ui_token_amount = row.get("uiTokenAmount")
    if not isinstance(ui_token_amount, Mapping):
        raise ValueError(
            "Unsupported Solana normalization case: uiTokenAmount is missing"
        )
    ui_amount_string = _optional_text(ui_token_amount.get("uiAmountString"))
    if ui_amount_string is not None:
        return Decimal(ui_amount_string)

    raw_amount = _require_text_value(ui_token_amount.get("amount"), "uiTokenAmount.amount")
    decimals = _require_int(ui_token_amount, "decimals")
    return Decimal(raw_amount) / (Decimal("10") ** decimals)


def _build_solana_row(
    *,
    wallet: str,
    tx_hash: str,
    block_time: str,
    token_in_address: str | None,
    token_out_address: str | None,
    amount_in: Decimal,
    amount_out: Decimal,
    usd_value: Decimal | None = None,
    fee_native: Decimal,
    event_type: EventType,
) -> dict[str, object]:
    return {
        "chain": Chain.SOLANA.value,
        "wallet": wallet,
        "tx_hash": tx_hash,
        "block_time": block_time,
        "token_in_address": token_in_address,
        "token_out_address": token_out_address,
        "amount_in": amount_in,
        "amount_out": amount_out,
        "usd_value": usd_value,
        "fee_native": fee_native,
        "fee_usd": None,
        "event_type": event_type.value,
        "source": None,
    }


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


def _require_mapping(raw: Mapping[str, object], key: str) -> Mapping[str, object]:
    value = raw.get(key)
    if not isinstance(value, Mapping):
        raise ValueError(f"Unsupported payload shape: expected object at '{key}'")
    return value


def _require_int(raw: Mapping[str, object], key: str) -> int:
    return _require_int_like_value(raw.get(key), key)


def _require_int_like_value(value: object, label: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"Unsupported Solana normalization case: {label} must be numeric")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError(f"Unsupported Solana normalization case: {label} is blank")
        return int(text)
    raise ValueError(f"Unsupported Solana normalization case: {label} must be numeric")


def _require_text_value(value: object, label: str) -> str:
    text = _optional_text(value)
    if text is None:
        raise ValueError(f"Unsupported Solana normalization case: {label} is missing")
    return text


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _lamports_to_sol(lamports: Decimal) -> Decimal:
    return lamports / LAMPORTS_PER_SOL
