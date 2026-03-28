"""Helpers for reviewing raw Solana JSON-RPC snapshots without normalizing.

These helpers are intentionally conservative. They only inspect field
availability and preserve raw transaction response bodies for later fixture-led
normalization work.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping


_INSPECTED_FIELD_PATHS = (
    "result.blockTime",
    "result.slot",
    "result.meta",
    "result.meta.err",
    "result.meta.fee",
    "result.meta.preBalances",
    "result.meta.postBalances",
    "result.meta.preTokenBalances",
    "result.meta.postTokenBalances",
    "result.transaction",
    "result.transaction.signatures",
    "result.transaction.message",
    "result.transaction.message.accountKeys",
    "result.transaction.message.instructions",
    "result.version",
)


def load_json_mapping(path: Path) -> dict[str, object]:
    """Load a JSON object from disk for fixture-backed review."""

    parsed = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return parsed


def inspect_solana_snapshot(snapshot: Mapping[str, object]) -> dict[str, object]:
    """Summarize raw Solana snapshot structure for later normalization work."""

    transaction_responses = snapshot.get("transaction_responses")
    if not isinstance(transaction_responses, list):
        raise ValueError("Solana snapshot must contain a list at 'transaction_responses'")

    signatures_response = snapshot.get("signatures_response")
    signature_result_count = 0
    if isinstance(signatures_response, Mapping):
        signature_rows = signatures_response.get("result")
        if isinstance(signature_rows, list):
            signature_result_count = len(signature_rows)

    payload_summaries: list[dict[str, object]] = []
    for index, payload in enumerate(transaction_responses):
        if not isinstance(payload, Mapping):
            raise ValueError("Each Solana transaction response must be an object")
        payload_summaries.append(
            inspect_solana_transaction_response(payload, index=index)
        )

    return {
        "snapshot_keys": sorted(str(key) for key in snapshot.keys()),
        "signature_response_keys": (
            sorted(str(key) for key in signatures_response.keys())
            if isinstance(signatures_response, Mapping)
            else []
        ),
        "signature_result_count": signature_result_count,
        "transaction_response_count": len(transaction_responses),
        "payloads": payload_summaries,
    }


def inspect_solana_transaction_response(
    payload: Mapping[str, object],
    *,
    index: int | None = None,
) -> dict[str, object]:
    """Inspect one raw `getTransaction` response body without decoding it."""

    result = payload.get("result")
    result_mapping = result if isinstance(result, Mapping) else None
    field_presence = {
        path: _has_path(payload, path) for path in _INSPECTED_FIELD_PATHS
    }

    account_keys = _get_path(payload, "result.transaction.message.accountKeys")
    instructions = _get_path(payload, "result.transaction.message.instructions")
    pre_token_balances = _get_path(payload, "result.meta.preTokenBalances")
    post_token_balances = _get_path(payload, "result.meta.postTokenBalances")

    notes: list[str] = []
    if not field_presence["result.blockTime"]:
        notes.append(
            "TODO: blockTime is absent, so canonical block_time would need another verified time source."
        )
    if not field_presence["result.transaction.signatures"]:
        notes.append(
            "TODO: transaction.signatures is absent, so tx_hash cannot be mapped from this payload alone."
        )
    if not field_presence["result.meta.fee"]:
        notes.append(
            "TODO: meta.fee is absent, so fee_native cannot be mapped from this payload alone."
        )
    if not field_presence["result.meta.preTokenBalances"] or not field_presence[
        "result.meta.postTokenBalances"
    ]:
        notes.append(
            "TODO: token balance arrays are incomplete, so token in/out reconstruction would need another verified source."
        )

    # TODO: Resolve which accountKeys entry corresponds to the tracked wallet
    # before mapping wallet-side inflows or outflows from raw balance changes.
    notes.append(
        "TODO: wallet-side token_in/token_out mapping still needs fixture-driven rules for account ownership and balance deltas."
    )
    # TODO: Review instruction program IDs and inner instructions only after raw
    # fixtures exist for the specific protocol flows we want to normalize.
    notes.append(
        "TODO: instruction payloads are present, but program-specific decoding is still unresolved and must not be guessed."
    )

    summary: dict[str, object] = {
        "index": index,
        "response_keys": sorted(str(key) for key in payload.keys()),
        "result_keys": (
            sorted(str(key) for key in result_mapping.keys())
            if result_mapping is not None
            else []
        ),
        "field_presence": field_presence,
        "field_counts": {
            "account_keys": len(account_keys) if isinstance(account_keys, list) else 0,
            "instructions": len(instructions) if isinstance(instructions, list) else 0,
            "pre_token_balances": (
                len(pre_token_balances) if isinstance(pre_token_balances, list) else 0
            ),
            "post_token_balances": (
                len(post_token_balances) if isinstance(post_token_balances, list) else 0
            ),
        },
        "notes": notes,
    }

    signature = _extract_signature(payload)
    if signature is not None:
        summary["signature"] = signature
    return summary


def export_representative_transaction_payloads(
    snapshot: Mapping[str, object],
    destination_dir: Path,
    *,
    limit: int = 2,
) -> tuple[Path, ...]:
    """Copy one or more raw Solana transaction response bodies into fixtures."""

    if limit <= 0:
        raise ValueError("limit must be positive")

    transaction_responses = snapshot.get("transaction_responses")
    if not isinstance(transaction_responses, list):
        raise ValueError("Solana snapshot must contain a list at 'transaction_responses'")

    destination_dir.mkdir(parents=True, exist_ok=True)
    written_paths: list[Path] = []

    for index, payload in enumerate(transaction_responses[:limit], start=1):
        if not isinstance(payload, Mapping):
            raise ValueError("Each Solana transaction response must be an object")
        signature = _extract_signature(payload) or f"payload_{index:02d}"
        filename = f"{index:02d}_{_safe_path_component(signature)}.json"
        output_path = destination_dir / filename
        output_path.write_text(
            json.dumps(payload, indent=2),
            encoding="utf-8",
        )
        written_paths.append(output_path)

    return tuple(written_paths)


def _get_path(payload: Mapping[str, object], dotted_path: str) -> object | None:
    current: object = payload
    for segment in dotted_path.split("."):
        if not isinstance(current, Mapping) or segment not in current:
            return None
        current = current[segment]
    return current


def _has_path(payload: Mapping[str, object], dotted_path: str) -> bool:
    current: object = payload
    for segment in dotted_path.split("."):
        if not isinstance(current, Mapping) or segment not in current:
            return False
        current = current[segment]
    return True


def _extract_signature(payload: Mapping[str, object]) -> str | None:
    signatures = _get_path(payload, "result.transaction.signatures")
    if not isinstance(signatures, list) or not signatures:
        return None
    first_signature = signatures[0]
    if not isinstance(first_signature, str):
        return None
    trimmed_signature = first_signature.strip()
    return trimmed_signature or None


def _safe_path_component(value: str) -> str:
    cleaned = "".join(
        character if character.isalnum() or character in {"-", "_"} else "_"
        for character in value.strip()
    )
    return cleaned or "payload"
