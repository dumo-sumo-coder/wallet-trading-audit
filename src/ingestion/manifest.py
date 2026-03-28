"""Manifest-driven raw wallet ingestion workflow."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from config import build_missing_env_message, get_env_var_status

from .evm_client import EvmWalletClient
from .solana_client import SolanaRpcClient

DEFAULT_WALLET_MANIFEST_PATH = Path("data") / "wallet_manifest.csv"
SUPPORTED_MANIFEST_CHAINS = frozenset({"solana", "bnb_evm"})
REQUIRED_MANIFEST_COLUMNS = ("wallet", "chain", "label")
OPTIONAL_MANIFEST_COLUMNS = ("group", "notes")


@dataclass(frozen=True, slots=True)
class WalletManifestEntry:
    wallet: str
    chain: str
    label: str
    group: str | None
    notes: str | None
    line_number: int


@dataclass(frozen=True, slots=True)
class ManifestFetchRecord:
    wallet: str
    chain: str
    label: str
    group: str | None
    notes: str | None
    provider: str
    fetched_at: str
    status: str
    error_message: str | None
    snapshot_path: str | None
    metadata_path: str
    line_number: int


@dataclass(frozen=True, slots=True)
class ManifestFetchRun:
    manifest_path: str
    records: tuple[ManifestFetchRecord, ...]

    @property
    def success_count(self) -> int:
        return sum(1 for record in self.records if record.status == "success")

    @property
    def failure_count(self) -> int:
        return sum(1 for record in self.records if record.status == "failure")


@dataclass(frozen=True, slots=True)
class ManifestPreflightResult:
    manifest_path: str
    total_wallet_count: int
    solana_wallet_count: int
    bnb_evm_wallet_count: int
    helius_api_key_status: str
    etherscan_api_key_status: str
    errors: tuple[str, ...]

    @property
    def is_ready(self) -> bool:
        return not self.errors


def load_wallet_manifest(manifest_path: Path) -> tuple[WalletManifestEntry, ...]:
    """Load and validate a wallet manifest CSV."""

    if not manifest_path.exists():
        raise FileNotFoundError(f"Wallet manifest not found: {manifest_path}")

    with manifest_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("Wallet manifest is empty")

        normalized_headers = tuple(_normalize_header_name(name) for name in reader.fieldnames)
        if any(not header for header in normalized_headers):
            raise ValueError("Wallet manifest contains a blank header name")

        missing_columns = [
            column for column in REQUIRED_MANIFEST_COLUMNS if column not in normalized_headers
        ]
        if missing_columns:
            missing_text = ", ".join(missing_columns)
            raise ValueError(f"Wallet manifest is missing required columns: {missing_text}")

        entries: list[WalletManifestEntry] = []
        for line_number, raw_row in enumerate(reader, start=2):
            if raw_row is None:
                raise ValueError(f"Wallet manifest row {line_number} is malformed")
            if None in raw_row:
                raise ValueError(
                    f"Wallet manifest row {line_number} has too many columns for the header"
                )

            row = {
                _normalize_header_name(key): _normalize_cell_text(value)
                for key, value in raw_row.items()
            }
            if all(value == "" for value in row.values()):
                raise ValueError(f"Wallet manifest row {line_number} is blank")

            wallet = row.get("wallet", "")
            chain = row.get("chain", "").lower()
            label = row.get("label", "")
            if not wallet:
                raise ValueError(f"Wallet manifest row {line_number} has a blank wallet value")
            if chain not in SUPPORTED_MANIFEST_CHAINS:
                supported = ", ".join(sorted(SUPPORTED_MANIFEST_CHAINS))
                raise ValueError(
                    f"Wallet manifest row {line_number} has unsupported chain "
                    f"{chain!r}; expected one of {supported}"
                )
            if not label:
                raise ValueError(f"Wallet manifest row {line_number} has a blank label value")

            entries.append(
                WalletManifestEntry(
                    wallet=wallet,
                    chain=chain,
                    label=label,
                    group=_optional_text(row.get("group")),
                    notes=_optional_text(row.get("notes")),
                    line_number=line_number,
                )
            )

    return tuple(entries)


def fetch_from_wallet_manifest(
    manifest_path: Path,
    *,
    repository_root: Path,
    solana_client: SolanaRpcClient | None = None,
    evm_client: EvmWalletClient | None = None,
    solana_limit: int = 20,
    evm_page: int = 1,
    evm_offset: int = 20,
) -> ManifestFetchRun:
    """Fetch raw wallet snapshots for every validated manifest row."""

    entries = load_wallet_manifest(manifest_path)
    solana_fetcher = solana_client
    evm_fetcher = evm_client
    records: list[ManifestFetchRecord] = []

    for entry in entries:
        chain_directory = repository_root / "data" / "raw" / entry.chain
        wallet_directory = chain_directory / _safe_path_component(entry.label or entry.wallet)
        wallet_directory.mkdir(parents=True, exist_ok=True)

        default_provider = _default_provider_for_chain(entry.chain)
        current_time = _utc_now()
        fetched_at = current_time.isoformat()
        provider = default_provider
        status = "failure"
        error_message: str | None = None
        snapshot_path: Path | None = None

        try:
            if entry.chain == "solana":
                if solana_fetcher is None:
                    solana_fetcher = SolanaRpcClient()
                snapshot = solana_fetcher.fetch_recent_transaction_history(
                    entry.wallet,
                    limit=solana_limit,
                )
            elif entry.chain == "bnb_evm":
                if evm_fetcher is None:
                    evm_fetcher = EvmWalletClient()
                snapshot = evm_fetcher.fetch_recent_transaction_history(
                    entry.wallet,
                    page=evm_page,
                    offset=evm_offset,
                )
            else:
                raise ValueError(f"Unsupported manifest chain: {entry.chain}")

            provider = _extract_provider(snapshot, default_provider=default_provider)
            fetched_at = _extract_fetched_at(snapshot, default_fetched_at=fetched_at)
            snapshot_path = wallet_directory / _snapshot_filename(
                entry=entry,
                fetched_at=fetched_at,
            )
            _write_json(snapshot_path, snapshot)
            status = "success"
        except Exception as exc:  # pragma: no cover - exercised by tests via failure metadata
            error_message = str(exc)

        metadata_path = wallet_directory / _metadata_filename(
            entry=entry,
            fetched_at=fetched_at,
        )
        metadata_payload = {
            "wallet": entry.wallet,
            "chain": entry.chain,
            "label": entry.label,
            "group": entry.group,
            "notes": entry.notes,
            "provider": provider,
            "fetched_at": fetched_at,
            "status": status,
            "error_message": error_message,
            "line_number": entry.line_number,
            "snapshot_path": (
                _relative_path_text(snapshot_path, repository_root) if snapshot_path else None
            ),
        }
        _write_json(metadata_path, metadata_payload)

        records.append(
            ManifestFetchRecord(
                wallet=entry.wallet,
                chain=entry.chain,
                label=entry.label,
                group=entry.group,
                notes=entry.notes,
                provider=provider,
                fetched_at=fetched_at,
                status=status,
                error_message=error_message,
                snapshot_path=(
                    _relative_path_text(snapshot_path, repository_root)
                    if snapshot_path is not None
                    else None
                ),
                metadata_path=_relative_path_text(metadata_path, repository_root),
                line_number=entry.line_number,
            )
        )

    return ManifestFetchRun(
        manifest_path=_relative_path_text(manifest_path, repository_root),
        records=tuple(records),
    )


def preflight_wallet_manifest(
    manifest_path: Path,
    *,
    repository_root: Path,
) -> ManifestPreflightResult:
    """Check manifest and env readiness without making provider calls."""

    entries = load_wallet_manifest(manifest_path)
    solana_wallet_count = sum(1 for entry in entries if entry.chain == "solana")
    bnb_evm_wallet_count = sum(1 for entry in entries if entry.chain == "bnb_evm")
    helius_api_key_status = get_env_var_status("HELIUS_API_KEY")
    etherscan_api_key_status = get_env_var_status("ETHERSCAN_API_KEY")
    errors: list[str] = []

    if solana_wallet_count == 0:
        errors.append("No Solana wallets were found in the wallet manifest.")
    if solana_wallet_count > 0 and helius_api_key_status != "present":
        errors.append(
            build_missing_env_message(
                "HELIUS_API_KEY",
                purpose_text=(
                    "At least one Solana wallet is present in the manifest, so Solana "
                    "preflight requires HELIUS_API_KEY."
                ),
            )
        )

    return ManifestPreflightResult(
        manifest_path=_relative_path_text(manifest_path, repository_root),
        total_wallet_count=len(entries),
        solana_wallet_count=solana_wallet_count,
        bnb_evm_wallet_count=bnb_evm_wallet_count,
        helius_api_key_status=helius_api_key_status,
        etherscan_api_key_status=etherscan_api_key_status,
        errors=tuple(errors),
    )


def _normalize_header_name(value: str | None) -> str:
    if value is None:
        return ""
    return value.strip().lower()


def _normalize_cell_text(value: str | None) -> str:
    if value is None:
        return ""
    return value.strip()


def _optional_text(value: str | None) -> str | None:
    if value is None or value == "":
        return None
    return value


def _safe_path_component(value: str) -> str:
    cleaned = "".join(
        character if character.isalnum() or character in {"-", "_"} else "_"
        for character in value.strip()
    )
    return cleaned or "wallet"


def _default_provider_for_chain(chain: str) -> str:
    if chain == "solana":
        return "solana_json_rpc"
    if chain == "bnb_evm":
        return "etherscan_v2_multichain"
    raise ValueError(f"Unsupported manifest chain: {chain}")


def _extract_provider(
    snapshot: dict[str, object],
    *,
    default_provider: str,
) -> str:
    source = snapshot.get("source")
    if not isinstance(source, dict):
        return default_provider
    provider = source.get("provider")
    if not isinstance(provider, str) or not provider.strip():
        return default_provider
    return provider


def _extract_fetched_at(
    snapshot: dict[str, object],
    *,
    default_fetched_at: str,
) -> str:
    fetched_at = snapshot.get("fetched_at_utc")
    if not isinstance(fetched_at, str) or not fetched_at.strip():
        return default_fetched_at
    return fetched_at


def _snapshot_filename(*, entry: WalletManifestEntry, fetched_at: str) -> str:
    return (
        f"raw_snapshot_line{entry.line_number}_"
        f"{_safe_path_component(entry.wallet)}_{_timestamp_token(fetched_at)}.json"
    )


def _metadata_filename(*, entry: WalletManifestEntry, fetched_at: str) -> str:
    return (
        f"fetch_metadata_line{entry.line_number}_"
        f"{_safe_path_component(entry.wallet)}_{_timestamp_token(fetched_at)}.json"
    )


def _timestamp_token(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        parsed = _utc_now()
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    else:
        parsed = parsed.astimezone(UTC)
    return parsed.strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _relative_path_text(path: Path, repository_root: Path) -> str:
    try:
        return path.relative_to(repository_root).as_posix()
    except ValueError:
        return str(path)


def _utc_now() -> datetime:
    return datetime.now(UTC)
