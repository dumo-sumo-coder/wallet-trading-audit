"""Import a pasted wallet list into the local manifest conservatively."""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ingestion.manifest import DEFAULT_WALLET_MANIFEST_PATH, load_wallet_manifest  # noqa: E402

MANIFEST_FIELDNAMES = ("wallet", "chain", "label", "group", "notes")
EVM_WALLET_PATTERN = re.compile(r"^0x[a-fA-F0-9]{40}$")
SOLANA_WALLET_PATTERN = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


@dataclass(frozen=True, slots=True)
class WalletImportDecision:
    wallet: str
    chain: str
    status: str
    label: str | None


@dataclass(frozen=True, slots=True)
class WalletImportSummary:
    manifest_path: str
    total_wallets_provided: int
    unique_wallets_provided: int
    added_wallets_count: int
    existing_wallets_count: int
    solana_count: int
    bnb_evm_count: int
    unknown_count: int
    added_wallets: tuple[WalletImportDecision, ...]
    existing_wallets: tuple[WalletImportDecision, ...]
    unknown_wallets: tuple[str, ...]
    manifest_updated: bool


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Import a pasted wallet list into data/wallet_manifest.csv without "
            "duplicating existing rows or forcing unknown wallets into a chain."
        ),
    )
    parser.add_argument(
        "--manifest-path",
        default=str(ROOT / DEFAULT_WALLET_MANIFEST_PATH),
        help="Wallet manifest CSV to update.",
    )
    parser.add_argument(
        "--wallet",
        action="append",
        default=[],
        help="Wallet address to import. Repeat for multiple wallets.",
    )
    parser.add_argument(
        "--wallet-list-file",
        default=None,
        help="Optional text file containing one wallet per line.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the import summary as JSON.",
    )
    return parser.parse_args(argv)


def import_wallet_list(
    wallet_lines: Sequence[str],
    *,
    manifest_path: Path,
) -> WalletImportSummary:
    existing_rows, fieldnames = _load_manifest_rows(manifest_path)
    existing_wallets = {row.get("wallet", "").strip() for row in existing_rows if row.get("wallet")}
    deduplicated_wallets = dedupe_wallets_preserving_order(wallet_lines)

    new_rows: list[dict[str, str]] = []
    added_wallets: list[WalletImportDecision] = []
    existing_wallet_decisions: list[WalletImportDecision] = []
    unknown_wallets: list[str] = []
    next_solana_index = _next_import_index(existing_rows, prefix="imported_solana_")
    next_bnb_index = _next_import_index(existing_rows, prefix="imported_bnb_evm_")
    solana_count = 0
    bnb_evm_count = 0
    unknown_count = 0

    for order_index, wallet in enumerate(deduplicated_wallets, start=1):
        chain = classify_wallet_conservatively(wallet)
        if chain == "solana":
            solana_count += 1
        elif chain == "bnb_evm":
            bnb_evm_count += 1
        else:
            unknown_count += 1
            unknown_wallets.append(wallet)
            continue

        if wallet in existing_wallets:
            existing_row = next(row for row in existing_rows if row.get("wallet", "").strip() == wallet)
            existing_wallet_decisions.append(
                WalletImportDecision(
                    wallet=wallet,
                    chain=chain,
                    status="existing",
                    label=(existing_row.get("label") or "").strip() or None,
                )
            )
            continue

        if chain == "solana":
            label = f"imported_solana_{next_solana_index:02d}"
            next_solana_index += 1
        else:
            label = f"imported_bnb_evm_{next_bnb_index:02d}"
            next_bnb_index += 1

        new_rows.append(
            {
                "wallet": wallet,
                "chain": chain,
                "label": label,
                "group": "Imported",
                "notes": f"Imported from wallet list; import_order={order_index:02d}",
            }
        )
        added_wallets.append(
            WalletImportDecision(
                wallet=wallet,
                chain=chain,
                status="added",
                label=label,
            )
        )

    manifest_updated = bool(new_rows)
    if manifest_updated:
        _write_manifest_rows(
            manifest_path,
            fieldnames=fieldnames,
            rows=(*existing_rows, *new_rows),
        )
        load_wallet_manifest(manifest_path)

    return WalletImportSummary(
        manifest_path=_relative_path_text(manifest_path),
        total_wallets_provided=len([line for line in wallet_lines if line.strip()]),
        unique_wallets_provided=len(deduplicated_wallets),
        added_wallets_count=len(added_wallets),
        existing_wallets_count=len(existing_wallet_decisions),
        solana_count=solana_count,
        bnb_evm_count=bnb_evm_count,
        unknown_count=unknown_count,
        added_wallets=tuple(added_wallets),
        existing_wallets=tuple(existing_wallet_decisions),
        unknown_wallets=tuple(unknown_wallets),
        manifest_updated=manifest_updated,
    )


def classify_wallet_conservatively(wallet: str) -> str:
    trimmed_wallet = wallet.strip()
    if EVM_WALLET_PATTERN.fullmatch(trimmed_wallet):
        return "bnb_evm"
    if SOLANA_WALLET_PATTERN.fullmatch(trimmed_wallet):
        return "solana"
    return "unknown"


def dedupe_wallets_preserving_order(wallet_lines: Sequence[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    deduplicated_wallets: list[str] = []
    for raw_wallet in wallet_lines:
        wallet = raw_wallet.strip()
        if not wallet or wallet in seen:
            continue
        seen.add(wallet)
        deduplicated_wallets.append(wallet)
    return tuple(deduplicated_wallets)


def parse_wallet_lines_from_text(text: str) -> tuple[str, ...]:
    return tuple(
        line.strip()
        for line in text.splitlines()
        if line.strip()
    )


def read_wallet_lines_from_args(args: argparse.Namespace) -> tuple[str, ...]:
    wallet_lines = list(args.wallet)
    if args.wallet_list_file:
        wallet_lines.extend(
            parse_wallet_lines_from_text(
                Path(args.wallet_list_file).read_text(encoding="utf-8")
            )
        )
    elif not sys.stdin.isatty():
        wallet_lines.extend(parse_wallet_lines_from_text(sys.stdin.read()))
    return tuple(wallet_lines)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    wallet_lines = read_wallet_lines_from_args(args)
    if not wallet_lines:
        print("ERROR: Provide wallets with --wallet, --wallet-list-file, or stdin.", file=sys.stderr)
        return 1

    try:
        summary = import_wallet_list(wallet_lines, manifest_path=Path(args.manifest_path))
    except (FileNotFoundError, ValueError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(_jsonify(asdict(summary)), indent=2))
        return 0

    print(f"Manifest: {summary.manifest_path}")
    print(f"Total wallets provided: {summary.total_wallets_provided}")
    print(f"Unique wallets reviewed: {summary.unique_wallets_provided}")
    print(f"Added wallets: {summary.added_wallets_count}")
    print(f"Already existing wallets: {summary.existing_wallets_count}")
    print(f"Solana count: {summary.solana_count}")
    print(f"BNB EVM count: {summary.bnb_evm_count}")
    print(f"Unknown count: {summary.unknown_count}")
    if summary.added_wallets:
        print("Added:")
        for item in summary.added_wallets:
            print(f"  {item.wallet} -> {item.chain} ({item.label})")
    if summary.existing_wallets:
        print("Already present:")
        for item in summary.existing_wallets:
            print(f"  {item.wallet} -> {item.chain} ({item.label})")
    if summary.unknown_wallets:
        print("Unknown wallets for review:")
        for wallet in summary.unknown_wallets:
            print(f"  {wallet}")
    return 0


def _load_manifest_rows(manifest_path: Path) -> tuple[tuple[dict[str, str], ...], tuple[str, ...]]:
    if not manifest_path.exists():
        raise FileNotFoundError(f"Wallet manifest not found: {manifest_path}")

    with manifest_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("Wallet manifest is empty")
        fieldnames = tuple(reader.fieldnames)
        rows = []
        for row in reader:
            if row is None:
                continue
            rows.append(
                {
                    fieldname: (row.get(fieldname, "") or "")
                    for fieldname in fieldnames
                }
            )
    return tuple(rows), fieldnames


def _write_manifest_rows(
    manifest_path: Path,
    *,
    fieldnames: tuple[str, ...],
    rows: Sequence[dict[str, str]],
) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def _next_import_index(rows: Sequence[dict[str, str]], *, prefix: str) -> int:
    max_index = 0
    for row in rows:
        label = (row.get("label") or "").strip().lower()
        if not label.startswith(prefix):
            continue
        suffix = label.removeprefix(prefix)
        if suffix.isdigit():
            max_index = max(max_index, int(suffix))
    return max_index + 1


def _relative_path_text(path: Path) -> str:
    try:
        return path.relative_to(ROOT).as_posix()
    except ValueError:
        return str(path)


def _jsonify(value: object) -> object:
    if isinstance(value, dict):
        return {str(key): _jsonify(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_jsonify(item) for item in value]
    if isinstance(value, list):
        return [_jsonify(item) for item in value]
    return value


if __name__ == "__main__":
    raise SystemExit(main())
