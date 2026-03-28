"""Print a structural review of raw Solana snapshot files."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ingestion.solana_review import inspect_solana_snapshot, load_json_mapping  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect raw Solana JSON-RPC snapshots without normalizing them.",
    )
    parser.add_argument(
        "snapshot_paths",
        nargs="+",
        help="One or more raw Solana snapshot JSON files.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    for index, raw_path in enumerate(args.snapshot_paths):
        path = Path(raw_path)
        summary = inspect_solana_snapshot(load_json_mapping(path))
        if index:
            print()
        print(f"# {path}")
        print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
