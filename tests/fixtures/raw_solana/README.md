This directory holds stable raw Solana fixture files for review-only tests.

Files here are provider-native shaped JSON and are intentionally not normalized.
The checked-in snapshot envelope matches this repo's Solana ingestion format, and
the included `getTransaction` response body mirrors the official Solana RPC
example structure closely enough to exercise field-presence review helpers.

The additional buy, sell, and transfer-like fixtures reuse the same raw
`getTransaction` shape with deterministic balance deltas so Solana normalization
tests can stay fixture-driven without inventing new provider field names.

TODO:
- Replace or supplement these fixtures with a live wallet snapshot exported by
  `scripts/fetch_raw_wallet_snapshot.py --chain solana --copy-solana-payload-fixtures`
  once a public test wallet is chosen for long-term fixture refreshes.
- Add additional fixtures for real swap and transfer flows before attempting
  broader Solana normalization from raw provider payloads.
