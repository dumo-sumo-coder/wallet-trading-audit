# Portfolio Subset Rules Report

Included wallets: 3
Original realized PnL: -178.2626952807300000000000001

## Top Repeated Leak Patterns
- 1. Losses were concentrated in a small token subset [token/setup blacklist threshold] (estimated drag 191.2865116887700000000000001, repeated in 3/3 wallets)
  Evidence: Top losing tokens contributed 0.4198731840658850587271171884 of total losses; the heaviest leaks included HFHMybtPneaeEMrgyuzNLhrxtUBV3MyUqJBr8sc8pump, Hxrn87ryK5vhx4PSrTytV2UZDevCk7VRdJGD2Dpopump, FM7cEUqXbkV2dbUyENA82xMkduUkY1gpbJeMVPhhpump. Positive improvement was observed in 3/3 included wallets.
- 2. Higher-cost entries lost money disproportionately [max cost basis] (estimated drag 178.2626952807300000000000001, repeated in 3/3 wallets)
  Evidence: Removing 43 trades above the tested cost-basis threshold would have changed realized PnL by 178.2626952807300000000000001. Positive improvement was observed in 3/3 included wallets.
- 3. Fast rotations were a recurring leak [hold-time floor] (estimated drag 105.8335215505200000000000002, repeated in 2/3 wallets)
  Evidence: 20 trades in the matched fast-rotation bucket contributed -105.8335215505200000000000000. Positive improvement was observed in 2/3 included wallets.

## Top Candidate Rules
- 1. Blacklist tokens after cumulative matched-trade loss exceeds $3 [token/setup blacklist threshold] (+191.2865116887700000000000001 to 13.02381640804000000000000000; improved 3/3 wallets)
  Rationale: Losses were concentrated enough that removing 29 trades across 20 flagged tokens, led by HFHMybtPneaeEMrgyuzNLhrxtUBV3MyUqJBr8sc8pump, would have improved PnL by 191.2865116887700000000000001.
- 2. Avoid entries above $5 cost basis [max cost basis] (+178.2626952807300000000000001 to 0.000000; improved 3/3 wallets)
  Rationale: Removing 43 higher-notional trades would have improved PnL by 178.2626952807300000000000001.
- 3. Require a minimum hold time of 5m [hold-time floor] (+105.8335215505200000000000002 to -72.42917373020999999999999994; improved 2/3 wallets)
  Rationale: 20 trades inside the tested fast-rotation bucket contributed -105.8335215505200000000000000, and removing them would have improved PnL by 105.8335215505200000000000002.

## Explore Next
- daily stop / losing-streak stop: the wallet hit a long losing streak and average PnL after prior losses stayed negative; simulate this next before adopting it.

## Cautions
- These recommendations are retrospective and based on a 3-wallet recent subset, not the full wallet manifest; they may overfit this sample.
- Excluded-trade simulations assume the remaining matched trades are unchanged, which may not hold in live trading.
- Supported-subset caveats, unsupported raw transactions, and skipped FIFO rows remain outside these coaching rules.
