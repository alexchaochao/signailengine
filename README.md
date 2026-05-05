# SignalEngine

SignalEngine is a staged Web3 hybrid trading platform built around deterministic signals, explicit lifecycle state, risk gating, and replay-ready execution records.

The current repository covers Phase 0 and Phase 1 foundation work.

## Included In This Revision

- Phase 0 decision records
- canonical schema documentation
- configuration strategy and risk policy
- Python project baseline
- repository scaffold
- settings loader
- Redis and PostgreSQL helpers
- logging and metrics helpers
- local Docker Compose stack
- smoke tests

## Local Setup

1. Create a virtual environment with Python 3.12.
2. Install dependencies:

```bash
pip install -e .[dev]
```

3. Copy environment defaults if needed:

```bash
cp .env.example .env
```

4. Start local infrastructure:

```bash
docker compose up -d
```

5. Run smoke tests:

```bash
pytest
```

6. Run lint and type checks:

```bash
ruff check .
mypy
```

7. Run the worker once against local Redis:

```bash
signalengine-worker --once --no-db
```

8. Run wallet intelligence sync once:

```bash
signalengine-worker --wallet-intelligence-sync --once
```

9. Run on-chain feature backfill from a JSONL file:

```bash
signalengine-worker --once --onchain-feature-backfill replay/datasets/onchain_feature_backfill.jsonl
```

10. Run on-chain feature backfill in loop mode against an append-only JSONL file:

```bash
signalengine-worker --onchain-feature-backfill replay/datasets/onchain_feature_backfill.jsonl --sleep-seconds 5
```

11. Run on-chain feature live sync once from configured sources:

```bash
signalengine-worker --onchain-feature-live --once
```

12. Run on-chain feature live sync continuously from configured sources:

```bash
signalengine-worker --onchain-feature-live
```

13. Probe launch alpha live sources once without writing pipeline state:

```bash
signalengine-launch-alpha --chain solana --limit 5 --json
```

14. Run a launch alpha end-to-end rehearsal through Redis streams and the worker:

```bash
signalengine-launch-alpha-rehearsal --json
```

15. Run catalyst alpha live sync once from configured announcement feeds:

```bash
signalengine-worker --catalyst-alpha-live --once
```

## Project Layout

```text
signalengine/
├── core/
├── docs/
├── execution/
├── infra/
├── portfolio/
├── replay/
├── sentinel/
└── tests/
```

## Current Status

- the repository has moved well beyond the original Phase 0 and Phase 1 foundation scope and now includes deterministic paper execution, multi-source alpha ingestion, EVM quote and trade acquisition, and Telegram notification delivery for first-time qualified alpha candidates
- Telegram publication is wired behind `alpha.candidate_qualified` events and currently publishes only first-time `QUALIFIED` `LAUNCH` and `CATALYST` candidates to one configured chat
- live trading remains guarded behind environment and rollout controls; local credentials may enable live paths, but production hardening is still incomplete
- social input remains effectively disabled as a first-class live signal source; message-surface coverage currently centers on exchange announcement and wallet-flow style inputs rather than broad social ingestion
- the active next engineering items are persistent FSM state storage, message-source expansion, Alibaba Cloud production deployment planning, and observability hardening
- no background worker processes are intentionally left running by default in the current handoff state; start only the workers you need for the next task

## Pipeline Runtime

- [core/worker.py](core/worker.py) provides a CLI worker entrypoint
- [execution/reconciliation.py](execution/reconciliation.py) updates in-memory position and portfolio state after paper fills
- [infra/postgres.py](infra/postgres.py) initializes simple storage tables and persists pipeline outputs

The worker now also:

- emits structured JSON logs
- exposes Prometheus metrics on the configured metrics host and port
- reloads `positions` and `portfolio_state` from PostgreSQL before processing each token batch when DB mode is enabled
- supports a dedicated wallet intelligence sync mode that imports OKX leaderboard addresses, refreshes tracked wallets, projects onchain trade flows, and republishes `wallet.cluster_snapshot`
- supports a dedicated on-chain feature backfill mode that ingests JSONL trade and quote records, persists `raw_events`, rebuilds Phase A feature snapshots, and republishes normalized `onchain.liquidity_snapshot`
- supports a dedicated on-chain feature live mode that polls configured real sources, persists normalized raw events, rebuilds feature snapshots, and republishes normalized `onchain.liquidity_snapshot`
- supports a dedicated Telegram publisher worker mode that consumes `alpha.candidate_qualified` events from `raw-events`, deduplicates by candidate and channel, and delivers first-time qualified notifications to Telegram

## Current Handoff

This repository state is ready for the next session to pick up without replaying prior chat context.

Delivered recently:

- quote-derived EVM market context now propagates into published on-chain liquidity snapshots so quote-only routes no longer default `volume_5m_usd` and `buy_pressure` to zero
- catalyst live sources were updated to use machine-readable Binance CMS data and Coinbase HTML fallback parsing instead of assuming valid RSS everywhere
- `alpha.candidate_qualified` is now published when a launch, catalyst, or flow candidate first transitions into `QUALIFIED`
- Telegram delivery is implemented with one chat target, synchronous plain-text templates, persistent deduplication in PostgreSQL, and a worker mode under `signalengine-worker --telegram-publisher-live`
- Telegram smoke testing was completed successfully against the configured bot and chat using a synthetic qualified launch candidate

Open next items:

- implement persistent FSM state storage so `StateEngine` transitions from prior state instead of recalculating from `UNKNOWN` on every batch
- add focused tests around persistent FSM runtime state and pipeline integration
- expand message-source coverage beyond exchange announcement feeds, starting with higher-signal external sources such as X and Reddit
- plan and harden Alibaba Cloud production deployment, including process supervision, externalized infrastructure, and observability

Operational notes for the next session:

- the workspace is not a git repository, so status and diff review rely on direct file inspection rather than git commands
- stale pid files were cleared during resource cleanup and no `python -m core.worker` background processes were intentionally left running in the last local cleanup pass
- if you need a minimal runtime for local work, start only the specific worker under test instead of the full stack

## On-Chain Feature Backfill

The Phase A acquisition slice now has a service entrypoint under [sentinel/onchain_feature_sync.py](sentinel/onchain_feature_sync.py).

Operational notes:

- `signalengine-worker --onchain-feature-backfill <path>` reads JSONL records with `source_type` and `payload`
- without `--once`, the worker reruns the same file on the configured poll interval; raw-event idempotency makes this safe for append-only files
- supported `source_type` values are `onchain_trade` and `dex_quote`
- each ingested record runs through collector -> derived store -> feature snapshot -> normalized event publish
- published normalized events land on `raw-events` as `onchain.liquidity_snapshot`, so the existing pipeline worker can consume them without new routing logic

## Local Paper Strategy Validation

The on-chain paper path now works directly from published feature snapshots.

Operational notes:

- `sentinel/onchain_feature_publisher.py` derives `volume_5m_usd` from buy and sell notional inputs and derives a conservative `liquidity_usd` proxy from quote slippage samples so `SignalEngine`, `Router`, and `RiskEngine` can make a deterministic paper decision from a single normalized event
- `core/signal_engine.py` now scores `feature_quality` explicitly, so stale or missing on-chain features reduce readiness instead of silently behaving like valid data
- deteriorating on-chain execution conditions can now transition an already open paper position into `DISTRIBUTION`, route `DEX_EXIT` or `CEX_EXIT`, and reconcile the position closed without being blocked by entry-only liquidity gates
- the shortest executable validation is `.venv/bin/pytest tests/test_onchain_feature_publisher.py tests/test_signal_engine.py`

Local run sequence:

1. Start Redis and PostgreSQL with `docker compose up -d`.
2. Ingest a replay dataset into feature storage with `signalengine-worker --once --onchain-feature-backfill replay/datasets/onchain_feature_backfill.jsonl`.
3. Run the pipeline worker once with `signalengine-worker --once` to consume the normalized `onchain.liquidity_snapshot` event and generate the paper routing and execution records.

Result verification:

1. Check Redis stream growth with `redis-cli XLEN raw-events`, `redis-cli XLEN signals`, `redis-cli XLEN decisions`, and `redis-cli XLEN executions`.
2. Inspect the latest paper execution with `redis-cli XREVRANGE executions + - COUNT 1`.
3. Check persisted pipeline outputs with `psql "$SIGNALENGINE_POSTGRES__URL" -c "SELECT token, created_at FROM token_signals ORDER BY id DESC LIMIT 5;"`.
4. Check the latest route and risk decisions with `psql "$SIGNALENGINE_POSTGRES__URL" -c "SELECT token, payload FROM route_decisions ORDER BY id DESC LIMIT 1;"` and `psql "$SIGNALENGINE_POSTGRES__URL" -c "SELECT intent_id, payload FROM risk_decisions ORDER BY id DESC LIMIT 1;"`.
5. Check the paper execution and final position state with `psql "$SIGNALENGINE_POSTGRES__URL" -c "SELECT intent_id, created_at FROM execution_reports ORDER BY id DESC LIMIT 5;"`, `psql "$SIGNALENGINE_POSTGRES__URL" -c "SELECT token, is_open, venue_type, token_exposure FROM positions ORDER BY token;"`, and `psql "$SIGNALENGINE_POSTGRES__URL" -c "SELECT total_portfolio_usd, token_exposure, chain_exposure, open_positions FROM portfolio_state;"`.

Fallback when local infra is unavailable:

1. If Docker is unavailable or Redis/PostgreSQL are not running locally, the narrow executable fallback is `.venv/bin/pytest tests/test_onchain_feature_publisher.py tests/test_signal_engine.py tests/test_pipeline.py`.
2. That regression proves both paper entry and paper exit from normalized on-chain feature events, but it does not validate your local Redis/PostgreSQL wiring.

The focused regression tests now prove that a published feature event can reach `DEX_ENTRY` on improving conditions and `DEX_EXIT` on deteriorating conditions, both through the paper execution path without requiring a separate wallet or social event.

## Deterministic Paper Scenarios

For local paper validation without manual `redis-cli XADD`, use the built-in scenario runner:

```bash
.venv/bin/python -m replay.paper_scenario entry --token PAPERBONK --json
.venv/bin/python -m replay.paper_scenario launch-entry --token PAPERLAUNCH --json
.venv/bin/python -m replay.paper_scenario exit --token PAPERBONK_EXIT --json
.venv/bin/python -m replay.paper_scenario roundtrip --token PAPERROUND --json
```

Operational notes:

- `entry` publishes a strong on-chain liquidity snapshot plus a wallet cluster snapshot, then runs the paper pipeline and expects `DEX_ENTRY`
- `launch-entry` publishes a qualified `alpha.launch_candidate` event, then runs the paper pipeline and expects the launch path to reach `DEX_ENTRY`
- `exit` seeds an open DEX position in PostgreSQL, publishes a deteriorating on-chain snapshot, then runs the paper pipeline and expects `DEX_EXIT`
- `roundtrip` executes both steps on the same token, so you can verify open-position and close-position transitions end to end
- the script writes the same Redis streams and PostgreSQL tables used by the worker, so you can inspect `raw-events`, `signals`, `decisions`, `executions`, `positions`, and `execution_reports` immediately after the run

## Launch Alpha Live Probe

Use the live probe to fetch currently qualified launch candidates from configured public sources without writing signals or execution state.

1. Start from the sample config in [infra/settings.launch-alpha.example.yaml](infra/settings.launch-alpha.example.yaml).
2. Point the worker at your desired settings file via environment overrides or by editing [infra/settings.yaml](infra/settings.yaml).
3. Probe Solana once:

```bash
signalengine-launch-alpha --chain solana --limit 5 --json
```

4. Probe Base once:

```bash
signalengine-launch-alpha --chain base --limit 5 --json
```

Operational notes:

- the current live implementation uses DexScreener public APIs: latest token profiles for seed discovery plus token pair details for snapshot conversion
- the transport now includes bounded retries, in-memory response caching, per-source request pacing, and fallback source URLs so repeated worker loops do less redundant work and degrade more cleanly under public API instability
- source-side filtering is applied before any candidate reaches the sync service, so low-liquidity or stale pools are dropped before they hit Redis or PostgreSQL
- the default thresholds were relaxed from the earlier stricter profile so the probe is more useful for observation, but hard filters for stale pools, quote assets, DEX allowlists, lock ratio, and creator concentration are still applied
- `signalengine-worker --launch-alpha-live --once` remains the persistence-enabled path when you want candidates published into `raw-events`

## Catalyst Alpha Live Sync

Use the catalyst live path to turn exchange announcement feeds into normalized `alpha.catalyst_candidate` events.

```bash
signalengine-worker --catalyst-alpha-live --once
```

Operational notes:

- the current live implementation uses RSS or Atom announcement feeds matched against configured token aliases under `acquisition.catalyst_alpha_sources`
- the sample config now includes both Binance and Coinbase feed entries, so a single exchange feed outage no longer means the repository only knows how to poll one real catalyst source
- each source keeps a small rolling checkpoint of seen `source_event_id` values in PostgreSQL, so repeated polls do not republish the same announcement
- matching is explicit and allowlist-based: a feed item must pass source keyword filters and also match one of the configured token aliases before it is converted into a catalyst snapshot
- the sample config includes Binance and Coinbase announcement feed examples in [infra/settings.yaml](infra/settings.yaml) and [infra/settings.launch-alpha.example.yaml](infra/settings.launch-alpha.example.yaml)

## Flow Alpha Backfill

Use the flow backfill path to publish normalized smart-money flow candidates into the existing signal pipeline.

```bash
signalengine-worker --once --flow-alpha-backfill replay/datasets/flow_alpha_backfill.jsonl
```

Operational notes:

- the initial implementation covers schema, scanner, sync service, raw-event persistence, and `alpha.flow_candidate` publication
- the default sample dataset is [replay/datasets/flow_alpha_backfill.jsonl](replay/datasets/flow_alpha_backfill.jsonl)
- the signal engine now consumes `flow_alpha_score` and `flow_candidate_status`, so a qualified flow candidate can directly push a token toward `EARLY_LIQUIDITY` or `NARRATIVE_EXPLOSION`

## Launch Alpha Rehearsal

Use the launch rehearsal command to validate the full local path from launch-candidate ingestion to worker consumption and paper execution.

```bash
signalengine-launch-alpha-rehearsal --dataset replay/datasets/launch_alpha_entry_rehearsal.jsonl --json
```

Operational notes:

- this command ingests a launch alpha dataset through [discovery/service.py](discovery/service.py), publishes the resulting `alpha.launch_candidate` event into Redis, then calls the real worker poll path
- unlike `replay.paper_scenario`, this flow validates Redis stream consumption and consumer-group processing in addition to signal/risk/router behavior

Flow Alpha now has an equivalent local rehearsal for the wallet-intelligence-backed path without requiring live OKX credentials.

```bash
.venv/bin/python -m replay.flow_alpha_rehearsal --json
```

- the rehearsal seeds tracked wallet registry rows and wallet token flows into PostgreSQL, runs the Flow Alpha live source against that seeded state, publishes a supporting `onchain.liquidity_snapshot`, and then polls the main worker once
- use it to verify the `wallet-intelligence store -> alpha.flow_candidate -> worker -> paper execution` path locally before wiring real OKX credentials into `live.wallet_intelligence`
- the default rehearsal dataset is [replay/datasets/launch_alpha_entry_rehearsal.jsonl](replay/datasets/launch_alpha_entry_rehearsal.jsonl)

Recommended local checks after `roundtrip`:

1. `redis-cli --raw XREVRANGE executions + - COUNT 5`
2. `PAGER=cat psql "$SIGNALENGINE_POSTGRES__URL" -c "SELECT intent_id, created_at FROM execution_reports ORDER BY id DESC LIMIT 5;"`
3. `PAGER=cat psql "$SIGNALENGINE_POSTGRES__URL" -c "SELECT token, is_open, venue_type, token_exposure FROM positions ORDER BY token;"`

## On-Chain Feature Live Sync

Live acquisition is configuration-driven under `acquisition` in [infra/settings.yaml](infra/settings.yaml).

Operational notes:

- `signalengine-worker --onchain-feature-live --once` executes one polling cycle against enabled live sources
- `signalengine-worker --onchain-feature-live` loops on `acquisition.sync_interval_seconds`
- `acquisition.solana_wallet_trade` polls Solana RPC for incremental wallet trades and persists its cursor in PostgreSQL checkpoints
- `acquisition.jupiter_quote` polls Jupiter quote and price endpoints and republishes normalized quote-derived liquidity snapshots
- `acquisition.evm_transfer_trade` is now a legacy compatibility entry; when enabled it is projected into the unified EVM route registry during settings load
- `acquisition.evm_chains` stores chain-level defaults such as `chain_id`, quote provider, and quote URLs
- `acquisition.evm_routes` is the multi-chain registry for EVM live sources, with `source_type=transfer_trade|pool_swap_trade|quote`
- repeated live-source failures now trigger source-level retry backoff and then cooldown, with state persisted in PostgreSQL checkpoints
- when a live source reaches `observability.max_consecutive_live_source_failures`, the worker emits `live_source_failure_threshold_exceeded`

Solana trade source modes:

- `source_kind=wallet`: use `wallet_address` as both the signature-watch address and owner filter
- `source_kind=address`: use `signature_address` to watch a pool or program-linked address, and optionally set `owner_address` to keep owner-filtered balance deltas

EVM trade source mode:

- if you still configure `acquisition.evm_transfer_trade`, it is treated as a legacy alias and projected into `acquisition.evm_routes.evm_transfer_trade`
- the current transfer-trade implementation infers trades from ERC20 `Transfer` deltas for the watched wallet, so it is suitable for wallet-level production flow tracking but not yet a full pool-wide swap decoder

EVM registry source modes:

- `source_type=transfer_trade`: wallet-level transfer inference, including the legacy `acquisition.evm_transfer_trade` compatibility path after projection into the registry
- `source_type=pool_swap_trade`: Uniswap V2, Uniswap V3, and Aerodrome CL style pool swap decoding against a configured `pool_address`, for pool-level buy/sell flow
- `source_type=quote`: provider-specific EVM quote polling, currently supporting 0x GET quotes and Odos POST quotes, plus DexScreener token pricing for normalized `dex_quote` inputs
- chain-level defaults live under `acquisition.evm_chains.<chain>`, while token and pair-specific settings live under `acquisition.evm_routes.<name>`
- each route entry is keyed under `acquisition.evm_routes.<name>` and can target a different chain in `venues.native_asset_rpc`
- V3 and CL-style pool sources also emit `route_diagnostics` fields such as `sqrt_price_x96`, `liquidity`, and `tick`, which are preserved in raw events for replay and debugging

## Wallet Intelligence Sync

Wallet intelligence sync is now configuration-driven under `live.wallet_intelligence` in [infra/settings.yaml](infra/settings.yaml).

Operational notes:

- run once for manual backfills: `signalengine-worker --wallet-intelligence-sync --once`
- run continuously for production sync: `signalengine-worker --wallet-intelligence-sync`
- if you already have tracked wallets from another source, project `onchain.trade_fact` into `wallet_token_flows` without calling OKX: `signalengine-worker --wallet-flow-project --once --wallet-chain <chain> --wallet-token <token>`
- sync state persists the last processed raw-event id in PostgreSQL, so repeated runs resume from the last watermark instead of rescanning from `0-0`
- CLI flags still override config for chain, token, timeframe, sort, wallet type, refresh limit, and raw-event batch size
- the new `--wallet-flow-project` path reuses the same cursor state and wallet cluster snapshot publication logic, but skips OKX import and wallet refresh so you can plug in a manual registry or your own smart-wallet database

## External APIs And Credentials

Current external integrations and where to configure them:

- OKX Web3 API for wallet intelligence: set `SIGNALENGINE_LIVE__CREDENTIALS__DEX_PROVIDERS__OKX__API_KEY`, `SIGNALENGINE_LIVE__CREDENTIALS__DEX_PROVIDERS__OKX__SECRET_KEY`, `SIGNALENGINE_LIVE__CREDENTIALS__DEX_PROVIDERS__OKX__API_PASSPHRASE`, and optionally `SIGNALENGINE_LIVE__CREDENTIALS__DEX_PROVIDERS__OKX__PROJECT_ID`
- Solana RPC for execution context, balances, and live wallet-trade polling: set `SIGNALENGINE_VENUES__SOLANA_RPC_URL`; if your provider uses an API key, place it in that URL or provider-specific endpoint
- EVM RPC endpoints for native balance sources: set `SIGNALENGINE_VENUES__NATIVE_ASSET_RPC__<CHAIN>__URL`, for example `SIGNALENGINE_VENUES__NATIVE_ASSET_RPC__ETHEREUM__URL`
- 0x quote API for EVM slippage sampling: optionally set `SIGNALENGINE_LIVE__CREDENTIALS__DEX_PROVIDERS__ZEROEX__API_KEY`
- Odos quote API for EVM slippage sampling: optionally set `SIGNALENGINE_LIVE__CREDENTIALS__DEX_PROVIDERS__ODOS__API_KEY`
- Chain wallet addresses for live balance and execution context: set `SIGNALENGINE_LIVE__CREDENTIALS__CHAIN_WALLETS__SOLANA__WALLET_ADDRESS` and chain-specific equivalents
- Jupiter quote source currently uses public endpoints and does not require a key in this implementation
- DexScreener price lookup for EVM quote normalization currently uses public endpoints and does not require a key in this implementation
- CoinGecko and Binance price endpoints are currently configured as public URLs under `live.pricing.native_asset_sources` and do not use injected keys in this repository

Example environment variables:

```bash
export SIGNALENGINE_LIVE__CREDENTIALS__DEX_PROVIDERS__OKX__API_KEY="..."
export SIGNALENGINE_LIVE__CREDENTIALS__DEX_PROVIDERS__OKX__SECRET_KEY="..."
export SIGNALENGINE_LIVE__CREDENTIALS__DEX_PROVIDERS__OKX__API_PASSPHRASE="..."
export SIGNALENGINE_VENUES__SOLANA_RPC_URL="https://your-solana-rpc.example/?api-key=..."
export SIGNALENGINE_ACQUISITION__SOLANA_WALLET_TRADE__ENABLED=true
export SIGNALENGINE_ACQUISITION__SOLANA_WALLET_TRADE__WALLET_ADDRESS="..."
export SIGNALENGINE_ACQUISITION__SOLANA_WALLET_TRADE__TOKEN_MINT="..."
export SIGNALENGINE_ACQUISITION__SOLANA_WALLET_TRADE__QUOTE_MINT="..."
export SIGNALENGINE_ACQUISITION__JUPITER_QUOTE__ENABLED=true
export SIGNALENGINE_ACQUISITION__JUPITER_QUOTE__INPUT_MINT="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
export SIGNALENGINE_ACQUISITION__JUPITER_QUOTE__OUTPUT_MINT="..."
export SIGNALENGINE_ACQUISITION__JUPITER_QUOTE_ROUTES__SOLANA_BONK__CHAIN="solana"
export SIGNALENGINE_ACQUISITION__JUPITER_QUOTE_ROUTES__SOLANA_BONK__TOKEN="BONK"
export SIGNALENGINE_ACQUISITION__JUPITER_QUOTE_ROUTES__SOLANA_BONK__INPUT_MINT="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
export SIGNALENGINE_ACQUISITION__JUPITER_QUOTE_ROUTES__SOLANA_BONK__OUTPUT_MINT="..."
export SIGNALENGINE_LIVE__CREDENTIALS__DEX_PROVIDERS__ZEROEX__API_KEY="..."
export SIGNALENGINE_ACQUISITION__EVM_CHAINS__BASE__CHAIN_ID=8453
export SIGNALENGINE_ACQUISITION__EVM_CHAINS__BASE__PROVIDER=evm_quote_api
export SIGNALENGINE_ACQUISITION__EVM_CHAINS__BASE__API_PROVIDER=zeroex
export SIGNALENGINE_ACQUISITION__EVM_CHAINS__BASE__QUOTE_API_URL="https://api.0x.org/swap/permit2/price"
export SIGNALENGINE_ACQUISITION__EVM_CHAINS__BASE__PRICE_URL="https://api.dexscreener.com/latest/dex/tokens"
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_QUOTE__ENABLED=true
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_QUOTE__SOURCE_TYPE=quote
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_QUOTE__CHAIN=base
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_QUOTE__TOKEN=AERO
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_QUOTE__TOKEN_CONTRACT="0x..."
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_QUOTE__QUOTE_CONTRACT="0x..."
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_QUOTE__QUOTE_SLIPPAGE_BPS=100
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_POOL__ENABLED=true
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_POOL__SOURCE_TYPE=pool_swap_trade
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_POOL__CHAIN=base
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_POOL__POOL_ADDRESS="0x..."
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_POOL__POOL_PROTOCOL=uniswap_v3
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_POOL__TOKEN=AERO
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_POOL__TOKEN_CONTRACT="0x..."
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_POOL__QUOTE_CONTRACT="0x..."
export SIGNALENGINE_LIVE__CREDENTIALS__DEX_PROVIDERS__ODOS__API_KEY="..."
export SIGNALENGINE_ACQUISITION__EVM_CHAINS__BASE__API_PROVIDER=odos
export SIGNALENGINE_ACQUISITION__EVM_CHAINS__BASE__QUOTE_API_URL="https://api.odos.xyz/sor/quote/v2"
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_ODOS_QUOTE__ENABLED=true
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_ODOS_QUOTE__SOURCE_TYPE=quote
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_ODOS_QUOTE__CHAIN=base
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_ODOS_QUOTE__TOKEN=AERO
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_ODOS_QUOTE__TOKEN_CONTRACT="0x..."
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_ODOS_QUOTE__QUOTE_CONTRACT="0x..."
```

## Architecture Decisions

ADR templates and future architecture decisions live under [docs/adr](docs/adr).

## Implementation Docs

- [docs/data-acquisition-implementation.md](docs/data-acquisition-implementation.md) defines the production data collection, feature-store, and field-level computation plan for signal inputs.
- [docs/issues-data-acquisition.md](docs/issues-data-acquisition.md) turns the data acquisition program into execution-ready work items.
- [docs/onchain-feature-phase-a.md](docs/onchain-feature-phase-a.md) specifies the first implementation slice for `buy_pressure` and `estimated_slippage_bps`.

## Replay Diagnostics

- `signalengine-replay --feature-replay-db-url ... --feature-chain <chain> --feature-token <token>` now includes snapshot diff input summaries in both text and JSON output
- replay snapshot diffs surface `source_inputs` and `target_inputs`, which makes provider-level quote context like `route_provider`, `path_id`, and other feature inputs visible during regression review

## Local EVM Validation Plan

Use this sequence to test the current EVM acquisition slice locally without needing a full live deployment.

1. Install dependencies and load defaults:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e .[dev]
cp .env.example .env
```

2. Run the focused test slices that cover the current EVM implementation:

```bash
pytest -q tests/test_onchain_live_sources.py tests/test_buy_pressure_aggregator.py tests/test_slippage_aggregator.py tests/test_feature_replay.py tests/test_replay_runner.py
```

3. If you want to exercise the worker path, start local infrastructure:

```bash
docker compose up -d
```

4. Configure one minimal quote source and one pool source in `.env` or your shell:

```bash
export SIGNALENGINE_VENUES__NATIVE_ASSET_RPC__BASE__URL="https://mainnet.base.org"
export SIGNALENGINE_LIVE__CREDENTIALS__DEX_PROVIDERS__ZEROEX__API_KEY="..."
export SIGNALENGINE_ACQUISITION__EVM_CHAINS__BASE__CHAIN_ID=8453
export SIGNALENGINE_ACQUISITION__EVM_CHAINS__BASE__PROVIDER=evm_quote_api
export SIGNALENGINE_ACQUISITION__EVM_CHAINS__BASE__QUOTE_API_URL="https://api.0x.org/swap/permit2/price"
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_QUOTE__ENABLED=true
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_QUOTE__SOURCE_TYPE=quote
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_QUOTE__CHAIN=base
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_QUOTE__TOKEN=AERO
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_QUOTE__TOKEN_CONTRACT="0x..."
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_QUOTE__QUOTE_CONTRACT="0x..."
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_POOL__ENABLED=true
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_POOL__SOURCE_TYPE=pool_swap_trade
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_POOL__CHAIN=base
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_POOL__POOL_PROTOCOL=uniswap_v3
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_POOL__POOL_ADDRESS="0x..."
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_POOL__TOKEN=AERO
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_POOL__TOKEN_CONTRACT="0x..."
export SIGNALENGINE_ACQUISITION__EVM_ROUTES__BASE_POOL__QUOTE_CONTRACT="0x..."
```

5. Run one live polling cycle:

```bash
signalengine-worker --onchain-feature-live --once
```

6. Validate replay visibility from the stored feature snapshots:

```bash
signalengine-replay --feature-replay-db-url "$SIGNALENGINE_POSTGRES__URL" --feature-chain base --feature-token AERO --json
```

What to look for:

- `route_summary.provider`, `path_id`, `gas_estimate`, and Odos route counters on quote-derived snapshots
- `route_diagnostics.sqrt_price_x96`, `liquidity`, and `tick` on V3/CL trade raw events
- `source_inputs` and `target_inputs` in feature replay diff output
- `feature_quality` rows with `curve_fallback`, `stale_quote`, or `low_sample` when you intentionally run with sparse data