# Data Acquisition Issue Specifications

This document converts the production data acquisition design into execution-ready work items.

Primary design reference:

- `docs/data-acquisition-implementation.md`

## DA-001 Add Raw Event Spine

### Goal

Create the common ingestion backbone for production collectors.

### Problem

Feature computation cannot be made replayable or reliable until source events are stored with idempotent keys, event-time metadata, and cursor checkpoints.

### Tasks

1. Add a raw event storage contract for `raw_events`.
2. Add cursor checkpoint storage for collectors.
3. Add a write path that enforces `(source_name, source_event_id)` idempotency.
4. Add source metadata fields such as `source_type`, `observed_at`, `ingested_at`, `cursor`, and `schema_version`.
5. Add helper APIs for collector writes and replay reads.

### Deliverables

- raw event storage module
- checkpoint storage module
- schema or migration spec for `raw_events`

### Acceptance Criteria

1. Source events can be written twice without duplication.
2. Replay code can read raw events ordered by event-time.
3. A collector restart can resume from persisted cursor state.

### Dependencies

- none

### Suggested Output Files

- `infra/raw_event_store.py`
- `infra/checkpoints.py`
- storage schema or migration file

---

## DA-002 Add On-Chain Trade Collector

### Goal

Collect replayable on-chain trade events from the primary DEX chain.

### Problem

`buy_pressure` and wallet-derived features depend on a reliable stream of trade facts. The repository currently has only normalized event builders, not live collectors.

### Tasks

1. Add a collector for swap or log events.
2. Add reconnect and backfill behavior.
3. Add deterministic source ids from transaction hash and log index.
4. Persist raw source payloads into `raw_events`.
5. Add metrics for lag, reconnect count, and dropped events.

### Deliverables

- on-chain trade collector
- collector tests with dedupe and reconnect coverage

### Acceptance Criteria

1. Collector emits replayable `raw_events` rows for on-chain trades.
2. Duplicate source events do not create duplicate writes.
3. Collector emits health metrics for freshness and failures.

### Dependencies

- DA-001

### Suggested Output Files

- `sentinel/onchain_collector.py`
- `tests/test_onchain_collector.py`

---

## DA-003 Add DEX Quote Collector

### Goal

Collect live quote samples required for slippage estimation.

### Problem

`estimated_slippage_bps` cannot be trusted until route quotes are sampled, stored, and replayable.

### Tasks

1. Add a DEX quote collector for standard notional sizes.
2. Persist quote samples into `raw_events` and `dex_quote_samples`.
3. Add quote freshness metadata and route diagnostics.
4. Add fallback handling when quote sources are slow or unavailable.

### Deliverables

- quote collector
- quote sample store
- quote diagnostics tests

### Acceptance Criteria

1. Quote samples are available by chain, token, and notional.
2. Quote events carry route diagnostics and freshness metadata.
3. Missing quotes degrade explicitly instead of silently returning zero slippage.

### Dependencies

- DA-001

### Suggested Output Files

- `sentinel/dex_quote_collector.py`
- `tests/test_dex_quote_collector.py`

---

## DA-004 Add Trade And Quote Derived Stores

### Goal

Create the derived-state tables needed for trade windows and slippage curves.

### Problem

Raw source payloads are necessary but insufficient. Feature builders need stable derived stores for classified trades and quote samples.

### Tasks

1. Add `dex_trade_facts` storage.
2. Add `token_trade_windows` storage.
3. Add `dex_quote_samples` storage.
4. Add `slippage_curves` storage.
5. Add write helpers and read models for these tables.

### Deliverables

- derived feature store modules
- table schema specs or migrations

### Acceptance Criteria

1. Trade and quote aggregators can update derived state incrementally.
2. Replay code can reconstruct feature outputs from raw events plus derived stores.

### Dependencies

- DA-001
- DA-002
- DA-003

### Suggested Output Files

- `infra/feature_store.py`
- storage schema or migration file

---

## DA-005 Publish `buy_pressure`

### Goal

Produce a reliable `buy_pressure` feature from event-time trade windows.

### Problem

The current signal pipeline accepts `buy_pressure` as an already-computed input. That is not sufficient for production.

### Tasks

1. Classify trade side into `buy` or `sell`.
2. Update rolling USD notional windows.
3. Compute `buy_pressure` for configured windows.
4. Publish feature snapshots with `sample_count`, `freshness_seconds`, and `quality_flag`.

### Deliverables

- trade classifier
- trade window aggregator
- feature publication path for `buy_pressure`

### Acceptance Criteria

1. `buy_pressure` is derived from trade windows rather than passed through manually.
2. Snapshots include quality metadata.
3. Replay can reproduce the same result from raw inputs.

### Dependencies

- DA-002
- DA-004

### Suggested Output Files

- `sentinel/feature_aggregator.py`
- `tests/test_buy_pressure_aggregator.py`

---

## DA-006 Publish `estimated_slippage_bps`

### Goal

Produce a reliable slippage estimate from quote samples and curve fitting.

### Problem

The signal pipeline currently treats `estimated_slippage_bps` as a supplied field. Production execution readiness depends on a real quote-based estimate.

### Tasks

1. Persist quote samples for standard notionals.
2. Build a slippage curve by chain and token.
3. Add optional OKX Market Price or Index Price reference-mid ingestion for quote sanity checks.
4. Produce feature snapshots for the selected signal notional basis.
5. Publish diagnostics and freshness metadata.

### Deliverables

- slippage estimator
- curve builder
- feature publication path for `estimated_slippage_bps`

### Acceptance Criteria

1. Slippage is computed from quote samples or an explicit interpolated fallback.
2. OKX price feeds, if enabled, are used only as reference diagnostics and never as the sole slippage source.
3. Snapshots expose route diagnostics and freshness.
4. Replay reproduces the same estimate.

### Dependencies

- DA-003
- DA-004

### Suggested Output Files

- `sentinel/feature_aggregator.py`
- `tests/test_slippage_estimator.py`

---

## DA-007 Add Holder Growth Pipeline

### Goal

Produce `holder_growth_15m` from incrementally maintained holder state.

### Problem

Holder growth cannot be trusted if it is computed from ad hoc scans or test fixtures.

### Tasks

1. Maintain token holder state from balance deltas.
2. Deduplicate owner addresses.
3. Add optional OKX Token API holder statistics checks to detect divergence and accelerate cold-start baselines.
4. Add 15-minute holder snapshots.
5. Publish normalized `holder_growth_15m` snapshots.

### Deliverables

- holder-state updater
- holder snapshot store
- holder growth publisher

### Acceptance Criteria

1. Holder counts are maintained incrementally.
2. Dust balances are excluded by policy.
3. OKX holder statistics, if enabled, are stored as secondary diagnostics rather than replacing internal holder state.
4. Snapshot quality degrades explicitly when holder state is stale.

### Dependencies

- DA-001

### Suggested Output Files

- `sentinel/holder_state_updater.py`
- `tests/test_holder_growth_pipeline.py`

---

## DA-008 Add Wallet Intelligence Pipeline

### Goal

Produce `wallet_inflow_score` and `wallet_outflow_score` from tracked wallet behavior.

### Problem

Wallet signals depend on a governed wallet registry and trade-derived wallet flows. The user wants fast bootstrap, so the registry cannot rely only on slow organic self-discovery.

### Tasks

1. Add tracked wallet registry storage with source provenance fields.
2. Build an OKX Strategy API leaderboard connector for wallet discovery across configured chain, timeframe, sort, and wallet-type combinations.
3. Add OKX Address Analysis, Balance, or Tx History refresh jobs for tracked-wallet enrichment.
4. Build wallet-token flow facts from trade facts.
5. Compute weighted inflow and outflow windows.
6. Publish wallet score snapshots with registry version and sample diagnostics.

Implementation note:

- wallet score snapshots now preserve `sample_count`, `quality_flag`, `registry_version`, and `freshness_seconds` in the emitted event payload

### Deliverables

- tracked wallet registry
- OKX leaderboard importer
- tracked wallet refresh jobs
- wallet flow aggregator
- wallet score publisher

### Acceptance Criteria

1. OKX-discovered wallets are persisted with source metadata that records wallet type, timeframe, sort key, and observation time.
2. Wallet scores are derived from registry-weighted flows rather than directly from OKX response values.
3. Registry versions are persisted and auditable.
4. Low-sample situations are flagged in quality metadata.
5. Stale or unavailable OKX refreshes degrade registry freshness but do not silently erase active tracked wallets.

### Dependencies

- DA-002
- DA-004

### Suggested Output Files

- `sentinel/okx_wallet_registry_importer.py`
- `sentinel/wallet_score_aggregator.py`
- `tests/test_wallet_score_pipeline.py`

---

## DA-009 Add Venue Announcement Pipeline

### Goal

Produce `cex_listing_confirmed` from official venue announcement sources.

### Problem

The repository currently accepts this field as a boolean input. Production use requires official-source confirmation and provenance.

### Tasks

1. Add official announcement collectors.
2. Normalize and persist announcement content.
3. Resolve confirmed listing events into `listing_confirmations`.
4. Publish `cex_listing_confirmed` snapshots.

### Deliverables

- announcement collector
- listing confirmation normalizer
- feature publication path for `cex_listing_confirmed`

### Acceptance Criteria

1. `cex_listing_confirmed` is only set from allowlisted official sources.
2. Announcement URL and confirmation provenance are retained.
3. Fetch failures degrade to stale or unknown rather than false.

### Dependencies

- DA-001

### Suggested Output Files

- `sentinel/announcement_collector.py`
- `tests/test_listing_confirmation_pipeline.py`

---

## DA-010 Add Social Feature Pipeline

### Goal

Produce `social_sentiment` and `social_velocity` from deduplicated social mention streams.

### Problem

Social features are highly noise-sensitive and require platform-aware dedupe, mention resolution, and weighted scoring.

### Tasks

1. Add social collectors.
2. Normalize posts and resolve token mentions.
3. Deduplicate repost and spam patterns.
4. Compute sentiment and velocity windows.
5. Publish quality-aware feature snapshots.

### Deliverables

- social collector
- mention resolver
- social scoring engine

### Acceptance Criteria

1. Platform posts are persisted as replayable raw events.
2. Mention resolution is versioned.
3. Sentiment and velocity snapshots include freshness and quality metadata.

### Dependencies

- DA-001

### Suggested Output Files

- `sentinel/social_collector.py`
- `tests/test_social_feature_pipeline.py`
