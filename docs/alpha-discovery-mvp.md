# Alpha Discovery MVP Implementation Plan

## Goal

Add a discovery layer that finds tradeable opportunities without requiring the user to pre-configure a token, wallet, or pool.

This layer must sit in front of the existing feature, signal, risk, and execution pipeline.

The first production-oriented milestone is not full-chain intelligence. It is a deterministic MVP that can:

1. discover new pools
2. filter for meaningful initial liquidity
3. observe the first 5 minutes of flow
4. score the opportunity with explicit rules
5. publish only qualified candidates into the existing paper pipeline

The target behavior is:

```text
new pool detected
-> LP add confirmed > 5k USD
-> first 5 minute flow window collected
-> alpha_score computed
-> qualified candidate published
-> existing pipeline decides paper entry / exit
```

## Current Gap

The current repository is strong in execution-layer plumbing but weak in discovery.

Already implemented well:

- normalized raw event ingestion
- feature aggregation for configured tokens / pools / wallets
- deterministic signal engine
- routing, risk, paper execution, reconciliation
- replay, metrics, alerts

Not implemented yet:

- full-universe opportunity discovery
- new pool scanner
- candidate lifecycle tracking
- 5 minute discovery flow engine
- candidate-level alpha score independent of pre-configured token targets

This document defines the MVP path to close that gap.

## MVP Scope

The MVP must implement only these two modules in full:

1. Opportunity Scanner
2. Flow Engine

The next two modules are deferred to later phases and should only be given interfaces for now:

3. Mempool Engine
4. Entity Engine

### In Scope

- Solana-first implementation
- EVM design compatibility, but not first delivery
- new pool detection
- LP add threshold filter
- 5 minute post-launch flow window
- deterministic alpha score
- integration into existing paper pipeline

### Out of Scope For MVP

- full mempool racing logic
- multi-DEX complete coverage on day one
- learned clustering / GNN / graph ML
- full dev / bot graph attribution
- live DEX execution production hardening
- CEX discovery-driven trading

## Target Architecture

```text
                +-----------------------------------+
                | Opportunity Scanner               |
                | new pools / LP add / metadata     |
                +----------------+------------------+
                                 |
                                 v
                +-----------------------------------+
                | Discovery Candidate Store         |
                | pool_candidates / lifecycle       |
                +----------------+------------------+
                                 |
                                 v
                +-----------------------------------+
                | Flow Engine                        |
                | 5m swaps / buyers / burst stats   |
                +----------------+------------------+
                                 |
                                 v
                +-----------------------------------+
                | Discovery Scoring Engine          |
                | liquidity_quality                 |
                | buyer_diversity                   |
                | flow_intensity                    |
                +----------------+------------------+
                                 |
                                 v
                +-----------------------------------+
                | discovery.discovery_snapshot      |
                +----------------+------------------+
                                 |
                                 v
                +-----------------------------------+
                | Existing Signal / Router / Risk   |
                | paper entry / exit pipeline       |
                +-----------------------------------+
```

## Module Plan

## 1. Opportunity Scanner

Purpose:

- discover new pools
- confirm meaningful initial liquidity
- register candidate pools for short-window observation

MVP trigger rules:

- new pool created
- first LP add observed
- estimated initial liquidity >= 5000 USD

### Solana MVP Sources

Start with one or two launch-heavy protocols only.

Recommended first targets:

- Raydium pool init / liquidity add
- Pump or Meteora pool initialization if the team wants earlier memecoin coverage

Scanner responsibilities:

- parse pool create instructions / logs
- parse initial LP add or mint liquidity event
- derive token mint(s), pool address, creator, LP provider
- estimate initial liquidity in USD
- emit candidate if liquidity threshold is met

### EVM Compatibility Path

The EVM version should follow the same contract shape but can be delivered later.

Target events:

- `PairCreated` or `PoolCreated`
- first `Mint` / add liquidity event

## 2. Flow Engine

Purpose:

Observe the first 5 minutes after candidate creation and compute deterministic launch quality signals.

Observation window:

- starts at `candidate_created_at`
- ends at `candidate_created_at + 300 seconds`

Tracked facts:

- total volume in USD
- buy volume in USD
- sell volume in USD
- trade count
- buy trade count
- sell trade count
- unique buyers
- unique sellers
- median buy size USD
- p90 buy size USD
- buyer concentration ratio
- burst count
- inter-trade arrival density

The Flow Engine must produce one final snapshot at the end of the window and may also produce intermediate snapshots every 30 to 60 seconds for debugging.

## 3. Mempool Engine

Deferred.

Only define the future interface now.

Future responsibilities:

- parse pending swaps
- detect large pending buys
- attach pre-confirmation pressure signals to existing candidates

Recommended future event:

- `discovery.mempool_snapshot`

## 4. Entity Engine

Deferred.

Only define the MVP-compatible interface now.

Future responsibilities:

- cluster deployer / LP provider / first-wave buyers
- classify likely dev / bot / smart-money entities
- enrich discovery candidates and flow snapshots

Recommended future event:

- `discovery.entity_snapshot`

## Scoring Model

The MVP score should stay deterministic and replayable.

Use:

$$
alpha\_score = 0.40 \cdot liquidity\_quality + 0.30 \cdot buyer\_diversity + 0.30 \cdot flow\_intensity
$$

Each component should be normalized to `[0, 1]`.

### liquidity_quality

Inputs:

- initial_liquidity_usd
- 5 minute quote slippage estimate
- liquidity retention over window
- ratio of volume to initial liquidity

Suggested rules:

- below 5k USD: reject candidate before scoring
- 5k to 20k USD: partial score
- > 20k USD and low slippage: high score
- rapid post-launch liquidity removal: heavy penalty

### buyer_diversity

Inputs:

- unique_buyers
- top buyer share
- buy size distribution
- repeated same-size or same-interval buys

Suggested rules:

- unique buyers < 5: low score
- top 3 buyers dominate > 70%: penalty
- wider buy size distribution: positive
- bot-like uniform small buys: penalty

### flow_intensity

Inputs:

- 5 minute total volume
- buy/sell imbalance
- burst count
- trade arrival rate

Suggested rules:

- volume / initial_liquidity ratio high: positive
- buy-dominant window: positive
- multiple bursts without single-wallet dominance: strong positive
- one isolated large buy without follow-through: weak or negative

## Alpha Thresholding

Suggested initial decision bands:

- `alpha_score < 0.55`: discard candidate
- `0.55 <= alpha_score < 0.70`: publish for observation only
- `alpha_score >= 0.70`: publish to execution-layer pipeline

These thresholds should be config-driven and chain-specific later.

## Canonical Events

The discovery layer should publish explicit event types instead of overloading current feature events.

### Event 1: discovery.pool_candidate

```json
{
  "event_type": "discovery.pool_candidate",
  "chain": "solana",
  "token": "TOKEN",
  "payload": {
    "dex": "raydium",
    "pool_address": "...",
    "base_token": "TOKEN",
    "quote_token": "USDC",
    "creator_address": "...",
    "lp_provider_address": "...",
    "initial_liquidity_usd": 12450.0,
    "candidate_opened_at": "2026-05-03T14:30:00Z",
    "scanner_version": "pool_scanner_v1"
  }
}
```

### Event 2: discovery.flow_snapshot

```json
{
  "event_type": "discovery.flow_snapshot",
  "chain": "solana",
  "token": "TOKEN",
  "payload": {
    "pool_address": "...",
    "window_name": "launch_5m",
    "window_start": "2026-05-03T14:30:00Z",
    "window_end": "2026-05-03T14:35:00Z",
    "total_volume_usd": 98230.0,
    "buy_volume_usd": 74110.0,
    "sell_volume_usd": 24120.0,
    "unique_buyers": 38,
    "median_buy_usd": 410.0,
    "p90_buy_usd": 2800.0,
    "buyer_concentration": 0.32,
    "burst_count": 4,
    "engine_version": "flow_engine_v1"
  }
}
```

### Event 3: discovery.discovery_snapshot

This is the bridge event into the existing signal engine.

```json
{
  "event_type": "discovery.discovery_snapshot",
  "chain": "solana",
  "token": "TOKEN",
  "payload": {
    "pool_address": "...",
    "pool_age_seconds": 300,
    "initial_liquidity_usd": 12450.0,
    "liquidity_quality": 0.74,
    "buyer_diversity": 0.71,
    "flow_intensity": 0.82,
    "discovery_alpha_score": 0.75,
    "candidate_status": "qualified",
    "score_version": "discovery_score_v1"
  }
}
```

## Storage Plan

Add the following tables.

### pool_candidates

Purpose:

- the candidate universe
- one row per discovered pool candidate

Suggested columns:

- `candidate_id`
- `chain`
- `dex`
- `pool_address`
- `base_token`
- `quote_token`
- `creator_address`
- `lp_provider_address`
- `initial_liquidity_usd`
- `candidate_opened_at`
- `candidate_status`
- `scanner_version`
- `created_at`
- `updated_at`

### pool_lifecycle_events

Purpose:

- append-only audit trail for create / LP add / liquidity changes

Suggested columns:

- `event_id`
- `candidate_id`
- `event_type`
- `observed_at`
- `payload`
- `created_at`

### flow_snapshots

Purpose:

- flow windows and derived launch metrics

Suggested columns:

- `snapshot_id`
- `candidate_id`
- `chain`
- `token`
- `pool_address`
- `window_name`
- `window_start`
- `window_end`
- `total_volume_usd`
- `buy_volume_usd`
- `sell_volume_usd`
- `unique_buyers`
- `unique_sellers`
- `buy_trade_count`
- `sell_trade_count`
- `median_buy_usd`
- `p90_buy_usd`
- `buyer_concentration`
- `burst_count`
- `payload`
- `created_at`

### discovery_signals

Purpose:

- final scored candidates ready for downstream trading logic

Suggested columns:

- `signal_id`
- `candidate_id`
- `chain`
- `token`
- `pool_address`
- `liquidity_quality`
- `buyer_diversity`
- `flow_intensity`
- `alpha_score`
- `qualified`
- `reasons`
- `score_version`
- `observed_at`
- `created_at`

## Repository Layout

Add a new package:

```text
signalengine/
├── discovery/
│   ├── __init__.py
│   ├── schemas.py
│   ├── repository.py
│   ├── pool_scanner.py
│   ├── flow_engine.py
│   ├── scoring.py
│   ├── publisher.py
│   └── worker.py
```

## Worker Plan

### discovery.worker

A dedicated worker should own candidate discovery.

Responsibilities:

- poll scanner sources
- persist candidates
- update lifecycle records
- open observation windows
- trigger flow window evaluation
- publish `discovery.discovery_snapshot`

This worker should not do execution directly.

### Existing pipeline worker

The existing [core/worker.py](../core/worker.py) remains the execution-layer worker.

Change required:

- allow `discovery.discovery_snapshot` to be merged by the signal engine
- keep routing, risk, and execution inside the existing pipeline

## Signal Engine Integration

The existing [core/signal_engine.py](../core/signal_engine.py) should be extended, not replaced.

New input fields:

- `discovery_alpha_score`
- `liquidity_quality`
- `buyer_diversity`
- `flow_intensity`
- `initial_liquidity_usd`
- `pool_age_seconds`

New sub-score or direct usage:

- either add a `discovery_momentum` sub-score
- or directly blend discovery features into `market_structure`

Recommended first implementation:

- add `discovery_momentum`
- blend it into `alpha_score` with a moderate weight
- add a new state candidate branch such as `EARLY_LIQUIDITY` or `NARRATIVE_EXPLOSION` when discovery score is high and pool age is low

## Router Integration

The [core/router.py](../core/router.py) logic should remain simple.

Recommended change:

- if `discovery_alpha_score` is strong and current state is an early-launch state, allow `DEX_ENTRY`
- do not bypass risk or execution-quality checks

That keeps discovery responsible for finding candidates, while the router remains responsible for turning them into intents.

## Metrics And Alerts

Add discovery-specific metrics.

Suggested counters / gauges:

- `pool_candidates_total{chain,dex,status}`
- `discovery_windows_total{window_name,outcome}`
- `discovery_alpha_score` gauge by token / pool
- `discovery_candidate_age_seconds`
- `discovery_publish_total{outcome}`

Suggested alerts:

- scanner stalled for a configured interval
- candidate count anomaly spike
- flow window evaluation lag
- discovery publish failure threshold exceeded

## Phased Delivery Plan

## Phase 1: Solana Opportunity Scanner

Deliverables:

- `discovery/schemas.py`
- `discovery/repository.py`
- `discovery/pool_scanner.py`
- PostgreSQL tables for candidates and lifecycle events
- one Solana DEX scanner path
- LP add filter > 5k USD

Validation:

- deterministic fixtures for pool creation and LP add
- persisted candidate rows
- replayable raw candidate events

## Phase 2: 5 Minute Flow Engine

Deliverables:

- `discovery/flow_engine.py`
- `discovery/scoring.py`
- `flow_snapshots` and `discovery_signals` persistence
- deterministic alpha scoring

Validation:

- fixture windows with low / medium / high quality launches
- final alpha score regression tests

## Phase 3: Pipeline Integration

Deliverables:

- `discovery.discovery_snapshot` publisher
- signal engine support for discovery features
- router integration for discovery-led paper entry
- replay coverage for discovery-driven runs

Validation:

- end-to-end paper scenario:
  `new pool -> 5m discovery -> paper DEX entry`

## Phase 4: EVM Scanner

Deliverables:

- factory and LP add scanners for one EVM DEX family
- shared discovery interfaces reused from Solana path

Validation:

- deterministic EVM logs -> candidate -> discovery snapshot

## MVP Acceptance Criteria

The discovery MVP is complete only when all of the following are true:

1. the system can discover a new pool without a pre-configured token
2. the system can reject pools with initial liquidity below threshold
3. the system can evaluate the first 5 minute flow window deterministically
4. the system can compute and persist `alpha_score`
5. only qualified candidates are published into the existing pipeline
6. the existing paper pipeline can open a DEX paper position from a discovery candidate
7. the full decision path is replayable and observable

## Recommended First Coding Step

Do not start with mempool or entity logic.

The first concrete implementation step should be:

1. add `discovery/` package
2. add candidate / flow / discovery schemas
3. add PostgreSQL tables
4. implement one Solana new-pool scanner
5. persist `discovery.pool_candidate`

That is the smallest cut that creates a real candidate universe and unlocks the rest of the MVP.
