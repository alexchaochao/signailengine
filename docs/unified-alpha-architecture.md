# Unified Alpha Architecture

## Purpose

This document redefines alpha in SignalEngine as a unified discovery and evaluation system.

The current repository already contains a strong execution-side backbone:

- normalized event ingestion
- deterministic signal building
- lifecycle state transitions
- routing and risk evaluation
- paper-first execution and reconciliation
- replay, metrics, and audit persistence

What it does not yet contain is a complete alpha discovery layer.

This document closes that gap by defining a single architecture that covers:

1. new pool / new token alpha
2. catalyst-driven alpha for existing tokens
3. flow-driven alpha from smart money, abnormal activity, or pending liquidity pressure

The target outcome is a system that does not depend on the user pre-selecting a token or pool. Instead, the system should discover candidate opportunities, score them, and pass only qualified candidates into the existing execution pipeline.

## Discovery Blacklist

The following design pattern is explicitly disallowed for all discovery-plane modules.

- requiring the operator to pre-configure token symbols, pools, cashtags, or project aliases in order to discover alpha

This is now treated as a discovery anti-pattern.

The user should not need to know which token may move ahead of time. The repository must discover candidate assets first, then resolve identity, then run confirmation and execution.

Blacklist examples:

- token-scoped RSS or CEX announcement matching via static token allowlists
- token-scoped social watchlists such as one `x_<token>_watch` source per asset
- token-scoped flow sources such as one `flow_alpha_<token>` source per asset
- token-scoped onchain discovery routes used as discovery entrypoints instead of confirmation sources

Allowed use:

- source-level discovery configuration for feeds, APIs, subscriptions, and transport credentials
- entity extraction, symbol resolution, and candidate creation after raw source ingestion
- token-specific execution routing only after a candidate has already been discovered and resolved

## Alpha Definition

Alpha in this system is any token-level opportunity that satisfies at least one of the following conditions.

### 1. Launch Alpha

Applies when a token is newly tradeable and early market structure may create an asymmetric opportunity.

Examples:

- new pool created
- new token launched
- meaningful initial LP add
- early buyer diversity and strong flow in the first few minutes

### 2. Catalyst Alpha → ⚠️ 已重新定义为「Token Lifecycle Stage Signal」

**2026-05-09 设计纠偏：CEX 上币监听不是 alpha 源。**

原来的定义——"existing token receives a new external catalyst that can alter price discovery"——在理论上成立，但在生产验证中发现：

- CEX 上币通知的 99% 不是交易机会（Coinbase 上 700 个交易对早已存在多年）
- 真正的 alpha 在上币之前就已经出现在链上数据中
- CEX 上币的正确用途是**更新 token 的生命周期阶段，告知路由层有新的交易 venue 可用**

所以 Catalyst Alpha 不再产出 `alpha.candidate_qualified` 事件，而是：

```
exchangeInfo / instruments 检测到新 symbol
  → 更新 SymbolRegistry.venue_lifecycle
  → 路由层读取 venue_lifecycle
  → 调整执行策略（DEX entry → CEX entry / 滑点调整等）
```

**Real Alpha Sources（2026-05-09 重新定义）**：

1. **Launch Alpha** — 新池/新 token 发现（保留）
2. **Smart Money Inflow** — 聪明钱包在新池中的买入行为（新增，P0）
3. **Volume Momentum** — 成交量爆发/价格动量（新增，P0）
4. **Flow Alpha** — 钱包流动分析（保留，但改为 discovery 层）
5. **Cross-Chain Expansion** — token 跨链迁移（新增，P1）

详见 [docs/dex-cex-listing-alert-plan.md](./dex-cex-listing-alert-plan.md) 第 4 节。

### 3. Flow Alpha

Applies when the token is not new, but market participants or transaction flow change materially.

Examples:

- smart money cluster inflow
- abnormal large buys
- burst activity
- anomalous buyer expansion
- mempool pressure from large pending swaps

Flow alpha should be treated as a discovery layer only when it can identify assets from wallet or transaction activity without a pre-configured token list. If a flow source needs a known token to operate, it belongs to confirmation or measurement, not discovery.

These three categories must feed the same downstream evaluation and execution framework.

For the detailed social retrieval, LLM enrichment, and candidate emission design, see [docs/social-llm-alpha-pipeline.md](./social-llm-alpha-pipeline.md).

For the current DEX/CEX listing-alert implementation, candidate qualification rules, and route direction policy, see [docs/dex-cex-listing-alert-plan.md](./dex-cex-listing-alert-plan.md).

For the cross-dimension async collection design that bridges discovery qualified candidates into the signal engine, see [docs/alpha-candidate-async-collection.md](./alpha-candidate-async-collection.md).

For the feasibility analysis of listing monitoring sources (DexScreener, Binance, Coinbase, etc.), see [docs/listing-monitor-feasibility.md](./listing-monitor-feasibility.md).

## Architectural Principle

Separate the system into two planes.

### Discovery Plane

Responsible for answering:

- what should the system pay attention to right now?
- why is this token becoming interesting?

### Evaluation And Execution Plane

Responsible for answering:

- is this candidate tradeable?
- what is the state?
- should the system route to DEX, CEX, or no-trade?
- does risk allow it?
- how should it execute?

The current repository is mostly an Evaluation And Execution Plane. The Discovery Plane must be added explicitly.

## Target Architecture

```text
                        +----------------------------------+
                        | External Discovery Sources       |
                        | onchain / CEX / social / wallet  |
                        +----------------+-----------------+
                                         |
                                         v
                +------------------------------------------------+
                | Alpha Discovery Plane                           |
                |                                                |
                | Launch Scanner     Catalyst Scanner             |
                | Flow Scanner       Entity Engine                |
                | Mempool Engine     Candidate Registry           |
                +----------------+-------------------------------+
                                 |
                                 v
                +------------------------------------------------+
                | Unified Alpha Candidate Layer                  |
                | alpha.candidate_opened                         |
                | alpha.snapshot_updated                         |
                | alpha.candidate_qualified                      |
                +----------------+-------------------------------+
                                 |
                                 v
                +------------------------------------------------+
                | Evaluation And Execution Plane                 |
                | signal -> state -> route -> risk -> execution  |
                +------------------------------------------------+
```

## Current Repository Mapping

### Already Present

These parts already exist and should be retained:

- [core/signal_engine.py](../core/signal_engine.py)
- [core/state_engine.py](../core/state_engine.py)
- [core/router.py](../core/router.py)
- [portfolio/risk_engine.py](../portfolio/risk_engine.py)
- [core/pipeline.py](../core/pipeline.py)
- [infra/metrics.py](../infra/metrics.py)
- [infra/alerts.py](../infra/alerts.py)
- [sentinel/wallet_intelligence_sync.py](../sentinel/wallet_intelligence_sync.py)

These modules form the downstream evaluation layer.

### Missing Or Incomplete

These parts need to be added or promoted to first-class modules:

- launch discovery
- catalyst discovery
- unified alpha candidate schemas
- candidate registry and persistence
- discovery-level metrics and alerts
- bridge events from discovery into the existing signal engine

## Discovery Plane Modules

The Discovery Plane should be expressed as five distinct modules.

## 1. Launch Scanner

Purpose:

- discover newly tradeable pools and tokens
- confirm minimum initial liquidity
- track the first observation window for launch quality

Primary signals:

- pool creation
- token launch event
- LP add / liquidity mint
- first 5 minute post-launch swap flow

Immediate MVP focus:

- Solana first
- EVM second

Output event family:

- `alpha.launch_candidate`

## 2. Catalyst Scanner

Purpose:

- detect off-chain or semi-off-chain catalysts for already trading tokens
- convert announcements and narrative changes into structured candidate events

Primary signals:

- CEX listing notices
- market listing schedules
- X discussion spikes
- project announcements
- derivatives enablement notices

Output event family:

- `alpha.catalyst_candidate`

## 3. Flow Scanner

Purpose:

- detect abnormal token flow independent of whether the token is new
- identify conditions where capital movement itself is the alpha source

Primary signals:

- smart money inflow
- buyer burst
- large swap cluster
- unique buyer expansion
- abnormal buy/sell imbalance

Design constraint:

- do not model flow discovery as one static source per token
- discover the asset from observed flow first, then create a candidate
- only use token-specific flow reads as downstream confirmation after candidate creation

Output event family:

- `alpha.flow_candidate`

## 4. Entity Engine

Purpose:

- classify addresses and entities that influence the quality of a candidate
- enrich launch and flow candidates with contextual identity hints
- resolve asset symbols and project identities extracted from announcements, social text, wallet flow, and onchain records

Examples:

- deployer / creator detection
- LP provider classification
- early buyer tagging
- bot-like pattern detection
- smart money label reuse

Output event family:

- `alpha.entity_snapshot`

## 5. Mempool Engine

Purpose:

- detect pre-confirmation pressure before it becomes visible in finalized swaps
- provide an optional incremental alpha layer for chains where mempool visibility is usable

Examples:

- large pending swap
- concentrated same-direction pending flow
- pending LP withdrawal or pending LP add surge

Output event family:

- `alpha.mempool_snapshot`

This should be designed now but implemented later.

## Unified Alpha Candidate Layer

All discovery modules must converge into one shared candidate model.

This is the architectural center of the redesigned alpha system.

All discovery entrypoints should converge here before any token-specific execution configuration is consulted.

### Candidate Lifecycle

A candidate moves through the following states:

1. `opened`
2. `observing`
3. `qualified`
4. `discarded`
5. `expired`
6. `executed`

### Canonical Event Types

#### alpha.candidate_opened

Used when any scanner opens a new candidate for observation.

#### alpha.snapshot_updated

Used when a scanner or engine enriches the candidate with new evidence.

#### alpha.candidate_qualified

Used when the candidate crosses the alpha threshold and should be evaluated by the execution pipeline.

These event types should be scanner-agnostic.

## Candidate Schema

A canonical candidate should contain these fields.

### Required Core Fields

- `candidate_id`
- `candidate_type` with values `launch`, `catalyst`, `flow`
- `chain`
- `token`
- `opened_at`
- `status`
- `source_family`
- `reasons`

### Optional Enrichment Fields

- `pool_address`
- `dex`
- `base_token`
- `quote_token`
- `initial_liquidity_usd`
- `pool_age_seconds`
- `listing_exchange`
- `listing_market_type`
- `social_source`
- `smart_money_inflow_score`
- `entity_risk_score`
- `mempool_pressure_score`

### Scoring Fields

- `launch_alpha_score`
- `catalyst_alpha_score`
- `flow_alpha_score`
- `composite_alpha_score`
- `score_version`

## Alpha Scoring Framework

The system should support both category-specific scoring and a unified composite score.

### Launch Alpha Score

The launch score should be based on:

- liquidity_quality
- buyer_diversity
- flow_intensity

Suggested formula:

$$
launch\_alpha = 0.40 \cdot liquidity\_quality + 0.30 \cdot buyer\_diversity + 0.30 \cdot flow\_intensity
$$

### Catalyst Alpha Score

The catalyst score should be based on:

- announcement_quality
- venue_quality
- social_confirmation
- onchain_confirmation

Suggested formula:

$$
catalyst\_alpha = 0.35 \cdot announcement\_quality + 0.30 \cdot venue\_quality + 0.20 \cdot social\_confirmation + 0.15 \cdot onchain\_confirmation
$$

### Flow Alpha Score

The flow score should be based on:

- smart_money_alignment
- anomaly_strength
- buyer_expansion
- execution_feasibility

Suggested formula:

$$
flow\_alpha = 0.35 \cdot smart\_money\_alignment + 0.25 \cdot anomaly\_strength + 0.20 \cdot buyer\_expansion + 0.20 \cdot execution\_feasibility
$$

### Composite Alpha Score

The system should compute a unified score only after category-level scores exist.

Suggested first implementation:

- if candidate is launch-driven, use launch score as the dominant signal
- if candidate is catalyst-driven, use catalyst score as the dominant signal
- if candidate is flow-driven, use flow score as the dominant signal
- allow cross-category boosts when more than one alpha source confirms the token

Example:

$$
composite\_alpha = dominant\_category\_score + cross\_confirmation\_bonus
$$

Clamp the final score to `[0, 1]`.

## Integration With Existing Signal Engine

The current [core/signal_engine.py](../core/signal_engine.py) should not be removed.

Instead, extend it to accept unified candidate features.

### New Signal Inputs

- `alpha_candidate_type`
- `launch_alpha_score`
- `catalyst_alpha_score`
- `flow_alpha_score`
- `composite_alpha_score`
- `pool_age_seconds`
- `initial_liquidity_usd`
- `listing_exchange_tier`
- `social_spike_score`
- `smart_money_inflow_score`

### New Sub-Score Recommendation

Add a dedicated `alpha_discovery` sub-score.

This lets the existing system remain understandable:

- market structure still measures tradeability
- wallet behavior still measures entity flow
- execution readiness still measures slippage/liquidity viability
- alpha discovery measures why the token entered the system at all

## State Engine Alignment

The current [core/state_engine.py](../core/state_engine.py) should be extended with discovery-aware early states.

Recommended future states:

- `DISCOVERY_LAUNCH`
- `DISCOVERY_CATALYST`
- `DISCOVERY_FLOW`

If adding new enum values is too disruptive initially, the first implementation can map discovery candidates into existing early states:

- high launch alpha -> `EARLY_LIQUIDITY`
- high catalyst alpha -> `CEX_LISTING` or `NARRATIVE_EXPLOSION`
- high flow alpha -> `NARRATIVE_EXPLOSION`

## Router Alignment

The [core/router.py](../core/router.py) remains the action selector.

Recommended behavior:

- launch and flow candidates should route to `DEX_ENTRY` when tradeability is strong
- catalyst candidates may route to `CEX_ENTRY` or `DEX_ENTRY` depending on venue mapping
- `DISTRIBUTION` should still override and prioritize exit logic

The router should not be responsible for discovering alpha. It should only consume already qualified candidates.

## Risk Alignment

The [portfolio/risk_engine.py](../portfolio/risk_engine.py) remains the tradeability gate.

Required additions later:

- candidate-type-specific sizing
- tighter caps for very new pools
- stricter slippage policy for launch alpha
- exchange-specific policy for catalyst alpha
- flow-based decay rules when abnormal inflow disappears

## External Data Sources By Alpha Type

### Launch Alpha Sources

- Solana DEX pool create events
- Solana LP add / initialize liquidity instructions
- EVM factory create events
- EVM liquidity mint events
- early quote / price / swap data

### Catalyst Alpha Sources

- CEX listing notices
- exchange announcement pages or APIs
- X / social discussion burst signals
- project communications
- derivatives listing feeds

### Flow Alpha Sources

- onchain trade flow
- wallet cluster snapshots
- smart money labels
- anomaly detectors
- pending transaction pressure where available

## Persistence Model

The discovery layer should add a separate persistence surface instead of overloading current feature tables.

Suggested tables:

- `alpha_candidates`
- `alpha_candidate_events`
- `alpha_snapshots`
- `alpha_scores`
- `alpha_entities`

This should coexist with the already existing downstream persistence tables.

## Metrics And Alerts

The discovery layer should add dedicated metrics.

Suggested metrics:

- `alpha_candidates_total{type,status,chain}`
- `alpha_candidate_open_seconds`
- `alpha_candidate_score{type}`
- `alpha_publish_total{event_type,outcome}`
- `alpha_scanner_polls_total{scanner,outcome}`
- `alpha_observation_windows_total{type,outcome}`

Suggested alerts:

- scanner stalled
- candidate flood anomaly
- observation lag exceeded
- listing listener stale
- social listener degraded
- candidate publication failure threshold exceeded

## Recommended Execution Plan

This plan is intentionally sequential. Do not build all alpha types at once.

## Phase 0: Architecture And Schema

Deliverables:

- this document
- canonical alpha candidate schemas
- persistence model
- event taxonomy

Exit criteria:

- a shared definition exists for launch, catalyst, and flow alpha
- downstream teams know what event types to publish and consume

## Phase 1: Launch Alpha MVP

Deliverables:

- Solana-first launch scanner
- pool candidate persistence
- LP add threshold filter
- 5 minute flow engine
- launch alpha scoring
- `alpha.candidate_qualified` publishing
- bridge into existing paper pipeline

Exit criteria:

- no manual token or pool configuration required for one supported Solana DEX path
- system can discover a new pool, score it, and produce a paper DEX entry candidate

## Phase 2: Catalyst Alpha MVP

Deliverables:

- CEX listing listener interface and one concrete source
- announcement normalization
- catalyst alpha scoring
- bridge into existing signal engine
- paper CEX or DEX routing based on venue policy

Exit criteria:

- an exchange listing event can create a candidate without pre-configured token monitoring
- qualified catalyst candidates can route through paper execution logic

## Phase 3: Flow Alpha MVP

Deliverables:

- abnormal buyer burst detector
- smart money cluster inflow integration
- flow alpha scoring
- candidate publication for existing tokens

Exit criteria:

- an existing token with no launch alpha and no listing notice can still become a qualified candidate from flow alone

## Phase 4: Entity Enrichment

Deliverables:

- deployer / LP provider tagging
- early buyer tagging
- basic bot-like behavior classifier
- smart money label enrichment

Exit criteria:

- launch and flow candidates can be upgraded or downgraded based on entity quality

## Phase 5: Mempool Expansion

Deliverables:

- pending swap parser for one supported chain
- pre-confirmation pressure feature
- mempool score integration

Exit criteria:

- mempool pressure can modify but not dominate composite alpha score

## Phase 6: Production Hardening

Deliverables:

- replay coverage for discovery events
- richer observability dashboards
- stronger failure handling and backoff
- tighter configuration and secret management
- execution readiness policy by alpha type

Exit criteria:

- discovery and execution paths can run continuously with clear operational visibility

## Immediate Next Step

The next implementation step should be strictly limited to Launch Alpha MVP.

Build in this order:

1. `alpha` or `discovery` schemas
2. `alpha_candidates` storage
3. Solana new-pool scanner
4. LP add thresholding
5. 5 minute flow engine
6. `alpha.candidate_qualified` event publishing
7. paper pipeline bridge

Do not begin catalyst listeners or mempool parsing before this path is complete and replayable.
