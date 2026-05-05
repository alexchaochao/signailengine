# Web3 Hybrid Trading System V2 Technical Design

## 1. Document Purpose

This document replaces the prior concept draft with an implementation-oriented technical design for a V2 Web3 hybrid trading system.

The system goal is not to build a fully autonomous hedge fund on day one. The goal is to deliver a staged platform that can:

1. Ingest on-chain and market events.
2. Normalize them into a unified signal model.
3. Classify token lifecycle state with explicit rules.
4. Route eligible opportunities to DEX or CEX execution paths.
5. Apply portfolio and risk constraints before any order is created.
6. Persist all decisions for replay, audit, and later model improvement.

This design assumes a V2-MVP first, then progressive hardening toward production.

---

## 2. Product Goal

### 2.1 Core Objective

Build an event-driven trading intelligence and execution platform that bridges:

- on-chain activity
- market structure
- optional social signals
- centralized exchange execution
- portfolio-level capital allocation

### 2.2 Success Criteria

The first successful version must satisfy all of the following:

1. A chain event can be ingested and converted into a normalized signal.
2. The signal can be evaluated by a deterministic state engine.
3. The execution router can produce a concrete execution intent.
4. The risk engine can accept or reject the intent with explicit reasons.
5. The system can emit either a paper trade or a live order request.
6. The full decision path can be replayed offline.

### 2.3 Non-Goals for V2-MVP

The following are explicitly out of scope for the first implementation phase:

- graph neural networks
- insider detection claims
- automated multi-chain arbitrage
- unconstrained AI-based capital allocation
- Telegram-scale narrative intelligence
- institutional latency targets across many venues

These may be added later only after replay, attribution, and risk controls are stable.

---

## 3. Scope Definition

### 3.1 Recommended Initial Scope

To reduce execution risk, the first build should use the following scope:

- Chains: Solana only
- DEX execution: one Solana venue path only
- CEX execution: one exchange adapter only
- Social input: optional, disabled by default
- Trading mode: paper trading first, then guarded live mode
- Signal engine: deterministic rules plus weighted features
- Storage: PostgreSQL + Redis
- Monitoring: Prometheus metrics + structured logs

### 3.2 Deferred Scope

The following should be deferred until after replay and validation exist:

- EVM support
- EVM multi-chain configuration abstraction
- Kafka
- ClickHouse
- Telegram ingestion
- wallet clustering using learned graph methods
- automated dynamic hedging
- real-time cross-venue arbitrage

---

## 4. High-Level System Architecture

```text
                                    +-----------------------------+
                                    |  External Data Sources      |
                                    |  On-chain / Market / Social |
                                    +-------------+---------------+
                                                                |
                                                                v
                                    +-----------------------------+
                                    | Ingestion Services          |
                                    | listener / parser / dedupe  |
                                    +-------------+---------------+
                                                                |
                                                                v
                                    +-----------------------------+
                                    | Event Bus                   |
                                    | Redis Streams               |
                                    +-------------+---------------+
                                                                |
                             +----------------+----------------+
                             |                                 |
                             v                                 v
     +-------------------------+       +-------------------------+
     | Signal Engine           |       | State Engine            |
     | feature extraction      | ----> | lifecycle classification|
     +------------+------------+       +------------+------------+
                                |                                 |
                                +----------------+----------------+
                                                                 |
                                                                 v
                                        +-----------------------------+
                                        | Execution Router            |
                                        | venue + mode selection      |
                                        +-------------+---------------+
                                                                    |
                                                                    v
                                        +-----------------------------+
                                        | Risk and Portfolio Engine   |
                                        | limits / sizing / cooldown  |
                                        +-------------+---------------+
                                                                    |
                                        +-------------+-------------+
                                        |                           |
                                        v                           v
                 +---------------------+   +----------------------+
                 | DEX Adapter          |   | CEX Adapter          |
                 | swap / bundle / sim  |   | API order execution  |
                 +---------------------+   +----------------------+

                                        +-----------------------------+
                                        | Persistence and Replay      |
                                        | PostgreSQL / audit / replay |
                                        +-----------------------------+
```

### 4.1 Design Principle

Each service must do one of the following, but not several at once:

- collect data
- transform data
- classify state
- make trade intent decisions
- enforce risk rules
- execute orders
- persist and expose observability

This separation is required for replay, debugging, and future model swaps.

---

## 5. Service Boundaries

### 5.1 Ingestion Layer

Responsibilities:

- connect to external data sources
- parse raw events into internal envelopes
- deduplicate messages
- publish normalized raw events to Redis Streams

Non-responsibilities:

- trading logic
- portfolio logic
- state classification

Planned services:

- `sentinel/onchain_listener.py`
- `sentinel/market_listener.py`
- `sentinel/social_listener.py`
- `sentinel/wallet_tracker.py`

### 5.2 Signal Engine

Responsibilities:

- enrich raw events into token-centric features
- calculate deterministic sub-scores
- produce a unified `TokenSignal`

Non-responsibilities:

- placing orders
- storing portfolio state
- directly mutating lifecycle state

### 5.3 State Engine

Responsibilities:

- convert features into a lifecycle state
- track transition reasons
- persist prior state

Important note:

Despite the term "graph" in the original concept, the first implementation is a rule-based lifecycle engine with transition metadata. A real graph inference engine is deferred.

### 5.4 Execution Router

Responsibilities:

- choose `DEX`, `CEX`, or `NO_TRADE`
- select execution mode based on state, liquidity, and venue readiness
- produce an `ExecutionIntent`

### 5.5 Risk and Portfolio Engine

Responsibilities:

- reject unsafe intents
- compute order sizing
- enforce exposure, cooldown, and drawdown limits
- record risk decisions for replay

### 5.6 Execution Adapters

Responsibilities:

- translate execution intents into venue-specific orders
- simulate or paper-trade when configured
- reconcile submitted and filled orders

Planned adapters:

- `execution/dex_executor.py`
- `execution/cex_bridge.py`

### 5.7 Persistence and Replay

Responsibilities:

- store signals, states, decisions, orders, fills, and position snapshots
- support time-ordered replay of historical events
- provide auditability for every decision

---

## 6. Canonical Domain Models

### 6.1 Event Envelope

All messages on the bus must be wrapped in an envelope.

```json
{
    "event_id": "uuid",
    "event_type": "onchain.swap_detected",
    "source": "solana_listener",
    "chain": "solana",
    "token": "BONK",
    "observed_at": "2026-05-02T10:00:00Z",
    "ingested_at": "2026-05-02T10:00:01Z",
    "payload": {}
}
```

Required properties:

- `event_id` for idempotency
- `event_type` for routing
- `observed_at` for replay ordering
- `source` for audit and debugging

### 6.2 TokenSignal

`TokenSignal` is the normalized object used by the state engine, router, and risk engine.

```json
{
    "token": "BONK",
    "chain": "solana",
    "state_candidate": "NARRATIVE_EXPLOSION",
    "features": {
        "liquidity_usd": 120000,
        "volume_5m_usd": 45000,
        "buy_pressure": 0.78,
        "holder_growth_15m": 0.22,
        "wallet_inflow_score": 0.62,
        "social_sentiment": 0.00,
        "social_velocity": 0.00,
        "cex_rumor_score": 0.10
    },
    "sub_scores": {
        "market_structure": 0.76,
        "wallet_behavior": 0.58,
        "social_momentum": 0.05,
        "execution_readiness": 0.81
    },
    "alpha_score": 0.64,
    "reasons": [
        "liquidity_breakout",
        "buy_pressure_high",
        "wallet_inflow_positive"
    ],
    "timestamp": 1777716000
}
```

### 6.3 Lifecycle State

```python
class TokenState(Enum):
        UNKNOWN = 0
        PRE_LAUNCH = 1
        EARLY_LIQUIDITY = 2
        NARRATIVE_EXPLOSION = 3
        CEX_LISTING = 4
        TRENDING = 5
        DISTRIBUTION = 6
        DEAD = 7
```

### 6.4 ExecutionIntent

```json
{
    "intent_id": "uuid",
    "token": "BONK",
    "chain": "solana",
    "venue_type": "DEX",
    "venue": "solana_primary",
    "action": "BUY",
    "confidence": 0.64,
    "target_notional_usd": 2500,
    "max_slippage_bps": 150,
    "state": "NARRATIVE_EXPLOSION",
    "strategy": "dex_momentum_v1",
    "reasons": [
        "state_allows_entry",
        "risk_budget_available"
    ]
}
```

### 6.5 RiskDecision

```json
{
    "intent_id": "uuid",
    "allowed": true,
    "adjusted_notional_usd": 1800,
    "violations": [],
    "warnings": [
        "single_token_exposure_near_limit"
    ],
    "timestamp": "2026-05-02T10:00:03Z"
}
```

---

## 7. Signal Computation Design

### 7.1 First Principle

The system must not combine raw metrics with incompatible scales.

The original concept used absolute liquidity next to 0 to 1 scores in one formula. That is not valid. All features must be normalized before weighting.

### 7.2 Feature Normalization Strategy

Allowed techniques for V2-MVP:

- min-max normalization within a bounded domain
- capped log transform for large notional metrics
- percentile bucketing from historical data
- zero-fill plus missingness flags where sources are optional

Example:

```python
normalized_liquidity = clip(log10(liquidity_usd + 1) / 6, 0, 1)
normalized_volume = clip(log10(volume_5m_usd + 1) / 6, 0, 1)
```

### 7.3 Recommended Sub-Score Structure

Instead of one opaque score, compute sub-scores first:

1. `market_structure_score`
2. `wallet_behavior_score`
3. `social_momentum_score`
4. `execution_readiness_score`

Then combine them into `alpha_score`.

```python
alpha_score = (
        market_structure_score * 0.40 +
        wallet_behavior_score * 0.25 +
        social_momentum_score * 0.10 +
        execution_readiness_score * 0.25
)
```

Social score should default to neutral when the social pipeline is disabled.

### 7.4 Explainability Requirement

Every sub-score must attach reason codes. If a score cannot explain itself, it is not accepted into the decision pipeline.

---

## 8. Lifecycle State Engine Design

### 8.1 Implementation Strategy

V2-MVP uses a deterministic rules engine with state memory.

State transitions must depend on:

- current signal features
- prior token state
- elapsed time since last transition
- optional hysteresis thresholds to prevent thrashing

### 8.2 Example Transition Logic

```python
def transition(previous_state, signal):
        if signal.features["liquidity_usd"] < 5000:
                return TokenState.PRE_LAUNCH, ["liquidity_below_threshold"]

        if (
                signal.sub_scores["market_structure"] > 0.75 and
                signal.sub_scores["wallet_behavior"] > 0.55
        ):
                return TokenState.NARRATIVE_EXPLOSION, [
                        "market_structure_strong",
                        "wallet_behavior_positive"
                ]

        if signal.features.get("cex_listing_confirmed", 0) == 1:
                return TokenState.CEX_LISTING, ["cex_listing_confirmed"]

        if (
                signal.sub_scores["market_structure"] < 0.35 and
                signal.features.get("wallet_outflow_score", 0) > 0.6
        ):
                return TokenState.DISTRIBUTION, [
                        "market_structure_weakening",
                        "wallet_outflow_rising"
                ]

        return previous_state, ["no_transition"]
```

### 8.3 State Engine Constraints

- transitions must be reproducible offline
- thresholds must be config-driven
- state changes must be persisted with reasons
- multiple transitions in a short window must be rate-limited

---

## 9. Smart Money Layer Design

### 9.1 V2-MVP Positioning

The smart money layer is heuristic in the first version. It should not claim true insider detection.

### 9.2 Allowed Capabilities in MVP

- wallet watchlists
- repeated profitable wallet detection
- pre-defined cluster tagging
- inflow and outflow concentration metrics
- copy-trade candidate generation as a soft signal only
- OKX Strategy API leaderboard bootstrap for candidate smart-wallet discovery
- OKX address analytics enrichment as a secondary labeling input

### 9.3 Deferred Capabilities

- inferred hidden wallet clusters
- graph embeddings
- behavioral anomaly models
- insider classification

### 9.4 Source Governance

OKX market and strategy APIs can accelerate wallet discovery, but the internal tracked-wallet registry must remain the canonical source used by the runtime.

- treat OKX leaderboard results as candidate labels and discovery hints, not as final smart-money truth
- persist source provenance for each imported wallet, including chain, wallet type, timeframe, sort basis, and observation time
- derive wallet inflow and outflow features from internal trade facts so replay and audit stay deterministic
- use OKX balance, portfolio, or transaction history endpoints only for enrichment, refresh, and cold-start support
- do not make wallet feature publication depend on the availability of a single upstream vendor

### 9.5 Signal Output Example

```json
{
    "wallet_behavior_score": 0.62,
    "wallet_tags": ["watchlist_whale", "early_rotation_wallet"],
    "reasons": [
        "tracked_wallet_accumulating",
        "cluster_inflow_above_baseline"
    ]
}
```

---

## 10. Execution Router Design

### 10.1 Router Decision Inputs

The router must not decide from lifecycle state alone.

Required inputs:

- current lifecycle state
- alpha score
- liquidity depth estimate
- expected slippage
- risk budget remaining
- current position state
- venue availability
- execution mode configuration

### 10.2 Router Output Modes

- `DEX_ENTRY`
- `DEX_EXIT`
- `CEX_ENTRY`
- `CEX_EXIT`
- `HOLD`
- `REJECT`

### 10.3 Example Routing Rules

```python
def route(signal, position, venue_status):
        if signal.alpha_score < 0.55:
                return "REJECT"

        if signal.state_candidate == "PRE_LAUNCH" and venue_status.dex_ready:
                return "DEX_ENTRY"

        if signal.state_candidate == "CEX_LISTING" and venue_status.cex_ready:
                return "CEX_ENTRY"

        if signal.state_candidate == "DISTRIBUTION" and position.is_open:
                return "DEX_EXIT" if position.venue_type == "DEX" else "CEX_EXIT"

        return "HOLD"
```

### 10.4 Routing Safeguards

- no router output may bypass risk checks
- no live order if venue status is degraded
- no repeated entry within cooldown window

---

## 11. Risk and Portfolio Design

### 11.1 Risk Philosophy

Risk control is a hard gate, not a post-processing suggestion.

### 11.2 Mandatory Risk Rules for MVP

- max exposure per token
- max exposure per chain
- max number of concurrent positions
- max daily loss
- max drawdown stop
- per-token cooldown after exit
- min liquidity threshold
- max slippage threshold
- duplicate intent suppression

### 11.3 Capital Allocation Strategy

The original example mixed strategy preference with capital sizing. The V2-MVP allocator must separate:

1. `should_trade`
2. `which_venue`
3. `how_much`

Recommended sizing pattern:

```python
def size_position(alpha_score, volatility_score, portfolio_headroom):
        base = min(max((alpha_score - 0.5) * 2, 0), 1)
        volatility_penalty = 1 - volatility_score
        return portfolio_headroom * base * volatility_penalty
```

### 11.4 Portfolio Engine Outputs

- approved notional
- adjusted notional
- rejection reasons
- exposure snapshots before and after execution

---

## 12. DEX Execution Design

### 12.1 V2-MVP DEX Capabilities

- quote estimation
- pre-trade validation
- slippage limit enforcement
- simulation or dry-run mode
- optional Jito bundle path for Solana only

### 12.2 Constraints

- Jito-specific logic must remain in the Solana adapter, not in generic execution code
- rug checks must be explicit rule checks, not hand-wavy placeholders
- every submitted transaction must return traceable identifiers

### 12.3 Minimum Pre-Trade Checks

- mint freeze authority risk
- token metadata sanity
- liquidity pool size threshold
- estimated slippage within cap
- wallet balance availability
- duplicate pending order check

### 12.4 OKX DEX SDK Fit Assessment

The OKX DEX SDK is functionally relevant, but it should not be treated as a drop-in dependency for the current runtime.

Observed SDK capabilities from the public usage documentation:

- multi-chain quote and swap support, including Solana
- transaction broadcast support for supported chains
- raw swap data retrieval for custom transaction handling
- slippage tolerance controls
- liquidity source and token discovery APIs
- transaction simulation, order tracking, and MEV-protected broadcast for approved accounts

Fit versus the current MVP architecture:

- good fit for quote acquisition and route discovery
- potentially useful for Solana swap execution if wrapped behind the existing adapter boundary
- useful as an optional external execution backend, not as routing or risk logic
- not a direct fit for the current Python worker runtime because the SDK is TypeScript-first

Direct-use constraints:

- requires OKX API credentials and project configuration
- advanced broadcast and simulation flows require API registration and whitelist approval
- secret handling must remain isolated from the core decision engine
- the current repository is Python-based, so direct in-process use would require introducing a Node.js runtime into the main worker path

Recommended integration stance:

- do not couple the Python pipeline directly to the SDK package
- use the SDK only through a dedicated adapter service or bridge process
- keep intent generation, risk gating, and replay semantics inside the existing Python domain layer
- treat OKX as one candidate Solana DEX backend behind `ExecutionAdapter`

Required implementation tasks before adoption:

- validate whether OKX Solana routes cover the target token universe and liquidity profile
- confirm whether required broadcast and simulation APIs are available for the intended account tier
- build a small bridge service that exposes quote, prepare, execute, and order-status primitives to the Python worker
- add contract tests to ensure bridge responses map cleanly into `ExecutionQuote`, `PreparedExecution`, and `ExecutionReport`
- keep a non-OKX fallback path so live availability of one vendor does not become a single point of failure

---

## 13. CEX Execution Design

### 13.1 Role of Freqtrade Bridge

Freqtrade should be treated as an execution or strategy adapter, not the central brain.

### 13.2 Event-Driven API Contract

```http
POST /v2/signal
Content-Type: application/json

{
    "pair": "BTC/USDT",
    "action": "buy",
    "confidence": 0.91,
    "strategy": "momentum_v2",
    "intent_id": "uuid",
    "max_position_usd": 2500
}
```

### 13.3 Required Bridge Behaviors

- authenticate callers
- validate idempotency with `intent_id`
- return accepted, rejected, or failed status
- expose order and fill callbacks back into the event bus

---

## 14. Data and Storage Design

### 14.1 Required Datastores for MVP

- Redis Streams for transient event flow
- PostgreSQL for durable state and audit data

### 14.2 PostgreSQL Logical Tables

Recommended tables:

- `raw_events`
- `token_signals`
- `token_states`
- `execution_intents`
- `risk_decisions`
- `orders`
- `fills`
- `positions`
- `portfolio_snapshots`
- `replay_runs`

### 14.3 Deferred Datastores

ClickHouse is useful later for high-volume analytics, but not required for first delivery.

Kafka is also deferred until Redis Streams becomes an operational bottleneck.

---

## 15. Replay and Evaluation Design

### 15.1 Replay Requirement

No strategy or threshold change should be accepted without replay support.

### 15.2 Replay Inputs

- historical raw events
- historical market snapshots
- historical configuration version
- deterministic state transition rules

### 15.3 Replay Outputs

- signal generation timeline
- state transitions timeline
- rejected versus accepted intents
- order simulation results
- PnL attribution
- latency and slippage diagnostics

### 15.4 Acceptance Requirement

Every production decision must be reproducible from stored inputs and config version.

---

## 16. Observability and Operations

### 16.1 Required Metrics

- events ingested per source
- signal generation latency
- state transition counts
- router decisions by type
- risk rejection counts by reason
- order submit success rate
- fill rate
- slippage distribution
- strategy PnL

### 16.2 Logging Requirements

All services must emit structured logs with:

- correlation id
- event id
- token
- chain
- service name
- outcome

### 16.3 Alerting Requirements

Minimum alerts:

- listener disconnected
- event lag above threshold
- order rejection spike
- repeated adapter failure
- replay mismatch versus live decisions

---

## 17. Security and Safety Requirements

### 17.1 Mandatory Safety Controls

- API authentication between internal services
- secret management outside source code
- role separation between paper and live environments
- global kill switch
- environment-based live trading guard
- audit trail for all order submissions

### 17.2 Live Trading Prerequisites

Live trading must remain disabled until all of the following exist:

1. replay framework
2. risk gate
3. order reconciliation
4. alerting
5. manual kill switch
6. position and balance checks

---

## 18. Recommended Repository Structure

```text
signalengine/
├── sentinel/
│   ├── onchain_listener.py
│   ├── market_listener.py
│   ├── social_listener.py
│   └── wallet_tracker.py
├── core/
│   ├── signal_engine.py
│   ├── state_engine.py
│   ├── router.py
│   ├── schemas.py
│   └── config.py
├── execution/
│   ├── dex_executor.py
│   ├── cex_bridge.py
│   └── reconciliation.py
├── portfolio/
│   ├── allocator.py
│   ├── risk_engine.py
│   └── exposure_tracker.py
├── infra/
│   ├── redis_stream.py
│   ├── postgres.py
│   ├── metrics.py
│   └── settings.yaml
├── replay/
│   ├── runner.py
│   └── evaluators.py
├── tests/
│   ├── test_state_engine.py
│   ├── test_router.py
│   └── test_risk_engine.py
└── README.md
```

---

## 19. Delivery Plan

### Phase 0: Specification Freeze

Duration: 3 to 5 days

Deliverables:

- final schema definitions
- config strategy
- venue selection
- risk policy baseline
- live trading disabled by default

Exit criteria:

- all required domain models agreed
- MVP chain and venues frozen

### Phase 1: Foundation

Duration: 1 to 2 weeks

Deliverables:

- project scaffold
- Redis and PostgreSQL wiring
- event envelope definitions
- structured logging and metrics

Exit criteria:

- services can publish and consume events locally

### Phase 2: Ingestion and Signal Pipeline

Duration: 2 to 3 weeks

Deliverables:

- on-chain listener
- market listener
- optional stub social listener
- signal normalization pipeline

Exit criteria:

- raw events become `TokenSignal` objects deterministically

### Phase 3: State, Routing, and Risk

Duration: 2 weeks

Deliverables:

- lifecycle state engine
- router
- portfolio and risk engine

Exit criteria:

- valid signals can produce approved or rejected execution intents with explicit reasons

### Phase 4: Execution Adapters

Duration: 2 to 3 weeks

Deliverables:

- DEX paper execution
- CEX bridge integration
- reconciliation flow
- OKX DEX SDK feasibility spike and adapter-bridge decision
- if approved, OKX-backed Solana adapter service behind the existing execution boundary

Exit criteria:

- end-to-end paper trading works

### Phase 5: Replay and Evaluation

Duration: 1 to 2 weeks

Deliverables:

- replay runner
- attribution metrics
- regression test dataset

Exit criteria:

- code or threshold changes can be evaluated offline before release

### Phase 6: Guarded Live Rollout

Duration: variable

Deliverables:

- live credentials isolation
- kill switch
- alerts
- capped notional rollout

Exit criteria:

- controlled live deployment with small fixed limits

---

## 20. MVP Acceptance Criteria

The V2-MVP is complete only when all of the following are true:

1. One supported chain is fully wired.
2. One DEX path and one CEX path are functional in paper mode.
3. Signals, state transitions, and risk decisions are persisted.
4. Every trade decision is explainable with reason codes.
5. Replay produces reproducible outputs.
6. Global kill switch exists.
7. Monitoring covers ingestion, routing, risk, and execution.

---

## 21. Open Design Questions

These decisions must be finalized before implementation starts:

1. Which Solana DEX path is primary?
2. Which CEX is first-class for the bridge?
3. Is social input enabled in MVP or deferred entirely?
4. What is the target token universe and liquidity floor?
5. Is live rollout allowed in V2, or is V2 strictly paper and replay only?
6. What are the default per-token and daily risk limits?
7. Is OKX DEX SDK approved as a Solana execution backend, and if so, is it bridge-only or allowed in the main runtime?

---

## 22. Summary

The original concept is directionally strong but too broad to implement safely as written. This design narrows V2 into a staged, testable architecture with:

- deterministic first-pass intelligence
- clear domain contracts
- explicit risk gating
- venue adapters separated from decision logic
- replay as a mandatory system capability

The correct build order is:

event pipeline -> normalized signal -> state engine -> router -> risk gate -> paper execution -> replay -> guarded live trading

Anything more ambitious should be treated as a later upgrade, not a requirement for the first implementation.
