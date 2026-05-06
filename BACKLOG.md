# SignalEngine V2 Backlog

## Purpose

This backlog converts the V2 technical design into executable work items for Phase 0 and Phase 1.

Phase definitions are inherited from [plan.md](./plan.md):

- Phase 0: specification freeze
- Phase 1: foundation

The goal of this backlog is to make the next implementation step unambiguous.

## Current Status

Backlog scan status as of 2026-05-06:

### Phase 0 & 1 (Foundation) — all complete

All BKL-001 through BKL-015 items are delivered.
See [Completion Snapshot](#completion-snapshot) below.

### Phase 2 (Alpha Discovery) — delivered

The following has been implemented since Phase 1:

- **Launch alpha** — DexScreener new-pool discovery, 5-minute flow evaluation, candidate qualification
- **Catalyst alpha** — RSS / CEX announcement ingestion, entity extraction (heuristic + LLM), candidate scoring
- **Social pipeline** — event-driven confirmation mode, LLM-based analysis (DeepSeek remote), provider-level config
- **Flow measurement** — wallet-intelligence-backed flow snapshots, renamed to measurement semantics
- **On-chain feature pipeline** — buy_pressure / slippage aggregators, live on-chain trade + quote sources
- **Wallet intelligence** — OKX wallet registry import / refresh, wallet flow projection
- **Telegram publisher** — alpha candidate notifications to Telegram
- **Replay system** — full replay runner, paper scenarios, flow / launch rehearsals
- **Dynamic measurement profiles** — event-driven on-chain profile registry, DexScreener address resolution

### Phase 3 (Hardening & Production) — remaining

The active engineering focus is now on:

- **Holder growth aggregator** — listed in data-acquisition plan but not yet implemented
- **Social discovery mode** — continuous broad social monitoring (not just event-triggered confirmation)
- **Execution rate-limiting / position sizing** — production guards on top of paper defaults
- **Alibaba Cloud production infra** — docs exist, actual deployment / secrets / domain not configured
- **CI pipeline hardening** — workflow runs pytest / ruff / mypy, but does not yet exercise the full integration matrix
- **End-to-end integration tests** — most coverage is unit-level; no full Redis+Postgres integration tests

---

## Delivery Strategy

Execution order for early delivery:

1. Freeze scope and contracts.
2. Lock environment and configuration strategy.
3. Create repository skeleton.
4. Add infrastructure wiring for Redis and PostgreSQL.
5. Add canonical schemas and structured logging.
6. Add basic observability and local bootstrapping.

Phase 0 produces decisions and contracts.

Phase 1 produces a runnable but minimal engineering base.

---

## Milestones

### M0: Scope and Contract Freeze

Outcome:

- MVP boundaries are frozen.
- unresolved architecture questions are reduced to explicit decisions.
- schema and config ownership are defined.

Exit criteria:

- one primary chain selected
- one primary DEX route selected
- one primary CEX integration selected
- paper-trading-only default agreed
- domain models approved
- risk baseline approved

### M1: Foundation Ready

Outcome:

- repository has a stable directory layout
- services can share configuration
- local infrastructure boots consistently
- event models and logging contracts exist

Exit criteria:

- local dev environment starts required dependencies
- Python package structure exists
- Redis and PostgreSQL connectivity can be smoke-tested
- canonical schemas compile and validate
- structured logs and baseline metrics are emitted

---

## Priority Order

### Phase 0 — Spec Freeze (complete)

| ID | Title |
| --- | --- |
| BKL-001 | Freeze MVP scope decisions |
| BKL-002 | Freeze canonical domain models |
| BKL-003 | Freeze configuration strategy |
| BKL-004 | Define risk policy baseline |

### Phase 1 — Foundation (complete)

| ID | Title |
| --- | --- |
| BKL-005 | Create repository scaffold |
| BKL-006 | Add Python project and dependency baseline |
| BKL-007 | Add infrastructure wiring for Redis and PostgreSQL |
| BKL-008 | Add canonical schemas module |
| BKL-009 | Add structured logging and metrics baseline |
| BKL-010 | Add local compose stack |
| BKL-011 | Add settings and environment templates |
| BKL-012 | Add smoke tests for infra boot and schema import |
| BKL-013 | Add contributor runbook for local startup |
| BKL-014 | Add CI placeholder workflow |
| BKL-015 | Add ADR template for future architecture decisions |

### Phase 2 — Alpha Discovery (complete)

| ID | Title |
| --- | --- |
| BKL-016 | Launch alpha discovery |
| BKL-017 | Catalyst alpha discovery & entity extraction |
| BKL-018 | Social confirmation pipeline + LLM analysis |
| BKL-019 | Flow measurement (wallet intelligence) |
| BKL-020 | On-chain feature pipeline |
| BKL-021 | Wallet intelligence sync |
| BKL-022 | Telegram alpha notifications |
| BKL-023 | Replay & rehearsal framework |
| BKL-024 | Unified alpha architecture document |
| BKL-025 | Social LLM pipeline document |
| BKL-026 | Dynamic on-chain measurement profiles |

### Phase 3 — Hardening & Production (open)

| ID | Title | Priority |
| --- | --- | --- |
| BKL-028 | Holder growth aggregator | P1 |
| BKL-030 | Social continuous discovery mode | P1 |
| BKL-031 | Execution rate-limiting & position sizing | P1 |
| BKL-032 | End-to-end integration tests | P1 |
| BKL-033 | CI pipeline with full test suite | P2 |
| BKL-034 | Alibaba Cloud production deployment | P2 |

---

## Dependency Map

- BKL-001 blocks BKL-002, BKL-003, BKL-004
- BKL-002 blocks BKL-008
- BKL-003 blocks BKL-006, BKL-007, BKL-011
- BKL-005 blocks all code-bearing Phase 1 work
- BKL-006 blocks BKL-007, BKL-008, BKL-009, BKL-012
- BKL-007 blocks BKL-010 and part of BKL-012
- BKL-008 blocks downstream application services in Phase 2
- BKL-009 blocks observability consistency for later services

---

## Suggested Execution Sequence

### Phase 0–1 (complete)

1. BKL-001 → BKL-002 → BKL-003 → BKL-004 → BKL-005 → BKL-006
2. BKL-011 → BKL-007 → BKL-008 → BKL-009 → BKL-010 → BKL-012 → BKL-013
3. BKL-014 → BKL-015

### Phase 2 (complete)

4. BKL-016 → BKL-020 → BKL-017 → BKL-018 → BKL-024 → BKL-025
5. BKL-019 → BKL-021 → BKL-023 → BKL-022 → BKL-026

### Phase 3 (next)

6. **BKL-028** → holder growth from trade windows
7. **BKL-031** → live execution hardening (rate limits, sizing checks)
8. **BKL-030** → continuous social monitoring (beyond event-triggered confirmation)
9. **BKL-032** → end-to-end integration coverage
10. **BKL-033** → CI that runs the full integration matrix
11. **BKL-034** → production deployment to Alibaba Cloud

---

## Completion Snapshot

### Phase 0 — Spec Freeze

| ID | Status | Evidence |
| --- | --- | --- |
| BKL-001 | complete | [docs/decisions.md](docs/decisions.md) |
| BKL-002 | complete | [docs/schemas.md](docs/schemas.md), [core/schemas.py](core/schemas.py) |
| BKL-003 | complete | [docs/configuration.md](docs/configuration.md), [.env.example](.env.example), [infra/settings.yaml](infra/settings.yaml) |
| BKL-004 | complete | [docs/risk-policy.md](docs/risk-policy.md) |

### Phase 1 — Foundation

| ID | Status | Evidence |
| --- | --- | --- |
| BKL-005 | complete | [README.md](README.md), [core/__init__.py](core/__init__.py), [tests/test_pipeline.py](tests/test_pipeline.py) |
| BKL-006 | complete | [pyproject.toml](pyproject.toml) |
| BKL-007 | complete | [infra/redis_stream.py](infra/redis_stream.py), [infra/postgres.py](infra/postgres.py), [tests/test_infra.py](tests/test_infra.py) |
| BKL-008 | complete | [core/schemas.py](core/schemas.py), [tests/test_schemas.py](tests/test_schemas.py) |
| BKL-009 | complete | [infra/logging.py](infra/logging.py), [infra/metrics.py](infra/metrics.py), [tests/test_logging.py](tests/test_logging.py), [tests/test_worker_observability.py](tests/test_worker_observability.py) |
| BKL-010 | complete | [docker-compose.yml](docker-compose.yml) |
| BKL-011 | complete | [.env.example](.env.example), [infra/settings.yaml](infra/settings.yaml), [tests/test_settings.py](tests/test_settings.py) |
| BKL-012 | complete | [tests/test_infra.py](tests/test_infra.py), [tests/test_schemas.py](tests/test_schemas.py), [tests/test_settings.py](tests/test_settings.py) |
| BKL-013 | complete | [README.md](README.md), [run_full_stack.sh](run_full_stack.sh) |
| BKL-014 | complete | [.github/workflows/ci.yml](.github/workflows/ci.yml) |
| BKL-015 | complete | [docs/adr/0000-template.md](docs/adr/0000-template.md), [docs/adr/README.md](docs/adr/README.md) |

### Phase 2 — Alpha Discovery

| ID | Status | Evidence |
| --- | --- | --- |
| BKL-016 | complete | [discovery/pool_scanner.py](discovery/pool_scanner.py), [discovery/live_sources.py](discovery/live_sources.py), [discovery/service.py](discovery/service.py) |
| BKL-017 | complete | [discovery/catalyst_scanner.py](discovery/catalyst_scanner.py), [discovery/catalyst_live_sources.py](discovery/catalyst_live_sources.py), [discovery/catalyst_entity_extractor.py](discovery/catalyst_entity_extractor.py) |
| BKL-018 | complete | [sentinel/social_live_sources.py](sentinel/social_live_sources.py), [sentinel/social_llm.py](sentinel/social_llm.py), [docs/social-llm-alpha-pipeline.md](docs/social-llm-alpha-pipeline.md) |
| BKL-019 | complete | [discovery/flow_live_sources.py](discovery/flow_live_sources.py), [discovery/flow_scanner.py](discovery/flow_scanner.py) |
| BKL-020 | complete | [sentinel/onchain_collector.py](sentinel/onchain_collector.py), [sentinel/feature_aggregator.py](sentinel/feature_aggregator.py), [sentinel/onchain_feature_sync.py](sentinel/onchain_feature_sync.py) |
| BKL-021 | complete | [sentinel/wallet_intelligence_sync.py](sentinel/wallet_intelligence_sync.py), [sentinel/okx_wallet_registry_importer.py](sentinel/okx_wallet_registry_importer.py) |
| BKL-022 | complete | [notifications/telegram_publisher.py](notifications/telegram_publisher.py) |
| BKL-023 | complete | [replay/runner.py](replay/runner.py), [replay/paper_scenario.py](replay/paper_scenario.py), [replay/flow_alpha_rehearsal.py](replay/flow_alpha_rehearsal.py) |
| BKL-024 | complete | [docs/unified-alpha-architecture.md](docs/unified-alpha-architecture.md) |
| BKL-025 | complete | [docs/social-llm-alpha-pipeline.md](docs/social-llm-alpha-pipeline.md) |
| BKL-026 | complete | [sentinel/onchain_live_sources.py](sentinel/onchain_live_sources.py) (MeasurementProfileRegistry), [core/schemas.py](core/schemas.py) (MeasurementProfile) |
| BKL-019 | Flow measurement (wallet intelligence) | 2 | P1 | 2d | complete |
| BKL-020 | On-chain feature pipeline (buy_pressure, slippage) | 2 | P0 | 3d | complete |
| BKL-021 | Wallet intelligence sync (OKX registry) | 2 | P1 | 2d | complete |
| BKL-022 | Telegram alpha notifications | 2 | P1 | 1d | complete |
| BKL-023 | Replay & rehearsal framework | 2 | P1 | 2d | complete |
| BKL-024 | Unified alpha architecture document | 2 | P0 | 0.5d | complete |
| BKL-025 | Social LLM pipeline document | 2 | P0 | 0.5d | complete |
| BKL-026 | Dynamic on-chain measurement profiles | 2 | P2 | 2d | complete |
| BKL-027 | Measurement profile persistence | 3 | P0 | 1.5d | complete |
| BKL-028 | Holder growth aggregator | 3 | P1 | 1d | open |
| BKL-029 | Wallet inflow/outflow scoring | 3 | P1 | 1d | complete |
| BKL-030 | Social continuous discovery mode | 3 | P1 | 2d | open |
| BKL-031 | Execution rate-limiting & position sizing | 3 | P1 | 1.5d | open |
| BKL-032 | End-to-end integration tests | 3 | P1 | 2d | open |
| BKL-033 | CI pipeline with full test suite | 3 | P2 | 1d | open |
| BKL-034 | Alibaba Cloud production deployment | 3 | P2 | 3d | open |

---

## Work Item Summary

### Phase 0 — Spec Freeze (complete)

| ID | Title | Phase | Priority | Estimate | Status |
| --- | --- | --- | --- | --- | --- |
| BKL-001 | Freeze MVP scope decisions | 0 | P0 | 0.5d | complete |
| BKL-002 | Freeze canonical domain models | 0 | P0 | 1d | complete |
| BKL-003 | Freeze configuration strategy | 0 | P0 | 0.5d | complete |
| BKL-004 | Define risk policy baseline | 0 | P0 | 0.5d | complete |

### Phase 1 — Foundation (complete)

| ID | Title | Phase | Priority | Estimate | Status |
| --- | --- | --- | --- | --- | --- |
| BKL-005 | Create repository scaffold | 1 | P0 | 0.5d | complete |
| BKL-006 | Add Python project baseline | 1 | P0 | 0.5d | complete |
| BKL-007 | Wire Redis and PostgreSQL base clients | 1 | P0 | 1d | complete |
| BKL-008 | Add canonical schemas module | 1 | P0 | 1d | complete |
| BKL-009 | Add logging and metrics baseline | 1 | P0 | 0.5d | complete |
| BKL-010 | Add local compose stack | 1 | P1 | 0.5d | complete |
| BKL-011 | Add settings and env templates | 1 | P1 | 0.5d | complete |
| BKL-012 | Add smoke tests | 1 | P1 | 0.5d | complete |
| BKL-013 | Add local runbook | 1 | P1 | 0.5d | complete |
| BKL-014 | Add CI placeholder workflow | 1 | P2 | 0.5d | complete |
| BKL-015 | Add ADR template | 1 | P2 | 0.25d | complete |

### Phase 2 — Alpha Discovery (complete)

| ID | Title | Phase | Priority | Estimate | Status |
| --- | --- | --- | --- | --- | --- |
| BKL-016 | Launch alpha discovery | 2 | P0 | 3d | complete |
| BKL-017 | Catalyst alpha discovery & entity extraction | 2 | P0 | 3d | complete |
| BKL-018 | Social confirmation pipeline + LLM analysis | 2 | P0 | 3d | complete |
| BKL-019 | Flow measurement (wallet intelligence) | 2 | P1 | 2d | complete |
| BKL-020 | On-chain feature pipeline | 2 | P0 | 3d | complete |
| BKL-021 | Wallet intelligence sync (OKX registry) | 2 | P1 | 2d | complete |
| BKL-022 | Telegram alpha notifications | 2 | P1 | 1d | complete |
| BKL-023 | Replay & rehearsal framework | 2 | P1 | 2d | complete |
| BKL-024 | Unified alpha architecture document | 2 | P0 | 0.5d | complete |
| BKL-025 | Social LLM pipeline document | 2 | P0 | 0.5d | complete |
| BKL-026 | Dynamic on-chain measurement profiles | 2 | P2 | 2d | complete |

### Phase 3 — Hardening & Production

| ID | Title | Phase | Priority | Estimate | Status |
| --- | --- | --- | --- | --- | --- |
| BKL-027 | Measurement profile persistence | 3 | P0 | 1.5d | complete |
| BKL-028 | Holder growth aggregator | 3 | P1 | 1d | open |
| BKL-029 | Wallet inflow/outflow scoring | 3 | P1 | 1d | complete |
| BKL-030 | Social continuous discovery mode | 3 | P1 | 2d | open |
| BKL-031 | Execution rate-limiting & position sizing | 3 | P1 | 1.5d | open |
| BKL-032 | End-to-end integration tests | 3 | P1 | 2d | open |
| BKL-033 | CI pipeline with full test suite | 3 | P2 | 1d | open |
| BKL-034 | Alibaba Cloud production deployment | 3 | P2 | 3d | open |

---

## Definition of Done

A backlog item is complete only if all of the following are true:

1. The deliverable exists in the repository.
2. Any new config or command is documented.
3. A narrow validation step has been run.
4. Any unresolved tradeoff is captured explicitly, not implied.

---

## Issue Specs

Detailed issue specifications are in [docs/issues-phase-0-1.md](./docs/issues-phase-0-1.md).