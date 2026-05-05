# SignalEngine V2 Backlog

## Purpose

This backlog converts the V2 technical design into executable work items for Phase 0 and Phase 1.

Phase definitions are inherited from [plan.md](./plan.md):

- Phase 0: specification freeze
- Phase 1: foundation

The goal of this backlog is to make the next implementation step unambiguous.

## Current Status

Backlog scan status as of 2026-05-05:

- Phase 0 and Phase 1 foundation items in this backlog have been delivered in the repository.
- The repository now contains the expected decision records, schema and config contracts, infra helpers, smoke tests, local startup docs, CI placeholder workflow, and ADR template.
- The active engineering focus has moved beyond this foundation backlog to state persistence, message-surface expansion, notification stability, and guarded live-readiness work.

Current next items:

- implement persistent token-state runtime storage so the lifecycle engine transitions from prior state instead of recalculating from `UNKNOWN` on each batch
- extend replay and audit outputs with FSM transition context where downstream consumers need stable state history
- expand message-source coverage beyond exchange announcement feeds, beginning with additional narrative sources such as X and Reddit after the persistent FSM work lands
- keep replay/live parity tight as more execution and notification paths are added
- continue guarded live-readiness work and Alibaba Cloud production planning after state persistence and notification surfaces stay stable

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

### P0 Critical

- BKL-001 Freeze MVP scope decisions
- BKL-002 Freeze canonical domain models
- BKL-003 Freeze configuration strategy
- BKL-004 Define risk policy baseline
- BKL-005 Create repository scaffold
- BKL-006 Add Python project and dependency baseline
- BKL-007 Add infrastructure wiring for Redis and PostgreSQL
- BKL-008 Add canonical schemas module
- BKL-009 Add structured logging and metrics baseline

### P1 Important

- BKL-010 Add local compose stack
- BKL-011 Add settings and environment templates
- BKL-012 Add smoke tests for infra boot and schema import
- BKL-013 Add contributor runbook for local startup

### P2 Follow-up

- BKL-014 Add CI placeholder workflow
- BKL-015 Add ADR template for future architecture decisions

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

1. BKL-001
2. BKL-002
3. BKL-003
4. BKL-004
5. BKL-005
6. BKL-006
7. BKL-011
8. BKL-007
9. BKL-008
10. BKL-009
11. BKL-010
12. BKL-012
13. BKL-013

---

## Completion Snapshot

| ID | Status | Evidence |
| --- | --- | --- |
| BKL-001 | complete | [docs/decisions.md](docs/decisions.md) |
| BKL-002 | complete | [docs/schemas.md](docs/schemas.md), [core/schemas.py](core/schemas.py) |
| BKL-003 | complete | [docs/configuration.md](docs/configuration.md), [.env.example](.env.example), [infra/settings.yaml](infra/settings.yaml) |
| BKL-004 | complete | [docs/risk-policy.md](docs/risk-policy.md) |
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

---

## Work Item Summary

| ID | Title | Phase | Priority | Estimate | Status |
| --- | --- | --- | --- | --- | --- |
| BKL-001 | Freeze MVP scope decisions | 0 | P0 | 0.5d | complete |
| BKL-002 | Freeze canonical domain models | 0 | P0 | 1d | complete |
| BKL-003 | Freeze configuration strategy | 0 | P0 | 0.5d | complete |
| BKL-004 | Define risk policy baseline | 0 | P0 | 0.5d | complete |
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