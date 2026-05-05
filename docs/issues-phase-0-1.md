# Phase 0 and Phase 1 Issue Specifications

This document provides execution-ready issue specs for the first two project phases.

---

## BKL-001 Freeze MVP Scope Decisions

### Goal

Freeze the first delivery scope to prevent implementation drift.

### Problem

The technical design intentionally narrows the system, but several key operating choices are still open. Without explicit decisions, Phase 1 scaffolding will encode assumptions that later force rework.

### Tasks

1. Select the primary chain.
2. Select the primary DEX execution path.
3. Select the first CEX integration target.
4. Decide whether social input is disabled or stubbed in MVP.
5. Confirm paper-trading-only default.
6. Define initial token universe constraints and liquidity floor.

### Deliverables

- a decision record section in the repository
- final values for all Phase 0 open questions

### Acceptance Criteria

1. All open design questions in [plan.md](../plan.md) section 21 have a chosen answer or an explicit defer tag.
2. There is no unresolved ambiguity about chain, DEX, CEX, or trading mode for MVP.

### Dependencies

- none

### Suggested Output Files

- `docs/decisions.md`

---

## BKL-002 Freeze Canonical Domain Models

### Goal

Lock the event and signal contracts used across all services.

### Problem

Without canonical models, each service will create its own ad hoc payload format, which will break replay and increase integration cost.

### Tasks

1. Finalize `EventEnvelope` fields.
2. Finalize `TokenSignal` fields and types.
3. Finalize `ExecutionIntent` fields and types.
4. Finalize `RiskDecision` fields and types.
5. Define required enums such as `TokenState`, `VenueType`, and `ActionType`.
6. Define versioning rules for schema evolution.

### Deliverables

- one canonical schema document
- one code module that represents these types

### Acceptance Criteria

1. Fields have names, required or optional status, and intended type.
2. Each model has clear ownership and producer or consumer mapping.
3. A schema versioning rule exists.

### Dependencies

- BKL-001

### Suggested Output Files

- `docs/schemas.md`
- `core/schemas.py`

---

## BKL-003 Freeze Configuration Strategy

### Goal

Define how configuration is structured, loaded, and overridden.

### Problem

Trading systems fail in practice when environment-specific settings are inconsistent or undocumented.

### Tasks

1. Define configuration sources: file, environment, secrets.
2. Define precedence rules.
3. Define environment names: local, paper, live.
4. Define settings groups: infra, risk, venues, runtime, observability.
5. Decide config file format.

### Deliverables

- configuration strategy note
- template settings file
- environment variable template

### Acceptance Criteria

1. A new developer can identify where to change local settings.
2. Live-only values are separated from source-controlled defaults.
3. Config precedence is documented.

### Dependencies

- BKL-001

### Suggested Output Files

- `docs/configuration.md`
- `.env.example`
- `infra/settings.yaml`

---

## BKL-004 Define Risk Policy Baseline

### Goal

Document the minimum risk limits that all later components must enforce.

### Problem

If risk limits are introduced after routing and execution work begins, trade semantics will need to be redesigned.

### Tasks

1. Define max per-token exposure.
2. Define max chain exposure.
3. Define max concurrent positions.
4. Define max daily loss.
5. Define cooldown policy.
6. Define live-trading guardrails.

### Deliverables

- baseline risk policy document

### Acceptance Criteria

1. The risk engine can later implement all documented constraints without adding new policy concepts.
2. There is a clear distinction between paper and live policies.

### Dependencies

- BKL-001

### Suggested Output Files

- `docs/risk-policy.md`

---

## BKL-005 Create Repository Scaffold

### Goal

Create the initial repository structure from the technical design.

### Problem

The current repository contains only planning documentation. Development cannot proceed consistently until there is a stable module layout.

### Tasks

1. Create the top-level package directories.
2. Add placeholder module files or package initializers.
3. Add a tests directory.
4. Add a docs directory for operating documents.

### Deliverables

- a stable repository layout

### Acceptance Criteria

1. Directory structure matches the approved design with only intentional deviations.
2. The project layout is import-safe for later Python packaging.

### Dependencies

- BKL-001

### Suggested Output Files

- package directories and `__init__.py` files

---

## BKL-006 Add Python Project Baseline

### Goal

Establish a minimal Python project baseline for application code and tests.

### Problem

Without a project baseline, dependency management, imports, and tests will diverge early.

### Tasks

1. Add project metadata file.
2. Add runtime dependencies.
3. Add test dependencies.
4. Add lint and format tool baseline if desired.
5. Add package version and interpreter target.

### Deliverables

- `pyproject.toml`

### Acceptance Criteria

1. Project dependencies can be installed in one step.
2. Test runner and import paths are defined.

### Dependencies

- BKL-003
- BKL-005

### Suggested Output Files

- `pyproject.toml`

---

## BKL-007 Wire Redis and PostgreSQL Base Clients

### Goal

Provide shared infrastructure helpers for Redis Streams and PostgreSQL connections.

### Problem

If each service creates its own infra wiring, connection behavior and retry logic will drift before the first real feature is built.

### Tasks

1. Add Redis client factory.
2. Add PostgreSQL engine or connection factory.
3. Add minimal health-check helpers.
4. Add connection settings mapping.

### Deliverables

- shared infra modules for Redis and PostgreSQL

### Acceptance Criteria

1. Both clients can be initialized from shared settings.
2. A smoke test can verify connectivity behavior or initialization path.

### Dependencies

- BKL-003
- BKL-005
- BKL-006

### Suggested Output Files

- `infra/redis_stream.py`
- `infra/postgres.py`

---

## BKL-008 Add Canonical Schemas Module

### Goal

Implement the approved domain models in code.

### Problem

Documentation-only schemas are insufficient once implementation starts; services need importable definitions.

### Tasks

1. Add enums.
2. Add data models.
3. Add serialization helpers if needed.
4. Add validation rules for required fields.

### Deliverables

- importable schema module

### Acceptance Criteria

1. The module covers all canonical models frozen in BKL-002.
2. Validation errors are explicit when required fields are missing.

### Dependencies

- BKL-002
- BKL-006

### Suggested Output Files

- `core/schemas.py`

---

## BKL-009 Add Structured Logging and Metrics Baseline

### Goal

Create a common observability foundation for all later services.

### Problem

If logging and metrics are added after service implementation begins, cross-service troubleshooting becomes inconsistent.

### Tasks

1. Define log format and required fields.
2. Add common logger helper.
3. Add Prometheus metrics helper.
4. Add base counters and latency metrics.

### Deliverables

- shared logging helper
- shared metrics helper

### Acceptance Criteria

1. A service can emit structured logs with correlation context.
2. A service can emit at least one counter and one latency metric.

### Dependencies

- BKL-005
- BKL-006

### Suggested Output Files

- `infra/metrics.py`
- `infra/logging.py`

---

## BKL-010 Add Local Compose Stack

### Goal

Allow local startup of Phase 1 dependencies with one command.

### Problem

Without local infra automation, developer setup becomes a source of non-deterministic failures.

### Tasks

1. Add Redis service.
2. Add PostgreSQL service.
3. Add port mappings.
4. Add persistent volume policy.

### Deliverables

- local compose file

### Acceptance Criteria

1. Redis and PostgreSQL start locally with documented commands.
2. Default settings align with config templates.

### Dependencies

- BKL-003
- BKL-007

### Suggested Output Files

- `docker-compose.yml`

---

## BKL-011 Add Settings and Environment Templates

### Goal

Provide safe defaults and an obvious local configuration path.

### Problem

The project needs a single source of truth for local configuration before any service code starts reading settings.

### Tasks

1. Add `.env.example`.
2. Add default settings file.
3. Document required overrides.
4. Keep live credentials out of source control.

### Deliverables

- env template
- default settings file

### Acceptance Criteria

1. A developer can copy the templates and run locally.
2. No sensitive live values are present.

### Dependencies

- BKL-003
- BKL-005

### Suggested Output Files

- `.env.example`
- `infra/settings.yaml`

---

## BKL-012 Add Smoke Tests

### Goal

Add narrow validation for the Phase 1 base.

### Problem

Without smoke tests, the foundation phase becomes a pile of files rather than a verified platform.

### Tasks

1. Add schema import test.
2. Add settings load test.
3. Add infra initialization test.

### Deliverables

- smoke test module set

### Acceptance Criteria

1. Tests can run locally with one command.
2. Tests validate schema imports and base initialization.

### Dependencies

- BKL-006
- BKL-007
- BKL-008
- BKL-011

### Suggested Output Files

- `tests/test_schemas.py`
- `tests/test_settings.py`
- `tests/test_infra.py`

---

## BKL-013 Add Local Runbook

### Goal

Document how to boot and validate the project locally.

### Problem

Foundation work is not complete until another developer can reproduce the local setup without tribal knowledge.

### Tasks

1. Document setup prerequisites.
2. Document install command.
3. Document infra startup command.
4. Document test command.
5. Document expected outputs.

### Deliverables

- local runbook section in README or docs

### Acceptance Criteria

1. A clean machine can follow the steps without guessing missing commands.
2. The runbook matches the actual repository structure.

### Dependencies

- BKL-006
- BKL-010
- BKL-012

### Suggested Output Files

- `README.md`

---

## BKL-014 Add CI Placeholder Workflow

### Goal

Reserve a minimal CI path for foundation validation.

### Tasks

1. Add test workflow skeleton.
2. Run smoke tests in CI.

### Dependencies

- BKL-012

---

## BKL-015 Add ADR Template

### Goal

Create a lightweight architecture decision template for future changes.

### Tasks

1. Add ADR markdown template.
2. Add naming convention.

### Dependencies

- none