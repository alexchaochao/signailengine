# MVP Decisions

## Scope Freeze

This document records the Phase 0 decisions for the first deliverable.

---

## BKL-001 Decisions

### Primary Chain

- Decision: Solana only
- Reason: lowest integration surface for the initial DEX execution path and the shortest route to a paper-trading loop

### Primary DEX Path

- Decision: Solana swap aggregator path via a single primary adapter
- Reason: abstract venue routing later while keeping the first DEX execution path simple

### First CEX Integration

- Decision: Binance spot paper or sandbox-compatible adapter contract
- Reason: broad liquidity coverage and a practical first bridge shape even if the first implementation remains paper-only

### Social Input for MVP

- Decision: disabled by default, represented as a stubbed optional source
- Reason: social signal reliability is lower than chain and market data, and it should not block the first end-to-end loop

### Trading Mode

- Decision: paper trading only by default
- Reason: replay, reconciliation, and kill switch controls do not exist yet

### Initial Token Universe

- Decision: Solana tokens from an explicit allowlist only
- Constraints:
  - minimum estimated liquidity: 100000 USD
  - minimum 5 minute notional volume: 25000 USD
  - no permissionless auto-discovery in MVP
- Reason: avoid low-liquidity tail risk before rug checks and venue simulation are mature

---

## BKL-002 Ownership Decisions

### Schema Ownership

- `core/schemas.py` is the source of truth for runtime models
- [docs/schemas.md](./schemas.md) is the human-readable contract reference

### Versioning Rule

- All canonical event-bearing models must include `schema_version`
- Backward-incompatible schema changes require a version bump
- Additive optional fields do not require a major version change

---

## BKL-003 Configuration Decisions

### Configuration Sources

Priority order, highest first:

1. environment variables
2. local secret injection outside the repository
3. `infra/settings.yaml`

### Environments

- `local`
- `paper`
- `live`

### Secret Handling

- no live credentials in source control
- `.env.example` contains placeholders only
- live secrets must be injected via environment or external secret manager

---

## BKL-004 Risk Policy Decisions

### Paper Environment Defaults

- max per-token exposure: 10 percent of portfolio
- max chain exposure: 40 percent of portfolio
- max concurrent positions: 5
- max daily loss: 3 percent of portfolio
- post-exit cooldown: 30 minutes

### Live Environment Rule

- live trading remains disabled until replay, reconciliation, alerts, and kill switch are implemented

---

## Deferred Decisions

- specific production DEX vendor implementation
- specific live CEX account mode and permissions
- social provider selection
- multi-chain rollout sequence after Solana