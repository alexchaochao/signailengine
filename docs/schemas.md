# Canonical Schemas

## Purpose

This document defines the canonical models shared across ingestion, decision, execution, and replay services.

Runtime source of truth: [core/schemas.py](../core/schemas.py)

---

## Versioning

- every event-bearing model includes `schema_version`
- current version for the initial foundation is `v1`
- backward-incompatible changes require an explicit version increment

---

## Model Ownership

| Model | Primary Producer | Primary Consumers |
| --- | --- | --- |
| `EventEnvelope` | ingestion services | signal engine, replay |
| `TokenSignal` | signal engine | state engine, router, replay |
| `ExecutionIntent` | router | risk engine, execution adapters |
| `RiskDecision` | risk engine | execution adapters, replay |
|

## Enums

### TokenState

- `UNKNOWN`
- `PRE_LAUNCH`
- `EARLY_LIQUIDITY`
- `NARRATIVE_EXPLOSION`
- `CEX_LISTING`
- `TRENDING`
- `DISTRIBUTION`
- `DEAD`

### VenueType

- `DEX`
- `CEX`
- `NO_TRADE`

### ActionType

- `BUY`
- `SELL`
- `EXIT`
- `HOLD`

---

## EventEnvelope

Required fields:

- `schema_version: str`
- `event_id: str`
- `event_type: str`
- `source: str`
- `chain: str`
- `token: str`
- `observed_at: datetime`
- `ingested_at: datetime`
- `payload: dict`

Rules:

- `event_id` must be globally unique for idempotency
- `observed_at` is the replay ordering timestamp

---

## TokenSignal

Required fields:

- `schema_version: str`
- `token: str`
- `chain: str`
- `state_candidate: TokenState`
- `features: dict[str, float | int | bool]`
- `sub_scores: dict[str, float]`
- `alpha_score: float`
- `reasons: list[str]`
- `timestamp: int`

Rules:

- `alpha_score` must be within 0 to 1
- `sub_scores` values must be within 0 to 1

---

## ExecutionIntent

Required fields:

- `schema_version: str`
- `intent_id: str`
- `token: str`
- `chain: str`
- `venue_type: VenueType`
- `venue: str`
- `action: ActionType`
- `confidence: float`
- `target_notional_usd: float`
- `max_slippage_bps: int`
- `state: TokenState`
- `strategy: str`
- `reasons: list[str]`

Rules:

- `confidence` must be within 0 to 1
- `target_notional_usd` must be non-negative

---

## RiskDecision

Required fields:

- `schema_version: str`
- `intent_id: str`
- `allowed: bool`
- `adjusted_notional_usd: float`
- `violations: list[str]`
- `warnings: list[str]`
- `timestamp: datetime`

Rules:

- `adjusted_notional_usd` must be non-negative

---

## Solana RPC Runtime Config

These settings control the live or stubbed Solana DEX adapter transport boundary.

- `venues.solana_rpc_url: str`
	- target JSON-RPC endpoint for blockhash lookup and transaction submission
- `venues.solana_rpc_timeout_seconds: float`
	- per-request network timeout for the Solana RPC client
	- must be greater than `0`
- `venues.solana_rpc_max_retries: int`
	- number of retry attempts after the first network failure
	- must be greater than or equal to `0`
- `venues.solana_quote_slippage_bps: int`
	- quote-time slippage budget recorded on `ExecutionQuote`
	- must be greater than `0`
- `venues.solana_jito_enabled: bool`
	- toggles the submission transport label to `jito`
	- only valid for live/stub Solana DEX adapters, not the paper DEX executor

## Solana RPC Error Semantics

The Solana RPC client raises explicit error codes so failures remain grep-friendly in logs and test output.

- `invalid_solana_rpc_http_status:<status>`
	- the endpoint returned a non-`2xx` HTTP response
- `invalid_solana_rpc_content_type`
	- the endpoint did not return `application/json`
- `invalid_solana_rpc_response_too_large`
	- the response body exceeded the built-in 1 MB safety limit
- `invalid_solana_rpc_jsonrpc`
	- the response `jsonrpc` version did not match the request envelope
- `invalid_solana_rpc_response_id`
	- the response `id` did not match the request `id`
- `invalid_solana_rpc_response`
	- the body could not be decoded into a valid JSON-RPC object
- `solana_rpc_error:<code>`
	- the remote node returned a JSON-RPC error payload
- `solana_rpc_transport_error`
	- all configured retry attempts were exhausted on network-level failures