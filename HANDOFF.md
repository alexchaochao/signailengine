# SignalEngine Handoff

Updated: 2026-05-05

## Snapshot

The repository has moved beyond the original Phase 0 and Phase 1 foundation backlog. The current codebase now includes:

- deterministic event-driven paper execution
- launch, catalyst, and flow alpha ingestion
- EVM quote and trade acquisition plus quote-derived fallback market context
- Telegram notification delivery for first-time `QUALIFIED` alpha candidates

Local resource cleanup was performed before handoff. No background `python -m core.worker` processes are expected to be running unless the next session starts them explicitly.

## Delivered In This Session Window

1. Added `alpha.candidate_qualified` publication when launch, catalyst, or flow candidates first transition into `QUALIFIED`.
2. Added Telegram notification configuration, persistent delivery deduplication, one-chat publisher worker mode, and focused tests.
3. Verified Telegram delivery with a real smoke test against the configured bot and chat.
4. Fixed EVM quote-market fallback propagation so quote-only routes now publish non-zero `volume_5m_usd` and `buy_pressure` when DexScreener market context is available.
5. Corrected DexScreener pair selection to prefer active market pairs instead of low-activity exact-quote matches.
6. Replaced brittle catalyst source assumptions with Binance CMS JSON parsing and Coinbase HTML article-card parsing.

## Current Technical Reality

1. `SignalEngine` computes a `state_candidate`, but `PipelineWorker` still calls `StateEngine.transition(None, signal, ...)`, so the lifecycle engine does not yet persist and reuse prior token state.
2. Telegram publishing is intentionally narrow: first-time `QUALIFIED` candidates only, `LAUNCH` and `CATALYST` only, one Telegram chat only, plain-text synchronous templates only.
3. Social-message coverage is still limited. Message-style alpha is currently driven by exchange-announcement and wallet-flow style inputs, not by broad X or Reddit ingestion.
4. The workspace does not contain a git repository, so session handoff and change review must rely on direct file inspection and focused validation commands.

## Current Next Tasks

1. Implement persistent FSM runtime state storage and wire it into the pipeline.
2. Add focused runtime-state and pipeline tests for the persistent FSM.
3. Expand message sources, beginning with X and Reddit style inputs.
4. Design and harden Alibaba Cloud production deployment.
5. Add production monitoring and alerting hardening.

## Suggested Restart Point

If the next session should continue implementation work, the best restart point is:

1. read [README.md](README.md)
2. read [BACKLOG.md](BACKLOG.md)
3. start with persistent FSM runtime state implementation in the pipeline and repository layer

## Minimal Local Runtime Advice

When local resources are constrained:

1. do not start the full stack unless the task needs it
2. start only the worker under test, for example `signalengine-worker --telegram-publisher-live --once` or `signalengine-worker --onchain-feature-live --once`
3. keep Redis and PostgreSQL local only when the task requires stream or persistence validation