# Social LLM Alpha Pipeline

## Purpose

This document defines a social discovery pipeline that can serve two roles at the same time:

1. continuously watch social platforms for token-level alpha candidates that were not discovered by onchain, launch, or flow scanners
2. perform targeted social confirmation after a token enters a meaningful lifecycle state in the evaluation plane

The design assumes that simple API retrieval is not sufficient. Social data must pass through a semantic enrichment layer so that noisy posts do not become false alpha candidates.

## Design Correction

Static token watchlists are no longer considered a valid discovery design.

The repository must not require operator-provided token symbols, cashtags, or one-source-per-token configuration such as `x_bonk_watch` or `reddit_bonk_watch` in order to discover alpha.

Discovery-phase social config should describe provider capability only:

- platform
- transport and auth
- rate limits
- endpoint shape
- polling or subscription mode

Token, project name, aliases, and final search terms must come from upstream candidate creation or entity extraction, not from static environment variables.

## Design Goals

- preserve low-latency social monitoring as one discovery source
- support event-driven retrieval for token-targeted confirmation
- add LLM-based denoising, entity resolution, and catalyst analysis without placing LLM calls on the execution-critical path
- emit replayable events into the existing raw event and candidate workflow
- keep deterministic scoring and routing separate from non-deterministic model inference

## Non-Goals

- direct trading decisions from a single social post
- blocking `PipelineWorker.process_events()` on external social or LLM requests
- open-world token identity resolution without a registry or alias set
- full firehose ingestion from X or Reddit in the first version

## Operating Modes

The social pipeline should support two distinct modes.

### 1. Social Discovery Mode

Use broad, source-level retrieval only when the provider can supply passive or open discovery signals without requiring a token watchlist.

This mode answers:

- what project, ticker, or narrative is suddenly being discussed?
- is this discussion likely to be actionable alpha rather than spam?
- should the system open a discovery candidate?

Constraint:

- do not create one configured watch source per token
- if the platform requires a token-specific query to return useful data, use it only in confirmation mode after another discovery module has produced an entity candidate

Primary output:

- `alpha.candidate_opened`
- `alpha.snapshot_updated`

### 2. Social Confirmation Mode

Use event-triggered social queries after a token has already entered a meaningful lifecycle stage such as `PRE_LAUNCH`, `EARLY_LIQUIDITY`, or `NARRATIVE_EXPLOSION`.

This mode answers:

- does current social evidence confirm the transition?
- is the narrative accelerating, weakening, or contradicting the chain signal?
- should the candidate score be boosted, held, or penalized?

Primary output:

- `social.query_requested`
- `social.analysis_completed`
- `alpha.snapshot_updated`

Both modes should converge into the same candidate registry and the same scoring framework.

Current direction for this repository:

- keep social confirmation mode
- remove static token-scoped social source configuration from discovery mode
- only form token-specific queries from upstream candidate identity or FSM context

## High-Level Architecture

```text
               +----------------------------------------------+
               | Social Providers                             |
               | X bridge | Reddit search | forums | news     |
               +----------------------+-----------------------+
                                      |
                                      v
               +----------------------------------------------+
               | Retrieval Layer                              |
               | query planner | fetchers | rate limits        |
               +----------------------+-----------------------+
                                      |
                                      v
               +----------------------------------------------+
               | Pre-LLM Reduction Layer                      |
               | dedupe | time-window aggregate | heuristics   |
               +----------------------+-----------------------+
                                      |
                                      v
               +----------------------------------------------+
               | LLM Enrichment Layer                         |
               | denoise | entity resolution | catalyst type   |
               | narrative summary | credibility | risk flags   |
               +----------------------+-----------------------+
                                      |
                  +-------------------+-------------------+
                  |                                       |
                  v                                       v
    +-------------------------------+      +-------------------------------+
    | Candidate Scoring Layer       |      | Social Snapshot Emitter       |
    | open/update/qualify/discard   |      | social.signal_snapshot        |
    +---------------+---------------+      +-------------------------------+
                    |
                    v
    +-------------------------------+
    | Unified Alpha Candidate Layer |
    | alpha.candidate_*             |
    +-------------------------------+
```

## Pipeline Stages

### Stage 1. Query Planning

The pipeline must support event-driven confirmation and optional broad discovery retrieval.

Inputs:

- platform config
- source mode: `discovery` or `confirmation`
- chain
- token identity package when available
- query template
- cooldown and polling windows

Query planning rules:

- if the request is in confirmation mode, build the query from the candidate or FSM context
- if the request is in discovery mode, use provider-level passive feeds, trending topics, or broad retrieval and then resolve mentioned assets after retrieval
- allow per-platform query parameter mapping rather than hard-coding X and Reddit URL formats into the planner

Blacklisted query planning pattern:

- operator-maintained token watchlists in environment variables or source config

Recommended identity package when a candidate already exists:

- `token`
- `chain`
- `project_name`
- `aliases`
- `cashtags`
- `official_accounts`
- `token_contract` or `token_mint` when known

This identity package should be produced by upstream discovery or entity resolution. It should not be manually maintained as static watchlist config.

### Stage 2. Retrieval Layer

The retrieval layer is responsible only for transport, pagination, and raw normalization.

Responsibilities:

- fetch provider payloads
- store request metadata and cursors
- normalize posts into a common raw social record
- attach source timestamps, author fields, engagement counts, and urls
- publish raw retrieval events when needed for replay

Suggested canonical event:

- `social.snapshot_retrieved`

Minimum normalized raw record fields:

- `platform`
- `query`
- `retrieval_mode`
- `post_id`
- `author_handle`
- `author_display_name`
- `published_at`
- `text`
- `language`
- `like_count`
- `reply_count`
- `repost_count`
- `view_count`
- `url`
- `raw_payload_ref`

### Stage 3. Pre-LLM Reduction

This layer reduces cost before model inference.

Responsibilities:

- exact and fuzzy deduplication
- repost and quote collapse
- time-window clustering
- low-information spam filtering
- author and content frequency thresholds
- candidate batch construction for LLM analysis

This layer should remain deterministic and cheap.

Examples of useful heuristics:

- repeated text above a similarity threshold
- posts shorter than a minimum token count with no asset evidence
- repeated hashtags with no informational content
- bursts dominated by a single author or author cluster

### Stage 4. LLM Enrichment

This is the semantic center of the pipeline.

The LLM should not produce final trade decisions. It should transform noisy text into structured evidence.

Recommended LLM tasks:

- relevance classification: is the batch actually about the candidate asset?
- entity resolution: which token or project is being discussed?
- catalyst extraction: listing, partnership, exploit, launch, treasury, KOL mention, meme breakout, rumor
- narrative classification: early emergence, acceleration, saturation, contradiction, collapse
- credibility assessment: firsthand, secondary rumor, copied spam, satire, unverifiable claim
- risk extraction: scam accusation, unlock concern, bot amplification, contract confusion, spoofed listing
- summarization: concise explanation with evidence spans

Recommended structured output fields:

- `relevance_score`
- `resolved_token`
- `resolved_chain`
- `resolved_project_name`
- `entity_confidence`
- `catalyst_type`
- `narrative_phase`
- `narrative_strength`
- `credibility_score`
- `noise_score`
- `risk_flags`
- `supporting_claims`
- `contradicting_claims`
- `summary`
- `analysis_version`

The LLM should analyze grouped evidence rather than isolated single posts whenever possible.

### Stage 5. Candidate Scoring

The scoring layer should remain deterministic.

Its job is to combine retrieval statistics, LLM outputs, and cross-source evidence into replayable candidate decisions.

Suggested social scoring dimensions:

- `discussion_heat`
- `author_diversity`
- `engagement_quality`
- `entity_confidence`
- `narrative_strength`
- `credibility_score`
- `noise_penalty`
- `risk_penalty`
- `cross_source_confirmation`

Suggested first social discovery score:

$$
social\_discovery = 0.20 \cdot discussion\_heat + 0.10 \cdot author\_diversity + 0.10 \cdot engagement\_quality + 0.20 \cdot entity\_confidence + 0.20 \cdot narrative\_strength + 0.15 \cdot credibility\_score + 0.05 \cdot cross\_source\_confirmation - noise\_penalty - risk\_penalty
$$

Suggested first social confirmation score:

$$
social\_confirmation = 0.35 \cdot relevance + 0.25 \cdot narrative\_strength + 0.20 \cdot credibility + 0.10 \cdot engagement\_quality + 0.10 \cdot cross\_source\_confirmation - contradiction\_penalty
$$

Both scores should be clamped to `[0, 1]`.

Decision rules:

- open candidate if `social_discovery >= open_threshold`
- update candidate if the score changed materially or narrative phase changed
- qualify candidate only when social evidence is confirmed by another plane or exceeds a higher threshold with strong credibility
- discard candidate when noise or risk penalties dominate

### Stage 6. Event Output Layer

This layer emits replayable, typed events into the existing system.

Required event families:

- `social.query_requested`
- `social.snapshot_retrieved`
- `social.analysis_completed`
- `alpha.candidate_opened`
- `alpha.snapshot_updated`
- `alpha.candidate_qualified`

Optional compatibility event:

- `social.signal_snapshot`

`social.signal_snapshot` should remain the compatibility bridge for the existing signal engine. The richer candidate and analysis events should feed the future discovery plane and candidate registry.

## Where LLM Should And Should Not Be Used

### Strong LLM Use Cases

- entity disambiguation for ambiguous tickers
- project alias expansion
- rumor versus confirmation classification
- multi-post summarization
- contradiction detection across posts and sources
- risk phrase extraction from noisy discussions
- catalyst type extraction from semi-structured announcements

### Poor LLM Use Cases

- high-frequency raw polling
- precise engagement counting
- primary dedupe and rate limiting
- final route and risk allow or deny decisions
- hard real-time execution path decisions

LLM output should always be treated as enrichment, not as an irreversible control action.

## Integration With Existing Pipeline

The existing repository already has two compatible insertion points.

### Path A. Discovery First

1. social discovery retrieves and analyzes a batch
2. candidate score crosses the open threshold
3. emit `alpha.candidate_opened`
4. candidate registry begins observation
5. if the candidate later qualifies, emit `alpha.candidate_qualified`
6. generate compatible raw events for the evaluation plane if needed

This path solves the problem where social discovers a token before other scanners do.

### Path B. FSM Confirmation

1. pipeline or candidate registry emits `social.query_requested`
2. social worker retrieves and analyzes evidence for the token
3. emit `social.analysis_completed`
4. candidate score or signal is updated with a social confirmation component
5. emit `alpha.snapshot_updated` and optionally `social.signal_snapshot`

This path solves the problem where state transitions need narrative confirmation.

The recommended architecture is to keep both paths.

## Service Boundaries

The pipeline should be split into separate workers.

### Social Retrieval Worker

- provider access
- polling windows
- transport errors
- raw normalized records

### Social Analysis Worker

- pre-LLM reduction
- LLM prompts and responses
- structured analysis output
- batch summaries

### Social Candidate Worker

- deterministic scoring
- candidate open, update, qualify, discard
- registry persistence
- replay-safe event emission

This separation prevents LLM latency from degrading the retrieval loop.

## Storage And Replay

At minimum, the system should persist:

- retrieval request metadata
- normalized raw social records
- LLM analysis outputs with versioned prompts and model identifiers
- candidate score snapshots
- emitted candidate and social analysis events

Replay rules:

- raw retrieval and normalized post records must be replayable without calling the provider again
- candidate scoring must be reproducible from normalized evidence and analysis output
- prompt version and model version must be stored for auditability

## Metrics And Alerts

Recommended metrics:

- `social_retrieval_requests_total{platform,outcome}`
- `social_retrieval_latency_seconds{platform}`
- `social_posts_retrieved_total{platform}`
- `social_posts_reduced_total{platform}`
- `social_llm_batches_total{platform,outcome}`
- `social_llm_latency_seconds{task}`
- `social_candidate_score{platform,mode}`
- `social_candidates_total{status,type}`
- `social_false_positive_feedback_total{platform}`

Recommended alerts:

- social provider degraded or rate-limited
- LLM enrichment backlog growing
- candidate open rate spiking unexpectedly
- high contradiction rate on a source
- social discovery dominated by a single author cluster

## Implementation Order

### Phase 1. Query-Driven Confirmation MVP

- add `social.query_requested`
- implement query template rendering
- support token-targeted retrieval for X and Reddit
- emit `social.analysis_completed`
- compute a first `social_confirmation` score

### Phase 2. Watchlist Social Discovery MVP

- run continuous social polling on configured token sets
- add pre-LLM reduction and semantic denoising
- open or update candidates from social evidence

### Phase 3. Open Discovery MVP

- support broader topic and trend queries
- resolve mentioned tokens from raw social batches
- create candidates even when the token was not known in advance

### Phase 4. Cross-Plane Fusion

- merge social candidates with launch, catalyst, and flow candidates
- add cross-confirmation bonuses and contradiction penalties
- let candidate registry drive confirmation requests into the evaluation plane

## Recommended First Implementation Choices

- keep X behind a bridge-backed JSON protocol
- keep Reddit on public search endpoints initially
- do not call the LLM for every raw post
- analyze clustered batches, not singletons
- require deterministic thresholds above LLM outputs before opening a candidate
- require either cross-plane confirmation or elevated credibility before qualification

## Summary

The correct role of social in SignalEngine is not just `social.signal_snapshot` retrieval.

It should become a dedicated discovery and confirmation subsystem with:

- query-driven retrieval
- deterministic pre-filtering
- LLM semantic enrichment
- deterministic candidate scoring
- replayable event emission

That architecture allows social to do two things at once:

- discover tokens before other scanners do
- confirm or contradict candidates that were opened elsewhere