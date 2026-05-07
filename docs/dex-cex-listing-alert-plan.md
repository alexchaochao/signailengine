# DEX/CEX Listing Alert Plan

## Purpose

This document turns the current alpha discovery stack into a practical
listing-alert system for both DEX and CEX venues.

The system is designed to detect early listing signals, qualify them into
structured alpha candidates, and then hand them off to the existing
signal/state/router/risk/execution pipeline.

## What the system does today

The repository already has three layers that work together:

1. Discovery sources ingest raw data.
2. Discovery services convert raw snapshots into `alpha.candidate_qualified` events.
3. The execution pipeline turns qualified signals into route decisions.

Current notification behavior:

- `telegram-publisher` sends `alpha.candidate_qualified` events.
- By default it publishes only `alpha_type in {LAUNCH, CATALYST}`.
- `FLOW` can be enabled explicitly, but it is not pushed by default.
- Telegram is a notification side channel off the candidate layer, not the final step after execution.

## Signal flow

```text
External source
  -> raw discovery snapshot
  -> candidate evaluation
  -> alpha.candidate_qualified
  -> telegram notification

alpha.candidate_qualified
  -> signal / state / route
  -> risk
  -> execution intent
```

Important: a "listing" event does not directly mean a trade.
It first becomes a candidate, then a signal, then a route decision.

## Current candidate types

### 1. Launch Alpha

Source family:

- DexScreener token profile feed
- pair detail requests for candidate validation

Qualification rules:

- `initial_liquidity_usd >= min_initial_liquidity_usd`
- `buy_notional_5m_usd >= min_buy_notional_5m_usd`
- `trade_count_5m >= min_trade_count_5m`
- `unique_wallets_5m >= min_unique_wallets_5m`
- not rejected by liquidity lock / creator hold limits

Current event payload includes:

- `alpha_type = LAUNCH`
- candidate metadata
- pool metadata
- launch-derived features such as buy pressure and wallet inflow score

### 2. Catalyst Alpha

Source family:

**信号优先级（最快 → 最慢）：**

| Tier | 速度 | 类型 | 源 | 状态 |
|------|------|------|-----|------|
| 1 | WS / 实时 | WebSocket 推送 | Binance Announcement WS（不存在的公开 API）, OKX/Bybit instruments WS | ❌ 待异步架构 |
| 2 | poll / 1-3s | REST symbol diff | Binance Spot/Futures exchangeInfo, Coinbase products, Binance Alpha, **OKX instruments**, **Bybit instruments** | ✅ 全部运行中 |
| 3 | poll / 45-60s | CMS 公告 | Binance CMS feed, Coinbase blog | ✅ 仅作 confirmation |

关键变化：
- OKX: 放弃 CMS API（404），改用 `GET /api/v5/public/instruments?instType=SPOT` REST 轮询
- Bybit: 放弃 CMS API（403），改用 `GET /v5/market/instruments-info?category=spot` REST 轮询
- OKX/Bybit WS 代码已就绪（`discovery/catalyst_ws_sources.py`），待异步架构启用

**exchangeInfo / instruments 新币检测** ✅（Tier 2，当前最快信号）:
  - `binance_exchange_info` — poll `/api/v3/exchangeInfo` @ 1s
  - `binance_futures_info` — poll `/fapi/v1/exchangeInfo` @ 1s
  - `binance_alpha_api` — poll Binance Alpha Token List @ 2s
  - `coinbase_products_api` — poll `/products` @ 2s
  - `okx_instruments_ws` — poll `GET /api/v5/public/instruments` @ 3s（provider 名保留 ws 便于未来升级）
  - `bybit_instruments_ws` — poll `GET /v5/market/instruments-info` @ 3s

**SymbolRegistry 跨源实体去重** ✅ 已上线 (2026-05-07):
  - `discovery/symbol_registry.py` — Redis 驱动的 canonical token registry
  - 每个 catalyst snapshot 处理后自动注册
  - 当前数据: 920 canonical tokens, 1023 venue entries
  - `binance_alpha_api` — 轮询 Binance Alpha Token List API
  - `coinbase_products_api` — 轮询 Coinbase `/products` API，diff 出新交易对

Qualification rules:

- `score = impact * 0.5 + credibility * 0.35 + timeliness * 0.15`
- `score >= 0.72`
- `credibility_score >= 0.6`

ExchangeInfo 源额外规则：

- `impact_score` 和 `credibility_score` 由配置固定值决定（Futures 上币 0.95，Alpha 0.92，Spot 0.9 等）
- 每个新 symbol 只触发一次（Redis 缓存 known symbols + dedup）

Current event payload includes:

- `alpha_type = CATALYST`
- headline
- catalyst type
- credibility score
- candidate metadata

### 3. Flow Alpha

Source family:

- wallet intelligence
- flow measurement / projected wallet flows

Qualification rules:

- `score >= 0.72`
- `netflow_15m_usd >= 50000`
- `smart_money_inflow_usd > smart_money_outflow_usd`

Current event payload includes:

- `alpha_type = FLOW`
- flow windows
- wallet inflow / outflow measures
- candidate metadata

## Current route behavior

The router is currently long-biased for entry.

### Entry routes

- `PRE_LAUNCH`, `EARLY_LIQUIDITY`, `NARRATIVE_EXPLOSION` -> `DEX_ENTRY`
- `CEX_LISTING` -> `CEX_ENTRY`

Both of those entry routes use `ActionType.BUY` today.

### Exit route

- `DISTRIBUTION` with an open position -> `DEX_EXIT` or `CEX_EXIT`
- the action is `ActionType.EXIT`

### Not implemented yet

- explicit `SELL` route
- explicit `SHORT` route
- separate route selection by `alpha_type`

The current system infers direction from the state machine and route:

- entry states mean buy / long
- distribution means exit

## Direct answer to "if listing is observed, does it trigger on-chain action?"

No, not by itself.

A listing keyword or listing announcement only becomes actionable after:

1. the source extracts a candidate
2. the candidate is qualified
3. the signal engine assigns a state
4. the router decides entry or exit
5. risk allows the intent

So the current system does not do "keyword -> trade".
It does "candidate -> signal -> route -> trade".

## Direction policy for the current stack

For the current implementation, the safe default is:

- launch / catalyst / flow alpha: treat as buy / long candidates
- distribution: treat as exit candidates
- do not infer shorting unless a dedicated short strategy exists

This matches the codebase today:

- `ActionType.BUY` is used for entry routes
- `ActionType.EXIT` is used for distribution exits
- `ActionType.SELL` exists in schemas but is not currently emitted by the router

## Operational recommendation

To keep notifications useful and avoid premature trades:

1. Notify on `alpha.candidate_qualified`.
2. Include route-relevant fields in the notification body.
3. Keep automatic trading disabled until direction rules are explicit.
4. Add a separate short strategy only after the repo supports a real reverse-direction route.

## Gaps still open

- `catalyst-alpha` source freshness still needs improvement.
- `launch-alpha` now avoids serial detail blocking, but still depends on live upstream latency.
- `SELL` / `SHORT` routing is not yet implemented.
- Direction is still inferred, not explicitly modeled as a first-class field.

## Related docs

- [Unified Alpha Architecture](./unified-alpha-architecture.md)
- [Social LLM Alpha Pipeline](./social-llm-alpha-pipeline.md)
- [Risk Policy](./risk-policy.md)
