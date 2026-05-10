# DEX/CEX Listing Alert Plan

## ⚠️ 设计纠偏（2026-05-09）

**CEX 上币监听不是 alpha 源，而是 token 生命周期阶段信号。**

这段历史文档最初将 CEX 上币检测视为"发现新机会的手段"，但经过生产验证后发现：

| 原假设 | 实际结论 |
|--------|---------|
| CEX 上币 = 新交易机会 | ❌ Coinbase/Binance 每天上大量币，大部分不是交易机会 |
| 公告越早上车越好 | ❌ 真正 alpha 在上币之前就已出现（链上早期发现） |
| 上币通知越多越好 | ❌ 99% 是噪音，1% 才是信号 |

**正确的设计定位**：

```
CEX 上币检测 → 不是产生 alpha，而是更新 token 的"生命周期阶段"
  → 告诉 routing 层："这个 token 现在有 CEX venue 了"
  → 路由决策变化：优先走 CEX 而非 DEX
  → 影响执行参数：slippage / 滑点 / 费用优化
```

**实时 alpha 的真正来源**：

```
链上聪明钱流动（Smart Money Flow）
  ↓
成交量爆发/价格动量（Volume Momentum）
  ↓
新池早期发现（New Pool Discovery）
  ↓
【以上三项才是真正的 alpha 来源——CEX 上币只是阶段确认】
```

## Purpose（修订后）

This document defines:

1. **Token Lifecycle Stage Detection** — 通过 exchangeInfo diff / instruments API 检测 token 在各 CEX 的上线状态，用于路由决策，**不是用于 alpha 挖掘**
2. **Real Alpha Sources** — 基于链上数据的 alpha 发现策略（聪明钱、成交量爆发、新池早期发现）
3. **Route Decision Mapping** — 不同生命周期阶段对应不同的路由和执行策略

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

### 2. Catalyst Alpha → 重定义为「Token Lifecycle Instance」

**⚠️ 设计纠偏：以下内容已从"alpha 源"重定义为"token 生命周期阶段检测"。**

不再视为 alpha 候选，而是 TokenStage 的输入信号——当 CEX 上币被检测到，更新 SymbolRegistry 中的 `venue_lifecycle` 记录，路由层据此调整执行策略。

Source family（保留，但用途改变）：

**信号优先级（最快 → 最慢）：**

| Tier | 速度 | 类型 | 源 | 用途 |
|------|------|------|-----|------|
| 1 | WS / 实时 | WebSocket | OKX/Bybit instruments WS | ❌ 待异步架构 |
| 2 | poll / 1-3s | REST symbol diff | Binance Spot/Futures exchangeInfo, Coinbase products, Binance Alpha, OKX/Bybit instruments | ✅ 用于 lifecycle 检测 |
| 3 | poll / 45-60s | CMS 公告 | Binance CMS feed, Coinbase blog | ✅ 备选/backup |

关键变化：
- OKX: 放弃 CMS API（404），改用 REST instruments API
- Bybit: 放弃 CMS API（403），改用 REST instruments API
- OKX/Bybit WS 代码已就绪，待异步架构

**exchangeInfo / instruments 检测** ✅:
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

Source family: wallet intelligence — OKX registry + wallet refresh + wallet flow projection

Qualification rules:

- `score >= 0.72`
- `netflow_15m_usd >= 50000`
- `smart_money_inflow_usd > smart_money_outflow_usd`

Current event payload includes:

- `alpha_type = FLOW`
- wallet inflow / outflow measures
- candidate metadata

---

## 4. 真实 Alpha 策略（2026-05-09 设计纠偏）

### 核心设计原则

```
CEX 上币检测 ≠ alpha 源
CEX 上币检测 = token 生命周期阶段信号 → 路由决策

链上数据 = 真正的 alpha 源
  ├─ 聪明钱流入（Smart Money Flow）
  ├─ 成交量爆发（Volume Momentum）
  └─ 新池早期发现（New Pool Launch）
```

CEX 上币阶段信号的用途（而不是作为 alpha 触发交易）：

| 检测到的情况 | 路由影响 |
|------------|---------|
| Token 首次出现在 Binance Spot | → 该 token 有 CEX venue 了，路由优先走 CEX |
| Token 出现在 Binance Futures | → 可做合约交易，考虑对冲/套利 |
| Token 首次出现在 Coinbase | → 美国流动性确认，扩大路由范围 |
| Token 出现在 Binance Alpha | → 可能即将现货上线，预热阶段 |

### 🅰 Smart Money Inflow Detection（P0，最高价值）

**原理**：当一个新池被 launch-alpha 发现时，检查 OKX registry 中的已知聪明钱包是否在买入该 token。

**依托的现有能力**：
- ✅ OKX wallet registry — 已有聪明钱包列表
- ✅ DexScreener new pool — 新池发现
- ✅ Wallet flow projection — 可按 token 投影流量
- ✅ SymbolRegistry — 跨源 token 关联

**核心逻辑**：
```
launch-alpha 发现新池
  → 从 OKX registry 加载活跃聪明钱包
  → 检查这些钱包是否有买入该 token 的记录
  → 买入的聪明钱包数量 N → 信号强度
  → N ≥ threshold → alpha.smart_money_flow 事件
  → 路由层据此决定执行
```

**开发成本**：~2 天（串联现有模块 + 评分函数）

### 🅱 Volume Momentum Detection（P0）

**原理**：持续监控已知 token 的成交量和价格变化，发现爆发式增长。

**依托的现有能力**：
- ✅ DexScreener pair detail API（已用於 launch-alpha）
- ✅ SymbolRegistry 已知 token 列表
- ✅ Cross-dimension collector 的 on-chain 采集

**核心逻辑**：
```
新服务 momentum-alpha
  ├─ 轮询 DexScreener Token-Boosts API
  │   → 社区投票 + 成交量爆发 = 高质量信号
  ├─ 或扫描已有 token 的 pair detail
  │   → volume_5m > 前值 × 5
  │   → price_change_1h > 20%
  │   → unique_wallets 增长 > 300%
  └─ 达标后 → alpha.candidate_qualified
```

**开发成本**：~3 天

### 🅲 Cross-Chain Expansion Detection（P1）

**原理**：token 从 Solana → Base → Ethereum 的跨链迁移，通常是 CEX 上线的预备动作。

**依托**：SymbolRegistry + DexScreener Search API

## 完整数据流

```
Real Alpha Sources（产生交易信号）
┌──────────────────────────┐
│ 🅰 Smart Money Inflow   │
│ 🅱 Volume Momentum      │
│ 🅲 Cross-Chain Expansion│
│ 🅳 New Pool Launch      │
└──────────────────────────┘
           ↓
    alpha.candidate_qualified
    → signal / state / route
    → risk → execution

Stage Signals（用于路由决策）
┌──────────────────────────┐
│ CEX Listing Detected    │
│ (exchangeInfo / instr.) │
└──────────────────────────┘
           ↓
    SymbolRegistry.venue_lifecycle
    → 路由层调整执行策略
      ├─ DEX entry → CEX entry
      ├─ 调整滑点/费用参数
      └─ Telegram 通知（可选）
```

## 方向策略

对于当前实现的 safe default：

- Smart Money Inflow / Volume Momentum / New Pool → 做多候选
- Distribution / 异常流出 → 退出候选
- 不做空，除非有专门的做空策略

## 运营建议

1. **关掉/大幅收紧** catalyst-alpha 的 Telegram 通知（`min_score=0.98` 以上）
2. **链上 alpha 通知门槛**适当降低（`min_score=0.75`）
3. 保持 `live_trading_enabled=false`，直到路线明确

## Gaps still open

- `catalyst-alpha` source freshness still needs improvement.
- `launch-alpha` now avoids serial detail blocking, but still depends on live upstream latency.
- `SELL` / `SHORT` routing is not yet implemented.
- Direction is still inferred, not explicitly modeled as a first-class field.

## Related docs

- [Unified Alpha Architecture](./unified-alpha-architecture.md)
- [Social LLM Alpha Pipeline](./social-llm-alpha-pipeline.md)
- [Risk Policy](./risk-policy.md)
