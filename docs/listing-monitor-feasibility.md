# 上币监听方案可行性分析

> 基于 [dex-cex-listing-alert-plan.md](./dex-cex-listing-alert-plan.md) 和当前代码实现的调研结果。

---

## 1. 当前上币监听能力总览

### 已实现

| 维度 | 状态 | 说明 |
|------|------|------|
| DEX 新池监听 | ✅ 可用 | DexScreener token profile feed + pair detail，已修复并发问题 |
| CEX 公告监听 | ✅ 可用 | Binance CMS feed + Coinbase blog，已确认源可通 |
| 候选评分 | ✅ 可用 | Launch / Catalyst / Flow 各有评分规则 |
| 候选通知 | ✅ 可用 | Telegram 在 `alpha.candidate_qualified` 后触发 |
| 路由执行 | ⚠️ 有限 | 只有 `BUY`/`EXIT`，不够 `SELL`/`SHORT` |

### 已知问题

| 问题 | 影响 | 优先级 |
|------|------|--------|
| Catalyst 源内容陈旧，被年龄窗口过滤 | 实际可用的 CEX 上币通知极少 | **高** |
| Discovery → Execution 无联动 | 候选 qualified 后不会主动采集链上/钱包数据 | **中** |
| Telegram 正文按 `alpha_type` 分支 | 不同源通知格式不一致 | **低** |
| 跨链 token 地址解析缺失 | 如果候选在 chainA，无法采集 chainB 的数据 | **中** |

---

## 2. DEX 上币监听可行性

### 2.1 技术路线

```
DexScreener 最新 profile feed
  -> 按链过滤（默认 solana）
  -> 提取 token address
  -> 并发请求 pair detail API（已优化为 ThreadPoolExecutor）
  -> _dexscreener_pair_snapshots 解析
  -> LaunchAlphaScanner.evaluate() 评分
  -> 达标则 alpha.candidate_qualified
```

### 2.2 可行性评估

| 维度 | 评估 |
|------|------|
| **源可用性** | ✅ DexScreener API 公开可用，无需认证 |
| **响应速度** | ⚠️ 初始 seed 快，但 pair detail 依赖上游；已做并发，但仍受源限制 |
| **数据质量** | ✅ 包含 liquidity / volume / txns / price 等关键字段 |
| **延迟** | ⚠️ 新池创建到 DexScreener 收录有数十秒到数分钟延迟 |
| **覆盖链** | ✅ 支持 30+ 条链（solana, ethereum, bsc, base, arbitrum 等） |
| **误报率** | ⚠️ 依赖 `_passes_filters` 的配置（min liquidity / min buy / 黑名单） |

### 2.3 风险

| 风险 | 缓解 |
|------|------|
| DexScreener 免费 API 频率限制 | 已配置 `min_request_interval_seconds` |
| Rug pull / honeypot token | 已有 `max_creator_hold_pct` / `min_liquidity_lock_ratio` 约束 |
| 流动性低导致无法成交 | `min_initial_liquidity_usd` 阈值可配 |

**结论**：DEX 上币监听**可行**，当前机制已可用，主要瓶颈在 DexScreener 收录延迟和免费 API 频率。

---

## 3. CEX 上币监听可行性

### 3.1 技术路线

```
Binance CMS RSS feed / API
  -> 解析公告内容
  -> 提取上线币种 + 链
  -> CatalystAlphaScanner.evaluate() 评分
  -> 达标则 alpha.candidate_qualified

Coinbase blog / roadmap
  -> 同上路径
```

### 3.2 可行性评估

| 维度 | 评估 |
|------|------|
| **源可用性** | ✅ Binance CMS API 可用，Coinbase blog 可抓取 |
| **响应速度** | ⚠️ 公告发布到被抓取有 5-30 分钟延迟（取决于扫描间隔） |
| **数据质量** | ✅ 公告内容结构化，实体提取准确 |
| **时效性关键问题** | **❌ 当前 `max_snapshot_age_seconds` (360-720 min) 过滤掉大部分公告，因为 Binance 的 CMS feed 包含的是已发布多天的历史公告** |
| **覆盖交易所** | ⚠️ 目前只覆盖 Binance + Coinbase，缺失 OKX / Bybit / Kraken / Upbit |

### 3.2 关键问题分析：源内容陈旧 → 已修复

**之前的问题**：
- Binance CMS API 返回的文章已发布 ~9884 分钟前，被 360/720 分钟的年龄窗口全部过滤
- Catalyst 源几乎收不到 CEX 上币通知

**修复方案（2026-05-07 上线）**：
不再依赖公告内容抓取，新增 4 个 exchangeInfo 轮询源：

| 源 | API | 检测方式 | 速度 |
|---|---|---|---|
| Binance Spot | `GET /api/v3/exchangeInfo` | 全量 symbol diff，Redis 缓存对比 | 秒级 |
| Binance Futures | `GET /fapi/v1/exchangeInfo` | 同上 | 秒级 |
| Binance Alpha | `GET /bapi/defi/v1/public/.../alpha/all/token/list` | Alpha token list diff | 秒级 |
| Coinbase Products | `GET /products` | 全量 product diff | 秒级 |

**工作原理**：`ExchangeInfoCatalystSource` 轮询交易所 API，将当前 symbol 列表与 Redis 缓存的已知列表做 diff，发现新 symbol 立即产出 `CatalystEventSnapshot`。不再受 CMS 年龄窗口限制。

原有 4 个公告源（binance_cms_api / coinbase_html_page / okx / bybit）保留作为补充回退。

### 3.4 建议改进方案

#### 方案 A：改用公告列表 API（推荐）

```
GET https://www.binance.com/bapi/composite/v1/public/cms/article/list/query
  ?type=1&pageNo=1&pageSize=20&catalogId=48
```

- 直接返回最新公告列表
- 按发布时间排序
- 只需扫描最新几条即可避免年龄过滤

#### 方案 B：扩大年龄窗口 + 去重

- `max_snapshot_age_seconds` 扩大到 86400 (24h)
- 存已处理的公告 ID 到 Redis，避免重复

#### 方案 C：增加交易所覆盖

| 交易所 | 源类型 | 可行性 |
|--------|--------|--------|
| OKX | REST API | ✅ 官网有公告列表 API |
| Bybit | REST API | ✅ 同上 |
| Kraken | RSS / 网页 | ⚠️ 需要 HTML 解析 |
| Upbit | REST API | ✅ 有公告 API |
| Gate.io | REST API | ⚠️ 需要验证 |

**结论**：CEX 上币监听**技术上可行**，但当前实现受限于源内容陈旧，需要**优先切换到 Binance 公告列表 API**，并拓展交易所覆盖。

---

## 4. 其他上币/退市信号源可行性

### 4.1 新 token / 新池（DEX）

已在 2. 中覆盖，**可行**。

### 4.2 社交讨论爆发

| 维度 | 评估 |
|------|------|
| 源 | X (Twitter) 搜索 / Reddit |
| 当前状态 | ⚠️ 有 `social_live_sources.py`，但 discovery-mode 的产出被 pipeline 隔离 |
| 可行性 | ✅ 技术上可行，延迟较公告源更大（讨论爆发后数小时） |

见 [`social-llm-alpha-pipeline.md`](./social-llm-alpha-pipeline.md)。

### 4.3 智能钱包流动

| 维度 | 评估 |
|------|------|
| 源 | wallet_intelligence 数据库 |
| 当前状态 | ⚠️ Flow alpha 默认 `observe_only=True` |
| 可行性 | ✅ 技术上可行，但依赖钱包注册表覆盖面 |

### 4.4 链上流动性变化

| 维度 | 评估 |
|------|------|
| 源 | DexScreener / onchain RPC |
| 当前状态 | ⚠️ 已有 `onchain.liquidity_snapshot` 但 pipeline 是"被动等待" |
| 可行性 | ✅ 技术上可行，但需要主动采集（见设计1） |

---

## 5. 方案对比

| 方案 | 开发成本 | 监听延迟 | 覆盖范围 | 推荐 |
|------|---------|---------|---------|------|
| **A：修复 Catalyst 源 + 扩大年龄窗口** | 低（1-2d） | 5-30 min | Binance + Coinbase | **优先做** |
| **B：新增交易所覆盖（OKX / Bybit）** | 中（3-5d） | 5-30 min | 3-4 家主流 CEX | 建议做 |
| **C：DexScreener 新池监控（已有）** | 已完成 | 数十秒-数分钟 | 30+ DEX | 已可用 |
| **D：异步跨维采集（设计1）** | 中（5-7d） | 30-60s | 链上+钱包+社交 | 建议做 |
| **E：社交发现+LLM 分析** | 高（2-3w） | 数小时 | X + Reddit | 阶段2 |

---

## 6. 执行状态

```text
Phase 1 (1-2天) ✅ 已完成 2026-05-07
  ├─ 新增 exchangeInfo 轮询源（Binance Spot/Futures/Alpha, Coinbase Products）
  ├─ 修复 CMS 公告源年龄窗口 + Redis 去重
  └─ 保留原有公告源作为补充

Phase 2 (5-7天) ✅ 已完成 2026-05-07
  ├─ 异步跨维采集（AsyncCollectorOrchestrator）
  ├─ 独立 AlphaPipelineWorker
  └─ Telegram 统一通知模板

Phase 3 (3-5天) ⚠️ 重构完成 2026-05-07
  ├─ OKX CMS（404）→ ✅ REST instruments API (`GET /api/v5/public/instruments`) 轮询
  ├─ Bybit CMS（403）→ ✅ REST instruments-info API (`GET /v5/market/instruments-info`) 轮询
  ├─ WS 代码已就绪（`discovery/catalyst_ws_sources.py`），待异步基础设施
  ├─ 多链 token 地址解析（multi_chain_resolver）✅
  └─ 教训：纯 REST 公告 API 不可靠，改用 instruments/market-data API 更稳定

Phase 4 (1-2周) ❌ 未开始
  ├─ 社交发现 + LLM 分析增强
  ├─ SELL / SHORT 路由
  └─ 动态池扫描优化

Phase 5 — 架构升级（2026-05-07 进度）
  ├─ P0: SymbolRegistry 跨源实体去重 ✅ 已上线
  │   920 canonical tokens / 1023 venue entries 已入库
  ├─ P0: WebSocket 基础设施 ❌ 待异步架构重构
  │   WS 代码已写（OKX/Bybit instruments）但当前基于 REST 轮询工作良好
  ├─ P1: 分层轮询策略 ✅ 已完成
  │   exchangeInfo: 1s | Alpha API: 2s | Instruments API: 3s | CMS: 45-60s
  ├─ P1: Coinbase Asset Roadmap API ❌ 无公开 API，维持 blog 抓取
  ├─ P1: 预上市源（Hyperliquid WS, Upbit REST, Jupiter Strict List）❌ 未开始
  └─ P2: Rug filter ✅ 字段已加入 config，执行依赖第三方数据源
```

---

## 7. ⚠️ 设计纠偏（2026-05-09）

### 7.1 CEX 上币监听不是 alpha 源

经过生产验证后，确认以下结论：

**CEX 上币监听不是产生交易信号的 alpha 源。**
**它是 token 生命周期阶段信号，用于路由决策。**

### 7.2 真正的 alpha 来源优先级

| 优先级 | 来源 | 说明 | 状态 |
|--------|------|------|------|
| 🅰 P0 | **Smart Money Inflow** | 聪明钱流入新池，最高纯度信号 | ❌ 待实现 |
| 🅱 P0 | **Volume Momentum** | 成交量爆发式增长 | ❌ 待实现 |
| 🅲 P1 | **Cross-Chain Expansion** | token 跨链迁移预示 CEX 上线 | ❌ 待实现 |
| 🅳 P0 | **New Pool Launch** | DEX 新池早期发现 | ✅ 已运行 |

### 7.3 后续开发路线

```
Phase 5 (2026-05-09 起)
  ├─ 🅰 Smart Money Inflow Detection（~2天）
  │   串联 OKX registry + wallet flow + launch-alpha
  │   当新池 + 聪明钱买入 = real alpha
  ├─ 🅱 Volume Momentum Detection（~3天）
  │   DexScreener Token-Boosts API + pair detail 监控
  │   volume breakout / price surge / wallet growth
  └─ 代码重构
       ├─ catalyst-alpha 产出不再标为 alpha candidate
       ├─ 改写入 SymbolRegistry.venue_lifecycle
       └─ 路由层消费 lifecycle 数据调整执行策略
```
