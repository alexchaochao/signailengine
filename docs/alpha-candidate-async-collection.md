# Alpha Candidate 异步跨维数据采集方案

## 1. 问题

当前 `alpha.candidate_qualified` 事件发出后：

- Telegram 直接消费并通知，但正文按 `alpha_type` 分三套模板，**没有包含完整的链上 / 钱包 / 社交信息**
- Signal engine 所在的 pipeline **不消费 `alpha.candidate_qualified`**
- pipeline 只消费 `onchain.liquidity_snapshot` / `wallet.cluster_snapshot` / `social.signal_snapshot`，**不会因为 qualified 事件主动去拉这些数据**
- 链上 / 钱包 / 社交数据需要 "碰巧" 有自己的 snapshot 事件到达才会被 signal engine 合并

**结果**：虽然 discovery 层可以发现候选机会，但 execution 层可能永远看不到这个 token 的完整数据，导致无法下单。

---

## 2. 方案概述

```
discovery 产出 alpha.candidate_qualified
  |
  v
Async Collector Orchestrator
  |-- 并行请求链上采集器（按 token 地址 + chain）
  |-- 并行请求钱包采集器（按 token 地址 + chain）
  |-- 并行请求社交采集器（按 token symbol）
  |-- 超时/熔断保护
  |
  v
Cross-Dimension Snapshot（统一合并快照）
  |
  |-- 送入 signal engine → state → route → risk → execution
  |-- 送入 notification formatter → telegram（统一格式）
```

---

## 3. 关键组件

### 3.1 AsyncCollectorOrchestrator

**职责**：收到 `alpha.candidate_qualified` 后，异步触发各维度采集，等待结果或超时。

**接口**：

```python
class AsyncCollectionRequest(BaseModel):
    request_id: str
    alpha_type: AlphaType
    chain: str
    token: str
    token_symbol: str
    pool_address: str | None = None
    dex: str | None = None
    requested_at: datetime
    timeout_seconds: float = 30.0  # 整体超时

class CollectionResult(BaseModel):
    request_id: str
    alpha_type: AlphaType
    chain: str
    token: str
    onchain: OnchainSnapshot | None = None
    wallet: WalletSnapshot | None = None
    social: SocialSnapshot | None = None
    collected_at: datetime
    timed_out: bool = False
    errors: dict[str, str] = Field(default_factory=dict)
```

**工作流**：

```
1. 监听 raw-events stream 的 alpha.candidate_qualified
2. 创建 AsyncCollectionRequest
3. 并行下发采集任务：
   a. OnChainCollectTask(chain, token, pool_address)
   b. WalletCollectTask(chain, token)
   c. SocialCollectTask(token_symbol, chain)
4. 等待所有任务完成或超时
5. 合并结果 → 发布 cross_dimension_snapshot 事件
6. 进入 signal engine
```

### 3.2 多链处理策略

候选事件中的 `chain` 字段决定主链。但 token 可能跨链存在（如 Solana + Ethereum）。

**决策规则**：

| 情况 | 策略 |
|------|------|
| `chain` 明确且唯一 | 只采集该链 |
| `chain == "unknown"`（如 catalyst 未确定链） | 从 `candidate_id` / `pool_address` / `dex` 推断，如仍未知则跳过链上采集，只做社交 |
| 候选已知跨链（如同时有 Solana + Ethereum 池） | 按配置的 `priority_chains` 排序，采集优先级最高的前 N 条链（默认 1 条） |
| 历史跨链记录 | 查询 `alpha_candidates` 表看该 token 是否有多个 chain 的记录，按流动性降序取 |

**配置**：

```yaml
alpha_collector:
  enabled: true
  timeout_seconds: 30.0
  max_chains_per_token: 1
  priority_chains:
    - solana
    - base
    - ethereum
    - bsc
```

### 3.3 采集器适配

#### 链上采集（OnChainCollectorAdapter）

使用现有 `sentinel/onchain_collector.py` 的 `OnchainTradeCollector`，针对 token 地址拉取：

```python
class OnChainCollectorAdapter:
    async def collect(chain: str, token: str, pool_address: str | None) -> OnchainSnapshot:
        # 1. 通过 DexScreener API 获取 token 的当前池子信息
        #    GET https://api.dexscreener.com/latest/dex/tokens/{token_address}
        # 2. 解析返回的 pairs，提取：
        #    - liquidity_usd
        #    - volume_5m_usd
        #    - price_usd
        #    - price_change_5m / 1h
        #    - fdv
        # 3. 如果是已配置的链，走 OnchainTradeCollector 拉近期交易
        # 4. 返回标准化结果
```

**现有依赖**：`discovery/live_sources.py` 已有 `_dexscreener_pair_snapshots` 函数，可直接复用。

#### 钱包采集（WalletCollectorAdapter）

基于 `sentinel/wallet_intelligence_sync.py` 的能力：

```python
class WalletCollectorAdapter:
    async def collect(chain: str, token: str) -> WalletSnapshot:
        # 1. 查询 wallet_intelligence 中该 token 的近期统计
        #    - smart_money_inflow_usd (15m / 1h)
        #    - smart_money_outflow_usd
        #    - unique_buyer_wallets
        #    - unique_seller_wallets
        #    - whale_buy_count
        # 2. 如果没有实时数据，返回空（不阻塞）
        # 3. 返回标准化结果
```

#### 社交采集（SocialCollectorAdapter）

使用现有 `sentinel/social_listener.py` 和 `sentinel/social_live_sources.py`：

```python
class SocialCollectorAdapter:
    async def collect(token_symbol: str, chain: str) -> SocialSnapshot:
        # 1. 按 token symbol 搜索近期社交讨论
        #    - X (Twitter) 搜索 "${symbol}"
        #    - Reddit 搜索 "${symbol} crypto"
        # 2. 提取：
        #    - social_sentiment (0-1)
        #    - social_velocity (0-1)
        #    - mention_count
        #    - unique_authors
        # 3. 返回标准化结果
```

### 3.4 CrossDimensionSnapshot 事件

采集完成后，发布统一事件到 `raw-events` stream：

```python
class CrossDimensionSnapshot(BaseModel):
    schema_version: str = "v1"
    snapshot_id: str
    alpha_type: AlphaType
    chain: str
    token: str
    token_symbol: str
    
    # 触发源信息
    trigger_source: str          # launch / catalyst / flow
    trigger_score: float         # qualified 时的分数
    trigger_reasons: list[str]
    
    # 链上快照
    onchain_liquidity_usd: float | None
    onchain_volume_5m_usd: float | None
    onchain_price_usd: float | None
    onchain_price_change_5m: float | None
    onchain_price_change_1h: float | None
    onchain_fdv: float | None
    onchain_pool_count: int = 0
    
    # 钱包快照
    wallet_smart_money_inflow_usd: float | None
    wallet_smart_money_outflow_usd: float | None
    wallet_unique_buyers: int | None
    wallet_unique_sellers: int | None
    wallet_whale_buys: int = 0
    
    # 社交快照
    social_sentiment: float | None
    social_velocity: float | None
    social_mention_count: int = 0
    social_unique_authors: int = 0
    
    # 采集元信息
    collected_chains: list[str]       # 实际采集的链列表
    timed_out: bool = False
    collection_latency_ms: int = 0
    errors: dict[str, str] = Field(default_factory=dict)
```

事件类型：`alpha.cross_dimension_snapshot`

---

## 4. 信号引擎集成：独立 AlphaPipelineWorker

### 4.1 设计决策：不走现有 PipelineWorker

`alpha.cross_dimension_snapshot` **不加入**现有 `PipelineWorker` 的 `SIGNAL_TRIGGER_EVENT_TYPES`。

原因：

- 现有 pipeline 处理 `onchain.liquidity_snapshot` / `wallet.cluster_snapshot` / `social.signal_snapshot`，这些事件的触发频率高、token 范围广
- 如果 cross_dimension_snapshot 也进入同一 pipeline，同一个 token 可能被**两套事件各触发一次 signal → state**，导致状态竞争
- 现有 pipeline 的 `StateEngine` 没有"这个 token 来自 discovery"的概念

**方案**：新建 `AlphaPipelineWorker`，专消费 `alpha.cross_dimension_snapshot`。

### 4.2 AlphaPipelineWorker 架构

```text
独立的 consumer group
  -> 只消费 alpha.cross_dimension_snapshot
  -> 复用 SignalEngine / StateEngine / Router / RiskEngine
  -> 复用 ExecutionAdapter
  -> 独立的 PositionState 读写（通过 Postgres 共享）
```

```python
class AlphaPipelineWorker:
    """Pipeline worker dedicated to cross-dimension snapshots.
    
    Runs as a separate process with its own consumer group on the
    raw-events stream.  Only processes alpha.cross_dimension_snapshot
    events.  Shares PositionState and PortfolioSnapshot with the
    existing PipelineWorker via the storage repository.
    """
    
    SIGNAL_TRIGGER_EVENT_TYPES = frozenset({
        "alpha.cross_dimension_snapshot",
    })
```

### 4.3 共享 PositionState 说明

两个 pipeline worker 从同一个 `StorageRepository` 读写 `PositionState`：

- Worker A（现有）：处理原始事件的 token
- Worker B（新增）：处理 discovery 出生的 token

如果两个 worker 同时操作同一个 token（极少见），Postgres 的行级锁/事务保证一致性。
如果 Worker A 已买入 Token X，Worker B 之后处理 Token X 的 cross_dimension_snapshot 时，`load_position()` 会返回 open 状态，`Router` 可按 `DISTRIBUTION` 做 EXIT。

### 4.4 SignalEngine 适配

`CrossDimensionSnapshot` 的字段通过 `_merge_payloads` 映射到 signal engine 的特征：

| CrossDimensionSnapshot 字段 | SignalEngine 特征 |
|---|---|
| `onchain_liquidity_usd` | → `liquidity_usd` |
| `onchain_volume_5m_usd` | → `volume_5m_usd` |
| `onchain_price_change_5m` | → 衍生 `buy_pressure` |
| `wallet_smart_money_inflow_usd` | → 衍生 `wallet_inflow_score` |
| `social_sentiment` | → `social_sentiment` |
| `social_velocity` | → `social_velocity` |
| `trigger_score` + `alpha_type` | → `launch_alpha_score` / `catalyst_alpha_score` / `flow_alpha_score` |
| `trigger_reasons` | → `launch_candidate_status` / `flow_candidate_status` |

注意：`_classify_state` 已经能通过 `launch_alpha_score >= 0.7` / `catalyst_alpha_score >= 0.72` 等条件推导状态，只是当前 pipeline 收不到这些字段。通过 cross_dimension_snapshot 注入后，这些逻辑会**首次实际生效**。

---

## 5. Telegram 统一通知格式

### 5.1 通知触发时机

**改为**：消费 `alpha.cross_dimension_snapshot` 事件，而不是 `alpha.candidate_qualified`。

### 5.2 统一通知模板

```
🔔 ALPHA DETECTED | {alpha_type}
Token: {token} ({chain})
Symbol: {token_symbol}

📡 触发源
  Source: {trigger_source}
  Score: {trigger_score:.4f}
  Reasons: {trigger_reasons}

⛓ 链上数据
  Liquidity: ${onchain_liquidity_usd:,.0f}
  Volume 5m: ${onchain_volume_5m_usd:,.0f}
  Price: ${onchain_price_usd:.8f}
  Price 5m: {onchain_price_change_5m:+.2f}%
  FDV: ${onchain_fdv:,.0f}

👛 钱包活动
  Smart Money Inflow: ${wallet_smart_money_inflow_usd:,.0f}
  Smart Money Outflow: ${wallet_smart_money_outflow_usd:,.0f}
  Unique Buyers: {wallet_unique_buyers}
  Whale Buys: {wallet_whale_buys}

💬 社交信号
  Sentiment: {social_sentiment:.3f}
  Velocity: {social_velocity:.3f}
  Mentions: {social_mention_count}

#alpha #{alpha_type_lower} #{chain}
```

**关键原则**：不管 `LAUNCH` / `CATALYST` / `FLOW` 哪种来源触发，Telegram 正文模板统一。缺失的字段显示 `N/A`。

### 5.3 兼容性

- 对于现有 `alpha.candidate_qualified` 的订阅者（如其他服务），保留该事件类型不变
- `alpha.cross_dimension_snapshot` 是**新增事件**，不破坏现有逻辑
- Telegram publisher 改为同时或仅消费 `alpha.cross_dimension_snapshot`

---

## 6 架构变更总结

| 组件 | 改动 |
|------|------|
| 新增 `AsyncCollectorOrchestrator` | 监听 `alpha.candidate_qualified`，触发异步跨维采集 |
| 新增 `CrossDimensionSnapshot` schema | 统一数据格式 |
| 新增 `alpha.cross_dimension_snapshot` 事件 | pipeline 和 telegram 的消费入口 |
| **新增 `AlphaPipelineWorker`** | 独立 pipeline worker，专消费 cross_dimension_snapshot，不与现有 pipeline 冲突 |
| 修改 `notifications/telegram_publisher.py` | 增加对 cross_dimension_snapshot 的支持 + 统一模板 |
| 新增 `AlphaCollectorConfig` | 多链优先级、超时、冷却期等配置 |

## 7 未解决的问题（需要进一步讨论）

1. **多链 token 地址解析**：如果 `alpha.candidate_qualified` 只带了链+symbol，但没有跨链地址，如何找到其他链上的合约地址？可能需要一个 token 解析服务（如 DexScreener 的 token search API）
2. **采集超时后的行为**：超时后是否进入信号引擎（携带部分数据）？还是直接跳过？建议：超时后照常进入，缺失字段置空
3. **幂等性**：如果同一个 token 短时间内多次 qualified，是否重复采集？建议：每 token 每 `collection_cooldown_seconds` 秒只采集一次
4. **重试**：单维采集失败（如社交 API 超时）是否重试？建议：每维最多重试 1 次
