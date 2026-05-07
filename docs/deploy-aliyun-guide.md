# signalengine 阿里云新加坡部署指南

## 前提

- **阿里云 ECS**：新加坡地域，Ubuntu 22.04，2C4G（最低），建议 4C8G
- **安全组**：开放端口 `22`(SSH), `6379`(Redis), `5432`(PG, 可选), `9000-9011`(metrics, 可选)
- **域名**（可选）：如需 Telegram 通知回调

---

## 方式一：一键部署脚本（推荐）

```bash
# 1. SSH 登录服务器
ssh root@your-singapore-ip

# 2. 上传脚本
# 在本机执行:
scp deploy_aliyun.sh root@your-singapore-ip:/root/

# 3. 在服务器执行
chmod +x deploy_aliyun.sh
SIGNALENGINE_DB_PASSWORD="your-strong-password" ./deploy_aliyun.sh

# 4. 编辑配置
vi /opt/signalengine/.env
# → 填入 SIGNALENGINE_LLM__API_KEY
# → 可选: Telegram bot_token / chat_id

# 5. 启动
systemctl start signalengine.target

# 6. 查看状态
systemctl status signalengine-catalyst-alpha
journalctl -u signalengine-catalyst-alpha -f --no-hostname | grep qualified
```

---

## 方式二：手动部署

```bash
# 系统依赖
apt update && apt install -y curl git redis-server postgresql python3.12-venv
systemctl enable redis-server postgresql

# 项目
git clone <your-repo> /opt/signalengine
cd /opt/signalengine
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e .
pip install websocket-client httpx

# 数据库
sudo -u postgres createuser signalengine -P
sudo -u postgres createdb signalengine -O signalengine

# 配置
cp .env.example .env   # 或手动创建
vi .env

# 启动
cd /opt/signalengine
nohup .venv/bin/python -m core.worker --catalyst-alpha-live >> .run/full-stack/catalyst-alpha.log 2>&1 &
# ... 其他 worker 同理
```

---

## 数据迁移

如果本地已有数据（PostgreSQL + Redis），迁移步骤：

### PostgreSQL

```bash
# 本地导出
pg_dump -U signalengine -h localhost signalengine > signalengine_dump.sql

# 传到服务器
scp signalengine_dump.sql root@your-singapore-ip:/tmp/

# 服务器导入
psql -U signalengine -d signalengine < /tmp/signalengine_dump.sql
```

### Redis

Redis 主要存缓存数据（symbol cache、dedup），可以不用迁移。首次启动会自动重建。

---

## 验证

部署完成后，验证各 API 连通性：

```bash
# Binance
curl -s -o /dev/null -w "binance: %{http_code} (%{time_total}s)\n" \
  --connect-timeout 10 "https://api.binance.com/api/v3/exchangeInfo"

# DexScreener
curl -s -o /dev/null -w "dexscreener: %{http_code} (%{time_total}s)\n" \
  --connect-timeout 10 "https://api.dexscreener.com/token-profiles/latest/v1"

# OKX
curl -s -o /dev/null -w "okx: %{http_code} (%{time_total}s)\n" \
  --connect-timeout 10 "https://www.okx.com/api/v5/public/instruments?instType=SPOT"

# 预期结果: 全部 < 1s, http_code 200
```

---

## 日常管理

```bash
# 查看所有 worker 状态
systemctl status signalengine.target
systemctl list-dependencies signalengine.target

# 查看特定 worker 日志
journalctl -u signalengine-catalyst-alpha -f --no-hostname

# 重启单个 worker
systemctl restart signalengine-catalyst-alpha

# 重启所有
systemctl restart signalengine.target

# 停止所有
systemctl stop signalengine.target
```

---

## 成本估算

| 配置 | 月费（预估） |
|------|------------|
| ECS 2C4G | ¥200-300 |
| ECS 4C8G | ¥400-600 |
| RDS PostgreSQL（可选） | ¥100-200 |
| Redis 增强版（可选） | ¥100-200 |
| **合计** | **¥200-800/月** |

建议：先开 2C4G + 自建 PG/Redis，后续按需升级。
