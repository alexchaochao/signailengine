#!/usr/bin/env bash
# =============================================================================
# signalengine — 阿里云新加坡一键部署脚本
# 适用环境: Ubuntu 22.04 / Debian 12, Python 3.12, 4GB+ RAM
# 用法:
#   chmod +x deploy_aliyun.sh
#   sudo ./deploy_aliyun.sh
#
# 执行后:
#   - 安装系统依赖 (Redis, PostgreSQL, Python 3.12, pip 依赖)
#   - 创建 signalengine 用户 + 项目目录
#   - 从当前机器同步代码 (或从 Git 克隆)
#   - 创建 PostgreSQL 数据库 + 用户
#   - 生成 systemd 服务 (12 个 worker)
#   - 输出管理命令
# =============================================================================

set -euo pipefail

# ── 配置区（按需修改）─────────────────────────────────────────────────────
APP_USER="signalengine"
APP_GROUP="signalengine"
APP_HOME="/opt/signalengine"
REPO_URL=""                      # 填空则从本地 rsync
GIT_BRANCH="main"
POSTGRES_DB="signalengine"
POSTGRES_USER="signalengine"
POSTGRES_PASSWORD="${SIGNALENGINE_DB_PASSWORD:-signalengine}"  # 建议通过环境变量传入
REDIS_PORT=6379

# 可选跳过 PostgreSQL（如果服务器已有）
SKIP_POSTGRES="${SKIP_POSTGRES:-0}"

# Python 版本
PYTHON_VERSION="3.12"
PIP_REQUIREMENTS="requirements.txt"

# ── 颜色 ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }

# ── 前置检查 ──────────────────────────────────────────────────────────────
if [[ $EUID -ne 0 ]]; then
    error "请以 root 身份运行: sudo ./deploy_aliyun.sh"
    exit 1
fi

OS="$(lsb_release -is 2>/dev/null || echo 'unknown')"
if [[ "$OS" != "Ubuntu" && "$OS" != "Debian" ]]; then
    warn "仅测试过 Ubuntu/Debian，其他发行版可能需要调整"
fi

info "=========================================="
info "  signalengine 阿里云新加坡部署"
info "=========================================="

# ── 步骤 1: 系统依赖 ──────────────────────────────────────────────────────
info "[1/8] 安装系统依赖..."

apt-get update -qq
apt-get install -y -qq \
    curl wget git rsync \
    build-essential libssl-dev zlib1g-dev libbz2-dev \
    libreadline-dev libsqlite3-dev libncursesw5-dev \
    xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev \
    postgresql postgresql-client postgresql-contrib \
    redis-server \
    nginx

# 确保 Python 3.12 可用
if ! command -v python3.12 &>/dev/null; then
    warn "Python 3.12 未安装，尝试通过 deadsnakes PPA 安装..."
    if [[ "$OS" == "Ubuntu" ]]; then
        apt-get install -y -qq software-properties-common
        add-apt-repository -y ppa:deadsnakes/ppa
        apt-get update -qq
        apt-get install -y -qq python3.12 python3.12-dev python3.12-venv python3.12-distutils
    else
        # Debian: 从源码编译 Python 3.12
        cd /tmp
        curl -SL https://www.python.org/ftp/python/3.12.3/Python-3.12.3.tgz -o python.tgz
        tar xzf python.tgz && cd Python-3.12.3
        ./configure --enable-optimizations --prefix=/usr/local
        make -j"$(nproc)" && make altinstall
        cd /tmp && rm -rf Python-3.12.3 python.tgz
    fi
fi

PYTHON_BIN="$(command -v python3.12)"
info "Python: $($PYTHON_BIN --version)"

# ── 步骤 2: 创建用户 + 目录 ──────────────────────────────────────────────
info "[2/8] 创建系统用户和项目目录..."

id -u "$APP_USER" &>/dev/null || useradd -m -s /bin/bash "$APP_USER"
mkdir -p "$APP_HOME" "$APP_HOME"/{.run/full-stack,logs}
chown -R "$APP_USER:$APP_GROUP" "$APP_HOME"

# ── 步骤 3: 同步代码 ──────────────────────────────────────────────────────
info "[3/8] 部署代码..."

if [[ -n "$REPO_URL" ]]; then
    # 从 Git 克隆
    if [[ -d "$APP_HOME/.git" ]]; then
        sudo -u "$APP_USER" git -C "$APP_HOME" pull origin "$GIT_BRANCH"
    else
        sudo -u "$APP_USER" git clone -b "$GIT_BRANCH" "$REPO_URL" "$APP_HOME"
    fi
else
    # 从本地 rsync（在本机运行脚本的情况下）
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    if [[ "$SCRIPT_DIR" != "$APP_HOME" ]]; then
        rsync -av --exclude='.venv' --exclude='.run' --exclude='__pycache__' \
            --exclude='*.pyc' --exclude='.git' --exclude='node_modules' \
            "$SCRIPT_DIR"/ "$APP_HOME"/
        chown -R "$APP_USER:$APP_GROUP" "$APP_HOME"
    fi
fi

# ── 步骤 4: Python 虚拟环境 + 依赖 ────────────────────────────────────────
info "[4/8] 创建 Python 虚拟环境和安装依赖..."

sudo -u "$APP_USER" "$PYTHON_BIN" -m venv "$APP_HOME/.venv"
source "$APP_HOME/.venv/bin/activate"

# 升级 pip
pip install --quiet --upgrade pip setuptools wheel

# 安装项目依赖
if [[ -f "$APP_HOME/$PIP_REQUIREMENTS" ]]; then
    pip install --quiet -r "$APP_HOME/$PIP_REQUIREMENTS"
else
    # 从 pyproject.toml 安装
    pip install --quiet -e "$APP_HOME"
fi

# 安装额外依赖（WebSocket、HTTPX）
pip install --quiet websocket-client httpx psycopg2-binary

deactivate
info "依赖安装完成"

# ── 步骤 5: 配置 Redis ────────────────────────────────────────────────────
info "[5/8] 配置 Redis..."

systemctl enable redis-server
systemctl restart redis-server
sleep 1
redis-cli ping || warn "Redis 未响应，请手动检查"

# ── 步骤 6: 配置 PostgreSQL ──────────────────────────────────────────────
if [[ "$SKIP_POSTGRES" -eq 1 ]]; then
    info "[6/8] 跳过 PostgreSQL（SKIP_POSTGRES=1）..."
    info "  确保 .env 中 SIGNALENGINE_POSTGRES__URL 指向你的现有数据库"
else
    info "[6/8] 配置 PostgreSQL..."

    systemctl enable postgresql
    systemctl restart postgresql
    sleep 2

    # 创建数据库和用户（幂等）
    sudo -u postgres psql -tc "SELECT 1 FROM pg_roles WHERE rolname='$POSTGRES_USER'" | grep -q 1 \
        || sudo -u postgres psql -c "CREATE ROLE $POSTGRES_USER LOGIN PASSWORD '$POSTGRES_PASSWORD';"
    sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname='$POSTGRES_DB'" | grep -q 1 \
        || sudo -u postgres psql -c "CREATE DATABASE $POSTGRES_DB OWNER $POSTGRES_USER;"
    sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE $POSTGRES_DB TO $POSTGRES_USER;"

    info "数据库 $POSTGRES_DB 就绪"
fi

# ── 步骤 7: 生成 .env 配置文件 ────────────────────────────────────────────
info "[7/8] 生成 .env 配置..."

if [[ ! -f "$APP_HOME/.env" ]]; then
    cat > "$APP_HOME/.env" << ENVEOF
# signalengine 生产配置 — 阿里云新加坡
# 由 deploy_aliyun.sh 自动生成于 $(date -Iseconds)

SIGNALENGINE_RUNTIME__ENVIRONMENT=production
SIGNALENGINE_RUNTIME__LOG_LEVEL=INFO

# Redis (本地)
SIGNALENGINE_REDIS__URL=redis://localhost:$REDIS_PORT/0

# PostgreSQL (本地)
SIGNALENGINE_POSTGRES__URL=postgresql+psycopg://$POSTGRES_USER:$POSTGRES_PASSWORD@localhost:5432/$POSTGRES_DB
SIGNALENGINE_POSTGRES__POOL_SIZE=5
SIGNALENGINE_POSTGRES__MAX_OVERFLOW=10

# ── 外部 API 无需代理（新加坡直连） ──────────────────────────────────────
# HTTP_PROXY=   ← 留空，新加坡服务器直连
# HTTPS_PROXY=

# DeepSeek LLM
SIGNALENGINE_LLM__ENABLED=true
SIGNALENGINE_LLM__PROVIDER=deepseek
SIGNALENGINE_LLM__MODEL=deepseek-v4-flash
SIGNALENGINE_LLM__BASE_URL=https://api.deepseek.com
SIGNALENGINE_LLM__TIMEOUT_SECONDS=15.0
SIGNALENGINE_LLM__API_KEY=${SIGNALENGINE_LLM__API_KEY:-sk-your-key-here}

# 执行配置（默认 paper only）
SIGNALENGINE_EXECUTION__MAX_RETRIES=2
SIGNALENGINE_EXECUTION__RECOVER_PENDING_ON_STARTUP=true
SIGNALENGINE_RISK__LIVE_TRADING_ENABLED=false

# Solana RPC
SIGNALENGINE_VENUES__SOLANA_RPC_URL=https://api.mainnet-beta.solana.com
SIGNALENGINE_VENUES__SOLANA_RPC_TIMEOUT_SECONDS=10.0
SIGNALENGINE_VENUES__SOLANA_RPC_MAX_RETRIES=3

# 采集频率（生产环境加速）
SIGNALENGINE_ACQUISITION__SYNC_INTERVAL_SECONDS=5.0
SIGNALENGINE_ACQUISITION__FAILURE_BACKOFF_SECONDS=5.0

# Telegram 通知门槛
SIGNALENGINE_NOTIFICATIONS__TELEGRAM__MIN_SCORE=0.5

# 钱包情报（可选）
SIGNALENGINE_LIVE__WALLET_INTELLIGENCE__MEASUREMENT_TOKEN=BONK
SIGNALENGINE_LIVE__WALLET_INTELLIGENCE__SYNC_INTERVAL_SECONDS=600.0
ENVEOF
    chown "$APP_USER:$APP_GROUP" "$APP_HOME/.env"
    info ".env 文件已生成，请编辑 API key: vi $APP_HOME/.env"
else
    warn ".env 已存在，跳过生成"
fi

# ── 步骤 8: 创建 systemd 服务 ─────────────────────────────────────────────
info "[8/8] 创建 systemd 服务..."

# 每个 worker 一个服务 + 一个 unified target
WORKERS=(
    "worker:--group signal-workers --consumer worker-full:9000"
    "onchain-feature:--onchain-feature-live:9001"
    "launch-alpha:--launch-alpha-live:9002"
    "catalyst-alpha:--catalyst-alpha-live:9003"
    "flow-measurement:--flow-measurement-live:9004"
    "social-live:--social-live:9005"
    "social-confirmation:--social-confirmation-live --group social-confirmation --consumer social-confirmation-1:9006"
    "telegram-publisher:--telegram-publisher-live:9007"
    "alpha-collector:--alpha-collector-live:9010"
    "alpha-pipeline:--alpha-pipeline-live:9011"
    "wallet-intelligence:--wallet-intelligence-sync:9008"
    "measurement-bridge:--measurement-bridge:9009"
)

for entry in "${WORKERS[@]}"; do
    IFS=':' read -r name args port <<< "$entry"

    cat > "/etc/systemd/system/signalengine-${name}.service" << SERVICEEOF
[Unit]
Description=signalengine ${name}
After=network.target redis-server.service postgresql.service
Requires=redis-server.service postgresql.service
PartOf=signalengine.target

[Service]
Type=simple
User=$APP_USER
Group=$APP_GROUP
WorkingDirectory=$APP_HOME
Environment=SIGNALENGINE_OBSERVABILITY__METRICS_PORT=$port
EnvironmentFile=$APP_HOME/.env
ExecStart=$APP_HOME/.venv/bin/python -m core.worker ${args}
Restart=always
RestartSec=5
StartLimitIntervalSec=60
StartLimitBurst=3

# 安全
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full

[Install]
WantedBy=signalengine.target
SERVICEEOF

    info "  创建 signalengine-${name}.service"
done

# 创建 target 聚合所有服务
cat > "/etc/systemd/system/signalengine.target" << TARGETEOF
[Unit]
Description=signalengine — All Workers
Requires=redis-server.service postgresql.service
After=redis-server.service postgresql.service
Wants=$(for entry in "${WORKERS[@]}"; do echo -n "signalengine-${entry%%:*}.service "; done)
TARGETEOF

systemctl daemon-reload
systemctl enable signalengine.target

# ── 完成 ──────────────────────────────────────────────────────────────────
info "=========================================="
info "  部署完成!"
info "=========================================="
echo ""
echo " 下一步:"
echo "  1. 编辑配置:  vi $APP_HOME/.env"
echo "     → 填入 SIGNALENGINE_LLM__API_KEY"
echo "     → 如需 Telegram 通知，配置 bot_token / chat_id"
echo ""
echo "  2. 启动所有 worker:"
echo "     systemctl start signalengine.target"
echo ""
echo "  3. 查看状态:"
echo "     systemctl status signalengine.target"
echo "     systemctl status signalengine-catalyst-alpha"
echo ""
echo "  4. 查看日志:"
echo "     journalctl -u signalengine-catalyst-alpha -f"
echo "     # 或查看文件日志:"
echo "     tail -f $APP_HOME/.run/full-stack/catalyst-alpha.log"
echo ""
echo "  5. 停止所有:"
echo "     systemctl stop signalengine.target"
echo ""
echo "  6. 测试连通性:"
echo "     $APP_HOME/.venv/bin/python -c \\"
echo "       'import urllib.request; r=urllib.request.urlopen("https://api.binance.com/api/v3/exchangeInfo", timeout=10); print(r.status)'"
echo ""
echo "  注意: 首次启动 catalyst-alpha 会做 REST bootstrap"
echo "  (OKX 约 1200+ symbol, Bybit 约 500+ symbol)，"
echo "  前几分钟日志中 qualified=0 是正常的。"
