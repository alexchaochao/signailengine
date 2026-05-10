#!/usr/bin/env bash
# signalengine 部署验证脚本
# 用法: bash verify_deploy.sh
set -euo pipefail
PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo -e "  [\e[32mPASS\e[0m] $1"; }
fail() { FAIL=$((FAIL+1)); echo -e "  [\e[31mFAIL\e[0m] $1"; }

echo "========================================"
echo "  signalengine 部署验证"
echo "========================================" && echo ""

echo "--- 1. 基础环境 ---"
PY=$(python3 --version 2>/dev/null || true)
echo "$PY" | grep -q "3.12" && ok "Python: $PY" || fail "Python 3.12: $PY"
redis-cli ping 2>/dev/null | grep -q PONG && ok "Redis: 运行中" || fail "Redis: 未响应"
psql -U signalengine -d signalengine -c "SELECT 1" &>/dev/null && ok "PostgreSQL: 可连接" || fail "PostgreSQL: 不可连接"
[[ -d /opt/signalengine/.venv ]] && ok "venv: 存在" || fail "venv: 不存在"
[[ -f /opt/signalengine/.env ]] && ok ".env: 存在" || fail ".env: 不存在"
echo ""

echo "--- 2. 外部 API 连通性 ---"
for e in "Binance|https://api.binance.com/api/v3/exchangeInfo|10" "DexScreener|https://api.dexscreener.com/token-profiles/latest/v1|10" "OKX|https://www.okx.com/api/v5/public/instruments?instType=SPOT|10" "Coinbase|https://api.exchange.coinbase.com/products|10" "DeepSeek|https://api.deepseek.com|10"; do
  IFS='|' read -r n u t <<< "$e"
  c=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout "$t" "$u" 2>/dev/null || echo "000")
  [[ "$c" == "200" || "$c" == "400" ]] && ok "$n: $c" || fail "$n: $c"
done
echo ""

echo "--- 3. Worker 服务 ---"
for s in signalengine-{worker-full,catalyst-alpha,launch-alpha,onchain-feature,flow-measurement,social-live,social-confirmation,telegram-publisher,alpha-collector,alpha-pipeline,wallet-intelligence,measurement-bridge}; do
  systemctl is-active --quiet "$s" 2>/dev/null && ok "$s: active" || fail "$s: 未运行"
done
echo ""

echo "--- 4. 日志摘要 ---"
LOG_DIR="/opt/signalengine/.run/full-stack"
if [[ -d "$LOG_DIR" ]]; then
  for f in "$LOG_DIR"/*.log; do
    n=$(basename "$f"); l=$(wc -l < "$f" 2>/dev/null || echo 0)
    e=$(grep -c '"level": "ERROR"' "$f" 2>/dev/null || echo 0)
    q=$(grep -c '"outcome": "qualified"' "$f" 2>/dev/null || echo 0)
    echo "  $n → ${l}行, ${e}个ERROR, ${q}个qualified"
  done
fi
echo ""
echo "结果: $PASS 通过, $FAIL 失败"
[[ $FAIL -gt 0 ]] && exit 1
