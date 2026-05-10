#!/usr/bin/env bash
# signalengine 线上自检命令
# 在阿里云上执行: bash self_check.sh
set -euo pipefail

echo "=========================================="
echo "  signalengine 线上自检"
echo "=========================================="
echo ""

# ── 1. 加载的 min_score ──────────────────────
echo "--- 1. Telegram min_score 实际加载值 ---"
cd /opt/signalengine
sudo -u signalengine bash -c 'set -a; . .env; set +a; .venv/bin/python -c "
from core.config import AppSettings
s = AppSettings()
n = s.notifications
if hasattr(n, \"telegram\"):
    t = n.telegram
    print(f\"enabled={t.enabled}\")
    print(f\"bot_token={'***' if t.bot_token else '(empty)'}\")
    print(f\"chat_id={'***' if t.chat_id else '(empty)'}\")
    print(f\"min_score={t.min_score}\")
    print(f\"publish_alpha_types={t.publish_alpha_types}\")
else:
    print(\"telegram config not found\")
"'
echo ""

# ── 2. 代码是否包含 trigger_score 过滤 ──────
echo "--- 2. trigger_score 过滤代码检查 ---"
grep -n 'trigger_score.*min_score\|trigger_score.*config\.' /opt/signalengine/notifications/telegram_publisher.py || echo "❌ 未找到 trigger_score 过滤代码"
echo ""

# ── 3. 最近 50 条通知的分数分布 ────────────
echo "--- 3. 最近通知的分数分布 (DB) ---"
PGPASSWORD=signalengine123 psql -U signalengine -h 127.0.0.1 -d signalengine -c "
SELECT 
  status, 
  COUNT(*) as count,
  ROUND(AVG(
    COALESCE(
      (payload::json->'payload'->'score')::text::numeric,
      (payload::json->'payload'->'trigger_score')::text::numeric,
      0
    )
  ), 4) as avg_score
FROM notification_deliveries 
WHERE channel='telegram'
  AND created_at > NOW() - INTERVAL '1 hour'
GROUP BY status
ORDER BY count DESC;
" 2>/dev/null || echo "❌ 无法查询 PG"
echo ""

echo "--- 3b. 最近发送的通知 (status=sent) ---"
PGPASSWORD=signalengine123 psql -U signalengine -h 127.0.0.1 -d signalengine -c "
SELECT 
  created_at,
  status,
  SUBSTRING(error_message FROM 1 FOR 40) as error,
  COALESCE(
    (payload::json->'payload'->'score')::text,
    (payload::json->'payload'->'trigger_score')::text,
    'N/A'
  ) as score,
  COALESCE(
    payload::json->'payload'->>'alpha_type',
    'N/A'
  ) as alpha_type
FROM notification_deliveries 
WHERE channel='telegram'
  AND created_at > NOW() - INTERVAL '1 hour'
ORDER BY created_at DESC
LIMIT 20;
" 2>/dev/null || echo "❌ 无法查询 PG"
echo ""

# ── 4. 检查 journal 中最近的通知日志 ────────
echo "--- 4. journal 中最近的通知日志 ---"
journalctl -u signalengine-telegram-publisher --no-hostname --since "1 hour ago" -n 30 --no-pager 2>/dev/null | grep -oE '"score":[0-9.]+|"message":"[^"]*"|"status":"[^"]*"' | head -20 || echo "无日志"
echo ""

# ── 5. 确认代码版本 ──────────────────────────
echo "--- 5. 运行中代码包含 trigger_score 过滤? ---"
grep 'trigger_score' /opt/signalengine/notifications/telegram_publisher.py && echo "✅ 包含" || echo "❌ 不包含"
echo ""

echo "=========================================="
echo "  自检完成"
echo "=========================================="
