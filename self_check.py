#!/usr/bin/env python3
"""signalengine 线上自检脚本
在阿里云上执行: cd /opt/signalengine && sudo -u signalengine .venv/bin/python self_check.py
"""
import os, subprocess, sys

# Let AppSettings.load() handle .env + env vars directly
os.environ.setdefault("SIGNALENGINE_RUNTIME__ENVIRONMENT", "production")
os.chdir("/opt/signalengine")

print("=" * 60)
print("  signalengine 线上自检")
print("=" * 60)
print()

# 1. min_score
print("--- 1. Telegram min_score 实际加载值 ---")
try:
    # Load .env — handle values with = signs and quotes
    with open("/opt/signalengine/.env") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip("'\"")
            os.environ[key] = val

    from core.config import AppSettings
    s = AppSettings.load()
    t = s.notifications.telegram
    print(f"  enabled={t.enabled}")
    print(f"  bot_token={'***' if t.bot_token else '(empty)'}")
    print(f"  chat_id={'***' if t.chat_id else '(empty)'}")
    print(f"  min_score={t.min_score}")
    print(f"  publish_alpha_types={t.publish_alpha_types}")
except Exception as e:
    print(f"  ❌ {e}")
print()

# 2. trigger_score check
print("--- 2. trigger_score 过滤代码检查 ---")
try:
    with open("/opt/signalengine/notifications/telegram_publisher.py") as f:
        content = f.read()
    if "trigger_score < config.min_score" in content:
        print("  ✅ trigger_score 过滤代码存在")
    else:
        print("  ❌ 未找到 trigger_score 过滤代码")
except FileNotFoundError:
    print("  ❌ telegram_publisher.py 不存在")
print()

# 3. DB 查询
print("--- 3. 最近通知的分数分布 (DB) ---")
try:
    import psycopg
    conn = psycopg.connect("dbname=signalengine user=signalengine password=signalengine123 host=127.0.0.1")
    cur = conn.cursor()
    cur.execute("""
        SELECT status, COUNT(*) as count
        FROM notification_deliveries
        WHERE channel='telegram' AND created_at > NOW() - INTERVAL '1 hour'
        GROUP BY status ORDER BY count DESC
    """)
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]}")
    print()
    print("  最近发送的通知 (status=sent):")
    cur.execute("""
        SELECT created_at, status,
            COALESCE(
                (payload::json->'payload'->>'score')::text,
                (payload::json->'payload'->>'trigger_score')::text,
                'N/A'
            ) as score,
            COALESCE(payload::json->'payload'->>'alpha_type', 'N/A') as alpha_type,
            SUBSTRING(COALESCE(error_message, '') FROM 1 FOR 40) as err
        FROM notification_deliveries
        WHERE channel='telegram' AND created_at > NOW() - INTERVAL '1 hour'
        ORDER BY created_at DESC LIMIT 20
    """)
    for row in cur.fetchall():
        print(f"  {row[0]} | {row[1]:7s} | score={row[2]:8s} | type={row[3]:8s} | {row[4]}")
    conn.close()
except Exception as e:
    print(f"  ❌ {e}")
print()

# 4. journal
print("--- 4. journal 中最近的通知日志 ---")
try:
    r = subprocess.run(
        ["journalctl", "-u", "signalengine-telegram-publisher", "--no-hostname",
         "--since", "1 hour ago", "-n", "30", "--no-pager"],
        capture_output=True, text=True, timeout=10
    )
    for line in r.stdout.splitlines():
        for kw in ['score', 'message', 'status', 'skipped', 'sent']:
            if kw in line:
                print(f"  {line.strip()[:120]}")
                break
except Exception as e:
    print(f"  ❌ {e}")
print()

# 5. 运行中的代码版本
print("--- 5. 运行中的版本 ---")
try:
    with open("/opt/signalengine/notifications/telegram_publisher.py") as f:
        content = f.read()
    if "trigger_score" in content:
        lines = [l.strip() for l in content.split("\n") if "trigger_score" in l]
        for l in lines[:3]:
            print(f"  ✅ trigger_score: {l[:80]}")
except Exception as e:
    print(f"  ❌ {e}")
print()

print("=" * 60)
print("  自检完成")
print("=" * 60)
