from __future__ import annotations

from datetime import UTC, datetime
from logging import Logger, getLogger
from typing import Any, Callable

import httpx
from redis import Redis

from core.config import AppSettings, NotificationsConfig
from core.schemas import EventEnvelope
from infra.metrics import Metrics
from infra.redis_stream import acknowledge_message, ensure_consumer_group, read_group_models
from infra.repository import StorageRepository

TelegramTransport = Callable[[str, str, str], str]


class TelegramPublisherService:
    def __init__(
        self,
        settings: AppSettings,
        redis_client: Redis,
        repository: StorageRepository,
        *,
        transport: TelegramTransport | None = None,
        logger: Logger | None = None,
        metrics: Metrics | None = None,
    ) -> None:
        self.settings = settings
        self.redis_client = redis_client
        self.repository = repository
        self.transport = transport or _send_telegram_message
        self.logger = logger or getLogger("signalengine.telegram_publisher")
        self.metrics = metrics or Metrics(settings.observability.service_namespace)

    def ensure_stream(self) -> None:
        config = _notifications_config(self.settings).telegram
        ensure_consumer_group(
            self.redis_client,
            self.settings.redis.raw_events_stream,
            config.consumer_group,
        )

    def process_once(self, *, count: int = 100, block_ms: int | None = None) -> int:
        config = _notifications_config(self.settings).telegram
        if not config.enabled:
            return 0
        self.metrics.mark_heartbeat(service="telegram_publisher", mode="process_once")
        events = read_group_models(
            self.redis_client,
            self.settings.redis.raw_events_stream,
            config.consumer_group,
            config.consumer_name,
            EventEnvelope,
            count=count,
            block_ms=block_ms,
        )
        processed = 0
        for message_id, event in events:
            try:
                self._process_event(event)
            except Exception as error:
                self._record_failed_delivery(event, str(error))
                self.logger.exception(
                    "telegram_delivery_failed",
                    extra={
                        "service": "telegram_publisher",
                        "outcome": event.event_type,
                        "token": event.token,
                        "chain": event.chain,
                    },
                )
            finally:
                acknowledge_message(
                    self.redis_client,
                    self.settings.redis.raw_events_stream,
                    config.consumer_group,
                    message_id,
                )
            processed += 1
        return processed

    def _process_event(self, event: EventEnvelope) -> None:
        if event.event_type not in {"alpha.candidate_qualified", "alpha.cross_dimension_snapshot"}:
            return
        config = _notifications_config(self.settings).telegram
        if not config.bot_token or not config.chat_id:
            raise ValueError("telegram_credentials_missing")

        if event.event_type == "alpha.cross_dimension_snapshot":
            self._process_cross_dimension_event(event, config)
            return

        # Original processing for alpha.candidate_qualified
        alpha_type = str(event.payload.get("alpha_type", "")).upper()
        candidate_id = str(event.payload.get("candidate_id", "")).strip()
        score = float(event.payload.get("score", 0.0) or 0.0)
        if not candidate_id:
            raise ValueError("telegram_candidate_id_missing")

        existing = self.repository.notifications.load_delivery(
            channel="telegram",
            destination=config.chat_id,
            candidate_id=candidate_id,
            event_type=event.event_type,
        )
        if existing is not None and str(existing.get("status", "")) in {"sent", "skipped"}:
            return

        if alpha_type not in set(config.publish_alpha_types):
            self.repository.notifications.save_delivery(
                channel="telegram",
                destination=config.chat_id,
                candidate_id=candidate_id,
                event_type=event.event_type,
                status="skipped",
                payload=event.model_dump(mode="json"),
                error_message=f"unsupported_alpha_type:{alpha_type}",
            )
            self.metrics.notification_deliveries.labels(channel="telegram", status="skipped").inc()
            return

        if score < config.min_score:
            self.repository.notifications.save_delivery(
                channel="telegram",
                destination=config.chat_id,
                candidate_id=candidate_id,
                event_type=event.event_type,
                status="skipped",
                payload=event.model_dump(mode="json"),
                error_message=f"score_below_min:{score}",
            )
            self.metrics.notification_deliveries.labels(channel="telegram", status="skipped").inc()
            return

        remote_message_id = self.transport(
            config.bot_token,
            config.chat_id,
            self._format_qualified_candidate_message(event),
        )
        self.repository.notifications.save_delivery(
            channel="telegram",
            destination=config.chat_id,
            candidate_id=candidate_id,
            event_type=event.event_type,
            status="sent",
            payload=event.model_dump(mode="json"),
            remote_message_id=remote_message_id,
            delivered_at=datetime.now(UTC),
        )
        self.metrics.notification_deliveries.labels(channel="telegram", status="sent").inc()

    def _process_cross_dimension_event(
        self,
        event: EventEnvelope,
        config: Any,
    ) -> None:
        """Process an alpha.cross_dimension_snapshot event with unified format."""
        snapshot_id = str(event.payload.get("snapshot_id", event.event_id))
        alpha_type = str(event.payload.get("alpha_type", "UNKNOWN")).upper()

        existing = self.repository.notifications.load_delivery(
            channel="telegram",
            destination=config.chat_id,
            candidate_id=snapshot_id,
            event_type=event.event_type,
        )
        if existing is not None and str(existing.get("status", "")) in {"sent", "skipped"}:
            return

        if alpha_type not in set(config.publish_alpha_types):
            self.repository.notifications.save_delivery(
                channel="telegram",
                destination=config.chat_id,
                candidate_id=snapshot_id,
                event_type=event.event_type,
                status="skipped",
                payload=event.model_dump(mode="json"),
                error_message=f"unsupported_alpha_type:{alpha_type}",
            )
            self.metrics.notification_deliveries.labels(channel="telegram", status="skipped").inc()
            return

        remote_message_id = self.transport(
            config.bot_token,
            config.chat_id,
            self._format_cross_dimension_message(event),
        )
        self.repository.notifications.save_delivery(
            channel="telegram",
            destination=config.chat_id,
            candidate_id=snapshot_id,
            event_type=event.event_type,
            status="sent",
            payload=event.model_dump(mode="json"),
            remote_message_id=remote_message_id,
            delivered_at=datetime.now(UTC),
        )
        self.metrics.notification_deliveries.labels(channel="telegram", status="sent").inc()

    def _record_failed_delivery(self, event: EventEnvelope, error_message: str) -> None:
        if event.event_type not in {"alpha.candidate_qualified", "alpha.cross_dimension_snapshot"}:
            return
        config = _notifications_config(self.settings).telegram
        candidate_id = str(event.payload.get("candidate_id", "")).strip()
        if not candidate_id or not config.chat_id:
            return
        self.repository.notifications.save_delivery(
            channel="telegram",
            destination=config.chat_id,
            candidate_id=candidate_id,
            event_type=event.event_type,
            status="failed",
            payload=event.model_dump(mode="json"),
            error_message=error_message,
        )
        self.metrics.notification_deliveries.labels(channel="telegram", status="failed").inc()

    def _format_qualified_candidate_message(self, event: EventEnvelope) -> str:
        lines = _base_qualified_candidate_message_lines(event)
        lines.extend(self._fsm_context_lines(event))
        return "\n".join(lines)

    def _format_cross_dimension_message(self, event: EventEnvelope) -> str:
        return "\n".join(_cross_dimension_message_lines(event))

    def _fsm_context_lines(self, event: EventEnvelope) -> list[str]:
        checkpoint = self.repository.checkpoints.load(
            f"fsm_state:{event.chain}:{event.token}"
        )
        if checkpoint is None:
            return []

        metadata = checkpoint.metadata
        reasons = metadata.get("reasons")
        reason_items = reasons if isinstance(reasons, list) else []
        last_transition_timestamp = metadata.get("last_transition_timestamp")
        lines = [f"FSM State: {checkpoint.cursor}"]

        if isinstance(last_transition_timestamp, int):
            lines.append(
                "FSM Last Transition: "
                f"{datetime.fromtimestamp(last_transition_timestamp, UTC).isoformat()}"
            )
        if reason_items:
            lines.append(
                f"FSM Reasons: {', '.join(str(item) for item in reason_items)}"
            )

        return lines


def _base_qualified_candidate_message_lines(event: EventEnvelope) -> list[str]:
    alpha_type = str(event.payload.get("alpha_type", "UNKNOWN")).upper()
    candidate = event.payload.get("candidate")
    snapshot = event.payload.get("snapshot")
    candidate_payload = candidate if isinstance(candidate, dict) else {}
    snapshot_payload = snapshot if isinstance(snapshot, dict) else {}
    reasons = event.payload.get("reasons")
    reason_items = reasons if isinstance(reasons, list) else []
    lines = [
        f"ALPHA QUALIFIED | {alpha_type}",
        f"Token: {event.token}",
        f"Chain: {event.chain}",
        f"Score: {float(event.payload.get('score', 0.0) or 0.0):.4f}",
    ]

    if alpha_type == "LAUNCH":
        lines.extend(
            [
                f"DEX: {candidate_payload.get('dex', '')}",
                f"Pool: {candidate_payload.get('pool_address', '')}",
                f"Initial Liquidity USD: {float(candidate_payload.get('initial_liquidity_usd', 0.0) or 0.0):.2f}",
                f"Buy Notional 5m USD: {float(candidate_payload.get('buy_notional_5m_usd', 0.0) or 0.0):.2f}",
                f"Trade Count 5m: {int(candidate_payload.get('trade_count_5m', 0) or 0)}",
                f"Unique Wallets 5m: {int(candidate_payload.get('unique_wallets_5m', 0) or 0)}",
            ]
        )
    elif alpha_type == "CATALYST":
        lines.extend(
            [
                f"Headline: {snapshot_payload.get('headline', '')}",
                f"Catalyst Type: {snapshot_payload.get('catalyst_type', '')}",
                f"Credibility: {float(snapshot_payload.get('credibility_score', 0.0) or 0.0):.2f}",
                f"Venue: {snapshot_payload.get('venue', '')}",
            ]
        )

    if reason_items:
        lines.append(f"Reasons: {', '.join(str(item) for item in reason_items)}")
    return lines


def _cross_dimension_message_lines(event: EventEnvelope) -> list[str]:
    """Unified Telegram message format for cross-dimension snapshots.

    Same structure regardless of alpha_type (LAUNCH / CATALYST / FLOW).
    Missing fields display as N/A.
    """
    p = event.payload
    alpha_type = str(p.get("alpha_type", "UNKNOWN")).upper()
    trigger_source = str(p.get("trigger_source", "unknown"))
    trigger_score = float(p.get("trigger_score", 0.0) or 0.0)
    trigger_reasons = p.get("trigger_reasons", [])
    reason_str = ", ".join(str(r) for r in trigger_reasons) if trigger_reasons else "N/A"

    onchain_liq = _fmt_usd(p.get("onchain_liquidity_usd"))
    onchain_vol = _fmt_usd(p.get("onchain_volume_5m_usd"))
    onchain_price = _fmt_price(p.get("onchain_price_usd"))
    onchain_pc_5m = _fmt_pct(p.get("onchain_price_change_5m"))
    onchain_pc_1h = _fmt_pct(p.get("onchain_price_change_1h"))
    onchain_fdv = _fmt_usd(p.get("onchain_fdv"))
    onchain_pools = int(p.get("onchain_pool_count", 0))

    wallet_inflow = _fmt_usd(p.get("wallet_smart_money_inflow_usd"))
    wallet_outflow = _fmt_usd(p.get("wallet_smart_money_outflow_usd"))
    wallet_buyers = _fmt_int(p.get("wallet_unique_buyers"))
    wallet_sellers = _fmt_int(p.get("wallet_unique_sellers"))
    wallet_whales = int(p.get("wallet_whale_buys", 0))

    social_sent = _fmt_float(p.get("social_sentiment"))
    social_vel = _fmt_float(p.get("social_velocity"))
    social_mentions = int(p.get("social_mention_count", 0))

    latency = int(p.get("collection_latency_ms", 0))
    errors = p.get("errors", {})
    error_str = f" ⚠️ {', '.join(errors.values())}" if errors else ""

    lines = [
        f"🔔 ALPHA DETECTED | {alpha_type}",
        f"Token: {event.token} ({event.chain})",
        "",
        "📡 Trigger",
        f"  Source: {trigger_source}",
        f"  Score: {trigger_score:.4f}",
        f"  Reasons: {reason_str}",
        "",
        "⛓ On-Chain",
        f"  Liquidity: {onchain_liq}",
        f"  Volume 5m: {onchain_vol}",
        f"  Price: {onchain_price}",
        f"  Price 5m: {onchain_pc_5m}",
        f"  Price 1h: {onchain_pc_1h}",
        f"  FDV: {onchain_fdv}",
        f"  Pools: {onchain_pools}",
        "",
        "👛 Wallet",
        f"  Smart Money Inflow: {wallet_inflow}",
        f"  Smart Money Outflow: {wallet_outflow}",
        f"  Unique Buyers: {wallet_buyers}",
        f"  Unique Sellers: {wallet_sellers}",
        f"  Whale Buys: {wallet_whales}",
        "",
        "💬 Social",
        f"  Sentiment: {social_sent}",
        f"  Velocity: {social_vel}",
        f"  Mentions: {social_mentions}",
        "",
        f"⚡ Latency: {latency}ms{error_str}",
        f"#alpha #{alpha_type.lower()} #{event.chain}",
    ]
    return lines


def _fmt_usd(value: object) -> str:
    if value is None:
        return "N/A"
    v = float(value)
    if v >= 1_000_000:
        return f"${v / 1_000_000:,.2f}M"
    if v >= 1_000:
        return f"${v:,.0f}"
    return f"${v:.2f}"


def _fmt_price(value: object) -> str:
    if value is None:
        return "N/A"
    v = float(value)
    if v < 0.0001:
        return f"{v:.10f}"
    if v < 1:
        return f"{v:.6f}"
    return f"{v:.4f}"


def _fmt_pct(value: object) -> str:
    if value is None:
        return "N/A"
    v = float(value)
    return f"{v:+.2f}%" if v != 0 else "0.00%"


def _fmt_int(value: object) -> str:
    if value is None:
        return "N/A"
    return str(int(value))


def _fmt_float(value: object) -> str:
    if value is None:
        return "N/A"
    return f"{float(value):.3f}"


def _send_telegram_message(bot_token: str, chat_id: str, text: str) -> str:
    response = httpx.post(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        },
        timeout=10.0,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or not payload.get("ok"):
        raise ValueError("telegram_send_failed")
    result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    return str(result.get("message_id", ""))


def _notifications_config(settings: AppSettings) -> NotificationsConfig:
    raw = settings.notifications
    if isinstance(raw, NotificationsConfig):
        return raw
    return NotificationsConfig.model_validate(raw)