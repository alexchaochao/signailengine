from __future__ import annotations

import argparse
import signal
from datetime import UTC, datetime
from threading import Event
from time import sleep
from pathlib import Path

from core.config import AcquisitionConfig, AppSettings
from core.schemas import CollectorCheckpoint
from discovery.catalyst_live_sources import build_catalyst_live_sources
from discovery.catalyst_live_sources import InvalidCatalystFeedError
from discovery.flow_live_sources import build_flow_live_sources
from discovery.live_sources import build_launch_live_sources
from discovery.service import CatalystAlphaSyncService
from discovery.service import FlowAlphaSyncService
from discovery.service import LaunchAlphaSyncService
from core.pipeline import PipelineWorker
from infra.alerts import AlertManager
from infra.logging import configure_logging, get_logger
from infra.metrics import Metrics, start_metrics_server
from infra.postgres import get_engine, init_storage, ping_postgres
from infra.redis_stream import get_redis_client, ping_redis, replay_dead_letters
from infra.repository import StorageRepository
from notifications.telegram_publisher import TelegramPublisherService
from sentinel.wallet_intelligence_sync import (
    WalletIntelligenceSyncRequest,
    WalletIntelligenceSyncService,
)
from sentinel.onchain_live_sources import (
    EvmTransferTradeSource,
    EvmPoolSwapTradeSource,
    EvmQuoteSource,
    JupiterQuoteSource,
    SolanaWalletTradeSource,
    build_live_sources,
)
from sentinel.onchain_feature_sync import OnchainFeatureSyncService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the SignalEngine pipeline worker")
    parser.add_argument("--group", default="signal-workers")
    parser.add_argument("--consumer", default="worker-1")
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--replay-count", type=int, default=100)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--sleep-seconds", type=float, default=1.0)
    parser.add_argument("--max-loops", type=int, default=0)
    parser.add_argument("--healthcheck", action="store_true")
    parser.add_argument("--replay-dead-letters", action="store_true")
    parser.add_argument("--wallet-intelligence-sync", action="store_true")
    parser.add_argument("--wallet-flow-project", action="store_true")
    parser.add_argument("--wallet-chain")
    parser.add_argument("--wallet-chain-index")
    parser.add_argument("--wallet-token")
    parser.add_argument("--wallet-time-frame")
    parser.add_argument("--wallet-sort-by")
    parser.add_argument("--wallet-type")
    parser.add_argument("--wallet-refresh-limit", type=int)
    parser.add_argument("--wallet-raw-event-count", type=int)
    parser.add_argument("--wallet-raw-last-id")
    parser.add_argument("--onchain-feature-backfill")
    parser.add_argument("--onchain-feature-live", action="store_true")
    parser.add_argument("--launch-alpha-backfill")
    parser.add_argument("--launch-alpha-live", action="store_true")
    parser.add_argument("--catalyst-alpha-backfill")
    parser.add_argument("--catalyst-alpha-live", action="store_true")
    parser.add_argument("--flow-alpha-backfill")
    parser.add_argument("--flow-alpha-live", action="store_true")
    parser.add_argument("--telegram-publisher-live", action="store_true")
    parser.add_argument("--no-db", action="store_true")
    return parser


def run_healthcheck(settings: AppSettings, *, include_db: bool) -> int:
    redis_ok = ping_redis(settings)
    db_ok = True if not include_db else ping_postgres(settings)
    return 0 if redis_ok and db_ok else 1


def run_dead_letter_replay(
    settings: AppSettings,
    *,
    count: int,
) -> int:
    redis_client = get_redis_client(settings)
    replay_dead_letters(redis_client, settings, count=count)
    return 0


def run_wallet_intelligence_sync(
    settings: AppSettings,
    *,
    chain: str | None = None,
    chain_index: str | None = None,
    token: str | None = None,
    time_frame: str | None = None,
    sort_by: str | None = None,
    wallet_type: str | None = None,
    refresh_limit: int | None = None,
    raw_event_count: int | None = None,
    raw_event_last_id: str | None = None,
) -> int:
    sync_config = settings.live.wallet_intelligence
    redis_client = get_redis_client(settings)
    engine = get_engine(settings)
    init_storage(engine)
    repository = StorageRepository(engine)
    service = WalletIntelligenceSyncService(settings, redis_client, repository)
    service.run(
        WalletIntelligenceSyncRequest(
            chain=chain or sync_config.chain,
            chain_index=chain_index or sync_config.chain_index,
            token=token or sync_config.token,
            time_frame=time_frame or sync_config.time_frame,
            sort_by=sort_by or sync_config.sort_by,
            wallet_type=wallet_type or sync_config.wallet_type,
            registry_version=sync_config.registry_version,
            refresh_limit=refresh_limit if refresh_limit is not None else sync_config.refresh_limit,
            raw_event_count=(
                raw_event_count
                if raw_event_count is not None
                else sync_config.raw_event_batch_size
            ),
            raw_event_last_id=raw_event_last_id or "0-0",
            sync_key=f"wallet_intelligence:{(chain or sync_config.chain)}:{(token or sync_config.token)}",
        )
    )
    return 0


def run_wallet_flow_projection(
    settings: AppSettings,
    *,
    chain: str | None = None,
    chain_index: str | None = None,
    token: str | None = None,
    time_frame: str | None = None,
    sort_by: str | None = None,
    wallet_type: str | None = None,
    raw_event_count: int | None = None,
    raw_event_last_id: str | None = None,
) -> int:
    sync_config = settings.live.wallet_intelligence
    redis_client = get_redis_client(settings)
    engine = get_engine(settings)
    init_storage(engine)
    repository = StorageRepository(engine)
    service = WalletIntelligenceSyncService(settings, redis_client, repository)
    service.project_existing_registry(
        WalletIntelligenceSyncRequest(
            chain=chain or sync_config.chain,
            chain_index=chain_index or sync_config.chain_index,
            token=token or sync_config.token,
            time_frame=time_frame or sync_config.time_frame,
            sort_by=sort_by or sync_config.sort_by,
            wallet_type=wallet_type or sync_config.wallet_type,
            registry_version=sync_config.registry_version,
            refresh_limit=0,
            raw_event_count=(
                raw_event_count
                if raw_event_count is not None
                else sync_config.raw_event_batch_size
            ),
            raw_event_last_id=raw_event_last_id or "0-0",
            sync_key=f"wallet_intelligence:{(chain or sync_config.chain)}:{(token or sync_config.token)}",
        )
    )
    return 0


def run_onchain_feature_backfill(
    settings: AppSettings,
    *,
    input_path: str | Path,
) -> int:
    redis_client = get_redis_client(settings)
    engine = get_engine(settings)
    init_storage(engine)
    repository = StorageRepository(engine)
    metrics = Metrics(settings.observability.service_namespace)
    service = OnchainFeatureSyncService(settings, redis_client, repository, metrics=metrics)
    service.ingest_jsonl(input_path)
    return 0


def run_onchain_feature_live_sync(settings: AppSettings) -> int:
    logger = get_logger("signalengine.onchain_feature_live")
    redis_client = get_redis_client(settings)
    engine = get_engine(settings)
    init_storage(engine)
    repository = StorageRepository(engine)
    metrics = Metrics(settings.observability.service_namespace)
    alert_manager = AlertManager(metrics, logger)
    service = OnchainFeatureSyncService(settings, redis_client, repository, metrics=metrics)
    acquisition_config = AcquisitionConfig.model_validate(settings.acquisition)
    now = datetime.now(UTC)

    for source in build_live_sources(settings):
        source_name = source.config.source_name
        chain = getattr(source.config, "chain", None)
        token = getattr(source.config, "token", None)
        state = _load_live_source_state(repository, source_name)
        if state.next_eligible_at is not None and now < state.next_eligible_at:
            metrics.live_source_polls.labels(source=source_name, outcome="cooldown").inc()
            metrics.live_source_consecutive_failures.labels(source=source_name).set(
                float(state.consecutive_failures)
            )
            metrics.live_source_next_eligible.labels(source=source_name).set(
                state.next_eligible_at.timestamp()
            )
            continue
        try:
            if isinstance(source, (SolanaWalletTradeSource, EvmTransferTradeSource, EvmPoolSwapTradeSource)):
                checkpoint = repository.checkpoints.load(source.config.checkpoint_key)
                last_cursor = checkpoint.cursor if checkpoint is not None else None
                records = source.fetch_trades(last_cursor=last_cursor)
                metrics.live_source_records.labels(
                    source=source_name,
                    record_type="onchain_trade",
                ).inc(len(records))
                for record in records:
                    service.ingest_trade(record.payload, source_name=source_name)
                    repository.checkpoints.save(
                        CollectorCheckpoint(
                            checkpoint_key=source.config.checkpoint_key,
                            cursor=record.cursor,
                            observed_at=record.observed_at,
                            metadata={
                                "source_name": source_name,
                                "provider": source.config.provider,
                            },
                        )
                    )
            elif isinstance(source, (JupiterQuoteSource, EvmQuoteSource)):
                payloads = source.fetch_quotes()
                metrics.live_source_records.labels(
                    source=source_name,
                    record_type="dex_quote",
                ).inc(len(payloads))
                for payload in payloads:
                    service.ingest_quote(payload, source_name=source_name)
            metrics.live_source_polls.labels(source=source_name, outcome="success").inc()
            metrics.live_source_last_success.labels(source=source_name).set(datetime.now(UTC).timestamp())
            metrics.live_source_consecutive_failures.labels(source=source_name).set(0.0)
            metrics.live_source_next_eligible.labels(source=source_name).set(0.0)
            _save_live_source_state(repository, source_name, consecutive_failures=0, next_eligible_at=None)
        except Exception:
            next_failures = state.consecutive_failures + 1
            next_eligible_at = datetime.now(UTC) + _source_retry_delay(
                settings.model_copy(update={"acquisition": acquisition_config}),
                consecutive_failures=next_failures,
            )
            _save_live_source_state(
                repository,
                source_name,
                consecutive_failures=next_failures,
                next_eligible_at=next_eligible_at,
            )
            metrics.live_source_polls.labels(source=source_name, outcome="error").inc()
            metrics.live_source_last_error.labels(source=source_name).set(datetime.now(UTC).timestamp())
            metrics.live_source_consecutive_failures.labels(source=source_name).set(
                float(next_failures)
            )
            metrics.live_source_next_eligible.labels(source=source_name).set(
                next_eligible_at.timestamp()
            )
            if next_failures == settings.observability.max_consecutive_live_source_failures:
                alert_manager.emit(
                    "live_source_failure_threshold_exceeded",
                    token=token,
                    chain=chain,
                    details={"outcome": source_name},
                )
            logger.exception(
                "onchain_feature_live_source_failed",
                extra={
                    "service": "onchain_feature_live",
                    "outcome": source_name,
                },
            )

    return 0


def run_launch_alpha_backfill(
    settings: AppSettings,
    *,
    input_path: str | Path,
) -> int:
    redis_client = get_redis_client(settings)
    engine = get_engine(settings)
    init_storage(engine)
    repository = StorageRepository(engine)
    service = LaunchAlphaSyncService(settings, redis_client, repository)
    service.ingest_jsonl(input_path)
    return 0


def run_catalyst_alpha_backfill(
    settings: AppSettings,
    *,
    input_path: str | Path,
) -> int:
    redis_client = get_redis_client(settings)
    engine = get_engine(settings)
    init_storage(engine)
    repository = StorageRepository(engine)
    service = CatalystAlphaSyncService(settings, redis_client, repository)
    service.ingest_jsonl(input_path)
    return 0


def run_flow_alpha_backfill(
    settings: AppSettings,
    *,
    input_path: str | Path,
) -> int:
    redis_client = get_redis_client(settings)
    engine = get_engine(settings)
    init_storage(engine)
    repository = StorageRepository(engine)
    service = FlowAlphaSyncService(settings, redis_client, repository)
    service.ingest_jsonl(input_path)
    return 0


def run_catalyst_alpha_live_sync(settings: AppSettings) -> int:
    logger = get_logger("signalengine.catalyst_alpha_live")
    redis_client = get_redis_client(settings)
    engine = get_engine(settings)
    init_storage(engine)
    repository = StorageRepository(engine)
    service = CatalystAlphaSyncService(settings, redis_client, repository)
    acquisition_config = AcquisitionConfig.model_validate(settings.acquisition)
    now = datetime.now(UTC)
    for source in build_catalyst_live_sources(settings):
        source_name = source.config.source_name or "catalyst_alpha_live"
        state = _load_live_source_state(repository, source_name)
        if state.next_eligible_at is not None and now < state.next_eligible_at:
            continue
        checkpoint_key = f"acquisition:catalyst_alpha_seen:{source.config.source_name}"
        seen_ids = _load_seen_source_event_ids(repository, checkpoint_key)
        try:
            snapshots = source.fetch_snapshots()
        except InvalidCatalystFeedError as error:
            next_failures = state.consecutive_failures + 1
            next_eligible_at = datetime.now(UTC) + _source_retry_delay(
                settings.model_copy(update={"acquisition": acquisition_config}),
                consecutive_failures=next_failures,
            )
            _save_live_source_state(
                repository,
                source_name,
                consecutive_failures=next_failures,
                next_eligible_at=next_eligible_at,
            )
            logger.warning(
                "catalyst_alpha_live_source_invalid_feed",
                extra={
                    "service": "catalyst_alpha_live",
                    "outcome": str(error),
                },
            )
            continue
        except Exception:
            next_failures = state.consecutive_failures + 1
            next_eligible_at = datetime.now(UTC) + _source_retry_delay(
                settings.model_copy(update={"acquisition": acquisition_config}),
                consecutive_failures=next_failures,
            )
            _save_live_source_state(
                repository,
                source_name,
                consecutive_failures=next_failures,
                next_eligible_at=next_eligible_at,
            )
            logger.exception(
                "catalyst_alpha_live_source_failed",
                extra={
                    "service": "catalyst_alpha_live",
                    "outcome": source_name,
                },
            )
            continue
        for snapshot in snapshots:
            if snapshot.source_event_id in seen_ids:
                continue
            service.ingest_snapshot(
                snapshot.model_dump(mode="json"),
                source_name=source.config.source_name or "catalyst_alpha_live",
            )
            seen_ids.append(snapshot.source_event_id)
        _save_seen_source_event_ids(repository, checkpoint_key, seen_ids)
        _save_live_source_state(repository, source_name, consecutive_failures=0, next_eligible_at=None)
    return 0


def run_flow_alpha_live_sync(settings: AppSettings) -> int:
    logger = get_logger("signalengine.flow_alpha_live")
    redis_client = get_redis_client(settings)
    engine = get_engine(settings)
    init_storage(engine)
    repository = StorageRepository(engine)
    service = FlowAlphaSyncService(settings, redis_client, repository)
    acquisition_config = AcquisitionConfig.model_validate(settings.acquisition)
    now = datetime.now(UTC)
    for source in build_flow_live_sources(settings, repository):
        source_name = source.config.source_name or "flow_alpha_live"
        state = _load_live_source_state(repository, source_name)
        if state.next_eligible_at is not None and now < state.next_eligible_at:
            continue
        checkpoint_key = f"acquisition:flow_alpha_seen:{source_name}"
        seen_ids = _load_seen_source_event_ids(repository, checkpoint_key)
        try:
            snapshots = source.fetch_snapshots()
        except Exception:
            next_failures = state.consecutive_failures + 1
            next_eligible_at = datetime.now(UTC) + _source_retry_delay(
                settings.model_copy(update={"acquisition": acquisition_config}),
                consecutive_failures=next_failures,
            )
            _save_live_source_state(
                repository,
                source_name,
                consecutive_failures=next_failures,
                next_eligible_at=next_eligible_at,
            )
            logger.exception(
                "flow_alpha_live_source_failed",
                extra={
                    "service": "flow_alpha_live",
                    "outcome": source_name,
                },
            )
            continue
        for snapshot in snapshots:
            if snapshot.source_event_id in seen_ids:
                continue
            service.ingest_snapshot(
                snapshot.model_dump(mode="json"),
                source_name=source_name,
                publish_event=not getattr(source.config, "observe_only", False),
            )
            seen_ids.append(snapshot.source_event_id)
        _save_seen_source_event_ids(repository, checkpoint_key, seen_ids)
        _save_live_source_state(repository, source_name, consecutive_failures=0, next_eligible_at=None)
    return 0


def run_launch_alpha_live_sync(settings: AppSettings) -> int:
    redis_client = get_redis_client(settings)
    engine = get_engine(settings)
    init_storage(engine)
    repository = StorageRepository(engine)
    service = LaunchAlphaSyncService(settings, redis_client, repository)
    for source in build_launch_live_sources(settings, repository):
        for snapshot in source.fetch_snapshots():
            service.ingest_snapshot(
                snapshot.model_dump(mode="json"),
                source_name=source.config.source_name or "launch_alpha_live",
            )
    return 0


def run_telegram_publisher_live(settings: AppSettings) -> int:
    redis_client = get_redis_client(settings)
    engine = get_engine(settings)
    init_storage(engine)
    repository = StorageRepository(engine)
    service = TelegramPublisherService(settings, redis_client, repository)
    service.ensure_stream()
    service.process_once(count=100, block_ms=1000)
    return 0


class _LiveSourceState:
    def __init__(self, consecutive_failures: int = 0, next_eligible_at: datetime | None = None) -> None:
        self.consecutive_failures = consecutive_failures
        self.next_eligible_at = next_eligible_at


def _load_live_source_state(
    repository: StorageRepository,
    source_name: str,
) -> _LiveSourceState:
    checkpoint = repository.checkpoints.load(f"acquisition_state:{source_name}")
    if checkpoint is None:
        return _LiveSourceState()
    metadata = checkpoint.metadata
    consecutive_failures = int(metadata.get("consecutive_failures", 0))
    next_eligible_raw = metadata.get("next_eligible_at")
    next_eligible_at = None
    if isinstance(next_eligible_raw, str) and next_eligible_raw:
        next_eligible_at = datetime.fromisoformat(next_eligible_raw.replace("Z", "+00:00")).astimezone(UTC)
    return _LiveSourceState(
        consecutive_failures=consecutive_failures,
        next_eligible_at=next_eligible_at,
    )


def _save_live_source_state(
    repository: StorageRepository,
    source_name: str,
    *,
    consecutive_failures: int,
    next_eligible_at: datetime | None,
) -> None:
    repository.checkpoints.save(
        CollectorCheckpoint(
            checkpoint_key=f"acquisition_state:{source_name}",
            cursor="state",
            observed_at=datetime.now(UTC),
            metadata={
                "consecutive_failures": consecutive_failures,
                "next_eligible_at": (
                    next_eligible_at.astimezone(UTC).isoformat()
                    if next_eligible_at is not None
                    else None
                ),
            },
        )
    )


def _load_seen_source_event_ids(
    repository: StorageRepository,
    checkpoint_key: str,
) -> list[str]:
    checkpoint = repository.checkpoints.load(checkpoint_key)
    if checkpoint is None:
        return []
    seen_ids = checkpoint.metadata.get("seen_ids", [])
    if not isinstance(seen_ids, list):
        return []
    return [str(item) for item in seen_ids if str(item)]


def _save_seen_source_event_ids(
    repository: StorageRepository,
    checkpoint_key: str,
    seen_ids: list[str],
) -> None:
    repository.checkpoints.save(
        CollectorCheckpoint(
            checkpoint_key=checkpoint_key,
            cursor=seen_ids[-1] if seen_ids else "0-0",
            observed_at=datetime.now(UTC),
            metadata={"seen_ids": seen_ids[-200:]},
        )
    )


def _source_retry_delay(settings: AppSettings, *, consecutive_failures: int):
    from datetime import timedelta

    acquisition = AcquisitionConfig.model_validate(settings.acquisition)
    seconds = (
        acquisition.source_cooldown_seconds
        if consecutive_failures >= settings.observability.max_consecutive_live_source_failures
        else acquisition.failure_backoff_seconds
    )
    return timedelta(seconds=seconds)


def main() -> int:
    args = build_parser().parse_args()
    settings = AppSettings.load()
    configure_logging(settings.runtime.log_level)
    logger = get_logger("signalengine.worker")

    if args.healthcheck:
        return run_healthcheck(settings, include_db=not args.no_db)

    if args.replay_dead_letters:
        return run_dead_letter_replay(settings, count=args.replay_count)

    if args.wallet_intelligence_sync:
        sync_config = settings.live.wallet_intelligence
        if args.once:
            return run_wallet_intelligence_sync(
                settings,
                chain=args.wallet_chain,
                chain_index=args.wallet_chain_index,
                token=args.wallet_token,
                time_frame=args.wallet_time_frame,
                sort_by=args.wallet_sort_by,
                wallet_type=args.wallet_type,
                refresh_limit=args.wallet_refresh_limit,
                raw_event_count=args.wallet_raw_event_count,
                raw_event_last_id=args.wallet_raw_last_id,
            )

        stop_event = Event()

        def _handle_sync_stop_signal(signum: int, frame: object) -> None:
            _ = frame
            logger.info(
                "wallet_intelligence_sync_stop_requested",
                extra={
                    "service": "wallet_intelligence_sync",
                    "outcome": f"signal_{signum}",
                },
            )
            stop_event.set()

        signal.signal(signal.SIGINT, _handle_sync_stop_signal)
        signal.signal(signal.SIGTERM, _handle_sync_stop_signal)

        while not stop_event.is_set():
            run_wallet_intelligence_sync(
                settings,
                chain=args.wallet_chain,
                chain_index=args.wallet_chain_index,
                token=args.wallet_token,
                time_frame=args.wallet_time_frame,
                sort_by=args.wallet_sort_by,
                wallet_type=args.wallet_type,
                refresh_limit=args.wallet_refresh_limit,
                raw_event_count=args.wallet_raw_event_count,
                raw_event_last_id=args.wallet_raw_last_id,
            )
            sleep(sync_config.sync_interval_seconds)
        return 0

    if args.wallet_flow_project:
        sync_config = settings.live.wallet_intelligence
        if args.once:
            return run_wallet_flow_projection(
                settings,
                chain=args.wallet_chain,
                chain_index=args.wallet_chain_index,
                token=args.wallet_token,
                time_frame=args.wallet_time_frame,
                sort_by=args.wallet_sort_by,
                wallet_type=args.wallet_type,
                raw_event_count=args.wallet_raw_event_count,
                raw_event_last_id=args.wallet_raw_last_id,
            )

        stop_event = Event()

        def _handle_project_stop_signal(signum: int, frame: object) -> None:
            _ = frame
            logger.info(
                "wallet_flow_projection_stop_requested",
                extra={
                    "service": "wallet_flow_projection",
                    "outcome": f"signal_{signum}",
                },
            )
            stop_event.set()

        signal.signal(signal.SIGINT, _handle_project_stop_signal)
        signal.signal(signal.SIGTERM, _handle_project_stop_signal)

        while not stop_event.is_set():
            run_wallet_flow_projection(
                settings,
                chain=args.wallet_chain,
                chain_index=args.wallet_chain_index,
                token=args.wallet_token,
                time_frame=args.wallet_time_frame,
                sort_by=args.wallet_sort_by,
                wallet_type=args.wallet_type,
                raw_event_count=args.wallet_raw_event_count,
                raw_event_last_id=args.wallet_raw_last_id,
            )
            sleep(sync_config.sync_interval_seconds)
        return 0

    if args.onchain_feature_backfill:
        if args.once:
            return run_onchain_feature_backfill(settings, input_path=args.onchain_feature_backfill)

        stop_event = Event()

        def _handle_onchain_backfill_stop_signal(signum: int, frame: object) -> None:
            _ = frame
            logger.info(
                "onchain_feature_backfill_stop_requested",
                extra={
                    "service": "onchain_feature_backfill",
                    "outcome": f"signal_{signum}",
                },
            )
            stop_event.set()

        signal.signal(signal.SIGINT, _handle_onchain_backfill_stop_signal)
        signal.signal(signal.SIGTERM, _handle_onchain_backfill_stop_signal)

        while not stop_event.is_set():
            run_onchain_feature_backfill(settings, input_path=args.onchain_feature_backfill)
            sleep(args.sleep_seconds)
        return 0

    if args.onchain_feature_live:
        acquisition_config = settings.acquisition
        if args.once:
            return run_onchain_feature_live_sync(settings)

        stop_event = Event()

        def _handle_onchain_live_stop_signal(signum: int, frame: object) -> None:
            _ = frame
            logger.info(
                "onchain_feature_live_stop_requested",
                extra={
                    "service": "onchain_feature_live",
                    "outcome": f"signal_{signum}",
                },
            )
            stop_event.set()

        signal.signal(signal.SIGINT, _handle_onchain_live_stop_signal)
        signal.signal(signal.SIGTERM, _handle_onchain_live_stop_signal)

        while not stop_event.is_set():
            run_onchain_feature_live_sync(settings)
            sleep(acquisition_config.sync_interval_seconds)
        return 0

    if args.launch_alpha_backfill:
        if args.once:
            return run_launch_alpha_backfill(settings, input_path=args.launch_alpha_backfill)

        stop_event = Event()

        def _handle_launch_alpha_stop_signal(signum: int, frame: object) -> None:
            _ = frame
            logger.info(
                "launch_alpha_backfill_stop_requested",
                extra={
                    "service": "launch_alpha_backfill",
                    "outcome": f"signal_{signum}",
                },
            )
            stop_event.set()

        signal.signal(signal.SIGINT, _handle_launch_alpha_stop_signal)
        signal.signal(signal.SIGTERM, _handle_launch_alpha_stop_signal)

        while not stop_event.is_set():
            run_launch_alpha_backfill(settings, input_path=args.launch_alpha_backfill)
            sleep(args.sleep_seconds)
        return 0

    if args.catalyst_alpha_backfill:
        if args.once:
            return run_catalyst_alpha_backfill(settings, input_path=args.catalyst_alpha_backfill)

        stop_event = Event()

        def _handle_catalyst_alpha_stop_signal(signum: int, frame: object) -> None:
            _ = frame
            logger.info(
                "catalyst_alpha_backfill_stop_requested",
                extra={
                    "service": "catalyst_alpha_backfill",
                    "outcome": f"signal_{signum}",
                },
            )
            stop_event.set()

        signal.signal(signal.SIGINT, _handle_catalyst_alpha_stop_signal)
        signal.signal(signal.SIGTERM, _handle_catalyst_alpha_stop_signal)

        while not stop_event.is_set():
            run_catalyst_alpha_backfill(settings, input_path=args.catalyst_alpha_backfill)
            sleep(args.sleep_seconds)
        return 0

    if args.flow_alpha_backfill:
        if args.once:
            return run_flow_alpha_backfill(settings, input_path=args.flow_alpha_backfill)

        stop_event = Event()

        def _handle_flow_alpha_stop_signal(signum: int, frame: object) -> None:
            _ = frame
            logger.info(
                "flow_alpha_backfill_stop_requested",
                extra={
                    "service": "flow_alpha_backfill",
                    "outcome": f"signal_{signum}",
                },
            )
            stop_event.set()

        signal.signal(signal.SIGINT, _handle_flow_alpha_stop_signal)
        signal.signal(signal.SIGTERM, _handle_flow_alpha_stop_signal)

        while not stop_event.is_set():
            run_flow_alpha_backfill(settings, input_path=args.flow_alpha_backfill)
            sleep(args.sleep_seconds)
        return 0

    if args.catalyst_alpha_live:
        acquisition_config = settings.acquisition
        if args.once:
            return run_catalyst_alpha_live_sync(settings)

        stop_event = Event()

        def _handle_catalyst_alpha_live_stop_signal(signum: int, frame: object) -> None:
            _ = frame
            logger.info(
                "catalyst_alpha_live_stop_requested",
                extra={
                    "service": "catalyst_alpha_live",
                    "outcome": f"signal_{signum}",
                },
            )
            stop_event.set()

        signal.signal(signal.SIGINT, _handle_catalyst_alpha_live_stop_signal)
        signal.signal(signal.SIGTERM, _handle_catalyst_alpha_live_stop_signal)

        while not stop_event.is_set():
            run_catalyst_alpha_live_sync(settings)
            sleep(acquisition_config.sync_interval_seconds)
        return 0

    if args.flow_alpha_live:
        acquisition_config = settings.acquisition
        if args.once:
            return run_flow_alpha_live_sync(settings)

        stop_event = Event()

        def _handle_flow_alpha_live_stop_signal(signum: int, frame: object) -> None:
            _ = frame
            logger.info(
                "flow_alpha_live_stop_requested",
                extra={
                    "service": "flow_alpha_live",
                    "outcome": f"signal_{signum}",
                },
            )
            stop_event.set()

        signal.signal(signal.SIGINT, _handle_flow_alpha_live_stop_signal)
        signal.signal(signal.SIGTERM, _handle_flow_alpha_live_stop_signal)

        while not stop_event.is_set():
            run_flow_alpha_live_sync(settings)
            sleep(acquisition_config.sync_interval_seconds)
        return 0

    if args.launch_alpha_live:
        acquisition_config = settings.acquisition
        if args.once:
            return run_launch_alpha_live_sync(settings)

        stop_event = Event()

        def _handle_launch_alpha_live_stop_signal(signum: int, frame: object) -> None:
            _ = frame
            logger.info(
                "launch_alpha_live_stop_requested",
                extra={
                    "service": "launch_alpha_live",
                    "outcome": f"signal_{signum}",
                },
            )
            stop_event.set()

        signal.signal(signal.SIGINT, _handle_launch_alpha_live_stop_signal)
        signal.signal(signal.SIGTERM, _handle_launch_alpha_live_stop_signal)

        while not stop_event.is_set():
            run_launch_alpha_live_sync(settings)
            sleep(acquisition_config.sync_interval_seconds)
        return 0

    if args.telegram_publisher_live:
        if args.once:
            return run_telegram_publisher_live(settings)

        stop_event = Event()

        def _handle_telegram_publisher_stop_signal(signum: int, frame: object) -> None:
            _ = frame
            logger.info(
                "telegram_publisher_stop_requested",
                extra={
                    "service": "telegram_publisher",
                    "outcome": f"signal_{signum}",
                },
            )
            stop_event.set()

        signal.signal(signal.SIGINT, _handle_telegram_publisher_stop_signal)
        signal.signal(signal.SIGTERM, _handle_telegram_publisher_stop_signal)

        while not stop_event.is_set():
            run_telegram_publisher_live(settings)
            sleep(args.sleep_seconds)
        return 0

    start_metrics_server(
        settings.observability.metrics_host,
        settings.observability.metrics_port,
    )
    redis_client = get_redis_client(settings)
    db_engine = None if args.no_db else get_engine(settings)
    metrics = Metrics(settings.observability.service_namespace)

    worker = PipelineWorker(settings, redis_client, db_engine=db_engine, metrics=metrics)
    worker.ensure_streams(args.group)
    stop_event = Event()

    def _handle_stop_signal(signum: int, frame: object) -> None:
        _ = frame
        logger.info(
            "worker_stop_requested",
            extra={
                "service": "worker",
                "outcome": f"signal_{signum}",
            },
        )
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_stop_signal)
    signal.signal(signal.SIGTERM, _handle_stop_signal)

    if args.once:
        worker.poll_once(args.group, args.consumer, count=args.count)
        return 0

    loop_count = 0
    while not stop_event.is_set():
        worker.poll_once(args.group, args.consumer, count=args.count)
        loop_count += 1
        if args.max_loops > 0 and loop_count >= args.max_loops:
            break
        sleep(args.sleep_seconds)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())