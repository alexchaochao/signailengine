from __future__ import annotations

from datetime import UTC, datetime, timedelta
from urllib.error import URLError

from core.config import AppSettings
from core.worker import (
    build_parser,
    run_dead_letter_replay,
    run_catalyst_alpha_live_sync,
    run_flow_alpha_backfill,
    run_flow_alpha_live_sync,
    run_healthcheck,
    run_catalyst_alpha_backfill,
    run_launch_alpha_backfill,
    run_launch_alpha_live_sync,
    run_onchain_feature_backfill,
    run_onchain_feature_live_sync,
    run_telegram_publisher_live,
    run_wallet_flow_projection,
    run_wallet_intelligence_sync,
)


def test_worker_parser_supports_once_and_no_db_flags() -> None:
    parser = build_parser()
    args = parser.parse_args([
        "--once",
        "--no-db",
        "--group",
        "test-group",
        "--max-loops",
        "2",
        "--replay-count",
        "25",
        "--wallet-intelligence-sync",
        "--wallet-flow-project",
        "--wallet-token",
        "WIF",
        "--onchain-feature-backfill",
        "inputs.jsonl",
        "--onchain-feature-live",
        "--launch-alpha-backfill",
        "launch.jsonl",
        "--launch-alpha-live",
        "--catalyst-alpha-backfill",
        "catalyst.jsonl",
        "--catalyst-alpha-live",
        "--flow-alpha-backfill",
        "flow.jsonl",
        "--flow-alpha-live",
        "--telegram-publisher-live",
    ])

    assert args.once is True
    assert args.no_db is True
    assert args.group == "test-group"
    assert args.max_loops == 2
    assert args.replay_count == 25
    assert args.wallet_intelligence_sync is True
    assert args.wallet_flow_project is True
    assert args.wallet_token == "WIF"
    assert args.onchain_feature_backfill == "inputs.jsonl"
    assert args.onchain_feature_live is True
    assert args.launch_alpha_backfill == "launch.jsonl"
    assert args.launch_alpha_live is True
    assert args.catalyst_alpha_backfill == "catalyst.jsonl"
    assert args.catalyst_alpha_live is True
    assert args.flow_alpha_backfill == "flow.jsonl"
    assert args.flow_alpha_live is True
    assert args.telegram_publisher_live is True


def test_telegram_publisher_live_returns_zero(monkeypatch) -> None:
    calls: list[object] = []

    class StubService:
        def __init__(self, settings, redis_client, repository) -> None:
            calls.append((settings, redis_client, repository))

        def ensure_stream(self) -> None:
            calls.append("ensure_stream")

        def process_once(self, count=100, block_ms=1000) -> int:
            calls.append((count, block_ms))
            return 1

    monkeypatch.setattr("core.worker.get_redis_client", lambda settings: object())
    monkeypatch.setattr("core.worker.get_engine", lambda settings: object())
    monkeypatch.setattr("core.worker.init_storage", lambda engine: None)
    monkeypatch.setattr("core.worker.StorageRepository", lambda engine: object())
    monkeypatch.setattr("core.worker.TelegramPublisherService", StubService)

    assert run_telegram_publisher_live(AppSettings.load()) == 0
    assert "ensure_stream" in calls
    assert (100, 1000) in calls


def test_healthcheck_returns_zero_when_db_is_skipped(monkeypatch) -> None:
    monkeypatch.setattr("core.worker.ping_redis", lambda settings: True)

    assert run_healthcheck(AppSettings.load(), include_db=False) == 0


def test_dead_letter_replay_returns_zero(monkeypatch) -> None:
    replay_calls: list[int] = []

    monkeypatch.setattr("core.worker.get_redis_client", lambda settings: object())
    monkeypatch.setattr(
        "core.worker.replay_dead_letters",
        lambda client, settings, count: replay_calls.append(count),
    )

    assert run_dead_letter_replay(AppSettings.load(), count=12) == 0
    assert replay_calls == [12]


def test_wallet_intelligence_sync_returns_zero(monkeypatch) -> None:
    calls: list[object] = []

    monkeypatch.setattr("core.worker.get_redis_client", lambda settings: object())
    monkeypatch.setattr("core.worker.get_engine", lambda settings: object())
    monkeypatch.setattr("core.worker.init_storage", lambda engine: None)
    monkeypatch.setattr("core.worker.StorageRepository", lambda engine: object())

    settings = AppSettings.load().model_copy(
        update={
            "live": AppSettings.load().live.model_copy(
                update={
                    "wallet_intelligence": AppSettings.load().live.wallet_intelligence.model_copy(
                        update={
                            "chain": "solana",
                            "chain_index": "501",
                            "token": "BONK",
                            "time_frame": "3",
                            "sort_by": "1",
                            "wallet_type": "3",
                            "refresh_limit": 10,
                            "raw_event_batch_size": 100,
                        }
                    )
                }
            )
        }
    )

    class StubService:
        def __init__(self, settings, redis_client, repository) -> None:
            calls.append((settings, redis_client, repository))

        def run(self, request) -> None:
            calls.append(request)

    monkeypatch.setattr("core.worker.WalletIntelligenceSyncService", StubService)

    assert (
        run_wallet_intelligence_sync(
            settings,
            chain="solana",
            chain_index="501",
            token="BONK",
            time_frame="3",
            sort_by="1",
            wallet_type="3",
            refresh_limit=10,
            raw_event_count=100,
            raw_event_last_id="0-0",
        )
        == 0
    )
    assert len(calls) == 2


def test_wallet_flow_projection_returns_zero(monkeypatch) -> None:
    calls: list[object] = []

    monkeypatch.setattr("core.worker.get_redis_client", lambda settings: object())
    monkeypatch.setattr("core.worker.get_engine", lambda settings: object())
    monkeypatch.setattr("core.worker.init_storage", lambda engine: None)
    monkeypatch.setattr("core.worker.StorageRepository", lambda engine: object())

    settings = AppSettings.load().model_copy(
        update={
            "live": AppSettings.load().live.model_copy(
                update={
                    "wallet_intelligence": AppSettings.load().live.wallet_intelligence.model_copy(
                        update={
                            "chain": "base",
                            "chain_index": "8453",
                            "token": "AERO",
                            "time_frame": "3",
                            "sort_by": "1",
                            "wallet_type": "3",
                            "raw_event_batch_size": 100,
                        }
                    )
                }
            )
        }
    )

    class StubService:
        def __init__(self, settings, redis_client, repository) -> None:
            calls.append((settings, redis_client, repository))

        def project_existing_registry(self, request) -> None:
            calls.append(request)

    monkeypatch.setattr("core.worker.WalletIntelligenceSyncService", StubService)

    assert (
        run_wallet_flow_projection(
            settings,
            chain="base",
            chain_index="8453",
            token="AERO",
            time_frame="3",
            sort_by="1",
            wallet_type="3",
            raw_event_count=100,
            raw_event_last_id="0-0",
        )
        == 0
    )
    assert len(calls) == 2


def test_onchain_feature_backfill_returns_zero(monkeypatch, tmp_path) -> None:
    calls: list[object] = []
    input_path = tmp_path / "backfill.jsonl"
    input_path.write_text("{}\n", encoding="utf-8")

    monkeypatch.setattr("core.worker.get_redis_client", lambda settings: object())
    monkeypatch.setattr("core.worker.get_engine", lambda settings: object())
    monkeypatch.setattr("core.worker.init_storage", lambda engine: None)
    monkeypatch.setattr("core.worker.StorageRepository", lambda engine: object())

    class StubService:
        def __init__(self, settings, redis_client, repository, metrics) -> None:
            calls.append((settings, redis_client, repository, metrics))

        def ingest_jsonl(self, path) -> None:
            calls.append(path)

    monkeypatch.setattr("core.worker.OnchainFeatureSyncService", StubService)

    assert run_onchain_feature_backfill(AppSettings.load(), input_path=input_path) == 0
    assert calls[-1] == input_path


def test_sample_onchain_feature_backfill_dataset_exists() -> None:
    dataset_path = "/home/alex/Desktop/signalengine/replay/datasets/onchain_feature_backfill.jsonl"

    with open(dataset_path, "r", encoding="utf-8") as handle:
        lines = [line.strip() for line in handle if line.strip()]

    assert len(lines) == 2
    assert '"source_type":"onchain_trade"' in lines[0]
    assert '"source_type":"dex_quote"' in lines[1]


def test_launch_alpha_backfill_returns_zero(monkeypatch, tmp_path) -> None:
    calls: list[object] = []
    input_path = tmp_path / "launch_alpha.jsonl"
    input_path.write_text("{}\n", encoding="utf-8")

    monkeypatch.setattr("core.worker.get_redis_client", lambda settings: object())
    monkeypatch.setattr("core.worker.get_engine", lambda settings: object())
    monkeypatch.setattr("core.worker.init_storage", lambda engine: None)
    monkeypatch.setattr("core.worker.StorageRepository", lambda engine: object())

    class StubService:
        def __init__(self, settings, redis_client, repository) -> None:
            calls.append((settings, redis_client, repository))

        def ingest_jsonl(self, path) -> None:
            calls.append(path)

    monkeypatch.setattr("core.worker.LaunchAlphaSyncService", StubService)

    assert run_launch_alpha_backfill(AppSettings.load(), input_path=input_path) == 0
    assert calls[-1] == input_path


def test_catalyst_alpha_backfill_returns_zero(monkeypatch, tmp_path) -> None:
    calls: list[object] = []
    input_path = tmp_path / "catalyst_alpha.jsonl"
    input_path.write_text("{}\n", encoding="utf-8")

    monkeypatch.setattr("core.worker.get_redis_client", lambda settings: object())
    monkeypatch.setattr("core.worker.get_engine", lambda settings: object())
    monkeypatch.setattr("core.worker.init_storage", lambda engine: None)
    monkeypatch.setattr("core.worker.StorageRepository", lambda engine: object())

    class StubService:
        def __init__(self, settings, redis_client, repository) -> None:
            calls.append((settings, redis_client, repository))

        def ingest_jsonl(self, path) -> None:
            calls.append(path)

    monkeypatch.setattr("core.worker.CatalystAlphaSyncService", StubService)

    assert run_catalyst_alpha_backfill(AppSettings.load(), input_path=input_path) == 0
    assert calls[-1] == input_path


def test_flow_alpha_backfill_returns_zero(monkeypatch, tmp_path) -> None:
    calls: list[object] = []
    input_path = tmp_path / "flow_alpha.jsonl"
    input_path.write_text("{}\n", encoding="utf-8")

    monkeypatch.setattr("core.worker.get_redis_client", lambda settings: object())
    monkeypatch.setattr("core.worker.get_engine", lambda settings: object())
    monkeypatch.setattr("core.worker.init_storage", lambda engine: None)
    monkeypatch.setattr("core.worker.StorageRepository", lambda engine: object())

    class StubService:
        def __init__(self, settings, redis_client, repository) -> None:
            calls.append((settings, redis_client, repository))

        def ingest_jsonl(self, path) -> None:
            calls.append(path)

    monkeypatch.setattr("core.worker.FlowAlphaSyncService", StubService)

    assert run_flow_alpha_backfill(AppSettings.load(), input_path=input_path) == 0
    assert calls[-1] == input_path


def test_launch_alpha_live_sync_returns_zero(monkeypatch) -> None:
    calls: list[object] = []

    class StubSource:
        config = type("Config", (), {"source_name": "launch_alpha_solana"})()

        def fetch_snapshots(self):
            calls.append("fetch_snapshots")
            return [
                type(
                    "Snapshot",
                    (),
                    {
                        "model_dump": lambda self, mode="json": {
                            "source_event_id": "launch-1",
                            "chain": "solana",
                            "token": "NEWTKN",
                            "pool_address": "pool-1",
                            "dex": "raydium",
                            "quote_asset": "USDC",
                            "observed_at": "2026-05-03T12:00:10Z",
                            "initial_liquidity_usd": 25000.0,
                            "liquidity_lock_ratio": 0.92,
                            "buy_notional_5m_usd": 18000.0,
                            "trade_count_5m": 16,
                            "unique_wallets_5m": 11,
                            "smart_money_wallets_5m": 3,
                            "creator_hold_pct": 0.08,
                        }
                    },
                )()
            ]

    class StubService:
        def __init__(self, settings, redis_client, repository) -> None:
            calls.append((settings, redis_client, repository))

        def ingest_snapshot(self, payload, source_name):
            calls.append((payload, source_name))

    monkeypatch.setattr("core.worker.get_redis_client", lambda settings: object())
    monkeypatch.setattr("core.worker.get_engine", lambda settings: object())
    monkeypatch.setattr("core.worker.init_storage", lambda engine: None)
    monkeypatch.setattr("core.worker.StorageRepository", lambda engine: object())
    monkeypatch.setattr("core.worker.LaunchAlphaSyncService", StubService)
    monkeypatch.setattr(
        "core.worker.build_launch_live_sources",
        lambda settings, repository=None: [StubSource()],
    )

    assert run_launch_alpha_live_sync(AppSettings.load()) == 0
    assert "fetch_snapshots" in calls


def test_catalyst_alpha_live_sync_returns_zero_and_deduplicates(monkeypatch) -> None:
    calls: list[object] = []

    class StubCheckpointStore:
        def __init__(self) -> None:
            self.saved = None

        def load(self, checkpoint_key):
            calls.append(("load_checkpoint", checkpoint_key))
            return None

        def save(self, checkpoint):
            calls.append(("save_checkpoint", checkpoint.checkpoint_key, checkpoint.metadata))

    class StubRepository:
        def __init__(self) -> None:
            self.checkpoints = StubCheckpointStore()

    class StubSource:
        config = type("Config", (), {"source_name": "catalyst_alpha_binance"})()

        def fetch_snapshots(self):
            calls.append("fetch_snapshots")
            return [
                type(
                    "Snapshot",
                    (),
                    {
                        "source_event_id": "rss:catalyst_alpha_binance:base:AERO:entry-1",
                        "model_dump": lambda self, mode="json": {
                            "source_event_id": "rss:catalyst_alpha_binance:base:AERO:entry-1",
                            "chain": "base",
                            "token": "AERO",
                            "catalyst_type": "cex_listing_announcement",
                            "headline": "Binance will list Aerodrome (AERO)",
                            "observed_at": "2026-05-03T12:00:00Z",
                            "impact_score": 0.88,
                            "credibility_score": 0.92,
                            "lead_time_minutes": 0,
                            "venue": "binance",
                            "metadata": {"link": "https://example.com/aero"},
                        },
                    },
                )(),
                type(
                    "Snapshot",
                    (),
                    {
                        "source_event_id": "rss:catalyst_alpha_binance:base:AERO:entry-1",
                        "model_dump": lambda self, mode="json": {
                            "source_event_id": "rss:catalyst_alpha_binance:base:AERO:entry-1",
                            "chain": "base",
                            "token": "AERO",
                            "catalyst_type": "cex_listing_announcement",
                            "headline": "Binance will list Aerodrome (AERO)",
                            "observed_at": "2026-05-03T12:00:00Z",
                            "impact_score": 0.88,
                            "credibility_score": 0.92,
                            "lead_time_minutes": 0,
                            "venue": "binance",
                            "metadata": {"link": "https://example.com/aero"},
                        },
                    },
                )(),
            ]

    class StubService:
        def __init__(self, settings, redis_client, repository) -> None:
            calls.append((settings, redis_client, repository))

        def ingest_snapshot(self, payload, source_name):
            calls.append((payload["source_event_id"], source_name))

    monkeypatch.setattr("core.worker.get_redis_client", lambda settings: object())
    monkeypatch.setattr("core.worker.get_engine", lambda settings: object())
    monkeypatch.setattr("core.worker.init_storage", lambda engine: None)
    monkeypatch.setattr("core.worker.StorageRepository", lambda engine: StubRepository())
    monkeypatch.setattr("core.worker.CatalystAlphaSyncService", StubService)
    monkeypatch.setattr("core.worker.build_catalyst_live_sources", lambda settings: [StubSource()])

    assert run_catalyst_alpha_live_sync(AppSettings.load()) == 0
    assert calls.count("fetch_snapshots") == 1
    assert calls.count(("rss:catalyst_alpha_binance:base:AERO:entry-1", "catalyst_alpha_binance")) == 1


def test_catalyst_alpha_live_sync_returns_zero_on_source_failure(monkeypatch) -> None:
    calls: list[object] = []

    class StubCheckpointStore:
        def load(self, checkpoint_key):
            calls.append(("load_checkpoint", checkpoint_key))
            return None

        def save(self, checkpoint):
            calls.append(("save_checkpoint", checkpoint.checkpoint_key, checkpoint.metadata))

    class StubRepository:
        def __init__(self) -> None:
            self.checkpoints = StubCheckpointStore()

    class StubSource:
        config = type("Config", (), {"source_name": "catalyst_alpha_binance"})()

        def fetch_snapshots(self):
            calls.append("fetch_snapshots")
            raise URLError("connection reset")

    class StubService:
        def __init__(self, settings, redis_client, repository) -> None:
            calls.append((settings, redis_client, repository))

    monkeypatch.setattr("core.worker.get_redis_client", lambda settings: object())
    monkeypatch.setattr("core.worker.get_engine", lambda settings: object())
    monkeypatch.setattr("core.worker.init_storage", lambda engine: None)
    monkeypatch.setattr("core.worker.StorageRepository", lambda engine: StubRepository())
    monkeypatch.setattr("core.worker.CatalystAlphaSyncService", StubService)
    monkeypatch.setattr("core.worker.build_catalyst_live_sources", lambda settings: [StubSource()])

    assert run_catalyst_alpha_live_sync(AppSettings.load()) == 0
    assert calls.count("fetch_snapshots") == 1


def test_catalyst_alpha_live_sync_skips_source_in_cooldown(monkeypatch) -> None:
    calls: list[object] = []

    class StubSource:
        config = type("Config", (), {"source_name": "catalyst_alpha_binance"})()

        def fetch_snapshots(self):
            calls.append("fetch_snapshots")
            return []

    monkeypatch.setattr("core.worker.get_redis_client", lambda settings: object())
    monkeypatch.setattr("core.worker.get_engine", lambda settings: object())
    monkeypatch.setattr("core.worker.init_storage", lambda engine: None)
    monkeypatch.setattr("core.worker.StorageRepository", lambda engine: object())
    monkeypatch.setattr("core.worker.CatalystAlphaSyncService", lambda settings, redis_client, repository: object())
    monkeypatch.setattr("core.worker.build_catalyst_live_sources", lambda settings: [StubSource()])
    monkeypatch.setattr(
        "core.worker._load_live_source_state",
        lambda repository, source_name: type(
            "State",
            (),
            {"consecutive_failures": 1, "next_eligible_at": datetime.now(UTC) + timedelta(minutes=1)},
        )(),
    )

    assert run_catalyst_alpha_live_sync(AppSettings.load()) == 0
    assert calls == []


def test_flow_alpha_live_sync_returns_zero(monkeypatch) -> None:
    calls: list[object] = []

    class StubCheckpointStore:
        def load(self, checkpoint_key):
            calls.append(("load_checkpoint", checkpoint_key))
            return None

        def save(self, checkpoint):
            calls.append(("save_checkpoint", checkpoint.checkpoint_key, checkpoint.metadata))

    class StubRepository:
        def __init__(self) -> None:
            self.checkpoints = StubCheckpointStore()

    class StubSource:
        config = type("Config", (), {"source_name": "flow_alpha_base_aero", "observe_only": True})()

        def fetch_snapshots(self):
            calls.append("fetch_snapshots")
            return [
                type(
                    "Snapshot",
                    (),
                    {
                        "source_event_id": "walletint:base:AERO:flow_alpha_base_aero:1",
                        "model_dump": lambda self, mode="json": {
                            "source_event_id": "walletint:base:AERO:flow_alpha_base_aero:1",
                            "chain": "base",
                            "token": "AERO",
                            "flow_type": "smart_money_rotation",
                            "venue": "aerodrome",
                            "observed_at": "2026-05-03T12:00:00Z",
                            "netflow_15m_usd": 50000.0,
                            "smart_money_inflow_usd": 60000.0,
                            "smart_money_outflow_usd": 10000.0,
                            "unique_buyer_wallets_15m": 5,
                            "unique_seller_wallets_15m": 2,
                            "whale_buy_count_15m": 2,
                            "exchange_outflow_usd": 50000.0,
                            "metadata": {},
                        },
                    },
                )()
            ]

    class StubService:
        def __init__(self, settings, redis_client, repository) -> None:
            calls.append((settings, redis_client, repository))

        def ingest_snapshot(self, payload, source_name, publish_event=True):
            calls.append((payload["source_event_id"], source_name, publish_event))

    monkeypatch.setattr("core.worker.get_redis_client", lambda settings: object())
    monkeypatch.setattr("core.worker.get_engine", lambda settings: object())
    monkeypatch.setattr("core.worker.init_storage", lambda engine: None)
    monkeypatch.setattr("core.worker.StorageRepository", lambda engine: StubRepository())
    monkeypatch.setattr("core.worker.FlowAlphaSyncService", StubService)
    monkeypatch.setattr("core.worker.build_flow_live_sources", lambda settings, repository: [StubSource()])

    assert run_flow_alpha_live_sync(AppSettings.load()) == 0
    assert calls.count("fetch_snapshots") == 1
    assert ("walletint:base:AERO:flow_alpha_base_aero:1", "flow_alpha_base_aero", False) in calls


def test_launch_alpha_live_sync_uses_persistent_cache_transport(monkeypatch) -> None:
    calls: list[object] = []

    class StubSource:
        config = type("Config", (), {"source_name": "launch_alpha_solana"})()

        def fetch_snapshots(self):
            calls.append("fetch_snapshots")
            return []

    monkeypatch.setattr("core.worker.get_redis_client", lambda settings: object())
    monkeypatch.setattr("core.worker.get_engine", lambda settings: object())
    monkeypatch.setattr("core.worker.init_storage", lambda engine: None)
    monkeypatch.setattr("core.worker.StorageRepository", lambda engine: object())
    monkeypatch.setattr("core.worker.LaunchAlphaSyncService", lambda settings, redis_client, repository: object())
    monkeypatch.setattr(
        "core.worker.build_launch_live_sources",
        lambda settings, repository=None: calls.append(repository) or [StubSource()],
    )

    assert run_launch_alpha_live_sync(AppSettings.load()) == 0
    assert calls[0] is not None


def test_sample_launch_alpha_backfill_dataset_exists() -> None:
    dataset_path = "/home/alex/Desktop/signalengine/replay/datasets/launch_alpha_backfill.jsonl"

    with open(dataset_path, "r", encoding="utf-8") as handle:
        lines = [line.strip() for line in handle if line.strip()]

    assert len(lines) == 2
    assert '"source_type":"launch_pool_snapshot"' in lines[0]


def test_sample_catalyst_alpha_backfill_dataset_exists() -> None:
    dataset_path = "/home/alex/Desktop/signalengine/replay/datasets/catalyst_alpha_backfill.jsonl"

    with open(dataset_path, "r", encoding="utf-8") as handle:
        lines = [line.strip() for line in handle if line.strip()]

    assert len(lines) == 2
    assert '"source_type":"catalyst_event_snapshot"' in lines[0]


def test_sample_flow_alpha_backfill_dataset_exists() -> None:
    dataset_path = "/home/alex/Desktop/signalengine/replay/datasets/flow_alpha_backfill.jsonl"

    with open(dataset_path, "r", encoding="utf-8") as handle:
        lines = [line.strip() for line in handle if line.strip()]

    assert len(lines) == 2
    assert '"source_type":"flow_activity_snapshot"' in lines[0]


def test_onchain_feature_live_sync_returns_zero(monkeypatch) -> None:
    calls: list[object] = []

    class StubCheckpointStore:
        def load(self, checkpoint_key):
            calls.append(("load", checkpoint_key))
            return None

        def save(self, checkpoint):
            calls.append(("save", checkpoint.checkpoint_key, checkpoint.cursor))

    class StubRepository:
        def __init__(self) -> None:
            self.checkpoints = StubCheckpointStore()

    class StubTradeSource:
        config = type(
            "Config",
            (),
            {
                "checkpoint_key": "trade-source",
                "source_name": "solana_rpc_wallet",
                "provider": "solana_rpc_wallet_watch",
            },
        )()

        def fetch_trades(self, last_cursor=None):
            calls.append(("fetch_trades", last_cursor))
            return [
                type(
                    "Record",
                    (),
                    {
                        "cursor": "sig-1",
                        "observed_at": datetime(2026, 5, 3, 12, 0, 0, tzinfo=UTC),
                        "payload": {"token": "BONK", "chain": "solana"},
                    },
                )()
            ]

    class StubQuoteSource:
        config = type("Config", (), {"source_name": "jupiter_quote_api"})()

        def fetch_quotes(self):
            calls.append("fetch_quotes")
            return [{"token": "BONK", "chain": "solana"}]

    class StubService:
        def __init__(self, settings, redis_client, repository, metrics) -> None:
            calls.append(("service", settings, redis_client, repository, metrics))

        def ingest_trade(self, payload, source_name):
            calls.append(("ingest_trade", payload, source_name))

        def ingest_quote(self, payload, source_name):
            calls.append(("ingest_quote", payload, source_name))

    repository = StubRepository()
    monkeypatch.setattr("core.worker.get_redis_client", lambda settings: object())
    monkeypatch.setattr("core.worker.get_engine", lambda settings: object())
    monkeypatch.setattr("core.worker.init_storage", lambda engine: None)
    monkeypatch.setattr("core.worker.StorageRepository", lambda engine: repository)
    monkeypatch.setattr("core.worker.SolanaWalletTradeSource", StubTradeSource)
    monkeypatch.setattr("core.worker.EvmTransferTradeSource", StubTradeSource)
    monkeypatch.setattr("core.worker.JupiterQuoteSource", StubQuoteSource)
    monkeypatch.setattr("core.worker.build_live_sources", lambda settings: [StubTradeSource(), StubQuoteSource()])
    monkeypatch.setattr("core.worker.OnchainFeatureSyncService", StubService)

    assert run_onchain_feature_live_sync(AppSettings.load()) == 0
    assert any(call[0] == "fetch_trades" for call in calls if isinstance(call, tuple))
    assert any(call == "fetch_quotes" for call in calls)