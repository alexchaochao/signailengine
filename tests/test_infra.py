from core.config import AppSettings
from infra.postgres import get_engine
from infra.redis_stream import get_redis_client


def test_redis_client_uses_configured_url() -> None:
    settings = AppSettings.load()
    client = get_redis_client(settings)

    assert client.connection_pool.connection_kwargs["host"] == "localhost"
    assert client.connection_pool.connection_kwargs["port"] == 6379


def test_postgres_engine_uses_configured_url() -> None:
    settings = AppSettings.load()
    engine = get_engine(settings)

    assert str(engine.url).startswith("postgresql+psycopg://signalengine:")