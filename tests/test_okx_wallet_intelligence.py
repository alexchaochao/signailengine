from __future__ import annotations

import base64
import hashlib
import hmac
from datetime import UTC, datetime
from typing import Any

from core.config import AppSettings
from sentinel import okx_client
from sentinel.okx_client import OkxApiClient, OkxApiCredentials, OkxRequestError, OkxTransportError
from sentinel.okx_wallet_registry_importer import (
    OkxLeaderboardRequest,
    OkxWalletRegistryImporter,
)
from sentinel.wallet_refresh_job import (
    OkxTrackedWalletRefreshJob,
    TrackedWalletRefreshRequest,
)
from sentinel.wallet_score_aggregator import WalletScoreAggregator, WalletTokenFlow
from sentinel.wallet_tracker import build_wallet_event_from_registry_flows


def _settings_with_okx_credentials() -> AppSettings:
    settings = AppSettings.load()
    return settings.model_copy(
        update={
            "live": settings.live.model_copy(
                update={
                    "credentials": settings.live.credentials.model_copy(
                        update={
                            "dex_providers": {
                                "okx": {
                                    "api_key": "test-key",
                                    "secret_key": "test-secret",
                                    "api_passphrase": "test-passphrase",
                                    "project_id": "test-project",
                                }
                            }
                        }
                    )
                }
            )
        }
    )


def test_okx_api_client_builds_signed_headers() -> None:
    client = OkxApiClient(
        OkxApiCredentials(
            api_key="test-key",
            secret_key="test-secret",
            api_passphrase="test-passphrase",
            project_id="test-project",
        )
    )

    headers = client.build_headers(
        "GET",
        "/api/v6/dex/market/leaderboard/list?chainIndex=501&sortBy=1&timeFrame=3",
        "2026-05-02T16:00:00.000Z",
    )

    expected = base64.b64encode(
        hmac.new(
            b"test-secret",
            b"2026-05-02T16:00:00.000ZGET/api/v6/dex/market/leaderboard/list?chainIndex=501&sortBy=1&timeFrame=3",
            hashlib.sha256,
        ).digest()
    ).decode("utf-8")
    assert headers["OK-ACCESS-SIGN"] == expected
    assert headers["OK-ACCESS-PROJECT"] == "test-project"


def test_okx_api_client_wraps_transport_failures_with_request_snapshot() -> None:
    def transport(url: str, headers: dict[str, str], timeout_seconds: float) -> dict[str, Any]:
        _ = url, headers, timeout_seconds
        raise OkxTransportError(
            reason="Forbidden",
            status_code=403,
            response_date="Mon, 04 May 2026 08:00:00 GMT",
            request_id="req-123",
            cf_ray="ray-123",
            ratelimit_remaining="183",
            ratelimit_reset="60",
            error_kind="HTTPStatusError",
            body_preview='{"code":"1010"}',
        )

    client = OkxApiClient(
        OkxApiCredentials(
            api_key="test-key-123456",
            secret_key="test-secret",
            api_passphrase="test-passphrase",
            project_id="test-project",
        ),
        transport=transport,
    )

    try:
        client.signed_get(
            "/api/v6/dex/market/leaderboard/list",
            {"chainIndex": "8453", "sortBy": "1", "timeFrame": "3", "walletType": "3"},
        )
    except OkxRequestError as error:
        assert error.method == "GET"
        assert error.request_path == "/api/v6/dex/market/leaderboard/list?chainIndex=8453&sortBy=1&timeFrame=3&walletType=3"
        assert error.status_code == 403
        assert error.request_id == "req-123"
        assert error.cf_ray == "ray-123"
        assert error.ratelimit_remaining == "183"
        assert error.ratelimit_reset == "60"
        assert error.has_project_header is True
        assert error.project_id == "test-project"
        assert error.api_key_suffix == "123456"
        assert error.body_preview == '{"code":"1010"}'
    else:
        raise AssertionError("expected OkxRequestError")


def test_okx_wallet_registry_importer_maps_leaderboard_rows() -> None:
    def transport(url: str, headers: dict[str, str], timeout_seconds: float) -> dict[str, Any]:
        assert "leaderboard/list" in url
        assert headers["OK-ACCESS-KEY"] == "test-key"
        assert timeout_seconds == 5.0
        return {
            "code": "0",
            "data": [
                {
                    "walletAddress": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
                    "realizedPnlUsd": "125430.56",
                    "realizedPnlPercent": "312.45",
                    "winRatePercent": "68.5",
                    "avgBuyValueUsd": "2340.00",
                    "txVolume": "890234.50",
                    "txs": "342",
                    "lastActiveTimestamp": "1697630501000",
                    "topPnlTokenList": [{"tokenSymbol": "BONK"}],
                }
            ],
        }

    importer = OkxWalletRegistryImporter(_settings_with_okx_credentials(), transport=transport)

    entries = importer.import_wallets(
        OkxLeaderboardRequest(
            chain="solana",
            chain_index="501",
            time_frame="3",
            sort_by="1",
            wallet_type="3",
        ),
        observed_at=datetime(2026, 5, 2, 16, 0, tzinfo=UTC),
    )

    assert len(entries) == 1
    assert entries[0].wallet_address == "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"
    assert entries[0].wallet_class == "smart_money"
    assert entries[0].source == "okx_leaderboard"
    assert entries[0].source_metadata["sort_by"] == "1"
    assert entries[0].weight > 0.0


def test_okx_tracked_wallet_refresh_job_combines_endpoint_data() -> None:
    def transport(url: str, headers: dict[str, str], timeout_seconds: float) -> dict[str, Any]:
        _ = headers, timeout_seconds
        if "portfolio/overview" in url:
            return {
                "code": "0",
                "data": {"realizedPnlUsd": "2400.50", "winRate": "61.5"},
            }
        if "balance/total-value-by-address" in url:
            return {"code": "0", "data": [{"totalValue": "18250.10"}]}
        if "transactions-by-address" in url:
            return {
                "code": "0",
                "data": [
                    {
                        "transactions": [
                            {"txHash": "a", "txTime": "1714665600000"},
                            {"txHash": "b", "txTime": "1714752000000"},
                        ],
                        "cursor": "next-cursor",
                    }
                ],
            }
        raise AssertionError(url)

    job = OkxTrackedWalletRefreshJob(_settings_with_okx_credentials(), transport=transport)

    snapshot = job.refresh_wallet(
        TrackedWalletRefreshRequest(
            chain="solana",
            chain_index="501",
            wallet_address="7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
            time_frame="3",
        )
    )

    assert snapshot.total_value_usd == 18250.10
    assert snapshot.realized_pnl_usd == 2400.50
    assert snapshot.win_rate == 61.5
    assert snapshot.recent_tx_count == 2
    assert snapshot.last_active_at == datetime(2024, 5, 3, 16, 0, tzinfo=UTC)


def test_okx_http_transport_falls_back_to_http1_after_http2_connect_error(monkeypatch) -> None:
    attempts: list[bool] = []
    request = okx_client.httpx.Request("GET", "https://web3.okx.com/test")

    class FakeClient:
        def __init__(self, *, http2: bool, follow_redirects: bool, timeout: float) -> None:
            _ = follow_redirects, timeout
            self.http2 = http2

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def get(self, url: str, headers: dict[str, str]):
            _ = url, headers
            attempts.append(self.http2)
            if self.http2:
                raise okx_client.httpx.ConnectError("connection reset", request=request)
            return okx_client.httpx.Response(200, request=request, json={"code": "0", "data": []})

    monkeypatch.setattr(okx_client.httpx, "Client", FakeClient)

    payload = okx_client._http_json_get_transport(
        "https://web3.okx.com/test",
        {},
        5.0,
        max_attempts=2,
        backoff_seconds=0.0,
        fallback_to_http1=True,
    )

    assert payload == {"code": "0", "data": []}
    assert attempts == [True, False]


def test_okx_http_transport_retries_rate_limit_then_succeeds(monkeypatch) -> None:
    sleep_calls: list[float] = []
    responses = [429, 200]
    request = okx_client.httpx.Request("GET", "https://web3.okx.com/test")

    class FakeClient:
        def __init__(self, *, http2: bool, follow_redirects: bool, timeout: float) -> None:
            _ = http2, follow_redirects, timeout

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def get(self, url: str, headers: dict[str, str]):
            _ = url, headers
            status_code = responses.pop(0)
            if status_code == 429:
                return okx_client.httpx.Response(
                    429,
                    request=request,
                    text='{"code":"429"}',
                    headers={"ratelimit-reset": "1"},
                )
            return okx_client.httpx.Response(200, request=request, json={"code": "0", "data": []})

    monkeypatch.setattr(okx_client.httpx, "Client", FakeClient)
    monkeypatch.setattr(okx_client.time, "sleep", sleep_calls.append)

    payload = okx_client._http_json_get_transport(
        "https://web3.okx.com/test",
        {},
        5.0,
        max_attempts=2,
        backoff_seconds=0.25,
        fallback_to_http1=False,
    )

    assert payload == {"code": "0", "data": []}
    assert sleep_calls == [0.25]


def test_wallet_score_aggregator_builds_weighted_scores() -> None:
    importer = OkxWalletRegistryImporter(
        _settings_with_okx_credentials(),
        transport=lambda url, headers, timeout_seconds: {"code": "0", "data": []},
    )
    registry_entries = importer.parse_leaderboard_response(
        {
            "code": "0",
            "data": [
                {
                    "walletAddress": "wallet-1",
                    "realizedPnlPercent": "200",
                    "winRatePercent": "60",
                    "txs": "50",
                },
                {
                    "walletAddress": "wallet-2",
                    "realizedPnlPercent": "50",
                    "winRatePercent": "80",
                    "txs": "10",
                },
            ],
        },
        OkxLeaderboardRequest(
            chain="solana",
            chain_index="501",
            time_frame="3",
            sort_by="1",
            wallet_type="3",
        ),
        datetime(2026, 5, 2, 16, 0, tzinfo=UTC),
        "registry-v1",
    )
    aggregator = WalletScoreAggregator(window_seconds=900, min_flow_count=2, min_wallet_count=2)

    snapshot = aggregator.build_snapshot(
        "solana",
        "BONK",
        registry_entries,
        [
            WalletTokenFlow(
                chain="solana",
                token="BONK",
                wallet_address="wallet-1",
                direction="inflow",
                notional_usd=1000.0,
                observed_at=datetime(2026, 5, 2, 15, 55, tzinfo=UTC),
            ),
            WalletTokenFlow(
                chain="solana",
                token="BONK",
                wallet_address="wallet-2",
                direction="outflow",
                notional_usd=250.0,
                observed_at=datetime(2026, 5, 2, 15, 57, tzinfo=UTC),
            ),
        ],
        window_end=datetime(2026, 5, 2, 16, 0, tzinfo=UTC),
    )

    assert snapshot.wallet_inflow_score > snapshot.wallet_outflow_score
    assert snapshot.tracked_wallet_count == 2
    assert snapshot.sample_count == 2
    assert snapshot.quality_flag == "ok"


def test_wallet_tracker_builds_event_from_registry_flows() -> None:
    importer = OkxWalletRegistryImporter(
        _settings_with_okx_credentials(),
        transport=lambda url, headers, timeout_seconds: {"code": "0", "data": []},
    )
    registry_entries = importer.parse_leaderboard_response(
        {
            "code": "0",
            "data": [
                {
                    "walletAddress": "wallet-1",
                    "realizedPnlPercent": "200",
                    "winRatePercent": "60",
                    "txs": "50",
                }
            ],
        },
        OkxLeaderboardRequest(
            chain="solana",
            chain_index="501",
            time_frame="3",
            sort_by="1",
            wallet_type="3",
        ),
        datetime(2026, 5, 2, 16, 0, tzinfo=UTC),
        "registry-v1",
    )

    event = build_wallet_event_from_registry_flows(
        "solana",
        "BONK",
        registry_entries,
        [
            WalletTokenFlow(
                chain="solana",
                token="BONK",
                wallet_address="wallet-1",
                direction="inflow",
                notional_usd=500.0,
                observed_at=datetime(2026, 5, 2, 15, 59, tzinfo=UTC),
            )
        ],
        aggregator=WalletScoreAggregator(window_seconds=900),
    )

    assert event.event_type == "wallet.cluster_snapshot"
    assert event.payload["wallet_inflow_score"] == 1.0
    assert event.payload["tracked_wallet_count"] == 1