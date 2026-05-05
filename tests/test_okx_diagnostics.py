from __future__ import annotations

from core.config import AppSettings
from sentinel import okx_diagnostics
from sentinel.okx_client import OkxTransportError
from sentinel.okx_diagnostics import build_leaderboard_diagnostic_report


def test_okx_diagnostic_report_matches_signing_contract() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "live": AppSettings.load().live.model_copy(
                update={
                    "credentials": AppSettings.load().live.credentials.model_copy(
                        update={
                            "dex_providers": {
                                "okx": {
                                    "api_key": "test-key-123456",
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

    report = build_leaderboard_diagnostic_report(
        settings,
        chain_index="8453",
        time_frame="3",
        sort_by="1",
        wallet_type="3",
        timestamp="2026-05-04T03:33:00.000Z",
        dry_run=True,
    )

    assert report.request_path == "/api/v6/dex/market/leaderboard/list?chainIndex=8453&sortBy=1&timeFrame=3&walletType=3"
    assert report.prehash == "2026-05-04T03:33:00.000ZGET/api/v6/dex/market/leaderboard/list?chainIndex=8453&sortBy=1&timeFrame=3&walletType=3"
    assert report.signature
    assert report.has_project_header is True
    assert report.project_id == "test-project"
    assert report.api_key_suffix == "123456"


def test_okx_diagnostic_report_can_omit_project_id() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "live": AppSettings.load().live.model_copy(
                update={
                    "credentials": AppSettings.load().live.credentials.model_copy(
                        update={
                            "dex_providers": {
                                "okx": {
                                    "api_key": "test-key-123456",
                                    "secret_key": "test-secret",
                                    "api_passphrase": "test-passphrase",
                                    "project_id": "01",
                                }
                            }
                        }
                    )
                }
            )
        }
    )

    report = build_leaderboard_diagnostic_report(
        settings,
        timestamp="2026-05-04T03:33:00.000Z",
        omit_project_id=True,
        dry_run=True,
    )

    assert report.has_project_header is False
    assert report.project_id is None


def test_okx_dispatch_request_maps_httpx_status_error(monkeypatch) -> None:
    def raise_status(url: str, headers: dict[str, str], timeout_seconds: float):
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

    monkeypatch.setattr(okx_diagnostics, "_http_json_get_transport", raise_status)

    result = okx_diagnostics._dispatch_request("https://web3.okx.com/test", {})

    assert result.status_code == 403
    assert result.reason == "Forbidden"
    assert result.body == '{"code":"1010"}'
    assert result.response_date == "Mon, 04 May 2026 08:00:00 GMT"
    assert result.request_id == "req-123"
    assert result.cf_ray == "ray-123"
    assert result.ratelimit_remaining == "183"
    assert result.ratelimit_reset == "60"
    assert result.error_kind == "HTTPStatusError"


def test_okx_dispatch_request_maps_httpx_network_error(monkeypatch) -> None:
    def raise_request_error(url: str, headers: dict[str, str], timeout_seconds: float):
        _ = url, headers, timeout_seconds
        raise OkxTransportError(reason="connection reset", error_kind="ConnectError")

    monkeypatch.setattr(okx_diagnostics, "_http_json_get_transport", raise_request_error)

    result = okx_diagnostics._dispatch_request("https://web3.okx.com/test", {})

    assert result.status_code is None
    assert result.reason == "connection reset"
    assert result.error_kind == "ConnectError"