from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from typing import Any

import httpx
from pydantic import BaseModel

from core.config import AppSettings
from sentinel.okx_client import (
    OkxApiClient,
    OkxApiCredentials,
    OkxTransportError,
    _coerce_okx_credentials,
    _http_json_get_transport,
)


class OkxDiagnosticResponse(BaseModel):
    status_code: int | None = None
    reason: str | None = None
    body: str | dict[str, Any] | None = None
    response_date: str | None = None
    request_id: str | None = None
    cf_ray: str | None = None
    ratelimit_remaining: str | None = None
    ratelimit_reset: str | None = None
    error_kind: str | None = None


class OkxDiagnosticReport(BaseModel):
    base_url: str
    method: str
    request_path: str
    request_url: str
    timestamp: str
    prehash: str
    signature: str
    has_project_header: bool
    project_id: str | None = None
    api_key_suffix: str
    response: OkxDiagnosticResponse | None = None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Diagnose OKX request signing and timestamp handling")
    parser.add_argument("--path", default="/api/v6/dex/market/leaderboard/list")
    parser.add_argument("--chain-index", default="8453")
    parser.add_argument("--time-frame", default="3")
    parser.add_argument("--sort-by", default="1")
    parser.add_argument("--wallet-type", default="3")
    parser.add_argument("--timestamp")
    parser.add_argument("--omit-project-id", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def build_leaderboard_diagnostic_report(
    settings: AppSettings,
    *,
    path: str = "/api/v6/dex/market/leaderboard/list",
    chain_index: str = "8453",
    time_frame: str = "3",
    sort_by: str = "1",
    wallet_type: str = "3",
    timestamp: str | None = None,
    omit_project_id: bool = False,
    dry_run: bool = False,
) -> OkxDiagnosticReport:
    raw_credentials = settings.live.credentials.dex_providers.get("okx")
    if raw_credentials is None:
        raise ValueError("missing_okx_credentials")
    credentials = _coerce_okx_credentials(raw_credentials)
    if omit_project_id:
        credentials = OkxApiCredentials(
            api_key=credentials.api_key,
            secret_key=credentials.secret_key,
            api_passphrase=credentials.api_passphrase,
            project_id=None,
        )
    client = OkxApiClient(credentials)
    params = {
        "chainIndex": str(chain_index),
        "sortBy": str(sort_by),
        "timeFrame": str(time_frame),
        "walletType": str(wallet_type),
    }
    query = "&".join(f"{key}={value}" for key, value in sorted(params.items()))
    request_path = path if not query else f"{path}?{query}"
    effective_timestamp = timestamp or datetime.now(UTC).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )
    headers = client.build_headers("GET", request_path, effective_timestamp)
    report = OkxDiagnosticReport(
        base_url=client.base_url,
        method="GET",
        request_path=request_path,
        request_url=f"{client.base_url}{request_path}",
        timestamp=effective_timestamp,
        prehash=f"{effective_timestamp}GET{request_path}",
        signature=headers["OK-ACCESS-SIGN"],
        has_project_header="OK-ACCESS-PROJECT" in headers,
        project_id=headers.get("OK-ACCESS-PROJECT"),
        api_key_suffix=credentials.api_key[-6:],
    )
    if dry_run:
        return report
    return report.model_copy(update={"response": _dispatch_request(report.request_url, headers)})


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = AppSettings.load()
    report = build_leaderboard_diagnostic_report(
        settings,
        path=args.path,
        chain_index=args.chain_index,
        time_frame=args.time_frame,
        sort_by=args.sort_by,
        wallet_type=args.wallet_type,
        timestamp=args.timestamp,
        omit_project_id=args.omit_project_id,
        dry_run=args.dry_run,
    )
    if args.json:
        print(report.model_dump_json(indent=2))
    else:
        print(json.dumps(report.model_dump(mode="json"), indent=2))
    return 0


def _dispatch_request(url: str, headers: dict[str, str]) -> OkxDiagnosticResponse:
    try:
        payload = _http_json_get_transport(url, headers, 10.0)
    except OkxTransportError as error:
        return OkxDiagnosticResponse(
            status_code=error.status_code,
            reason=error.reason,
            body=error.body_preview,
            response_date=error.response_date,
            request_id=error.request_id,
            cf_ray=error.cf_ray,
            ratelimit_remaining=error.ratelimit_remaining,
            ratelimit_reset=error.ratelimit_reset,
            error_kind=error.error_kind,
        )
    return OkxDiagnosticResponse(status_code=200, reason="OK", body=payload)


if __name__ == "__main__":
    raise SystemExit(main())