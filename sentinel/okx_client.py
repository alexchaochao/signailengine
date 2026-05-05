from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Callable
from urllib import parse

import httpx

from core.config import AppSettings, DexProviderCredentialsConfig

OKX_BASE_URL = "https://web3.okx.com"
OKX_ERROR_BODY_PREVIEW_LIMIT = 2048


@dataclass(frozen=True)
class OkxApiCredentials:
    api_key: str
    secret_key: str
    api_passphrase: str
    project_id: str | None = None


@dataclass(frozen=True)
class OkxTransportError(RuntimeError):
    reason: str
    status_code: int | None = None
    response_date: str | None = None
    request_id: str | None = None
    cf_ray: str | None = None
    ratelimit_remaining: str | None = None
    ratelimit_reset: str | None = None
    error_kind: str | None = None
    body_preview: str | None = None

    def __post_init__(self) -> None:
        RuntimeError.__init__(self, self.reason)


@dataclass(frozen=True)
class OkxRequestError(RuntimeError):
    method: str
    request_path: str
    request_url: str
    timestamp: str
    has_project_header: bool
    project_id: str | None
    api_key_suffix: str
    reason: str
    status_code: int | None = None
    response_date: str | None = None
    request_id: str | None = None
    cf_ray: str | None = None
    ratelimit_remaining: str | None = None
    ratelimit_reset: str | None = None
    error_kind: str | None = None
    body_preview: str | None = None

    def __post_init__(self) -> None:
        RuntimeError.__init__(self, self.reason)


OkxTransport = Callable[[str, dict[str, str], float], dict[str, Any]]


class OkxApiClient:
    def __init__(
        self,
        credentials: AppSettings | DexProviderCredentialsConfig | OkxApiCredentials,
        transport: OkxTransport | None = None,
        base_url: str = OKX_BASE_URL,
        timeout_seconds: float = 5.0,
        max_attempts: int = 3,
        backoff_seconds: float = 0.5,
        backoff_multiplier: float = 2.0,
        fallback_to_http1: bool = True,
    ) -> None:
        self.credentials = _coerce_okx_credentials(credentials)
        self.transport = transport or _build_default_transport(
            max_attempts=max_attempts,
            backoff_seconds=backoff_seconds,
            backoff_multiplier=backoff_multiplier,
            fallback_to_http1=fallback_to_http1,
        )
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def signed_get(self, path: str, params: dict[str, str]) -> dict[str, Any]:
        query = parse.urlencode(sorted(params.items()))
        request_path = path if not query else f"{path}?{query}"
        timestamp = datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        headers = self.build_headers("GET", request_path, timestamp)
        request_url = f"{self.base_url}{request_path}"
        try:
            return self.transport(request_url, headers, self.timeout_seconds)
        except OkxTransportError as error:
            raise OkxRequestError(
                method="GET",
                request_path=request_path,
                request_url=request_url,
                timestamp=timestamp,
                has_project_header="OK-ACCESS-PROJECT" in headers,
                project_id=headers.get("OK-ACCESS-PROJECT"),
                api_key_suffix=self.credentials.api_key[-6:],
                reason=error.reason,
                status_code=error.status_code,
                response_date=error.response_date,
                request_id=error.request_id,
                cf_ray=error.cf_ray,
                ratelimit_remaining=error.ratelimit_remaining,
                ratelimit_reset=error.ratelimit_reset,
                error_kind=error.error_kind,
                body_preview=error.body_preview,
            ) from error

    def build_headers(self, method: str, request_path: str, timestamp: str) -> dict[str, str]:
        prehash = f"{timestamp}{method.upper()}{request_path}"
        digest = hmac.new(
            self.credentials.secret_key.encode("utf-8"),
            prehash.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        signature = base64.b64encode(digest).decode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "OK-ACCESS-KEY": self.credentials.api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-PASSPHRASE": self.credentials.api_passphrase,
            "OK-ACCESS-TIMESTAMP": timestamp,
        }
        if self.credentials.project_id:
            headers["OK-ACCESS-PROJECT"] = self.credentials.project_id
        return headers


def _coerce_okx_credentials(
    credentials: AppSettings | DexProviderCredentialsConfig | OkxApiCredentials | dict[str, Any],
) -> OkxApiCredentials:
    if isinstance(credentials, OkxApiCredentials):
        return credentials
    if isinstance(credentials, AppSettings):
        raw = credentials.live.credentials.dex_providers.get("okx")
        if raw is None:
            raise ValueError("missing_okx_credentials")
        credentials = raw
    if isinstance(credentials, dict):
        credentials = DexProviderCredentialsConfig.model_validate(credentials)
    api_key = credentials.api_key
    secret_key = credentials.secret_key
    api_passphrase = credentials.api_passphrase
    if not api_key or not secret_key or not api_passphrase:
        raise ValueError("missing_okx_credentials")
    return OkxApiCredentials(
        api_key=api_key,
        secret_key=secret_key,
        api_passphrase=api_passphrase,
        project_id=credentials.project_id,
    )


def _http_json_get_transport(
    url: str,
    headers: dict[str, str],
    timeout_seconds: float,
    *,
    max_attempts: int = 1,
    backoff_seconds: float = 0.0,
    backoff_multiplier: float = 2.0,
    fallback_to_http1: bool = False,
) -> dict[str, Any]:
    request_headers = {"Accept": "application/json", **headers}
    attempts = max(1, max_attempts)
    next_delay = max(0.0, backoff_seconds)
    last_error: OkxTransportError | None = None

    for attempt_index in range(attempts):
        protocols = [True, False] if fallback_to_http1 else [True]
        for http2_enabled in protocols:
            try:
                return _http_json_get_once(
                    url,
                    request_headers,
                    timeout_seconds,
                    http2=http2_enabled,
                )
            except OkxTransportError as error:
                last_error = error
                if error.status_code is not None and not _should_retry_status(error.status_code):
                    raise
                if http2_enabled and not _can_retry_error(error) and fallback_to_http1:
                    continue
                if not http2_enabled:
                    break
                if not fallback_to_http1:
                    break
        if attempt_index == attempts - 1:
            break
        if last_error is None or not _can_retry_error(last_error):
            break
        if next_delay > 0.0:
            time.sleep(next_delay)
            next_delay *= max(backoff_multiplier, 1.0)
    if last_error is not None:
        raise last_error
    raise OkxTransportError(reason="okx_request_failed")


def _build_default_transport(
    *,
    max_attempts: int,
    backoff_seconds: float,
    backoff_multiplier: float,
    fallback_to_http1: bool,
) -> OkxTransport:
    def transport(url: str, headers: dict[str, str], timeout_seconds: float) -> dict[str, Any]:
        return _http_json_get_transport(
            url,
            headers,
            timeout_seconds,
            max_attempts=max_attempts,
            backoff_seconds=backoff_seconds,
            backoff_multiplier=backoff_multiplier,
            fallback_to_http1=fallback_to_http1,
        )

    return transport


def _http_json_get_once(
    url: str,
    headers: dict[str, str],
    timeout_seconds: float,
    *,
    http2: bool,
) -> dict[str, Any]:
    try:
        with httpx.Client(http2=http2, follow_redirects=True, timeout=timeout_seconds) as client:
            response = client.get(url, headers=headers)
            response.raise_for_status()
            raw_body = response.content
    except httpx.HTTPStatusError as error:
        raise OkxTransportError(
            reason=error.response.reason_phrase,
            status_code=error.response.status_code,
            response_date=error.response.headers.get("Date"),
            request_id=error.response.headers.get("x-requestid"),
            cf_ray=error.response.headers.get("cf-ray"),
            ratelimit_remaining=error.response.headers.get("ratelimit-remaining"),
            ratelimit_reset=error.response.headers.get("ratelimit-reset"),
            error_kind=type(error).__name__,
            body_preview=_truncate_error_body(error.response.text),
        ) from error
    except httpx.RequestError as error:
        raise OkxTransportError(
            reason=str(error),
            error_kind=type(error).__name__,
        ) from error
    try:
        payload = json.loads(raw_body.decode("utf-8"))
    except json.JSONDecodeError as error:
        raise OkxTransportError(
            reason="invalid_okx_response",
            error_kind=type(error).__name__,
            body_preview=_truncate_error_body(raw_body.decode("utf-8", errors="replace")),
        ) from error
    if not isinstance(payload, dict):
        raise OkxTransportError(
            reason="invalid_okx_response",
            body_preview=_truncate_error_body(str(payload)),
        )
    return payload


def _can_retry_error(error: OkxTransportError) -> bool:
    if error.status_code is None:
        return True
    return _should_retry_status(error.status_code)


def _should_retry_status(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code < 600


def _truncate_error_body(body: str) -> str:
    if len(body) <= OKX_ERROR_BODY_PREVIEW_LIMIT:
        return body
    return f"{body[:OKX_ERROR_BODY_PREVIEW_LIMIT]}..."