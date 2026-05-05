from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from core.config import AppSettings, DexProviderCredentialsConfig
from sentinel.okx_client import OkxApiClient, OkxTransport


@dataclass(frozen=True)
class OkxLeaderboardRequest:
    chain: str
    chain_index: str
    time_frame: str
    sort_by: str
    wallet_type: str | None = None
    min_realized_pnl_usd: str | None = None
    min_win_rate_percent: str | None = None
    min_txs: str | None = None
    min_tx_volume: str | None = None

    def to_params(self) -> dict[str, str]:
        params = {
            "chainIndex": self.chain_index,
            "timeFrame": self.time_frame,
            "sortBy": self.sort_by,
        }
        if self.wallet_type is not None:
            params["walletType"] = self.wallet_type
        if self.min_realized_pnl_usd is not None:
            params["minRealizedPnlUsd"] = self.min_realized_pnl_usd
        if self.min_win_rate_percent is not None:
            params["minWinRatePercent"] = self.min_win_rate_percent
        if self.min_txs is not None:
            params["minTxs"] = self.min_txs
        if self.min_tx_volume is not None:
            params["minTxVolume"] = self.min_tx_volume
        return params


@dataclass(frozen=True)
class TrackedWalletRegistryEntry:
    wallet_address: str
    chain: str
    wallet_class: str
    weight: float
    status: str
    source: str
    source_metadata: dict[str, Any]
    version: str
    discovered_at: datetime
    last_seen_at: datetime
    updated_at: datetime


class OkxWalletRegistryImporter:
    def __init__(
        self,
        credentials: AppSettings | DexProviderCredentialsConfig,
        transport: OkxTransport | None = None,
        timeout_seconds: float = 5.0,
    ) -> None:
        self.client = OkxApiClient(
            credentials,
            transport=transport,
            timeout_seconds=timeout_seconds,
        )

    def import_wallets(
        self,
        request: OkxLeaderboardRequest,
        observed_at: datetime | None = None,
        registry_version: str = "okx_registry_v1",
    ) -> list[TrackedWalletRegistryEntry]:
        payload = self.client.signed_get(
            "/api/v6/dex/market/leaderboard/list",
            request.to_params(),
        )
        timestamp = observed_at.astimezone(UTC) if observed_at else datetime.now(UTC)
        return self.parse_leaderboard_response(payload, request, timestamp, registry_version)

    def parse_leaderboard_response(
        self,
        payload: dict[str, Any],
        request: OkxLeaderboardRequest,
        observed_at: datetime,
        registry_version: str,
    ) -> list[TrackedWalletRegistryEntry]:
        rows = _unwrap_okx_data(payload)
        entries: list[TrackedWalletRegistryEntry] = []
        for row in rows:
            wallet_address = str(row.get("walletAddress", "")).strip()
            if not wallet_address:
                continue
            source_metadata = {
                "chain_index": request.chain_index,
                "time_frame": request.time_frame,
                "sort_by": request.sort_by,
                "wallet_type": request.wallet_type,
                "realized_pnl_usd": row.get("realizedPnlUsd"),
                "realized_pnl_percent": row.get("realizedPnlPercent"),
                "win_rate_percent": row.get("winRatePercent"),
                "avg_buy_value_usd": row.get("avgBuyValueUsd"),
                "tx_volume": row.get("txVolume"),
                "txs": row.get("txs"),
                "last_active_timestamp": row.get("lastActiveTimestamp"),
                "top_pnl_token_list": row.get("topPnlTokenList", []),
            }
            entries.append(
                TrackedWalletRegistryEntry(
                    wallet_address=wallet_address,
                    chain=request.chain,
                    wallet_class=_wallet_class(request.wallet_type),
                    weight=_derive_weight(row),
                    status="active",
                    source="okx_leaderboard",
                    source_metadata=source_metadata,
                    version=registry_version,
                    discovered_at=observed_at,
                    last_seen_at=observed_at,
                    updated_at=observed_at,
                )
            )
        return entries


def _unwrap_okx_data(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if str(payload.get("code", "0")) != "0":
        raise ValueError("okx_api_error")
    data = payload.get("data", [])
    if not isinstance(data, list):
        raise ValueError("invalid_okx_response")
    rows: list[dict[str, Any]] = []
    for row in data:
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _wallet_class(wallet_type: str | None) -> str:
    mapping = {
        "1": "kol",
        "2": "developer",
        "3": "smart_money",
        "4": "whale",
        "5": "new_wallet",
        "6": "insider",
        "7": "sniper",
        "8": "phishing_risk",
        "9": "bundled_trader",
        "10": "pump_smart_money",
    }
    return mapping.get(wallet_type or "3", "smart_money")


def _derive_weight(row: dict[str, Any]) -> float:
    win_rate = _bounded_percent(row.get("winRatePercent"))
    pnl_percent = _bounded_percent(row.get("realizedPnlPercent"), scale=500.0)
    tx_count = min(max(_as_float(row.get("txs")) / 100.0, 0.0), 1.0)
    return round(0.45 * win_rate + 0.35 * pnl_percent + 0.20 * tx_count, 6)


def _bounded_percent(value: Any, scale: float = 100.0) -> float:
    return min(max(_as_float(value) / scale, 0.0), 1.0)


def _as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0