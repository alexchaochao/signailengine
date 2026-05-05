from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from core.config import AppSettings, DexProviderCredentialsConfig
from infra.logging import get_logger
from sentinel.okx_client import OkxApiClient, OkxTransport
from sentinel.okx_wallet_registry_importer import _unwrap_okx_data


@dataclass(frozen=True)
class TrackedWalletRefreshRequest:
    chain: str
    chain_index: str
    wallet_address: str
    time_frame: str = "3"
    tx_limit: int = 20
    asset_type: str = "0"


@dataclass(frozen=True)
class RefreshedWalletState:
    wallet_address: str
    chain: str
    refreshed_at: datetime
    total_value_usd: float | None
    realized_pnl_usd: float | None
    win_rate: float | None
    recent_tx_count: int
    last_active_at: datetime | None
    source_data: dict[str, Any]


class OkxTrackedWalletRefreshJob:
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
        self.logger = get_logger("signalengine.wallet_refresh")

    def refresh_wallet(self, request: TrackedWalletRefreshRequest) -> RefreshedWalletState:
        overview_payload = self.client.signed_get(
            "/api/v6/dex/market/portfolio/overview",
            {
                "chainIndex": request.chain_index,
                "walletAddress": request.wallet_address,
                "timeFrame": request.time_frame,
            },
        )
        balance_payload = self.client.signed_get(
            "/api/v6/dex/balance/total-value-by-address",
            {
                "address": request.wallet_address,
                "chains": request.chain_index,
                "assetType": request.asset_type,
                "excludeRiskToken": "true",
            },
        )
        tx_history_payload = self.client.signed_get(
            "/api/v6/dex/post-transaction/transactions-by-address",
            {
                "address": request.wallet_address,
                "chains": request.chain_index,
                "limit": str(request.tx_limit),
            },
        )

        overview = _first_row(overview_payload)
        balance = _first_row(balance_payload)
        tx_summary = _tx_summary(tx_history_payload)
        refreshed_at = datetime.now(UTC)
        return RefreshedWalletState(
            wallet_address=request.wallet_address,
            chain=request.chain,
            refreshed_at=refreshed_at,
            total_value_usd=_float_or_none(balance.get("totalValue")),
            realized_pnl_usd=_float_or_none(overview.get("realizedPnlUsd")),
            win_rate=_float_or_none(overview.get("winRate")),
            recent_tx_count=tx_summary["recent_tx_count"],
            last_active_at=tx_summary["last_active_at"],
            source_data={
                "portfolio_overview": overview,
                "balance_total_value": balance,
                "tx_history": tx_summary["transactions"],
                "tx_cursor": tx_summary["cursor"],
            },
        )

    def refresh_wallets(
        self,
        requests: list[TrackedWalletRefreshRequest],
    ) -> list[RefreshedWalletState]:
        snapshots: list[RefreshedWalletState] = []
        for request in requests:
            try:
                snapshots.append(self.refresh_wallet(request))
            except Exception as error:
                self.logger.warning(
                    "wallet_refresh_failed",
                    extra={
                        "service": "wallet_refresh",
                        "wallet_address": request.wallet_address,
                        "chain": request.chain,
                        "error": str(error),
                    },
                )
        return snapshots


def _first_row(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    if isinstance(data, dict):
        return data
    rows = _unwrap_okx_data(payload)
    if not rows:
        return {}
    return rows[0]


def _tx_summary(payload: dict[str, Any]) -> dict[str, Any]:
    rows = _unwrap_okx_data(payload)
    if not rows:
        return {"recent_tx_count": 0, "last_active_at": None, "transactions": [], "cursor": None}
    row = rows[0]
    transactions = row.get("transactions", [])
    if not isinstance(transactions, list):
        transactions = []
    last_active_at: datetime | None = None
    for item in transactions:
        if not isinstance(item, dict):
            continue
        tx_time = _datetime_from_millis(item.get("txTime"))
        if tx_time is None:
            continue
        if last_active_at is None or tx_time > last_active_at:
            last_active_at = tx_time
    return {
        "recent_tx_count": len(transactions),
        "last_active_at": last_active_at,
        "transactions": transactions,
        "cursor": row.get("cursor"),
    }


def _datetime_from_millis(value: Any) -> datetime | None:
    try:
        timestamp_ms = int(str(value))
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None