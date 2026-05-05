from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from core.config import AppSettings, FlowAlphaLiveSourceConfig
from discovery.schemas import FlowActivitySnapshot
from infra.repository import StorageRepository
from sentinel.wallet_score_aggregator import WalletTokenFlow


@dataclass(frozen=True)
class WalletIntelligenceFlowSnapshotSource:
	settings: AppSettings
	repository: StorageRepository
	config: FlowAlphaLiveSourceConfig

	def fetch_snapshots(self) -> list[FlowActivitySnapshot]:
		if self.config.provider != "wallet_intelligence_store":
			raise ValueError(f"unsupported_flow_alpha_provider:{self.config.provider}")
		window_end = datetime.now(UTC)
		window_start = window_end - timedelta(minutes=self.config.window_minutes)
		flows = self.repository.wallet_intelligence.load_wallet_flows(
			self.config.chain,
			self.config.token,
			since=window_start,
		)
		if not flows:
			return []
		active_wallets = {
			entry.wallet_address
			for entry in self.repository.wallet_intelligence.list_active_registry_entries(
				self.config.chain
			)
		}
		filtered_flows = [flow for flow in flows if flow.wallet_address in active_wallets]
		if not filtered_flows:
			return []
		snapshot = _build_snapshot(self.config, filtered_flows, window_end=window_end)
		if snapshot is None:
			return []
		return [snapshot]


def build_flow_live_sources(
	settings: AppSettings,
	repository: StorageRepository,
) -> list[WalletIntelligenceFlowSnapshotSource]:
	sources: list[WalletIntelligenceFlowSnapshotSource] = []
	for source_key, source_config in sorted(settings.acquisition.flow_alpha_sources.items()):
		config = source_config.model_copy(
			update={"source_name": source_config.source_name or f"flow_alpha_{source_key}"}
		)
		if not config.enabled:
			continue
		sources.append(WalletIntelligenceFlowSnapshotSource(settings, repository, config))
	return sources


def _build_snapshot(
	config: FlowAlphaLiveSourceConfig,
	flows: list[WalletTokenFlow],
	*,
	window_end: datetime,
) -> FlowActivitySnapshot | None:
	inflows = [flow for flow in flows if flow.direction == "inflow"]
	outflows = [flow for flow in flows if flow.direction == "outflow"]
	smart_money_inflow_usd = round(sum(flow.notional_usd for flow in inflows), 6)
	smart_money_outflow_usd = round(sum(flow.notional_usd for flow in outflows), 6)
	netflow_15m_usd = round(max(smart_money_inflow_usd - smart_money_outflow_usd, 0.0), 6)
	unique_buyer_wallets_15m = len({flow.wallet_address for flow in inflows})
	unique_seller_wallets_15m = len({flow.wallet_address for flow in outflows})
	whale_buy_count_15m = sum(
		1 for flow in inflows if flow.notional_usd >= config.min_whale_buy_usd
	)
	exchange_outflow_usd = netflow_15m_usd
	if (
		netflow_15m_usd < config.min_netflow_15m_usd
		or smart_money_inflow_usd < config.min_smart_money_inflow_usd
		or unique_buyer_wallets_15m < config.min_unique_buyer_wallets_15m
		or exchange_outflow_usd < config.min_exchange_outflow_usd
	):
		return None
	latest_observed_at = max(flow.observed_at for flow in flows).astimezone(UTC)
	return FlowActivitySnapshot(
		source_event_id=(
			f"walletint:{config.chain}:{config.token}:{config.source_name}:{int(window_end.timestamp())}"
		),
		chain=config.chain,
		token=config.token,
		flow_type="smart_money_rotation",
		venue=config.venue,
		observed_at=latest_observed_at,
		netflow_15m_usd=netflow_15m_usd,
		smart_money_inflow_usd=smart_money_inflow_usd,
		smart_money_outflow_usd=smart_money_outflow_usd,
		unique_buyer_wallets_15m=unique_buyer_wallets_15m,
		unique_seller_wallets_15m=unique_seller_wallets_15m,
		whale_buy_count_15m=whale_buy_count_15m,
		exchange_outflow_usd=exchange_outflow_usd,
		metadata={
			"provider": config.provider,
			"window_minutes": config.window_minutes,
			"source_name": config.source_name,
		},
	)