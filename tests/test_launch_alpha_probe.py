from __future__ import annotations

from datetime import UTC, datetime

from core.config import AppSettings, LaunchAlphaLiveSourceConfig
from discovery.live_sources import HttpLaunchSnapshotSource
from discovery.probe import build_parser, run_launch_alpha_probe


def test_launch_alpha_probe_parser_supports_chain_limit_and_json() -> None:
    parser = build_parser()

    args = parser.parse_args(["--chain", "solana", "--limit", "3", "--json"])

    assert args.chain == "solana"
    assert args.limit == 3
    assert args.json is True


def test_launch_alpha_probe_filters_sources_and_limits_results() -> None:
    now = datetime.now(UTC)

    class StubSource(HttpLaunchSnapshotSource):
        def __init__(self, chain: str, source_name: str, token: str) -> None:
            config = LaunchAlphaLiveSourceConfig(
                enabled=True,
                source_name=source_name,
                chain=chain,
                provider="http_snapshot_json",
                source_url="https://example.invalid",
            )
            super().__init__(AppSettings.load(), config, transport=lambda url, timeout_seconds: {"records": []})
            self._token = token

        def fetch_snapshots(self):
            from discovery.schemas import LaunchPoolSnapshot

            return [
                LaunchPoolSnapshot(
                    source_event_id=f"{self.config.chain}:{self._token}",
                    chain=self.config.chain,
                    token=self._token,
                    pool_address=f"pool-{self._token}",
                    dex="raydium",
                    quote_asset="USDC",
                    observed_at=now,
                    initial_liquidity_usd=100000.0,
                    buy_notional_5m_usd=30000.0,
                    trade_count_5m=20,
                    unique_wallets_5m=15,
                )
            ]

    snapshots = run_launch_alpha_probe(
        AppSettings.load(),
        chain="solana",
        limit=1,
        sources=[
            StubSource("solana", "launch_alpha_solana", "AAA"),
            StubSource("base", "launch_alpha_base", "BBB"),
        ],
    )

    assert len(snapshots) == 1
    assert snapshots[0].chain == "solana"
    assert snapshots[0].token == "AAA"