from __future__ import annotations

import argparse
import json
from pathlib import Path

from core.config import AppSettings
from discovery.live_sources import HttpLaunchSnapshotSource, build_launch_live_sources
from discovery.schemas import LaunchPoolSnapshot


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Probe configured launch alpha live sources")
    parser.add_argument("--settings", type=Path)
    parser.add_argument("--chain")
    parser.add_argument("--source-name")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--json", action="store_true")
    return parser


def run_launch_alpha_probe(
    settings: AppSettings,
    *,
    chain: str | None = None,
    source_name: str | None = None,
    limit: int = 10,
    sources: list[HttpLaunchSnapshotSource] | None = None,
) -> list[LaunchPoolSnapshot]:
    selected_sources = sources if sources is not None else build_launch_live_sources(settings)
    snapshots: list[LaunchPoolSnapshot] = []
    for source in selected_sources:
        configured_name = source.config.source_name or ""
        if source_name is not None and configured_name != source_name:
            continue
        if chain is not None and source.config.chain != chain:
            continue
        snapshots.extend(source.fetch_snapshots())
        if len(snapshots) >= limit:
            break
    return snapshots[:limit]


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = AppSettings.load(args.settings) if args.settings else AppSettings.load()
    snapshots = run_launch_alpha_probe(
        settings,
        chain=args.chain,
        source_name=args.source_name,
        limit=args.limit,
    )
    if args.json:
        print(json.dumps([snapshot.model_dump(mode="json") for snapshot in snapshots], indent=2))
    else:
        for snapshot in snapshots:
            print(
                " ".join(
                    [
                        f"chain={snapshot.chain}",
                        f"token={snapshot.token}",
                        f"dex={snapshot.dex}",
                        f"pool={snapshot.pool_address}",
                        f"liquidity_usd={snapshot.initial_liquidity_usd}",
                        f"buy_notional_5m_usd={snapshot.buy_notional_5m_usd}",
                        f"trade_count_5m={snapshot.trade_count_5m}",
                    ]
                )
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())