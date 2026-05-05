from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.schemas import (
    DexQuoteSample,
    DexTradeFact,
    FeatureQualityRecord,
    FeatureSnapshot,
    SlippageCurve,
    TokenTradeWindow,
)


class FeatureStore:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def save_snapshot(self, snapshot: FeatureSnapshot) -> FeatureSnapshot:
        normalized = snapshot.model_copy(
            update={
                "id": snapshot.id or str(uuid4()),
                "as_of": snapshot.as_of.astimezone(UTC),
                "created_at": (snapshot.created_at or datetime.now(UTC)).astimezone(UTC),
            }
        )
        statement = text(
            "INSERT INTO feature_snapshots ("
            "id, chain, token, feature_name, feature_value, window_name, as_of, sample_count, "
            "freshness_seconds, quality_flag, formula_version, inputs, created_at"
            ") VALUES ("
            ":id, :chain, :token, :feature_name, :feature_value, :window_name, :as_of, :sample_count, "
            ":freshness_seconds, :quality_flag, :formula_version, :inputs, :created_at"
            ") ON CONFLICT (chain, token, feature_name, window_name, as_of, formula_version) DO UPDATE SET "
            "feature_value = EXCLUDED.feature_value, "
            "sample_count = EXCLUDED.sample_count, "
            "freshness_seconds = EXCLUDED.freshness_seconds, "
            "quality_flag = EXCLUDED.quality_flag, "
            "inputs = EXCLUDED.inputs, "
            "created_at = EXCLUDED.created_at"
        )
        with self.engine.begin() as connection:
            connection.execute(
                statement,
                {
                    "id": normalized.id,
                    "chain": normalized.chain,
                    "token": normalized.token,
                    "feature_name": normalized.feature_name,
                    "feature_value": normalized.feature_value,
                    "window_name": normalized.window_name,
                    "as_of": normalized.as_of,
                    "sample_count": normalized.sample_count,
                    "freshness_seconds": normalized.freshness_seconds,
                    "quality_flag": normalized.quality_flag,
                    "formula_version": normalized.formula_version,
                    "inputs": json.dumps(normalized.inputs, sort_keys=True),
                    "created_at": normalized.created_at,
                },
            )

        existing = self.load_snapshot(
            normalized.chain,
            normalized.token,
            normalized.feature_name,
            normalized.window_name,
            normalized.as_of,
            normalized.formula_version,
        )
        if existing is None:
            raise RuntimeError("feature_snapshot_persist_failed")
        return existing

    def save_quality(self, record: FeatureQualityRecord) -> FeatureQualityRecord:
        normalized = record.model_copy(
            update={
                "id": record.id or str(uuid4()),
                "as_of": record.as_of.astimezone(UTC),
                "created_at": (record.created_at or datetime.now(UTC)).astimezone(UTC),
            }
        )
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO feature_quality ("
                    "id, chain, token, feature_name, as_of, freshness_seconds, source_lag_seconds, "
                    "missing_sources, degraded_reason, created_at"
                    ") VALUES ("
                    ":id, :chain, :token, :feature_name, :as_of, :freshness_seconds, :source_lag_seconds, "
                    ":missing_sources, :degraded_reason, :created_at"
                    ")"
                ),
                {
                    "id": normalized.id,
                    "chain": normalized.chain,
                    "token": normalized.token,
                    "feature_name": normalized.feature_name,
                    "as_of": normalized.as_of,
                    "freshness_seconds": normalized.freshness_seconds,
                    "source_lag_seconds": normalized.source_lag_seconds,
                    "missing_sources": json.dumps(normalized.missing_sources, sort_keys=True),
                    "degraded_reason": normalized.degraded_reason,
                    "created_at": normalized.created_at,
                },
            )
        return normalized

    def load_latest_quality(
        self,
        chain: str,
        token: str,
        feature_name: str,
    ) -> FeatureQualityRecord | None:
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT id, chain, token, feature_name, as_of, freshness_seconds, source_lag_seconds, "
                    "missing_sources, degraded_reason, created_at "
                    "FROM feature_quality WHERE chain = :chain AND token = :token "
                    "AND feature_name = :feature_name ORDER BY as_of DESC, created_at DESC LIMIT 1"
                ),
                {
                    "chain": chain,
                    "token": token,
                    "feature_name": feature_name,
                },
            ).mappings().first()

        if row is None:
            return None

        return FeatureQualityRecord(
            id=str(row["id"]),
            chain=str(row["chain"]),
            token=str(row["token"]),
            feature_name=str(row["feature_name"]),
            as_of=row["as_of"],
            freshness_seconds=float(row["freshness_seconds"]),
            source_lag_seconds=float(row["source_lag_seconds"]),
            missing_sources=list(json.loads(str(row["missing_sources"] or "[]"))),
            degraded_reason=(
                str(row["degraded_reason"]) if row["degraded_reason"] is not None else None
            ),
            created_at=row["created_at"],
        )

    def load_latest_snapshot(
        self,
        chain: str,
        token: str,
        feature_name: str,
        window_name: str,
    ) -> FeatureSnapshot | None:
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT id, chain, token, feature_name, feature_value, window_name, as_of, sample_count, "
                    "freshness_seconds, quality_flag, formula_version, inputs, created_at "
                    "FROM feature_snapshots WHERE chain = :chain AND token = :token "
                    "AND feature_name = :feature_name AND window_name = :window_name "
                    "ORDER BY as_of DESC, created_at DESC LIMIT 1"
                ),
                {
                    "chain": chain,
                    "token": token,
                    "feature_name": feature_name,
                    "window_name": window_name,
                },
            ).mappings().first()

        if row is None:
            return None

        return _row_to_feature_snapshot(row)

    def load_snapshot(
        self,
        chain: str,
        token: str,
        feature_name: str,
        window_name: str,
        as_of: datetime,
        formula_version: str,
    ) -> FeatureSnapshot | None:
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT id, chain, token, feature_name, feature_value, window_name, as_of, sample_count, "
                    "freshness_seconds, quality_flag, formula_version, inputs, created_at "
                    "FROM feature_snapshots WHERE chain = :chain AND token = :token "
                    "AND feature_name = :feature_name AND window_name = :window_name "
                    "AND as_of = :as_of AND formula_version = :formula_version"
                ),
                {
                    "chain": chain,
                    "token": token,
                    "feature_name": feature_name,
                    "window_name": window_name,
                    "as_of": as_of.astimezone(UTC),
                    "formula_version": formula_version,
                },
            ).mappings().first()

        if row is None:
            return None

        return _row_to_feature_snapshot(row)

    def upsert_trade_fact(self, trade: DexTradeFact) -> DexTradeFact:
        statement = (
            text(
                "INSERT OR REPLACE INTO dex_trade_facts ("
                "trade_id, chain, token, pool_address, wallet_address, side, token_amount, "
                "quote_amount_usd, observed_at, source_event_id, classification_version"
                ") VALUES ("
                ":trade_id, :chain, :token, :pool_address, :wallet_address, :side, :token_amount, "
                ":quote_amount_usd, :observed_at, :source_event_id, :classification_version"
                ")"
            )
            if self.engine.dialect.name == "sqlite"
            else text(
                "INSERT INTO dex_trade_facts ("
                "trade_id, chain, token, pool_address, wallet_address, side, token_amount, "
                "quote_amount_usd, observed_at, source_event_id, classification_version"
                ") VALUES ("
                ":trade_id, :chain, :token, :pool_address, :wallet_address, :side, :token_amount, "
                ":quote_amount_usd, :observed_at, :source_event_id, :classification_version"
                ") ON CONFLICT (trade_id) DO UPDATE SET "
                "wallet_address = EXCLUDED.wallet_address, "
                "side = EXCLUDED.side, "
                "token_amount = EXCLUDED.token_amount, "
                "quote_amount_usd = EXCLUDED.quote_amount_usd, "
                "observed_at = EXCLUDED.observed_at, "
                "source_event_id = EXCLUDED.source_event_id, "
                "classification_version = EXCLUDED.classification_version"
            )
        )
        with self.engine.begin() as connection:
            connection.execute(
                statement,
                {
                    "trade_id": trade.trade_id,
                    "chain": trade.chain,
                    "token": trade.token,
                    "pool_address": trade.pool_address,
                    "wallet_address": trade.wallet_address,
                    "side": trade.side,
                    "token_amount": trade.token_amount,
                    "quote_amount_usd": trade.quote_amount_usd,
                    "observed_at": trade.observed_at.astimezone(UTC),
                    "source_event_id": trade.source_event_id,
                    "classification_version": trade.classification_version,
                },
            )
        return trade

    def load_trade_facts(
        self,
        chain: str,
        token: str,
        *,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
        limit: int = 5000,
    ) -> list[DexTradeFact]:
        where_clauses = ["chain = :chain", "token = :token"]
        params: dict[str, object] = {"chain": chain, "token": token, "limit": limit}
        if start_at is not None:
            where_clauses.append("observed_at > :start_at")
            params["start_at"] = start_at.astimezone(UTC)
        if end_at is not None:
            where_clauses.append("observed_at <= :end_at")
            params["end_at"] = end_at.astimezone(UTC)
        where_sql = " AND ".join(where_clauses)

        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    "SELECT trade_id, chain, token, pool_address, wallet_address, side, token_amount, "
                    "quote_amount_usd, observed_at, source_event_id, classification_version "
                    f"FROM dex_trade_facts WHERE {where_sql} "
                    "ORDER BY observed_at ASC, trade_id ASC LIMIT :limit"
                ),
                params,
            ).mappings().all()

        return [
            DexTradeFact(
                trade_id=str(row["trade_id"]),
                chain=str(row["chain"]),
                token=str(row["token"]),
                pool_address=str(row["pool_address"]),
                wallet_address=(
                    str(row["wallet_address"]) if row["wallet_address"] is not None else None
                ),
                side=str(row["side"]),
                token_amount=float(row["token_amount"]),
                quote_amount_usd=float(row["quote_amount_usd"]),
                observed_at=_coerce_timestamp(row["observed_at"]),
                source_event_id=str(row["source_event_id"]),
                classification_version=str(row["classification_version"]),
            )
            for row in rows
        ]

    def upsert_trade_window(self, window: TokenTradeWindow) -> TokenTradeWindow:
        normalized = window.model_copy(
            update={
                "window_end": window.window_end.astimezone(UTC),
                "updated_at": (window.updated_at or datetime.now(UTC)).astimezone(UTC),
            }
        )
        statement = (
            text(
                "INSERT OR REPLACE INTO token_trade_windows ("
                "chain, token, window_name, window_end, buy_notional_usd, sell_notional_usd, "
                "trade_count, unique_wallets, updated_at"
                ") VALUES ("
                ":chain, :token, :window_name, :window_end, :buy_notional_usd, :sell_notional_usd, "
                ":trade_count, :unique_wallets, :updated_at"
                ")"
            )
            if self.engine.dialect.name == "sqlite"
            else text(
                "INSERT INTO token_trade_windows ("
                "chain, token, window_name, window_end, buy_notional_usd, sell_notional_usd, "
                "trade_count, unique_wallets, updated_at"
                ") VALUES ("
                ":chain, :token, :window_name, :window_end, :buy_notional_usd, :sell_notional_usd, "
                ":trade_count, :unique_wallets, :updated_at"
                ") ON CONFLICT (chain, token, window_name, window_end) DO UPDATE SET "
                "buy_notional_usd = EXCLUDED.buy_notional_usd, "
                "sell_notional_usd = EXCLUDED.sell_notional_usd, "
                "trade_count = EXCLUDED.trade_count, "
                "unique_wallets = EXCLUDED.unique_wallets, "
                "updated_at = EXCLUDED.updated_at"
            )
        )
        with self.engine.begin() as connection:
            connection.execute(
                statement,
                {
                    "chain": normalized.chain,
                    "token": normalized.token,
                    "window_name": normalized.window_name,
                    "window_end": normalized.window_end,
                    "buy_notional_usd": normalized.buy_notional_usd,
                    "sell_notional_usd": normalized.sell_notional_usd,
                    "trade_count": normalized.trade_count,
                    "unique_wallets": normalized.unique_wallets,
                    "updated_at": normalized.updated_at,
                },
            )
        return normalized

    def load_trade_window(
        self,
        chain: str,
        token: str,
        window_name: str,
        window_end: datetime,
    ) -> TokenTradeWindow | None:
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT chain, token, window_name, window_end, buy_notional_usd, sell_notional_usd, "
                    "trade_count, unique_wallets, updated_at FROM token_trade_windows "
                    "WHERE chain = :chain AND token = :token AND window_name = :window_name "
                    "AND window_end = :window_end"
                ),
                {
                    "chain": chain,
                    "token": token,
                    "window_name": window_name,
                    "window_end": window_end.astimezone(UTC),
                },
            ).mappings().first()

        if row is None:
            return None

        return TokenTradeWindow(
            chain=str(row["chain"]),
            token=str(row["token"]),
            window_name=str(row["window_name"]),
            window_end=_coerce_timestamp(row["window_end"]),
            buy_notional_usd=float(row["buy_notional_usd"]),
            sell_notional_usd=float(row["sell_notional_usd"]),
            trade_count=int(row["trade_count"]),
            unique_wallets=int(row["unique_wallets"]),
            updated_at=_coerce_timestamp(row["updated_at"]),
        )

    def append_quote_sample(self, sample: DexQuoteSample) -> DexQuoteSample:
        statement = (
            text(
                "INSERT OR REPLACE INTO dex_quote_samples ("
                "quote_id, chain, token, quote_notional_usd, expected_out_usd, reference_mid_usd, "
                "slippage_bps, route_summary, quoted_at, source_event_id"
                ") VALUES ("
                ":quote_id, :chain, :token, :quote_notional_usd, :expected_out_usd, :reference_mid_usd, "
                ":slippage_bps, :route_summary, :quoted_at, :source_event_id"
                ")"
            )
            if self.engine.dialect.name == "sqlite"
            else text(
                "INSERT INTO dex_quote_samples ("
                "quote_id, chain, token, quote_notional_usd, expected_out_usd, reference_mid_usd, "
                "slippage_bps, route_summary, quoted_at, source_event_id"
                ") VALUES ("
                ":quote_id, :chain, :token, :quote_notional_usd, :expected_out_usd, :reference_mid_usd, "
                ":slippage_bps, :route_summary, :quoted_at, :source_event_id"
                ") ON CONFLICT (quote_id) DO UPDATE SET "
                "expected_out_usd = EXCLUDED.expected_out_usd, "
                "reference_mid_usd = EXCLUDED.reference_mid_usd, "
                "slippage_bps = EXCLUDED.slippage_bps, "
                "route_summary = EXCLUDED.route_summary, "
                "quoted_at = EXCLUDED.quoted_at, "
                "source_event_id = EXCLUDED.source_event_id"
            )
        )
        with self.engine.begin() as connection:
            connection.execute(
                statement,
                {
                    "quote_id": sample.quote_id,
                    "chain": sample.chain,
                    "token": sample.token,
                    "quote_notional_usd": sample.quote_notional_usd,
                    "expected_out_usd": sample.expected_out_usd,
                    "reference_mid_usd": sample.reference_mid_usd,
                    "slippage_bps": sample.slippage_bps,
                    "route_summary": json.dumps(sample.route_summary, sort_keys=True),
                    "quoted_at": sample.quoted_at.astimezone(UTC),
                    "source_event_id": sample.source_event_id,
                },
            )
        return sample

    def load_quote_samples(
        self,
        chain: str,
        token: str,
        *,
        limit: int = 100,
    ) -> list[DexQuoteSample]:
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    "SELECT quote_id, chain, token, quote_notional_usd, expected_out_usd, "
                    "reference_mid_usd, slippage_bps, route_summary, quoted_at, source_event_id "
                    "FROM dex_quote_samples WHERE chain = :chain AND token = :token "
                    "ORDER BY quoted_at DESC, quote_id DESC LIMIT :limit"
                ),
                {"chain": chain, "token": token, "limit": limit},
            ).mappings().all()

        return [
            DexQuoteSample(
                quote_id=str(row["quote_id"]),
                chain=str(row["chain"]),
                token=str(row["token"]),
                quote_notional_usd=float(row["quote_notional_usd"]),
                expected_out_usd=float(row["expected_out_usd"]),
                reference_mid_usd=float(row["reference_mid_usd"]),
                slippage_bps=float(row["slippage_bps"]),
                route_summary=_json_loads_dict(row["route_summary"]),
                quoted_at=_coerce_timestamp(row["quoted_at"]),
                source_event_id=str(row["source_event_id"]),
            )
            for row in rows
        ]

    def upsert_slippage_curve(self, curve: SlippageCurve) -> SlippageCurve:
        normalized = curve.model_copy(
            update={
                "curve_as_of": curve.curve_as_of.astimezone(UTC),
                "updated_at": (curve.updated_at or datetime.now(UTC)).astimezone(UTC),
            }
        )
        statement = (
            text(
                "INSERT OR REPLACE INTO slippage_curves ("
                "chain, token, curve_as_of, sample_points, curve_version, freshness_seconds, updated_at"
                ") VALUES ("
                ":chain, :token, :curve_as_of, :sample_points, :curve_version, :freshness_seconds, :updated_at"
                ")"
            )
            if self.engine.dialect.name == "sqlite"
            else text(
                "INSERT INTO slippage_curves ("
                "chain, token, curve_as_of, sample_points, curve_version, freshness_seconds, updated_at"
                ") VALUES ("
                ":chain, :token, :curve_as_of, :sample_points, :curve_version, :freshness_seconds, :updated_at"
                ") ON CONFLICT (chain, token, curve_as_of, curve_version) DO UPDATE SET "
                "sample_points = EXCLUDED.sample_points, "
                "freshness_seconds = EXCLUDED.freshness_seconds, "
                "updated_at = EXCLUDED.updated_at"
            )
        )
        with self.engine.begin() as connection:
            connection.execute(
                statement,
                {
                    "chain": normalized.chain,
                    "token": normalized.token,
                    "curve_as_of": normalized.curve_as_of,
                    "sample_points": json.dumps(normalized.sample_points, sort_keys=True),
                    "curve_version": normalized.curve_version,
                    "freshness_seconds": normalized.freshness_seconds,
                    "updated_at": normalized.updated_at,
                },
            )
        return normalized

    def load_latest_slippage_curve(self, chain: str, token: str) -> SlippageCurve | None:
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT chain, token, curve_as_of, sample_points, curve_version, freshness_seconds, updated_at "
                    "FROM slippage_curves WHERE chain = :chain AND token = :token "
                    "ORDER BY curve_as_of DESC, updated_at DESC LIMIT 1"
                ),
                {"chain": chain, "token": token},
            ).mappings().first()

        if row is None:
            return None

        return SlippageCurve(
            chain=str(row["chain"]),
            token=str(row["token"]),
            curve_as_of=_coerce_timestamp(row["curve_as_of"]),
            sample_points=list(json.loads(str(row["sample_points"]))),
            curve_version=str(row["curve_version"]),
            freshness_seconds=float(row["freshness_seconds"]),
            updated_at=_coerce_timestamp(row["updated_at"]),
        )


def _row_to_feature_snapshot(row: object) -> FeatureSnapshot:
    mapping = dict(row)
    return FeatureSnapshot(
        id=str(mapping["id"]),
        chain=str(mapping["chain"]),
        token=str(mapping["token"]),
        feature_name=str(mapping["feature_name"]),
        feature_value=float(mapping["feature_value"]),
        window_name=str(mapping["window_name"]),
        as_of=_coerce_timestamp(mapping["as_of"]),
        sample_count=int(mapping["sample_count"]),
        freshness_seconds=float(mapping["freshness_seconds"]),
        quality_flag=str(mapping["quality_flag"]),
        formula_version=str(mapping["formula_version"]),
        inputs=_json_loads_dict(mapping["inputs"]),
        created_at=_coerce_timestamp(mapping["created_at"]),
    )


def _json_loads_dict(payload: object) -> dict[str, object]:
    if isinstance(payload, str):
        parsed = json.loads(payload)
        if isinstance(parsed, dict):
            return parsed
    return {}


def _coerce_timestamp(value: object) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    raise ValueError("invalid_timestamp_value")