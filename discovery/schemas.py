from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator

from core.schemas import SCHEMA_VERSION


class AlphaCandidateStatus(str, Enum):
    OBSERVED = "OBSERVED"
    QUALIFIED = "QUALIFIED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


class AlphaType(str, Enum):
    LAUNCH = "LAUNCH"
    CATALYST = "CATALYST"
    FLOW = "FLOW"


class LaunchPoolSnapshot(BaseModel):
    schema_version: str = Field(default=SCHEMA_VERSION)
    source_event_id: str
    chain: str
    token: str
    pool_address: str
    dex: str
    quote_asset: str
    observed_at: datetime
    initial_liquidity_usd: float
    liquidity_lock_ratio: float | None = None
    buy_notional_5m_usd: float = 0.0
    trade_count_5m: int = 0
    unique_wallets_5m: int = 0
    smart_money_wallets_5m: int = 0
    creator_hold_pct: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("initial_liquidity_usd", "buy_notional_5m_usd")
    @classmethod
    def validate_non_negative_float(cls, value: float) -> float:
        if value < 0:
            raise ValueError("launch snapshot values must be non-negative")
        return value

    @field_validator("trade_count_5m", "unique_wallets_5m", "smart_money_wallets_5m")
    @classmethod
    def validate_non_negative_int(cls, value: int) -> int:
        if value < 0:
            raise ValueError("launch snapshot counts must be non-negative")
        return value


class AlphaCandidate(BaseModel):
    schema_version: str = Field(default=SCHEMA_VERSION)
    candidate_id: str
    alpha_type: AlphaType
    chain: str
    token: str
    pool_address: str
    dex: str
    quote_asset: str
    status: AlphaCandidateStatus
    score: float
    first_seen_at: datetime
    last_seen_at: datetime
    initial_liquidity_usd: float
    liquidity_lock_ratio: float | None = None
    buy_notional_5m_usd: float = 0.0
    trade_count_5m: int = 0
    unique_wallets_5m: int = 0
    smart_money_wallets_5m: int = 0
    creator_hold_pct: float | None = None
    reasons: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @field_validator("score")
    @classmethod
    def validate_score(cls, value: float) -> float:
        if not 0 <= value <= 1:
            raise ValueError("score must be between 0 and 1")
        return value


class AlphaCandidateEvent(BaseModel):
    schema_version: str = Field(default=SCHEMA_VERSION)
    event_id: str
    candidate_id: str
    event_type: str
    observed_at: datetime
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None


class AlphaSnapshot(BaseModel):
    schema_version: str = Field(default=SCHEMA_VERSION)
    snapshot_id: str
    candidate_id: str
    alpha_type: AlphaType
    chain: str
    token: str
    observed_at: datetime
    status: AlphaCandidateStatus
    score: float
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None


class CatalystEventSnapshot(BaseModel):
    schema_version: str = Field(default=SCHEMA_VERSION)
    source_event_id: str
    chain: str
    token: str
    catalyst_type: str
    headline: str
    observed_at: datetime
    impact_score: float
    credibility_score: float
    lead_time_minutes: int = 0
    venue: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("impact_score", "credibility_score")
    @classmethod
    def validate_scores(cls, value: float) -> float:
        if not 0 <= value <= 1:
            raise ValueError("catalyst scores must be between 0 and 1")
        return value

    @field_validator("lead_time_minutes")
    @classmethod
    def validate_lead_time(cls, value: int) -> int:
        if value < 0:
            raise ValueError("lead_time_minutes must be non-negative")
        return value


class FlowActivitySnapshot(BaseModel):
    schema_version: str = Field(default=SCHEMA_VERSION)
    source_event_id: str
    chain: str
    token: str
    flow_type: str = "smart_money_rotation"
    venue: str | None = None
    observed_at: datetime
    netflow_15m_usd: float
    smart_money_inflow_usd: float
    smart_money_outflow_usd: float = 0.0
    unique_buyer_wallets_15m: int = 0
    unique_seller_wallets_15m: int = 0
    whale_buy_count_15m: int = 0
    exchange_outflow_usd: float = 0.0
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator(
        "netflow_15m_usd",
        "smart_money_inflow_usd",
        "smart_money_outflow_usd",
        "exchange_outflow_usd",
    )
    @classmethod
    def validate_non_negative_flow_float(cls, value: float) -> float:
        if value < 0:
            raise ValueError("flow snapshot values must be non-negative")
        return value

    @field_validator(
        "unique_buyer_wallets_15m",
        "unique_seller_wallets_15m",
        "whale_buy_count_15m",
    )
    @classmethod
    def validate_non_negative_flow_int(cls, value: int) -> int:
        if value < 0:
            raise ValueError("flow snapshot counts must be non-negative")
        return value
