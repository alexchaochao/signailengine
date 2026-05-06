from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator

SCHEMA_VERSION = "v1"


class TokenState(str, Enum):
    UNKNOWN = "UNKNOWN"
    PRE_LAUNCH = "PRE_LAUNCH"
    EARLY_LIQUIDITY = "EARLY_LIQUIDITY"
    NARRATIVE_EXPLOSION = "NARRATIVE_EXPLOSION"
    CEX_LISTING = "CEX_LISTING"
    TRENDING = "TRENDING"
    DISTRIBUTION = "DISTRIBUTION"
    DEAD = "DEAD"


class VenueType(str, Enum):
    DEX = "DEX"
    CEX = "CEX"
    NO_TRADE = "NO_TRADE"


class ActionType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    EXIT = "EXIT"
    HOLD = "HOLD"


class EventEnvelope(BaseModel):
    schema_version: str = Field(default=SCHEMA_VERSION)
    event_id: str
    event_type: str
    source: str
    chain: str
    token: str
    observed_at: datetime
    ingested_at: datetime
    payload: dict[str, Any] = Field(default_factory=dict)


class TokenSignal(BaseModel):
    schema_version: str = Field(default=SCHEMA_VERSION)
    token: str
    chain: str
    state_candidate: TokenState
    features: dict[str, float | int | bool] = Field(default_factory=dict)
    sub_scores: dict[str, float] = Field(default_factory=dict)
    alpha_score: float
    reasons: list[str] = Field(default_factory=list)
    timestamp: int

    @field_validator("alpha_score")
    @classmethod
    def validate_alpha_score(cls, value: float) -> float:
        if not 0 <= value <= 1:
            raise ValueError("alpha_score must be between 0 and 1")
        return value

    @field_validator("sub_scores")
    @classmethod
    def validate_sub_scores(cls, value: dict[str, float]) -> dict[str, float]:
        for name, score in value.items():
            if not 0 <= score <= 1:
                raise ValueError(f"sub_score {name} must be between 0 and 1")
        return value


class ExecutionIntent(BaseModel):
    schema_version: str = Field(default=SCHEMA_VERSION)
    intent_id: str
    token: str
    chain: str
    venue_type: VenueType
    venue: str
    action: ActionType
    confidence: float
    target_notional_usd: float
    max_slippage_bps: int
    state: TokenState
    strategy: str
    reasons: list[str] = Field(default_factory=list)

    @field_validator("confidence")
    @classmethod
    def validate_confidence(cls, value: float) -> float:
        if not 0 <= value <= 1:
            raise ValueError("confidence must be between 0 and 1")
        return value

    @field_validator("target_notional_usd")
    @classmethod
    def validate_target_notional(cls, value: float) -> float:
        if value < 0:
            raise ValueError("target_notional_usd must be non-negative")
        return value


class RiskDecision(BaseModel):
    schema_version: str = Field(default=SCHEMA_VERSION)
    intent_id: str
    allowed: bool
    adjusted_notional_usd: float
    violations: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    timestamp: datetime
    fsm_context: FsmContext | None = None

    @field_validator("adjusted_notional_usd")
    @classmethod
    def validate_adjusted_notional(cls, value: float) -> float:
        if value < 0:
            raise ValueError("adjusted_notional_usd must be non-negative")
        return value


class PositionState(BaseModel):
    is_open: bool = False
    venue_type: VenueType = VenueType.NO_TRADE
    token_exposure: float = 0.0
    last_exit_timestamp: int | None = None


class VenueStatus(BaseModel):
    dex_ready: bool = True
    cex_ready: bool = True
    degraded: bool = False


class PortfolioSnapshot(BaseModel):
    total_portfolio_usd: float = 10000.0
    token_exposure: float = 0.0
    chain_exposure: float = 0.0
    open_positions: int = 0
    daily_pnl_fraction: float = 0.0


class OrderRecord(BaseModel):
    schema_version: str = Field(default=SCHEMA_VERSION)
    intent_id: str
    token: str
    venue_type: VenueType
    venue: str
    action: ActionType
    state: TokenState
    confidence: float
    requested_notional_usd: float
    adjusted_notional_usd: float
    status: str
    created_at: datetime


class StateTransition(BaseModel):
    previous_state: TokenState
    new_state: TokenState
    changed: bool
    reasons: list[str] = Field(default_factory=list)
    timestamp: int


class FsmContext(BaseModel):
    chain: str
    token: str
    previous_state: TokenState
    current_state: TokenState
    changed: bool
    reasons: list[str] = Field(default_factory=list)
    last_transition_timestamp: int | None = None


class SocialQueryRequest(BaseModel):
    request_id: str
    source_name: str | None = None
    platform: str | None = None
    chain: str
    token: str
    query: str | None = None
    mode: str = "confirmation"
    requested_at: datetime
    candidate_id: str | None = None
    fsm_context: FsmContext | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ExecutionReport(BaseModel):
    schema_version: str = Field(default=SCHEMA_VERSION)
    intent_id: str
    venue_type: VenueType
    venue: str
    adapter_name: str = ""
    external_order_id: str | None = None
    quote_id: str | None = None
    status: str
    executed_notional_usd: float
    message: str
    simulation: bool = False
    timestamp: datetime
    fsm_context: FsmContext | None = None


class ExecutionQuote(BaseModel):
    schema_version: str = Field(default=SCHEMA_VERSION)
    quote_id: str
    venue_type: VenueType
    venue: str
    estimated_notional_usd: float
    estimated_slippage_bps: int
    valid: bool = True
    reasons: list[str] = Field(default_factory=list)
    timestamp: datetime


class PreparedExecution(BaseModel):
    schema_version: str = Field(default=SCHEMA_VERSION)
    intent: ExecutionIntent
    quote: ExecutionQuote
    adapter_name: str
    requested_notional_usd: float
    simulation: bool = True


class ExecutionLedgerEntry(BaseModel):
    schema_version: str = Field(default=SCHEMA_VERSION)
    intent_id: str
    token: str
    venue_type: VenueType
    venue: str
    stage: str
    status: str
    notional_usd: float
    message: str
    timestamp: datetime
    fsm_context: FsmContext | None = None


class DeadLetterRecord(BaseModel):
    schema_version: str = Field(default=SCHEMA_VERSION)
    source_stream: str
    message_id: str
    kind: str
    reason: str
    payload: dict[str, Any] = Field(default_factory=dict)
    replay_count: int = 0
    failed_at: datetime


class ReconciliationResult(BaseModel):
    schema_version: str = Field(default=SCHEMA_VERSION)
    intent_id: str
    position: PositionState
    portfolio: PortfolioSnapshot
    applied: bool
    reasons: list[str] = Field(default_factory=list)
    timestamp: datetime
    fsm_context: FsmContext | None = None


class RawEventRecord(BaseModel):
    id: str | None = None
    source_type: str
    source_name: str
    source_event_id: str
    chain: str | None = None
    token: str | None = None
    observed_at: datetime
    ingested_at: datetime
    cursor: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    payload_hash: str | None = None
    replayable: bool = True
    schema_version: str = Field(default=SCHEMA_VERSION)
    created_at: datetime | None = None


class CollectorCheckpoint(BaseModel):
    checkpoint_key: str
    cursor: str
    observed_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    updated_at: datetime | None = None


class FeatureSnapshot(BaseModel):
    id: str | None = None
    chain: str
    token: str
    feature_name: str
    feature_value: float
    window_name: str
    as_of: datetime
    sample_count: int
    freshness_seconds: float
    quality_flag: str
    formula_version: str
    inputs: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None


class FeatureQualityRecord(BaseModel):
    id: str | None = None
    chain: str
    token: str
    feature_name: str
    as_of: datetime
    freshness_seconds: float
    source_lag_seconds: float
    missing_sources: list[str] = Field(default_factory=list)
    degraded_reason: str | None = None
    created_at: datetime | None = None


class DexTradeFact(BaseModel):
    trade_id: str
    chain: str
    token: str
    pool_address: str
    wallet_address: str | None = None
    side: str
    token_amount: float
    quote_amount_usd: float
    observed_at: datetime
    source_event_id: str
    classification_version: str


class TokenTradeWindow(BaseModel):
    chain: str
    token: str
    window_name: str
    window_end: datetime
    buy_notional_usd: float
    sell_notional_usd: float
    trade_count: int
    unique_wallets: int
    updated_at: datetime | None = None


class DexQuoteSample(BaseModel):
    quote_id: str
    chain: str
    token: str
    quote_notional_usd: float
    expected_out_usd: float
    reference_mid_usd: float
    slippage_bps: float
    route_summary: dict[str, Any] = Field(default_factory=dict)
    quoted_at: datetime
    source_event_id: str


class SlippageCurve(BaseModel):
    chain: str
    token: str
    curve_as_of: datetime
    sample_points: list[dict[str, float]] = Field(default_factory=list)
    curve_version: str
    freshness_seconds: float
    updated_at: datetime | None = None


class MeasurementProfile(BaseModel):
    """Temporary on-chain measurement profile registered from a discovery event.

    A profile links a discovered token (from launch/catalyst events) to
    the on-chain addresses needed to build live measurement sources.
    """

    profile_id: str
    chain: str
    token: str
    discovery_event_type: str
    discovery_event_id: str
    registered_at: datetime
    ttl_seconds: float = 3600.0

    # On-chain addresses — populated after resolution
    pool_address: str | None = None
    token_mint: str | None = None
    quote_mint: str | None = None
    token_contract: str | None = None
    quote_contract: str | None = None
    dex: str | None = None

    # "solana" or "evm" — set after resolution
    chain_type: str | None = None