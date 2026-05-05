from __future__ import annotations

import base64
import json
from datetime import UTC, datetime
from dataclasses import dataclass

from solders.keypair import Keypair
from solders.message import to_bytes_versioned
from solders.transaction import VersionedTransaction

from core.config import AppSettings, JupiterQuoteSourceConfig, VenueConfig
from core.schemas import (
    ActionType,
    ExecutionIntent,
    ExecutionQuote,
    ExecutionReport,
    PreparedExecution,
    RiskDecision,
    VenueType,
)
from execution.base import ExecutionAdapter
from execution.solana_jupiter import JupiterSwapClient
from execution.solana_rpc import SolanaRpcClient
from portfolio.price_provider import PriceProvider, build_price_provider


SOL_NATIVE_MINT = "So11111111111111111111111111111111111111112"


@dataclass(frozen=True)
class SolanaWalletCredentials:
    wallet_address: str
    private_key: str


@dataclass(frozen=True)
class SolanaSwapRequest:
    route: JupiterQuoteSourceConfig
    input_mint: str
    output_mint: str
    amount_atomic: int


class SolanaDexAdapter(ExecutionAdapter):
    adapter_name = "solana_dex_stub"

    def __init__(
        self,
        config: AppSettings | VenueConfig,
        rpc_client: SolanaRpcClient | None = None,
        jupiter_client: JupiterSwapClient | None = None,
        price_provider: PriceProvider | None = None,
    ) -> None:
        self.settings = config if isinstance(config, AppSettings) else None
        self.config = config.venues if isinstance(config, AppSettings) else config
        self.rpc_client = rpc_client or SolanaRpcClient(self.config)
        self.jupiter_client = jupiter_client
        self.price_provider = price_provider or (
            build_price_provider(self.settings) if self.settings is not None else None
        )
        if self._live_execution_enabled():
            self.adapter_name = "solana_dex_live"

    def prepare(self, intent: ExecutionIntent, risk: RiskDecision) -> PreparedExecution:
        quote = self.quote(intent, risk)
        return PreparedExecution(
            intent=intent,
            quote=quote,
            adapter_name=self.adapter_name,
            requested_notional_usd=risk.adjusted_notional_usd,
            simulation=not self._live_execution_enabled(),
        )

    def quote(self, intent: ExecutionIntent, risk: RiskDecision) -> ExecutionQuote:
        quote_context = self.rpc_client.quote_context()
        reasons = [f"rpc:{quote_context.rpc_url}"]
        if quote_context.jito_enabled:
            reasons.append("jito_enabled")
        estimated_notional = risk.adjusted_notional_usd
        estimated_slippage_bps = quote_context.slippage_bps

        if self._live_execution_enabled():
            swap_request = self._build_swap_request(intent, risk.adjusted_notional_usd)
            jupiter_quote = self._jupiter_client_for_route(swap_request.route).quote(
                input_mint=swap_request.input_mint,
                output_mint=swap_request.output_mint,
                amount_atomic=swap_request.amount_atomic,
                slippage_bps=intent.max_slippage_bps,
            )
            reasons.append(f"jupiter:{jupiter_quote.input_mint}->{jupiter_quote.output_mint}")
            reasons.append(f"jupiter_out_atomic:{jupiter_quote.out_amount_atomic}")
            estimated_slippage_bps = max(
                int(round(jupiter_quote.price_impact_pct * 10_000)),
                quote_context.slippage_bps,
            )

        return ExecutionQuote(
            quote_id=f"solana-quote:{intent.intent_id}",
            venue_type=VenueType.DEX,
            venue=intent.venue,
            estimated_notional_usd=estimated_notional,
            estimated_slippage_bps=estimated_slippage_bps,
            reasons=reasons,
            timestamp=datetime.now(UTC),
        )

    def execute(self, prepared: PreparedExecution) -> ExecutionReport:
        if self._live_execution_enabled() and not prepared.simulation:
            swap_request = self._build_swap_request(prepared.intent, prepared.requested_notional_usd)
            wallet = self._require_wallet_credentials()
            jupiter_quote = self._jupiter_client_for_route(swap_request.route).quote(
                input_mint=swap_request.input_mint,
                output_mint=swap_request.output_mint,
                amount_atomic=swap_request.amount_atomic,
                slippage_bps=prepared.intent.max_slippage_bps,
            )
            swap_transaction = self._jupiter_client_for_route(swap_request.route).swap_transaction(
                quote_response=jupiter_quote.raw_quote,
                user_public_key=wallet.wallet_address,
            )
            signed_transaction = _sign_serialized_transaction(
                swap_transaction,
                wallet.private_key,
            )
            submission = self.rpc_client.submit_order(
                prepared.intent.intent_id,
                signed_transaction=signed_transaction,
            )
            if not self.rpc_client.confirm_submission(submission.signature):
                raise ValueError("solana_swap_unconfirmed")
            return ExecutionReport(
                intent_id=prepared.intent.intent_id,
                venue_type=VenueType.DEX,
                venue=prepared.intent.venue,
                adapter_name=self.adapter_name,
                external_order_id=submission.external_order_id,
                quote_id=prepared.quote.quote_id,
                status="FILLED",
                executed_notional_usd=prepared.requested_notional_usd,
                message=f"solana_dex_live_fill:{submission.transport}",
                simulation=False,
                timestamp=datetime.now(UTC),
            )

        simulated_fill = prepared.simulation
        submission = self.rpc_client.submit_order(prepared.intent.intent_id)
        return ExecutionReport(
            intent_id=prepared.intent.intent_id,
            venue_type=VenueType.DEX,
            venue=prepared.intent.venue,
            adapter_name=self.adapter_name,
            external_order_id=submission.external_order_id,
            quote_id=prepared.quote.quote_id,
            status="FILLED" if simulated_fill else "SUBMITTED",
            executed_notional_usd=prepared.requested_notional_usd if simulated_fill else 0.0,
            message=(
                f"solana_dex_simulated_fill:{submission.transport}"
                if simulated_fill
                else f"solana_dex_submitted_stub:{submission.transport}"
            ),
            simulation=prepared.simulation,
            timestamp=datetime.now(UTC),
        )

    def _jupiter_client_for_route(self, route: JupiterQuoteSourceConfig) -> JupiterSwapClient:
        if self.jupiter_client is not None:
            return self.jupiter_client
        return JupiterSwapClient(route)

    def _live_execution_enabled(self) -> bool:
        if self.settings is None:
            return False
        return (
            self.settings.runtime.environment == "live"
            and self.settings.risk.live_trading_enabled
            and not self.settings.venues.paper_execution_enabled
        )

    def _build_swap_request(self, intent: ExecutionIntent, requested_notional_usd: float) -> SolanaSwapRequest:
        route = self._resolve_route_config(intent)
        if intent.action == ActionType.BUY:
            return SolanaSwapRequest(
                route=route,
                input_mint=route.input_mint,
                output_mint=route.output_mint,
                amount_atomic=max(1, int(round(requested_notional_usd * (10**route.input_decimals)))),
            )
        if intent.action == ActionType.EXIT:
            token_price_usd = self._token_price_usd(route)
            if token_price_usd <= 0:
                raise ValueError("invalid_solana_exit_price")
            token_amount = requested_notional_usd / token_price_usd
            return SolanaSwapRequest(
                route=route,
                input_mint=route.output_mint,
                output_mint=route.input_mint,
                amount_atomic=max(1, int(round(token_amount * (10**route.output_decimals)))),
            )
        raise ValueError(f"unsupported_solana_action:{intent.action.value}")

    def _token_price_usd(self, route: JupiterQuoteSourceConfig) -> float:
        if route.output_mint == SOL_NATIVE_MINT and self.price_provider is not None:
            price_snapshot = self.price_provider.get_native_token_price_usd("solana")
            if price_snapshot is not None:
                return price_snapshot.price
        return self._jupiter_client_for_route(route).price_usd(route.output_mint)

    def _resolve_route_config(self, intent: ExecutionIntent) -> JupiterQuoteSourceConfig:
        if self.settings is None:
            raise ValueError("solana_live_route_config_unavailable")
        route = _match_jupiter_route(self.settings, intent)
        if not route.input_mint or not route.output_mint:
            raise ValueError("missing_solana_route_mints")
        return route

    def _require_wallet_credentials(self) -> SolanaWalletCredentials:
        if self.settings is None:
            raise ValueError("solana_live_credentials_unavailable")
        credentials = self.settings.live.credentials.chain_wallets.get("solana")
        if isinstance(credentials, dict):
            wallet_address = credentials.get("wallet_address")
            private_key = credentials.get("private_key")
        elif credentials is not None:
            wallet_address = credentials.wallet_address
            private_key = credentials.private_key
        else:
            wallet_address = None
            private_key = None
        if not wallet_address or not private_key:
            raise ValueError("solana_live_wallet_credentials_missing")
        return SolanaWalletCredentials(
            wallet_address=wallet_address,
            private_key=private_key,
        )


def _sign_serialized_transaction(serialized_transaction: str, private_key: str) -> str:
    raw_transaction = VersionedTransaction.from_bytes(base64.b64decode(serialized_transaction))
    keypair = _load_keypair(private_key)
    signed_transaction = VersionedTransaction.populate(
        raw_transaction.message,
        [keypair.sign_message(to_bytes_versioned(raw_transaction.message))],
    )
    return base64.b64encode(bytes(signed_transaction)).decode("utf-8")


def _load_keypair(private_key: str) -> Keypair:
    normalized = private_key.strip()
    if normalized.startswith("["):
        key_material = json.loads(normalized)
        if not isinstance(key_material, list) or not all(isinstance(value, int) for value in key_material):
            raise ValueError("invalid_solana_private_key")
        key_bytes = bytes(key_material)
        if len(key_bytes) == 32:
            return Keypair.from_seed(key_bytes)
        return Keypair.from_bytes(key_bytes)
    try:
        return Keypair.from_base58_string(normalized)
    except ValueError as exc:
        raise ValueError("invalid_solana_private_key") from exc


def _match_jupiter_route(
    settings: AppSettings,
    intent: ExecutionIntent,
) -> JupiterQuoteSourceConfig:
    route_candidates = settings.acquisition.jupiter_quote_routes
    if route_candidates:
        exact_key = f"{intent.chain}:{intent.token}".lower()
        for route_key, route in route_candidates.items():
            normalized_key = route_key.replace("_", ":").lower()
            if normalized_key == exact_key:
                return route
            if route.chain == intent.chain and route.token == intent.token:
                return route

    fallback_route = settings.acquisition.jupiter_quote
    if fallback_route.chain == intent.chain and fallback_route.token == intent.token:
        return fallback_route
    raise ValueError(f"missing_solana_route_config:{intent.chain}:{intent.token}")