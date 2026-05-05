from __future__ import annotations

from datetime import UTC, datetime
from dataclasses import dataclass
from typing import Protocol

from eth_account import Account

from core.config import AppSettings, EvmLiveSourceConfig, resolve_evm_routes
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
from execution.evm_odos import EvmAssembledTransaction, EvmOdosQuoteState, OdosSwapClient
from execution.evm_rpc import EvmRpcClient


@dataclass(frozen=True)
class EvmWalletCredentials:
    wallet_address: str
    private_key: str


@dataclass(frozen=True)
class EvmSwapRequest:
    route: EvmLiveSourceConfig
    sell_token: str
    buy_token: str
    sell_amount_atomic: int


@dataclass(frozen=True)
class EvmApprovalRequirement:
    token_contract: str
    spender: str
    required_amount_atomic: int


class EvmSwapClient(Protocol):
    def quote(
        self,
        *,
        wallet_address: str,
        sell_token: str,
        buy_token: str,
        sell_amount_atomic: int,
        slippage_bps: int,
    ) -> EvmOdosQuoteState:
        ...

    def assemble(self, *, path_id: str, wallet_address: str) -> EvmAssembledTransaction:
        ...

    def token_price_usd(self, token_contract: str) -> float:
        ...


class EvmDexAdapter(ExecutionAdapter):
    adapter_name = "evm_dex_live"

    def __init__(
        self,
        settings: AppSettings,
        rpc_client: EvmRpcClient | None = None,
        swap_client: EvmSwapClient | None = None,
    ) -> None:
        self.settings = settings
        self.rpc_client = rpc_client
        self.swap_client = swap_client

    def prepare(self, intent: ExecutionIntent, risk: RiskDecision) -> PreparedExecution:
        quote = self.quote(intent, risk)
        return PreparedExecution(
            intent=intent,
            quote=quote,
            adapter_name=self.adapter_name,
            requested_notional_usd=risk.adjusted_notional_usd,
            simulation=False,
        )

    def quote(self, intent: ExecutionIntent, risk: RiskDecision) -> ExecutionQuote:
        route = _match_evm_route(self.settings, intent)
        if route.api_provider != "odos":
            raise ValueError(f"unsupported_evm_execution_provider:{route.api_provider}")
        wallet = self._require_wallet_credentials(intent.chain)
        swap_request = self._build_swap_request(intent, risk.adjusted_notional_usd, route)
        quote_state = self._swap_client(route).quote(
            wallet_address=wallet.wallet_address,
            sell_token=swap_request.sell_token,
            buy_token=swap_request.buy_token,
            sell_amount_atomic=swap_request.sell_amount_atomic,
            slippage_bps=intent.max_slippage_bps,
        )
        return ExecutionQuote(
            quote_id=f"evm-quote:{intent.intent_id}",
            venue_type=VenueType.DEX,
            venue=intent.venue,
            estimated_notional_usd=risk.adjusted_notional_usd,
            estimated_slippage_bps=intent.max_slippage_bps,
            reasons=[
                f"odos_path:{quote_state.path_id}",
                f"odos_out_atomic:{quote_state.out_amount_atomic}",
            ],
            timestamp=datetime.now(UTC),
        )

    def execute(self, prepared: PreparedExecution) -> ExecutionReport:
        route = _match_evm_route(self.settings, prepared.intent)
        wallet = self._require_wallet_credentials(prepared.intent.chain)
        swap_request = self._build_swap_request(
            prepared.intent,
            prepared.requested_notional_usd,
            route,
        )
        quote_state = self._swap_client(route).quote(
            wallet_address=wallet.wallet_address,
            sell_token=swap_request.sell_token,
            buy_token=swap_request.buy_token,
            sell_amount_atomic=swap_request.sell_amount_atomic,
            slippage_bps=prepared.intent.max_slippage_bps,
        )
        assembled = self._swap_client(route).assemble(
            path_id=quote_state.path_id,
            wallet_address=wallet.wallet_address,
        )
        self._ensure_token_approval(
            wallet=wallet,
            swap_request=swap_request,
            assembled=assembled,
            route=route,
        )
        signed_transaction = _sign_assembled_transaction(
            assembled.transaction,
            private_key=wallet.private_key,
            chain_id=route.chain_id,
            nonce=self._rpc_client(route).transaction_count(wallet.wallet_address),
            current_base_fee_per_gas=_latest_base_fee_per_gas(self._rpc_client(route)),
        )
        submission = self._rpc_client(route).send_raw_transaction(signed_transaction)
        if not self._rpc_client(route).confirm_submission(submission.transaction_hash):
            raise ValueError("evm_swap_unconfirmed")
        return ExecutionReport(
            intent_id=prepared.intent.intent_id,
            venue_type=VenueType.DEX,
            venue=prepared.intent.venue,
            adapter_name=self.adapter_name,
            external_order_id=submission.external_order_id,
            quote_id=prepared.quote.quote_id,
            status="FILLED",
            executed_notional_usd=prepared.requested_notional_usd,
            message=f"evm_dex_live_fill:{submission.transport}",
            simulation=False,
            timestamp=datetime.now(UTC),
        )

    def _swap_client(self, route: EvmLiveSourceConfig) -> EvmSwapClient:
        if self.swap_client is not None:
            return self.swap_client
        return OdosSwapClient(self.settings, route)

    def _rpc_client(self, route: EvmLiveSourceConfig) -> EvmRpcClient:
        if self.rpc_client is not None:
            return self.rpc_client
        return EvmRpcClient(self.settings.venues, chain=route.chain)

    def _build_swap_request(
        self,
        intent: ExecutionIntent,
        requested_notional_usd: float,
        route: EvmLiveSourceConfig,
    ) -> EvmSwapRequest:
        if intent.action == ActionType.BUY:
            return EvmSwapRequest(
                route=route,
                sell_token=str(route.quote_contract),
                buy_token=str(route.token_contract),
                sell_amount_atomic=max(1, int(round(requested_notional_usd * (10**route.quote_decimals)))),
            )
        if intent.action == ActionType.EXIT:
            token_price_usd = self._swap_client(route).token_price_usd(str(route.token_contract))
            if token_price_usd <= 0:
                raise ValueError("invalid_evm_exit_price")
            token_amount = requested_notional_usd / token_price_usd
            return EvmSwapRequest(
                route=route,
                sell_token=str(route.token_contract),
                buy_token=str(route.quote_contract),
                sell_amount_atomic=max(1, int(round(token_amount * (10**route.token_decimals)))),
            )
        raise ValueError(f"unsupported_evm_action:{intent.action.value}")

    def _require_wallet_credentials(self, chain: str) -> EvmWalletCredentials:
        credentials = self.settings.live.credentials.chain_wallets.get(chain)
        if isinstance(credentials, dict):
            wallet_address = credentials.get("wallet_address")
            private_key = credentials.get("private_key")
        elif credentials is not None:
            wallet_address = credentials.wallet_address
            private_key = credentials.private_key
        else:
            wallet_address = None
            private_key = None
        if not private_key:
            raise ValueError(f"evm_live_wallet_credentials_missing:{chain}")
        signer = Account.from_key(private_key)
        signer_address = signer.address
        if wallet_address and wallet_address.lower() != signer_address.lower():
            raise ValueError(f"evm_live_wallet_address_mismatch:{chain}")
        return EvmWalletCredentials(
            wallet_address=wallet_address or signer_address,
            private_key=private_key,
        )

    def _ensure_token_approval(
        self,
        *,
        wallet: EvmWalletCredentials,
        swap_request: EvmSwapRequest,
        assembled: EvmAssembledTransaction,
        route: EvmLiveSourceConfig,
    ) -> None:
        approval = _approval_requirement(swap_request, assembled)
        if approval is None:
            return
        rpc_client = self._rpc_client(route)
        current_allowance = _erc20_allowance(
            rpc_client,
            token_contract=approval.token_contract,
            owner_address=wallet.wallet_address,
            spender=approval.spender,
        )
        if current_allowance >= approval.required_amount_atomic:
            return
        approval_transaction = _build_erc20_approve_transaction(
            token_contract=approval.token_contract,
            spender=approval.spender,
            current_base_fee_per_gas=_latest_base_fee_per_gas(rpc_client),
            reference_transaction=assembled.transaction,
            from_address=wallet.wallet_address,
            nonce=rpc_client.transaction_count(wallet.wallet_address),
            gas_limit=_estimate_approval_gas(
                rpc_client,
                token_contract=approval.token_contract,
                from_address=wallet.wallet_address,
                spender=approval.spender,
            ),
        )
        signed_approval = _sign_assembled_transaction(
            approval_transaction,
            private_key=wallet.private_key,
            chain_id=route.chain_id,
            nonce=approval_transaction["nonce"],
            current_base_fee_per_gas=_latest_base_fee_per_gas(rpc_client),
        )
        submission = rpc_client.send_raw_transaction(signed_approval)
        if not rpc_client.confirm_submission(submission.transaction_hash):
            raise ValueError("evm_approval_unconfirmed")


def _match_evm_route(settings: AppSettings, intent: ExecutionIntent) -> EvmLiveSourceConfig:
    exact_key = f"{intent.chain}:{intent.token}".lower()
    for route_key, route in resolve_evm_routes(settings.acquisition).items():
        normalized_key = route_key.replace("_", ":").lower()
        if route.source_type != "quote":
            continue
        if normalized_key == exact_key:
            return route
        if route.chain == intent.chain and route.token == intent.token:
            return route
    raise ValueError(f"missing_evm_route_config:{intent.chain}:{intent.token}")


def _sign_assembled_transaction(
    transaction: dict[str, object],
    *,
    private_key: str,
    chain_id: int,
    nonce: int,
    current_base_fee_per_gas: int | None = None,
) -> str:
    to_address = transaction.get("to")
    if not isinstance(to_address, str) or not to_address.startswith("0x"):
        raise ValueError("invalid_evm_assembled_transaction")
    data = transaction.get("data")
    if not isinstance(data, str) or not data.startswith("0x"):
        raise ValueError("invalid_evm_assembled_transaction")
    gas = _coerce_quantity(transaction.get("gas"))
    if gas is None:
        gas = _coerce_quantity(transaction.get("gasLimit"))
    if gas is None:
        raise ValueError("invalid_evm_assembled_transaction")
    tx: dict[str, int | str | bytes] = {
        "to": to_address,
        "data": data,
        "value": _coerce_quantity(transaction.get("value")) or 0,
        "gas": gas,
        "nonce": nonce,
        "chainId": chain_id,
    }
    gas_price = _coerce_quantity(transaction.get("gasPrice"))
    max_fee_per_gas = _coerce_quantity(transaction.get("maxFeePerGas"))
    max_priority_fee_per_gas = _coerce_quantity(transaction.get("maxPriorityFeePerGas"))
    if gas_price is not None:
        tx["gasPrice"] = gas_price
    else:
        if max_fee_per_gas is not None:
            max_fee_per_gas = _apply_eip1559_fee_floor(
                max_fee_per_gas,
                max_priority_fee_per_gas=max_priority_fee_per_gas,
                current_base_fee_per_gas=current_base_fee_per_gas,
            )
            tx["maxFeePerGas"] = max_fee_per_gas
        if max_priority_fee_per_gas is not None:
            tx["maxPriorityFeePerGas"] = max_priority_fee_per_gas
        if "maxFeePerGas" in tx or "maxPriorityFeePerGas" in tx:
            tx["type"] = 2
    signed = Account.sign_transaction(tx, private_key)
    raw_transaction = getattr(signed, "raw_transaction", None)
    if raw_transaction is None:
        raw_transaction = getattr(signed, "rawTransaction", None)
    if raw_transaction is None:
        raise ValueError("invalid_evm_signed_transaction")
    return raw_transaction.hex()


def _coerce_quantity(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        if value.startswith("0x"):
            return int(value, 16)
        return int(value)
    return None


def _latest_base_fee_per_gas(rpc_client: object) -> int | None:
    latest_base_fee = getattr(rpc_client, "latest_base_fee_per_gas", None)
    if latest_base_fee is None:
        return None
    return latest_base_fee()


def _approval_requirement(
    swap_request: EvmSwapRequest,
    assembled: EvmAssembledTransaction,
) -> EvmApprovalRequirement | None:
    spender = _approval_spender(assembled)
    if spender is None:
        return None
    return EvmApprovalRequirement(
        token_contract=swap_request.sell_token,
        spender=spender,
        required_amount_atomic=swap_request.sell_amount_atomic,
    )


def _approval_spender(assembled: EvmAssembledTransaction) -> str | None:
    raw_response = getattr(assembled, "raw_response", {})
    if not isinstance(raw_response, dict):
        raw_response = {}
    direct_spender = raw_response.get("spender")
    if isinstance(direct_spender, str) and direct_spender.startswith("0x"):
        return direct_spender
    allowance_target = raw_response.get("allowanceTarget")
    if isinstance(allowance_target, str) and allowance_target.startswith("0x"):
        return allowance_target
    approval_target = raw_response.get("approvalTarget")
    if isinstance(approval_target, str) and approval_target.startswith("0x"):
        return approval_target
    transaction_to = assembled.transaction.get("to")
    if isinstance(transaction_to, str) and transaction_to.startswith("0x"):
        return transaction_to
    return None


def _erc20_allowance(
    rpc_client: object,
    *,
    token_contract: str,
    owner_address: str,
    spender: str,
) -> int:
    call_method = getattr(rpc_client, "call", None)
    if call_method is None:
        return (2**256) - 1
    result = call_method(
        {
            "to": token_contract,
            "data": _erc20_allowance_call_data(owner_address, spender),
        }
    )
    if not isinstance(result, str) or not result.startswith("0x"):
        raise ValueError("invalid_evm_allowance_response")
    return int(result, 16)


def _estimate_approval_gas(
    rpc_client: object,
    *,
    token_contract: str,
    from_address: str,
    spender: str,
) -> int:
    estimate_method = getattr(rpc_client, "estimate_gas", None)
    if estimate_method is None:
        return 100_000
    return estimate_method(
        {
            "from": from_address,
            "to": token_contract,
            "data": _erc20_approve_call_data(spender, (2**256) - 1),
            "value": "0x0",
        }
    )


def _build_erc20_approve_transaction(
    *,
    token_contract: str,
    spender: str,
    current_base_fee_per_gas: int | None,
    reference_transaction: dict[str, object],
    from_address: str,
    nonce: int,
    gas_limit: int,
) -> dict[str, object]:
    transaction: dict[str, object] = {
        "from": from_address,
        "to": token_contract,
        "data": _erc20_approve_call_data(spender, (2**256) - 1),
        "value": "0x0",
        "gas": hex(gas_limit),
        "nonce": nonce,
    }
    gas_price = reference_transaction.get("gasPrice")
    if gas_price is not None:
        transaction["gasPrice"] = gas_price
        return transaction
    max_fee_per_gas = _coerce_quantity(reference_transaction.get("maxFeePerGas"))
    max_priority_fee_per_gas = _coerce_quantity(reference_transaction.get("maxPriorityFeePerGas"))
    if max_fee_per_gas is not None:
        transaction["maxFeePerGas"] = hex(
            _apply_eip1559_fee_floor(
                max_fee_per_gas,
                max_priority_fee_per_gas=max_priority_fee_per_gas,
                current_base_fee_per_gas=current_base_fee_per_gas,
            )
        )
    if max_priority_fee_per_gas is not None:
        transaction["maxPriorityFeePerGas"] = hex(max_priority_fee_per_gas)
    return transaction


def _erc20_allowance_call_data(owner_address: str, spender: str) -> str:
    return _erc20_selector_payload("dd62ed3e", owner_address, spender)


def _erc20_approve_call_data(spender: str, amount: int) -> str:
    return _erc20_selector_payload("095ea7b3", spender, hex(amount))


def _erc20_selector_payload(selector: str, first_arg: str, second_arg: str) -> str:
    first_word = _abi_word(first_arg)
    second_word = _abi_word(second_arg)
    return f"0x{selector}{first_word}{second_word}"


def _abi_word(value: str) -> str:
    normalized = value[2:] if value.startswith("0x") else value
    return normalized.lower().rjust(64, "0")


def _apply_eip1559_fee_floor(
    max_fee_per_gas: int,
    *,
    max_priority_fee_per_gas: int | None,
    current_base_fee_per_gas: int | None,
) -> int:
    if current_base_fee_per_gas is None:
        return max_fee_per_gas
    min_max_fee_per_gas = (current_base_fee_per_gas * 2) + max(
        max_priority_fee_per_gas or 0,
        0,
    )
    return max(max_fee_per_gas, min_max_fee_per_gas)