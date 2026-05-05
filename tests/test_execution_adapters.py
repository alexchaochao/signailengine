from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from urllib.error import URLError

import pytest

from core.config import AppSettings
from core.schemas import ActionType, ExecutionIntent, RiskDecision, TokenState, VenueType
from execution.base import ExecutionAdapter
from execution.cex_bridge import CexPaperExecutor
from execution.dex_executor import DexPaperExecutor
from execution.evm_adapter import (
    EvmDexAdapter,
    _apply_eip1559_fee_floor,
    _sign_assembled_transaction,
)
from execution.evm_rpc import EvmRpcClient
from execution.factory import build_cex_adapter, build_dex_adapter
from execution.solana_jupiter import JupiterQuoteState
from execution.solana_adapter import SolanaDexAdapter
from execution.solana_rpc import (
    SolanaHttpTransportResponse,
    SolanaQuoteContext,
    SolanaRpcClient,
    SolanaSubmissionResult,
)


def _solana_transport(
    responses: dict[str, Any] | None = None,
    failures: list[Exception] | None = None,
) -> Any:
    response_map = responses or {}
    failure_queue = list(failures or [])

    def transport(
        rpc_url: str,
        payload: dict[str, Any],
        timeout_seconds: float,
    ) -> SolanaHttpTransportResponse:
        _ = rpc_url, timeout_seconds
        if failure_queue:
            raise failure_queue.pop(0)

        method = payload["method"]
        if method in response_map:
            response = response_map[method]
            if not isinstance(response, SolanaHttpTransportResponse):
                raise TypeError("transport response must be SolanaHttpTransportResponse")
            return response
        if method == "getLatestBlockhash":
            return SolanaHttpTransportResponse(
                status_code=200,
                content_type="application/json",
                payload={
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "result": {
                        "context": {"slot": 1},
                        "value": {
                            "blockhash": "stub-blockhash",
                            "lastValidBlockHeight": 123,
                        },
                    },
                },
                body_size_bytes=128,
            )
        if method == "sendTransaction":
            signed_transaction = payload["params"][0]
            return SolanaHttpTransportResponse(
                status_code=200,
                content_type="application/json; charset=utf-8",
                payload={
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "result": str(signed_transaction).removeprefix("stub-signed:"),
                },
                body_size_bytes=96,
            )
        if method == "getBalance":
            return SolanaHttpTransportResponse(
                status_code=200,
                content_type="application/json; charset=utf-8",
                payload={
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "result": {
                        "context": {"slot": 1},
                        "value": 2_500_000_000,
                    },
                },
                body_size_bytes=96,
            )
        if method == "getSignatureStatuses":
            return SolanaHttpTransportResponse(
                status_code=200,
                content_type="application/json; charset=utf-8",
                payload={
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "result": {
                        "context": {"slot": 1},
                        "value": [
                            {
                                "slot": 1,
                                "confirmations": None,
                                "err": None,
                                "confirmationStatus": "confirmed",
                            }
                        ],
                    },
                },
                body_size_bytes=96,
            )
        raise AssertionError(f"unexpected method: {method}")

    return transport


def _evm_transport(
    responses: dict[str, SolanaHttpTransportResponse] | None = None,
    failures: list[Exception] | None = None,
) -> Any:
    failure_queue = list(failures or [])
    response_map = responses or {}

    def transport(
        rpc_url: str,
        payload: dict[str, Any],
        timeout_seconds: float,
    ) -> SolanaHttpTransportResponse:
        _ = rpc_url, timeout_seconds
        if failure_queue:
            raise failure_queue.pop(0)
        if payload["method"] in response_map:
            return response_map[payload["method"]]
        if payload["method"] == "eth_getTransactionCount":
            return SolanaHttpTransportResponse(
                status_code=200,
                content_type="application/json; charset=utf-8",
                payload={
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "result": hex(7),
                },
                body_size_bytes=96,
            )
        if payload["method"] == "eth_sendRawTransaction":
            return SolanaHttpTransportResponse(
                status_code=200,
                content_type="application/json; charset=utf-8",
                payload={
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "result": "0xabc123",
                },
                body_size_bytes=96,
            )
        if payload["method"] == "eth_getTransactionReceipt":
            return SolanaHttpTransportResponse(
                status_code=200,
                content_type="application/json; charset=utf-8",
                payload={
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "result": {"status": "0x1"},
                },
                body_size_bytes=96,
            )
        if payload["method"] == "eth_getBlockByNumber":
            return SolanaHttpTransportResponse(
                status_code=200,
                content_type="application/json; charset=utf-8",
                payload={
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "result": {"baseFeePerGas": hex(2_000_000)},
                },
                body_size_bytes=96,
            )
        if payload["method"] == "eth_call":
            return SolanaHttpTransportResponse(
                status_code=200,
                content_type="application/json; charset=utf-8",
                payload={
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "result": hex((2**256) - 1),
                },
                body_size_bytes=96,
            )
        if payload["method"] == "eth_estimateGas":
            return SolanaHttpTransportResponse(
                status_code=200,
                content_type="application/json; charset=utf-8",
                payload={
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "result": hex(100_000),
                },
                body_size_bytes=96,
            )
        if payload["method"] != "eth_getBalance":
            raise AssertionError(f"unexpected method: {payload['method']}")
        return SolanaHttpTransportResponse(
            status_code=200,
            content_type="application/json; charset=utf-8",
            payload={
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": hex(2_500_000_000_000_000_000),
            },
            body_size_bytes=96,
        )

    return transport


def _build_intent(venue_type: VenueType, venue: str) -> ExecutionIntent:
    return ExecutionIntent(
        intent_id="intent-1",
        token="BONK",
        chain="solana",
        venue_type=venue_type,
        venue=venue,
        action=ActionType.BUY,
        confidence=0.8,
        target_notional_usd=500.0,
        max_slippage_bps=100,
        state=TokenState.NARRATIVE_EXPLOSION,
        strategy="paper_test",
    )


def _build_risk() -> RiskDecision:
    return RiskDecision(
        intent_id="intent-1",
        allowed=True,
        adjusted_notional_usd=400.0,
        timestamp=datetime.now(UTC),
    )


def test_dex_paper_executor_returns_execution_report() -> None:
    executor = DexPaperExecutor()
    prepared = executor.prepare(
        _build_intent(VenueType.DEX, "solana_primary"),
        _build_risk(),
    )
    report = executor.execute(prepared)

    assert prepared.quote.quote_id.startswith("dex-quote:")
    assert report.venue_type == VenueType.DEX
    assert report.executed_notional_usd == 400.0
    assert report.status == "FILLED"
    assert report.adapter_name == "solana_dex_paper"
    assert report.external_order_id == "dex-paper:intent-1"
    assert report.quote_id == prepared.quote.quote_id
    assert report.simulation is True


def test_cex_paper_executor_returns_execution_report() -> None:
    executor = CexPaperExecutor()
    prepared = executor.prepare(
        _build_intent(VenueType.CEX, "binance_paper"),
        _build_risk(),
    )
    report = executor.execute(prepared)

    assert prepared.quote.quote_id.startswith("cex-quote:")
    assert report.venue_type == VenueType.CEX
    assert report.executed_notional_usd == 400.0
    assert report.status == "FILLED"
    assert report.adapter_name == "binance_cex_paper"
    assert report.external_order_id == "cex-paper:intent-1"
    assert report.quote_id == prepared.quote.quote_id
    assert report.simulation is True


def test_solana_dex_adapter_returns_quote_and_stubbed_submission() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "venues": AppSettings.load().venues.model_copy(
                update={
                    "dex_adapter": "solana_stub",
                    "paper_execution_enabled": False,
                    "solana_jito_enabled": True,
                }
            )
        }
    )
    adapter = SolanaDexAdapter(
        settings.venues,
        rpc_client=SolanaRpcClient(settings.venues, transport=_solana_transport()),
    )
    prepared = adapter.prepare(
        _build_intent(VenueType.DEX, "solana_primary"),
        _build_risk(),
    ).model_copy(update={"simulation": False})
    report = adapter.execute(prepared)

    assert prepared.quote.quote_id.startswith("solana-quote:")
    assert "jito_enabled" in prepared.quote.reasons
    assert report.adapter_name == "solana_dex_stub"
    assert report.external_order_id == "solana-submit:intent-1"
    assert report.status == "SUBMITTED"
    assert report.executed_notional_usd == 0.0
    assert report.simulation is False
    assert report.message == "solana_dex_submitted_stub:jito"


def test_dex_factory_selects_solana_stub_from_settings() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "venues": AppSettings.load().venues.model_copy(
                update={
                    "dex_adapter": "solana_stub",
                    "paper_execution_enabled": False,
                }
            )
        }
    )

    adapter = build_dex_adapter(settings)

    assert isinstance(adapter, ExecutionAdapter)
    assert isinstance(adapter, SolanaDexAdapter)


def test_dex_factory_selects_evm_primary_from_settings() -> None:
    base_settings = AppSettings.load()
    settings = base_settings.model_copy(
        update={
            "runtime": base_settings.runtime.model_copy(update={"environment": "live"}),
            "risk": base_settings.risk.model_copy(update={"live_trading_enabled": True}),
            "venues": base_settings.venues.model_copy(
                update={
                    "dex_adapter": "evm_primary",
                    "paper_execution_enabled": False,
                }
            ),
        }
    )

    adapter = build_dex_adapter(settings)

    assert isinstance(adapter, ExecutionAdapter)
    assert isinstance(adapter, EvmDexAdapter)


def test_solana_rpc_client_returns_quote_context_and_submission_result() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "venues": AppSettings.load().venues.model_copy(
                update={
                    "solana_rpc_url": "https://rpc.testnet.solana.example",
                    "solana_rpc_timeout_seconds": 7.5,
                    "solana_rpc_max_retries": 4,
                    "solana_quote_slippage_bps": 125,
                    "solana_jito_enabled": True,
                }
            )
        }
    )
    client = SolanaRpcClient(settings.venues, transport=_solana_transport())

    quote_request = client.build_quote_request()
    quote_context = client.quote_context()
    balance_request = client.build_balance_request("wallet-1")
    balance_state = client.wallet_balance("wallet-1")
    submit_request = client.build_submit_request("intent-1")
    submission = client.submit_order("intent-1")
    confirmed = client.confirm_submission("intent-1")

    assert quote_request.to_payload() == {
        "jsonrpc": "2.0",
        "id": "solana-quote-context",
        "method": "getLatestBlockhash",
        "params": [{"commitment": "processed"}],
    }
    assert quote_context.rpc_url == "https://rpc.testnet.solana.example"
    assert quote_context.slippage_bps == 125
    assert quote_context.jito_enabled is True
    assert quote_context.latest_blockhash == "stub-blockhash"
    assert quote_context.rpc_method == "getLatestBlockhash"
    assert client.timeout_seconds == 7.5
    assert client.max_retries == 4
    assert balance_request.to_payload() == {
        "jsonrpc": "2.0",
        "id": "solana-balance:wallet-1",
        "method": "getBalance",
        "params": ["wallet-1", {"commitment": "processed"}],
    }
    assert balance_state.wallet_address == "wallet-1"
    assert balance_state.lamports == 2_500_000_000
    assert submit_request.to_payload() == {
        "jsonrpc": "2.0",
        "id": "solana-submit:intent-1",
        "method": "sendTransaction",
        "params": [
            "stub-signed:intent-1",
            {
                "encoding": "base64",
                "skipPreflight": False,
                "maxRetries": 3,
            },
        ],
    }
    assert submission.external_order_id == "solana-submit:intent-1"
    assert submission.signature == "intent-1"
    assert submission.submitted is True
    assert submission.transport == "jito"
    assert confirmed is True


def test_solana_dex_adapter_executes_live_jupiter_swap_path(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeJupiterSwapClient:
        def __init__(self) -> None:
            self.quote_calls: list[dict[str, Any]] = []
            self.swap_quote_response: dict[str, Any] | None = None

        def quote(
            self,
            *,
            input_mint: str,
            output_mint: str,
            amount_atomic: int,
            slippage_bps: int,
        ) -> JupiterQuoteState:
            self.quote_calls.append(
                {
                    "input_mint": input_mint,
                    "output_mint": output_mint,
                    "amount_atomic": amount_atomic,
                    "slippage_bps": slippage_bps,
                }
            )
            return JupiterQuoteState(
                input_mint=input_mint,
                output_mint=output_mint,
                in_amount_atomic=amount_atomic,
                out_amount_atomic=123_456,
                price_impact_pct=0.008,
                raw_quote={"routePlan": [{"swapInfo": {"label": "Raydium"}}]},
            )

        def swap_transaction(self, *, quote_response: dict[str, Any], user_public_key: str) -> str:
            self.swap_quote_response = quote_response
            assert user_public_key == "wallet-address"
            return "serialized-swap"

        def price_usd(self, mint: str) -> float:
            _ = mint
            return 0.00002

    class FakeLiveSolanaRpcClient:
        def __init__(self) -> None:
            self.signed_transaction: str | None = None
            self.confirmed_signature: str | None = None

        def quote_context(self) -> SolanaQuoteContext:
            return SolanaQuoteContext(
                rpc_url="https://rpc.live.example",
                slippage_bps=100,
                jito_enabled=False,
                latest_blockhash="blockhash",
                rpc_method="getLatestBlockhash",
            )

        def submit_order(
            self,
            intent_id: str,
            *,
            signed_transaction: str | None = None,
        ) -> SolanaSubmissionResult:
            assert intent_id == "intent-1"
            self.signed_transaction = signed_transaction
            return SolanaSubmissionResult(
                external_order_id="solana-submit:live-signature",
                signature="live-signature",
                submitted=True,
                transport="rpc",
            )

        def confirm_submission(self, signature: str, *, confirmation_checks: int | None = None) -> bool:
            _ = confirmation_checks
            self.confirmed_signature = signature
            return True

    base_settings = AppSettings.load()
    settings = base_settings.model_copy(
        update={
            "runtime": base_settings.runtime.model_copy(update={"environment": "live"}),
            "risk": base_settings.risk.model_copy(update={"live_trading_enabled": True}),
            "venues": base_settings.venues.model_copy(update={"paper_execution_enabled": False}),
            "acquisition": base_settings.acquisition.model_copy(
                update={
                    "jupiter_quote_routes": {},
                    "jupiter_quote": base_settings.acquisition.jupiter_quote.model_copy(
                        update={
                            "token": "BONK",
                            "input_mint": "usdc-mint",
                            "output_mint": "bonk-mint",
                            "input_decimals": 6,
                            "output_decimals": 5,
                        }
                    )
                }
            ),
            "live": base_settings.live.model_copy(
                update={
                    "credentials": base_settings.live.credentials.model_copy(
                        update={
                            "chain_wallets": {
                                "solana": {
                                    "wallet_address": "wallet-address",
                                    "private_key": "private-key",
                                }
                            }
                        }
                    )
                }
            ),
        }
    )
    rpc_client = FakeLiveSolanaRpcClient()
    jupiter_client = FakeJupiterSwapClient()
    adapter = SolanaDexAdapter(settings, rpc_client=rpc_client, jupiter_client=jupiter_client)
    monkeypatch.setattr("execution.solana_adapter._sign_serialized_transaction", lambda tx, pk: f"signed:{tx}:{pk}")

    prepared = adapter.prepare(
        _build_intent(VenueType.DEX, "solana_primary"),
        _build_risk(),
    )
    report = adapter.execute(prepared)

    assert prepared.simulation is False
    assert prepared.adapter_name == "solana_dex_live"
    assert any(reason.startswith("jupiter:") for reason in prepared.quote.reasons)
    assert jupiter_client.quote_calls[0]["input_mint"] == "usdc-mint"
    assert rpc_client.signed_transaction == "signed:serialized-swap:private-key"
    assert rpc_client.confirmed_signature == "live-signature"
    assert report.status == "FILLED"
    assert report.executed_notional_usd == 400.0
    assert report.simulation is False
    assert report.message == "solana_dex_live_fill:rpc"


def test_solana_dex_adapter_uses_jupiter_route_table_for_matching_token() -> None:
    class FakeJupiterSwapClient:
        def __init__(self) -> None:
            self.quote_calls: list[dict[str, Any]] = []

        def quote(
            self,
            *,
            input_mint: str,
            output_mint: str,
            amount_atomic: int,
            slippage_bps: int,
        ) -> JupiterQuoteState:
            self.quote_calls.append(
                {
                    "input_mint": input_mint,
                    "output_mint": output_mint,
                    "amount_atomic": amount_atomic,
                    "slippage_bps": slippage_bps,
                }
            )
            return JupiterQuoteState(
                input_mint=input_mint,
                output_mint=output_mint,
                in_amount_atomic=amount_atomic,
                out_amount_atomic=123_456,
                price_impact_pct=0.008,
                raw_quote={"routePlan": []},
            )

        def swap_transaction(self, *, quote_response: dict[str, Any], user_public_key: str) -> str:
            _ = quote_response, user_public_key
            return "unused"

        def price_usd(self, mint: str) -> float:
            _ = mint
            return 0.1

    base_settings = AppSettings.load()
    settings = base_settings.model_copy(
        update={
            "runtime": base_settings.runtime.model_copy(update={"environment": "live"}),
            "risk": base_settings.risk.model_copy(update={"live_trading_enabled": True}),
            "venues": base_settings.venues.model_copy(update={"paper_execution_enabled": False}),
            "acquisition": base_settings.acquisition.model_copy(
                update={
                    "jupiter_quote": base_settings.acquisition.jupiter_quote.model_copy(
                        update={
                            "token": "DEFAULT",
                            "input_mint": "default-in",
                            "output_mint": "default-out",
                        }
                    ),
                    "jupiter_quote_routes": {
                        "solana_bonk": base_settings.acquisition.jupiter_quote.model_copy(
                            update={
                                "chain": "solana",
                                "token": "BONK",
                                "input_mint": "usdc-mint",
                                "output_mint": "bonk-mint",
                                "input_decimals": 6,
                                "output_decimals": 5,
                            }
                        )
                    },
                }
            ),
        }
    )
    adapter = SolanaDexAdapter(
        settings,
        rpc_client=type(
            "FakeRpcClient",
            (),
            {
                "quote_context": lambda self: SolanaQuoteContext(
                    rpc_url="https://rpc.live.example",
                    slippage_bps=100,
                    jito_enabled=False,
                    latest_blockhash="blockhash",
                    rpc_method="getLatestBlockhash",
                ),
            },
        )(),
        jupiter_client=FakeJupiterSwapClient(),
    )

    prepared = adapter.prepare(
        _build_intent(VenueType.DEX, "solana_primary"),
        _build_risk(),
    )

    assert prepared.simulation is False
    assert any(reason == "jupiter:usdc-mint->bonk-mint" for reason in prepared.quote.reasons)


def test_solana_dex_adapter_rejects_missing_route_table_match() -> None:
    base_settings = AppSettings.load()
    settings = base_settings.model_copy(
        update={
            "runtime": base_settings.runtime.model_copy(update={"environment": "live"}),
            "risk": base_settings.risk.model_copy(update={"live_trading_enabled": True}),
            "venues": base_settings.venues.model_copy(update={"paper_execution_enabled": False}),
            "acquisition": base_settings.acquisition.model_copy(
                update={
                    "jupiter_quote": base_settings.acquisition.jupiter_quote.model_copy(
                        update={
                            "chain": "solana",
                            "token": "NOT_BONK",
                            "input_mint": "default-in",
                            "output_mint": "default-out",
                        }
                    ),
                    "jupiter_quote_routes": {},
                }
            ),
        }
    )
    adapter = SolanaDexAdapter(
        settings,
        rpc_client=type(
            "FakeRpcClient",
            (),
            {
                "quote_context": lambda self: SolanaQuoteContext(
                    rpc_url="https://rpc.live.example",
                    slippage_bps=100,
                    jito_enabled=False,
                    latest_blockhash="blockhash",
                    rpc_method="getLatestBlockhash",
                ),
            },
        )(),
    )

    with pytest.raises(ValueError, match="missing_solana_route_config:solana:BONK"):
        adapter.prepare(
            _build_intent(VenueType.DEX, "solana_primary"),
            _build_risk(),
        )


def test_solana_dex_adapter_uses_native_price_provider_for_sol_exit() -> None:
    class FakeJupiterSwapClient:
        def quote(
            self,
            *,
            input_mint: str,
            output_mint: str,
            amount_atomic: int,
            slippage_bps: int,
        ) -> JupiterQuoteState:
            _ = slippage_bps
            return JupiterQuoteState(
                input_mint=input_mint,
                output_mint=output_mint,
                in_amount_atomic=amount_atomic,
                out_amount_atomic=123_456,
                price_impact_pct=0.002,
                raw_quote={"routePlan": []},
            )

        def swap_transaction(self, *, quote_response: dict[str, Any], user_public_key: str) -> str:
            _ = quote_response, user_public_key
            return "unused"

        def price_usd(self, mint: str) -> float:
            raise AssertionError(f"unexpected_jupiter_price_lookup:{mint}")

    class FakePriceProvider:
        def get_native_token_price_usd(self, chain: str):
            assert chain == "solana"
            return type("PriceSnapshot", (), {"price": 150.0})()

    base_settings = AppSettings.load()
    settings = base_settings.model_copy(
        update={
            "runtime": base_settings.runtime.model_copy(update={"environment": "live"}),
            "risk": base_settings.risk.model_copy(update={"live_trading_enabled": True}),
            "venues": base_settings.venues.model_copy(update={"paper_execution_enabled": False}),
            "acquisition": base_settings.acquisition.model_copy(
                update={
                    "jupiter_quote_routes": {
                        "solana_sol": base_settings.acquisition.jupiter_quote.model_copy(
                            update={
                                "chain": "solana",
                                "token": "SOL",
                                "input_mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                                "output_mint": "So11111111111111111111111111111111111111112",
                                "input_decimals": 6,
                                "output_decimals": 9,
                            }
                        )
                    },
                }
            ),
        }
    )
    adapter = SolanaDexAdapter(
        settings,
        rpc_client=type(
            "FakeRpcClient",
            (),
            {
                "quote_context": lambda self: SolanaQuoteContext(
                    rpc_url="https://rpc.live.example",
                    slippage_bps=100,
                    jito_enabled=False,
                    latest_blockhash="blockhash",
                    rpc_method="getLatestBlockhash",
                ),
            },
        )(),
        jupiter_client=FakeJupiterSwapClient(),
        price_provider=FakePriceProvider(),
    )
    intent = _build_intent(VenueType.DEX, "solana_primary").model_copy(
        update={"token": "SOL", "action": ActionType.EXIT}
    )
    prepared = adapter.prepare(intent, _build_risk())

    assert prepared.simulation is False
    assert any(reason == "jupiter:So11111111111111111111111111111111111111112->EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" for reason in prepared.quote.reasons)


def test_evm_rpc_client_returns_balance_state() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "venues": AppSettings.load().venues.model_copy(
                update={
                    "native_asset_rpc": {
                        "ethereum": {
                            "url": "https://rpc.ethereum.example",
                            "timeout_seconds": 6.5,
                            "max_retries": 3,
                        }
                    },
                }
            )
        }
    )
    client = EvmRpcClient(settings.venues, chain="ethereum", transport=_evm_transport())

    request_message = client.build_balance_request("0xabc")
    balance_state = client.wallet_balance("0xabc")

    assert request_message.to_payload() == {
        "jsonrpc": "2.0",
        "id": "evm-balance:0xabc",
        "method": "eth_getBalance",
        "params": ["0xabc", "latest"],
    }
    assert client.timeout_seconds == 6.5
    assert client.max_retries == 3
    assert balance_state.wallet_address == "0xabc"
    assert balance_state.wei_balance == 2_500_000_000_000_000_000


def test_evm_rpc_client_returns_nonce_submission_and_receipt() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "venues": AppSettings.load().venues.model_copy(
                update={
                    "native_asset_rpc": {
                        "base": {
                            "url": "https://rpc.base.example",
                            "timeout_seconds": 6.5,
                            "max_retries": 3,
                        }
                    },
                }
            )
        }
    )
    client = EvmRpcClient(settings.venues, chain="base", transport=_evm_transport())

    nonce_request = client.build_transaction_count_request("0xabc")
    submit_request = client.build_send_raw_transaction_request("0xsigned")
    normalized_submit_request = client.build_send_raw_transaction_request("signed")
    receipt_request = client.build_transaction_receipt_request("0xabc123")
    block_request = client.build_block_request("latest")

    assert nonce_request.to_payload() == {
        "jsonrpc": "2.0",
        "id": "evm-nonce:0xabc",
        "method": "eth_getTransactionCount",
        "params": ["0xabc", "pending"],
    }
    assert submit_request.to_payload() == {
        "jsonrpc": "2.0",
        "id": "evm-submit",
        "method": "eth_sendRawTransaction",
        "params": ["0xsigned"],
    }
    assert normalized_submit_request.to_payload() == {
        "jsonrpc": "2.0",
        "id": "evm-submit",
        "method": "eth_sendRawTransaction",
        "params": ["0xsigned"],
    }
    assert receipt_request.to_payload() == {
        "jsonrpc": "2.0",
        "id": "evm-receipt:0xabc123",
        "method": "eth_getTransactionReceipt",
        "params": ["0xabc123"],
    }
    assert block_request.to_payload() == {
        "jsonrpc": "2.0",
        "id": "evm-block:latest",
        "method": "eth_getBlockByNumber",
        "params": ["latest", False],
    }
    assert client.transaction_count("0xabc") == 7
    assert client.latest_base_fee_per_gas() == int("0x1e8480", 16)
    assert (
        client.call(
            {
                "to": "0x1111111111111111111111111111111111111111",
                "data": "0xdeadbeef",
            }
        )
        == hex((2**256) - 1)
    )
    assert client.estimate_gas({"to": "0x1111111111111111111111111111111111111111"}) == 100_000
    submission = client.send_raw_transaction("0xsigned")
    assert submission.transaction_hash == "0xabc123"
    assert submission.external_order_id == "evm-submit:0xabc123"
    assert client.confirm_submission("0xabc123") is True


def test_evm_dex_adapter_executes_live_odos_swap_path(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeOdosSwapClient:
        def __init__(self) -> None:
            self.quote_calls: list[dict[str, Any]] = []
            self.assemble_calls: list[dict[str, Any]] = []

        def quote(
            self,
            *,
            wallet_address: str,
            sell_token: str,
            buy_token: str,
            sell_amount_atomic: int,
            slippage_bps: int,
        ):
            self.quote_calls.append(
                {
                    "wallet_address": wallet_address,
                    "sell_token": sell_token,
                    "buy_token": buy_token,
                    "sell_amount_atomic": sell_amount_atomic,
                    "slippage_bps": slippage_bps,
                }
            )
            return type(
                "QuoteState",
                (),
                {
                    "path_id": "path-1",
                    "out_amount_atomic": 123456789,
                    "raw_quote": {"pathId": "path-1"},
                },
            )()

        def assemble(self, *, path_id: str, wallet_address: str):
            self.assemble_calls.append(
                {
                    "path_id": path_id,
                    "wallet_address": wallet_address,
                }
            )
            return type(
                "AssembledTransaction",
                (),
                {
                    "transaction": {
                        "to": "0x1111111111111111111111111111111111111111",
                        "data": "0xdeadbeef",
                        "value": "0x0",
                        "gas": "0x5208",
                        "gasPrice": "0x3b9aca00",
                    }
                },
            )()

        def token_price_usd(self, token_contract: str) -> float:
            _ = token_contract
            return 2.0

    class FakeLiveEvmRpcClient:
        def __init__(self) -> None:
            self.sent_transaction: str | None = None
            self.confirmed_hash: str | None = None

        def transaction_count(self, wallet_address: str) -> int:
            assert wallet_address == "0x19E7E376E7C213B7E7E7E46CC70A5DD086DAFF2A"
            return 7

        def latest_base_fee_per_gas(self) -> int | None:
            return None

        def send_raw_transaction(self, signed_transaction: str):
            self.sent_transaction = signed_transaction
            return type(
                "Submission",
                (),
                {
                    "external_order_id": "evm-submit:0xabc123",
                    "transaction_hash": "0xabc123",
                    "transport": "rpc",
                },
            )()

        def confirm_submission(self, transaction_hash: str) -> bool:
            self.confirmed_hash = transaction_hash
            return True

    base_settings = AppSettings.load()
    settings = base_settings.model_copy(
        update={
            "runtime": base_settings.runtime.model_copy(update={"environment": "live"}),
            "risk": base_settings.risk.model_copy(update={"live_trading_enabled": True}),
            "venues": base_settings.venues.model_copy(
                update={
                    "dex_adapter": "evm_primary",
                    "paper_execution_enabled": False,
                }
            ),
            "acquisition": base_settings.acquisition.model_copy(
                update={
                    "evm_sources": {
                        "base_aero": {
                            "enabled": True,
                            "source_type": "quote",
                            "chain": "base",
                            "chain_id": 8453,
                            "token": "AERO",
                            "token_contract": "0x940181a94A35A4569E4529A3CDfB74e38FD98631",
                            "quote_contract": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
                            "token_decimals": 18,
                            "quote_decimals": 6,
                            "api_provider": "odos",
                            "quote_api_url": "https://api.odos.xyz/sor/quote/v2",
                        }
                    }
                }
            ),
            "live": base_settings.live.model_copy(
                update={
                    "credentials": base_settings.live.credentials.model_copy(
                        update={
                            "chain_wallets": {
                                "base": {
                                    "wallet_address": "0x19E7E376E7C213B7E7E7E46CC70A5DD086DAFF2A",
                                    "private_key": "0x1111111111111111111111111111111111111111111111111111111111111111",
                                }
                            }
                        }
                    )
                }
            ),
        }
    )
    adapter = EvmDexAdapter(
        settings,
        rpc_client=FakeLiveEvmRpcClient(),
        swap_client=FakeOdosSwapClient(),
    )
    monkeypatch.setattr(
        "execution.evm_adapter._sign_assembled_transaction",
        lambda transaction, *, private_key, chain_id, nonce, current_base_fee_per_gas=None: (
            f"signed:{transaction['to']}:{private_key}:{chain_id}:{nonce}:{current_base_fee_per_gas}"
        ),
    )

    intent = _build_intent(VenueType.DEX, "evm_primary").model_copy(
        update={"chain": "base", "token": "AERO", "venue": "odos"}
    )
    prepared = adapter.prepare(intent, _build_risk())
    report = adapter.execute(prepared)

    assert prepared.simulation is False
    assert prepared.adapter_name == "evm_dex_live"
    assert any(reason == "odos_path:path-1" for reason in prepared.quote.reasons)
    assert report.status == "FILLED"
    assert report.executed_notional_usd == 400.0
    assert report.external_order_id == "evm-submit:0xabc123"
    assert report.message == "evm_dex_live_fill:rpc"


def test_evm_dex_adapter_submits_approval_when_allowance_is_insufficient(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeOdosSwapClient:
        def quote(self, **kwargs):
            _ = kwargs
            return type("QuoteState", (), {"path_id": "path-1", "out_amount_atomic": 123456789, "raw_quote": {}})()

        def assemble(self, *, path_id: str, wallet_address: str):
            _ = path_id, wallet_address
            return type(
                "AssembledTransaction",
                (),
                {
                    "transaction": {
                        "to": "0xa669e7a0d4b3e4fa48af2de86bd4cd7126be4e13",
                        "data": "0xdeadbeef",
                        "value": "0x0",
                        "gas": "0x5208",
                        "maxFeePerGas": hex(20_008_000),
                        "maxPriorityFeePerGas": hex(1_000),
                    },
                    "raw_response": {},
                },
            )()

        def token_price_usd(self, token_contract: str) -> float:
            _ = token_contract
            return 2.0

    class FakeLiveEvmRpcClient:
        def __init__(self) -> None:
            self.sent_transactions: list[str] = []
            self.call_count = 0

        def transaction_count(self, wallet_address: str) -> int:
            _ = wallet_address
            return 7 + len(self.sent_transactions)

        def latest_base_fee_per_gas(self) -> int | None:
            return 20_022_000

        def call(self, call_object: dict[str, Any], block_tag: str = "latest") -> str:
            _ = block_tag
            self.call_count += 1
            if call_object["data"].startswith("0xdd62ed3e"):
                return hex(0)
            return "0x"

        def estimate_gas(self, call_object: dict[str, Any]) -> int:
            _ = call_object
            return 65_000

        def send_raw_transaction(self, signed_transaction: str):
            self.sent_transactions.append(signed_transaction)
            tx_hash = f"0xabc{len(self.sent_transactions)}"
            return type(
                "Submission",
                (),
                {
                    "external_order_id": f"evm-submit:{tx_hash}",
                    "transaction_hash": tx_hash,
                    "transport": "rpc",
                },
            )()

        def confirm_submission(self, transaction_hash: str) -> bool:
            _ = transaction_hash
            return True

    base_settings = AppSettings.load()
    settings = base_settings.model_copy(
        update={
            "runtime": base_settings.runtime.model_copy(update={"environment": "live"}),
            "risk": base_settings.risk.model_copy(update={"live_trading_enabled": True}),
            "venues": base_settings.venues.model_copy(
                update={
                    "dex_adapter": "evm_primary",
                    "paper_execution_enabled": False,
                }
            ),
            "acquisition": base_settings.acquisition.model_copy(
                update={
                    "evm_sources": {
                        "arbitrum_arb": {
                            "enabled": True,
                            "source_type": "quote",
                            "chain": "arbitrum",
                            "chain_id": 42161,
                            "token": "ARB",
                            "token_contract": "0x912CE59144191C1204E64559FE8253a0e49E6548",
                            "quote_contract": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                            "token_decimals": 18,
                            "quote_decimals": 6,
                            "api_provider": "odos",
                            "quote_api_url": "https://api.odos.xyz/sor/quote/v2",
                        }
                    }
                }
            ),
            "live": base_settings.live.model_copy(
                update={
                    "credentials": base_settings.live.credentials.model_copy(
                        update={
                            "chain_wallets": {
                                "arbitrum": {
                                    "wallet_address": "0x19E7E376E7C213B7E7E7E46CC70A5DD086DAFF2A",
                                    "private_key": "0x1111111111111111111111111111111111111111111111111111111111111111",
                                }
                            }
                        }
                    )
                }
            ),
        }
    )
    rpc_client = FakeLiveEvmRpcClient()
    adapter = EvmDexAdapter(settings, rpc_client=rpc_client, swap_client=FakeOdosSwapClient())

    monkeypatch.setattr(
        "execution.evm_adapter._sign_assembled_transaction",
        lambda transaction, *, private_key, chain_id, nonce, current_base_fee_per_gas=None: (
            f"signed:{transaction['to']}:{private_key}:{chain_id}:{nonce}:{current_base_fee_per_gas}"
        ),
    )

    intent = _build_intent(VenueType.DEX, "evm_primary").model_copy(
        update={"chain": "arbitrum", "token": "ARB", "venue": "evm_primary"}
    )
    prepared = adapter.prepare(intent, _build_risk())
    report = adapter.execute(prepared)

    assert report.status == "FILLED"
    assert len(rpc_client.sent_transactions) == 2
    assert rpc_client.sent_transactions[0].startswith(
        "signed:0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
    )


def test_apply_eip1559_fee_floor_raises_max_fee_to_cover_current_base_fee() -> None:
    adjusted = _apply_eip1559_fee_floor(
        20_008_000,
        max_priority_fee_per_gas=1_000,
        current_base_fee_per_gas=20_022_000,
    )

    assert adjusted == 40_045_000


def test_solana_rpc_client_retries_timeout_then_succeeds() -> None:
    settings = AppSettings.load()
    client = SolanaRpcClient(
        settings.venues,
        transport=_solana_transport(failures=[TimeoutError("rpc timed out")]),
        max_retries=1,
    )

    quote_context = client.quote_context()

    assert quote_context.latest_blockhash == "stub-blockhash"


def test_solana_rpc_client_raises_after_transport_retries_exhausted() -> None:
    settings = AppSettings.load()
    client = SolanaRpcClient(
        settings.venues,
        transport=_solana_transport(failures=[URLError("down"), TimeoutError("still down")]),
        max_retries=1,
    )

    with pytest.raises(ValueError, match="solana_rpc_transport_error"):
        client.quote_context()


def test_solana_rpc_client_raises_on_error_response() -> None:
    settings = AppSettings.load()
    client = SolanaRpcClient(
        settings.venues,
        transport=_solana_transport(
            responses={
                "getLatestBlockhash": SolanaHttpTransportResponse(
                    status_code=200,
                    content_type="application/json",
                    payload={
                        "jsonrpc": "2.0",
                        "id": "solana-quote-context",
                        "error": {"code": -32005, "message": "node is behind"},
                    },
                )
            }
        ),
    )

    with pytest.raises(ValueError, match=r"solana_rpc_error:-32005"):
        client.quote_context()


def test_solana_rpc_client_rejects_non_2xx_http_status() -> None:
    settings = AppSettings.load()
    client = SolanaRpcClient(
        settings.venues,
        transport=_solana_transport(
            responses={
                "getLatestBlockhash": SolanaHttpTransportResponse(
                    status_code=503,
                    content_type="application/json",
                    payload={"jsonrpc": "2.0", "id": "solana-quote-context", "result": {}},
                    body_size_bytes=64,
                )
            }
        ),
    )

    with pytest.raises(ValueError, match=r"invalid_solana_rpc_http_status:503"):
        client.quote_context()


def test_solana_rpc_client_rejects_non_json_content_type() -> None:
    settings = AppSettings.load()
    client = SolanaRpcClient(
        settings.venues,
        transport=_solana_transport(
            responses={
                "getLatestBlockhash": SolanaHttpTransportResponse(
                    status_code=200,
                    content_type="text/plain",
                    payload={"jsonrpc": "2.0", "id": "solana-quote-context", "result": {}},
                    body_size_bytes=64,
                )
            }
        ),
    )

    with pytest.raises(ValueError, match="invalid_solana_rpc_content_type"):
        client.quote_context()


def test_solana_rpc_client_rejects_oversized_response_body() -> None:
    settings = AppSettings.load()
    client = SolanaRpcClient(
        settings.venues,
        transport=_solana_transport(
            responses={
                "getLatestBlockhash": SolanaHttpTransportResponse(
                    status_code=200,
                    content_type="application/json",
                    payload={
                        "jsonrpc": "2.0",
                        "id": "solana-quote-context",
                        "result": {
                            "context": {"slot": 1},
                            "value": {"blockhash": "stub-blockhash"},
                        },
                    },
                    body_size_bytes=1_000_001,
                )
            }
        ),
    )

    with pytest.raises(ValueError, match="invalid_solana_rpc_response_too_large"):
        client.quote_context()


def test_solana_rpc_client_rejects_mismatched_response_id() -> None:
    settings = AppSettings.load()
    client = SolanaRpcClient(
        settings.venues,
        transport=_solana_transport(
            responses={
                "getLatestBlockhash": SolanaHttpTransportResponse(
                    status_code=200,
                    content_type="application/json",
                    payload={
                        "jsonrpc": "2.0",
                        "id": "wrong-id",
                        "result": {
                            "context": {"slot": 1},
                            "value": {"blockhash": "stub-blockhash"},
                        },
                    },
                    body_size_bytes=128,
                )
            }
        ),
    )

    with pytest.raises(ValueError, match="invalid_solana_rpc_response_id"):
        client.quote_context()


def test_solana_rpc_client_rejects_mismatched_jsonrpc_version() -> None:
    settings = AppSettings.load()
    client = SolanaRpcClient(
        settings.venues,
        transport=_solana_transport(
            responses={
                "getLatestBlockhash": SolanaHttpTransportResponse(
                    status_code=200,
                    content_type="application/json",
                    payload={
                        "jsonrpc": "1.0",
                        "id": "solana-quote-context",
                        "result": {
                            "context": {"slot": 1},
                            "value": {"blockhash": "stub-blockhash"},
                        },
                    },
                    body_size_bytes=128,
                )
            }
        ),
    )

    with pytest.raises(ValueError, match="invalid_solana_rpc_jsonrpc"):
        client.quote_context()


def test_solana_rpc_client_rejects_malformed_quote_payload() -> None:
    settings = AppSettings.load()
    client = SolanaRpcClient(
        settings.venues,
        transport=_solana_transport(
            responses={
                "getLatestBlockhash": SolanaHttpTransportResponse(
                    status_code=200,
                    content_type="application/json",
                    payload={
                        "jsonrpc": "2.0",
                        "id": "solana-quote-context",
                        "result": {
                            "context": {"slot": "bad-slot"},
                            "value": {"blockhash": 123},
                        },
                    },
                    body_size_bytes=128,
                )
            }
        ),
    )

    with pytest.raises(ValueError, match="invalid_solana_quote_response"):
        client.quote_context()


def test_dex_factory_rejects_invalid_solana_rpc_url() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "venues": AppSettings.load().venues.model_copy(
                update={
                    "dex_adapter": "solana_stub",
                    "solana_rpc_url": "not-a-url",
                }
            )
        }
    )

    with pytest.raises(ValueError, match="invalid_solana_rpc_url"):
        build_dex_adapter(settings)


def test_dex_factory_allows_invalid_solana_rpc_url_for_paper_executor() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "venues": AppSettings.load().venues.model_copy(
                update={
                    "dex_adapter": "solana_primary",
                    "paper_execution_enabled": True,
                    "solana_rpc_url": "not-a-url",
                }
            )
        }
    )

    adapter = build_dex_adapter(settings)

    assert isinstance(adapter, DexPaperExecutor)


def test_dex_factory_rejects_invalid_quote_slippage() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "venues": AppSettings.load().venues.model_copy(
                update={
                    "dex_adapter": "solana_stub",
                    "solana_quote_slippage_bps": 0,
                }
            )
        }
    )

    with pytest.raises(ValueError, match="invalid_solana_quote_slippage_bps"):
        build_dex_adapter(settings)


def test_dex_factory_rejects_invalid_solana_rpc_timeout() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "venues": AppSettings.load().venues.model_copy(
                update={
                    "dex_adapter": "solana_stub",
                    "solana_rpc_timeout_seconds": 0.0,
                }
            )
        }
    )

    with pytest.raises(ValueError, match="invalid_solana_rpc_timeout_seconds"):
        build_dex_adapter(settings)


def test_dex_factory_rejects_negative_solana_rpc_retries() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "venues": AppSettings.load().venues.model_copy(
                update={
                    "dex_adapter": "solana_stub",
                    "solana_rpc_max_retries": -1,
                }
            )
        }
    )

    with pytest.raises(ValueError, match="invalid_solana_rpc_max_retries"):
        build_dex_adapter(settings)


def test_dex_factory_rejects_jito_for_paper_executor() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "venues": AppSettings.load().venues.model_copy(
                update={
                    "dex_adapter": "solana_primary",
                    "paper_execution_enabled": True,
                    "solana_jito_enabled": True,
                }
            )
        }
    )

    with pytest.raises(ValueError, match="solana_jito_requires_live_dex_adapter"):
        build_dex_adapter(settings)


def test_cex_factory_selects_paper_adapter_when_live_dex_mode_is_enabled() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "venues": AppSettings.load().venues.model_copy(
                update={
                    "cex_adapter": "binance_paper",
                    "paper_execution_enabled": False,
                }
            )
        }
    )

    adapter = build_cex_adapter(settings)

    assert isinstance(adapter, CexPaperExecutor)