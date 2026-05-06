from __future__ import annotations

from typing import Any

from core.config import AcquisitionConfig, AppSettings
from execution.solana_rpc import SolanaHttpTransportResponse
from sentinel.onchain_live_sources import (
    EvmPoolSwapTradeSource,
    EvmQuoteSource,
    EvmTransferTradeSource,
    JupiterQuoteSource,
    SolanaWalletTradeSource,
    build_live_sources,
)


def test_build_live_sources_returns_enabled_sources() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "acquisition": {
                "solana_wallet_trade": {
                    "enabled": True,
                    "wallet_address": "wallet-1",
                    "token_mint": "token-mint",
                    "quote_mint": "quote-mint",
                },
                "jupiter_quote": {
                    "enabled": True,
                    "input_mint": "usdc-mint",
                    "output_mint": "bonk-mint",
                },
                "evm_transfer_trade": {
                    "enabled": True,
                    "chain": "base",
                    "wallet_address": "0x00000000000000000000000000000000000000aa",
                    "token_contract": "0x00000000000000000000000000000000000000bb",
                    "quote_contract": "0x00000000000000000000000000000000000000cc",
                },
                "evm_sources": {
                    "base_quote": {
                        "enabled": True,
                        "source_type": "quote",
                        "chain": "base",
                        "chain_id": 8453,
                        "token": "AERO",
                        "token_contract": "0x00000000000000000000000000000000000000bb",
                        "quote_contract": "0x00000000000000000000000000000000000000cc",
                    },
                    "base_pool": {
                        "enabled": True,
                        "source_type": "pool_swap_trade",
                        "chain": "base",
                        "token": "AERO",
                        "pool_address": "0x00000000000000000000000000000000000000dd",
                        "token_contract": "0x00000000000000000000000000000000000000bb",
                        "quote_contract": "0x00000000000000000000000000000000000000cc",
                    },
                },
            }
        }
    )

    sources = build_live_sources(settings)

    assert len(sources) == 5
    assert isinstance(sources[0], SolanaWalletTradeSource)
    assert isinstance(sources[1], JupiterQuoteSource)
    assert sum(isinstance(source, EvmTransferTradeSource) for source in sources) == 1
    assert sum(isinstance(source, EvmPoolSwapTradeSource) for source in sources) == 1
    assert sum(isinstance(source, EvmQuoteSource) for source in sources) == 1


def test_build_live_sources_skips_incomplete_measurement_templates() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "acquisition": {
                "solana_wallet_trade": {
                    "enabled": True,
                    "wallet_address": "wallet-1",
                    "token": "",
                    "token_mint": "",
                    "quote_mint": "",
                },
                "jupiter_quote": {
                    "enabled": True,
                    "token": "",
                    "input_mint": "",
                    "output_mint": "",
                },
                "evm_sources": {
                    "base_quote": {
                        "enabled": True,
                        "source_type": "quote",
                        "chain": "base",
                        "token": "",
                        "token_contract": "",
                        "quote_contract": "",
                    },
                    "base_pool": {
                        "enabled": True,
                        "source_type": "pool_swap_trade",
                        "chain": "base",
                        "token": "",
                        "pool_address": "",
                        "token_contract": "",
                        "quote_contract": "",
                    },
                },
            }
        }
    )

    sources = build_live_sources(settings)

    assert sources == []


def test_jupiter_quote_source_fetches_normalized_quote() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "acquisition": {
                "jupiter_quote": {
                    "enabled": True,
                    "token": "BONK",
                    "input_mint": "usdc-mint",
                    "output_mint": "bonk-mint",
                    "quote_notional_usd": 5000.0,
                    "output_decimals": 5,
                }
            }
        }
    )

    def quote_transport(url: str, headers: dict[str, str], timeout_seconds: float) -> dict[str, Any]:
        _ = url, headers, timeout_seconds
        return {
            "outAmount": "405000000000",
            "contextSlot": 123,
            "routePlan": [{"swapInfo": {}}, {"swapInfo": {}}],
        }

    def price_transport(url: str, headers: dict[str, str], timeout_seconds: float) -> dict[str, Any]:
        _ = url, headers, timeout_seconds
        return {"data": {"bonk-mint": {"price": 0.000012}}}

    source = JupiterQuoteSource(
        settings,
        AcquisitionConfig.model_validate(settings.acquisition).jupiter_quote,
        quote_transport=quote_transport,
        price_transport=price_transport,
    )

    payloads = source.fetch_quotes()

    assert len(payloads) == 1
    assert payloads[0]["token"] == "BONK"
    assert payloads[0]["route_summary"]["provider"] == "jupiter"
    assert payloads[0]["route_summary"]["hops"] == 2
    assert payloads[0]["reference_mid_usd"] == 5000.0


def test_solana_wallet_trade_source_fetches_normalized_trade() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "acquisition": {
                "solana_wallet_trade": {
                    "enabled": True,
                    "wallet_address": "wallet-1",
                    "token": "BONK",
                    "token_mint": "token-mint",
                    "quote_asset": "USDC",
                    "quote_mint": "quote-mint",
                    "pool_address": "pool-1",
                }
            }
        }
    )

    def transport(rpc_url: str, payload: dict[str, Any], timeout_seconds: float) -> SolanaHttpTransportResponse:
        _ = rpc_url, timeout_seconds
        if payload["method"] == "getSignaturesForAddress":
            return SolanaHttpTransportResponse(
                status_code=200,
                content_type="application/json",
                payload={
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "result": [{"signature": "sig-1"}],
                },
            )
        return SolanaHttpTransportResponse(
            status_code=200,
            content_type="application/json",
            payload={
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": {
                    "slot": 88,
                    "blockTime": 1777771203,
                    "meta": {
                        "preTokenBalances": [
                            {
                                "owner": "wallet-1",
                                "mint": "token-mint",
                                "uiTokenAmount": {"uiAmount": 10.0},
                            },
                            {
                                "owner": "wallet-1",
                                "mint": "quote-mint",
                                "uiTokenAmount": {"uiAmount": 5000.0},
                            },
                        ],
                        "postTokenBalances": [
                            {
                                "owner": "wallet-1",
                                "mint": "token-mint",
                                "uiTokenAmount": {"uiAmount": 20.0},
                            },
                            {
                                "owner": "wallet-1",
                                "mint": "quote-mint",
                                "uiTokenAmount": {"uiAmount": 4900.0},
                            },
                        ],
                    },
                },
            },
        )

    source = SolanaWalletTradeSource(
        settings,
        AcquisitionConfig.model_validate(settings.acquisition).solana_wallet_trade,
        transport=transport,
    )

    records = source.fetch_trades()

    assert len(records) == 1
    assert records[0].cursor == "sig-1"
    assert records[0].payload["side"] == "buy"
    assert records[0].payload["quote_amount_usd"] == 100.0


def test_solana_wallet_trade_source_supports_address_watch_without_owner_filter() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "acquisition": {
                "solana_wallet_trade": {
                    "enabled": True,
                    "source_kind": "address",
                    "signature_address": "pool-address-1",
                    "token": "BONK",
                    "token_mint": "token-mint",
                    "quote_asset": "USDC",
                    "quote_mint": "quote-mint",
                    "pool_address": "pool-address-1",
                }
            }
        }
    )

    def transport(rpc_url: str, payload: dict[str, Any], timeout_seconds: float) -> SolanaHttpTransportResponse:
        _ = rpc_url, timeout_seconds
        if payload["method"] == "getSignaturesForAddress":
            assert payload["params"][0] == "pool-address-1"
            return SolanaHttpTransportResponse(
                status_code=200,
                content_type="application/json",
                payload={"jsonrpc": "2.0", "id": payload["id"], "result": [{"signature": "sig-2"}]},
            )
        return SolanaHttpTransportResponse(
            status_code=200,
            content_type="application/json",
            payload={
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": {
                    "slot": 89,
                    "blockTime": 1777771204,
                    "meta": {
                        "preTokenBalances": [
                            {"owner": "trader-a", "mint": "token-mint", "uiTokenAmount": {"uiAmount": 5.0}},
                            {"owner": "trader-a", "mint": "quote-mint", "uiTokenAmount": {"uiAmount": 500.0}},
                        ],
                        "postTokenBalances": [
                            {"owner": "trader-a", "mint": "token-mint", "uiTokenAmount": {"uiAmount": 15.0}},
                            {"owner": "trader-a", "mint": "quote-mint", "uiTokenAmount": {"uiAmount": 400.0}},
                        ],
                    },
                },
            },
        )

    source = SolanaWalletTradeSource(
        settings,
        AcquisitionConfig.model_validate(settings.acquisition).solana_wallet_trade,
        transport=transport,
    )

    records = source.fetch_trades()

    assert len(records) == 1
    assert records[0].payload["wallet_address"] == "trader-a"
    assert records[0].payload["route_hint"] == "solana_rpc_address_watch"


def test_evm_transfer_trade_source_fetches_normalized_trade() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "acquisition": {
                "evm_transfer_trade": {
                    "enabled": True,
                    "chain": "base",
                    "wallet_address": "0x00000000000000000000000000000000000000aa",
                    "token": "AERO",
                    "token_contract": "0x00000000000000000000000000000000000000bb",
                    "quote_asset": "USDC",
                    "quote_contract": "0x00000000000000000000000000000000000000cc",
                    "token_decimals": 18,
                    "quote_decimals": 6,
                }
            },
            "venues": {
                "native_asset_rpc": {
                    "base": {
                        "url": "https://rpc.base.example",
                        "timeout_seconds": 5.0,
                        "max_retries": 2,
                    }
                }
            },
        }
    )

    wallet = "0x00000000000000000000000000000000000000aa"

    def transport(rpc_url: str, payload: dict[str, Any], timeout_seconds: float) -> SolanaHttpTransportResponse:
        _ = rpc_url, timeout_seconds
        method = payload["method"]
        if method == "eth_blockNumber":
            return SolanaHttpTransportResponse(
                status_code=200,
                content_type="application/json",
                payload={"jsonrpc": "2.0", "id": payload["id"], "result": "0x64"},
            )
        if method == "eth_getBlockByNumber":
            return SolanaHttpTransportResponse(
                status_code=200,
                content_type="application/json",
                payload={
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "result": {"timestamp": "0x68160b80"},
                },
            )
        if method == "eth_getLogs" and payload["params"][0]["address"].endswith("bb"):
            return SolanaHttpTransportResponse(
                status_code=200,
                content_type="application/json",
                payload={
                    "jsonrpc": "2.0",
                    "id": payload["id"],
                    "result": [
                        {
                            "transactionHash": "0xtx1",
                            "blockNumber": "0x64",
                            "topics": [
                                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55aeb0b5a2b88",
                                "0x0000000000000000000000001111111111111111111111111111111111111111",
                                "0x00000000000000000000000000000000000000000000000000000000000000aa",
                            ],
                            "data": hex(10**18),
                        }
                    ],
                },
            )
        return SolanaHttpTransportResponse(
            status_code=200,
            content_type="application/json",
            payload={
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": [
                    {
                        "transactionHash": "0xtx1",
                        "blockNumber": "0x64",
                        "topics": [
                            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55aeb0b5a2b88",
                            "0x00000000000000000000000000000000000000000000000000000000000000aa",
                            "0x0000000000000000000000002222222222222222222222222222222222222222",
                        ],
                        "data": hex(100 * 10**6),
                    }
                ],
            },
        )

    source = EvmTransferTradeSource(
        settings,
        AcquisitionConfig.model_validate(settings.acquisition).evm_transfer_trade,
        transport=transport,
    )

    records = source.fetch_trades(last_cursor="98")

    assert len(records) == 1
    assert records[0].cursor == "100"
    assert records[0].payload["side"] == "buy"
    assert records[0].payload["chain"] == "base"
    assert records[0].payload["quote_amount_usd"] == 100.0


def test_evm_pool_swap_trade_source_decodes_uniswap_v2_style_swap() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "acquisition": {
                "evm_sources": {
                    "base_pool": {
                        "enabled": True,
                        "source_type": "pool_swap_trade",
                        "chain": "base",
                        "token": "AERO",
                        "pool_address": "0x00000000000000000000000000000000000000dd",
                        "token_contract": "0x00000000000000000000000000000000000000bb",
                        "quote_contract": "0x00000000000000000000000000000000000000cc",
                        "token_is_token0": False,
                        "quote_decimals": 6,
                        "token_decimals": 18,
                    }
                }
            },
            "venues": {
                "native_asset_rpc": {
                    "base": {
                        "url": "https://rpc.base.example",
                        "timeout_seconds": 5.0,
                        "max_retries": 2,
                    }
                }
            },
        }
    )

    def transport(rpc_url: str, payload: dict[str, Any], timeout_seconds: float) -> SolanaHttpTransportResponse:
        _ = rpc_url, timeout_seconds
        if payload["method"] == "eth_blockNumber":
            return SolanaHttpTransportResponse(200, "application/json", {"jsonrpc": "2.0", "id": payload["id"], "result": "0x64"})
        if payload["method"] == "eth_getBlockByNumber":
            return SolanaHttpTransportResponse(200, "application/json", {"jsonrpc": "2.0", "id": payload["id"], "result": {"timestamp": "0x68160b80"}})
        return SolanaHttpTransportResponse(
            200,
            "application/json",
            {
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": [
                    {
                        "transactionHash": "0xtx2",
                        "blockNumber": "0x64",
                        "logIndex": "0x1",
                        "data": "0x"
                        + f"{100 * 10**6:064x}"
                        + f"{0:064x}"
                        + f"{0:064x}"
                        + f"{10**18:064x}",
                    }
                ],
            },
        )

    registry = AcquisitionConfig.model_validate(settings.acquisition).evm_sources
    source = EvmPoolSwapTradeSource(settings, registry["base_pool"], transport=transport)

    records = source.fetch_trades(last_cursor="98")

    assert len(records) == 1
    assert records[0].payload["side"] == "buy"
    assert records[0].payload["quote_amount_usd"] == 100.0
    assert records[0].payload["route_hint"] == "evm_uniswap_v2_swap_watch"


def test_evm_pool_swap_trade_source_supports_aerodrome_protocol_alias() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "acquisition": {
                "evm_sources": {
                    "base_pool": {
                        "enabled": True,
                        "source_type": "pool_swap_trade",
                        "chain": "base",
                        "token": "AERO",
                        "pool_address": "0x00000000000000000000000000000000000000dd",
                        "pool_protocol": "aerodrome",
                        "token_contract": "0x00000000000000000000000000000000000000bb",
                        "quote_contract": "0x00000000000000000000000000000000000000cc",
                        "token_is_token0": False,
                        "quote_decimals": 6,
                        "token_decimals": 18,
                    }
                }
            },
            "venues": {
                "native_asset_rpc": {
                    "base": {
                        "url": "https://rpc.base.example",
                        "timeout_seconds": 5.0,
                        "max_retries": 2,
                    }
                }
            },
        }
    )

    def transport(rpc_url: str, payload: dict[str, Any], timeout_seconds: float) -> SolanaHttpTransportResponse:
        _ = rpc_url, timeout_seconds
        if payload["method"] == "eth_blockNumber":
            return SolanaHttpTransportResponse(200, "application/json", {"jsonrpc": "2.0", "id": payload["id"], "result": "0x64"})
        if payload["method"] == "eth_getBlockByNumber":
            return SolanaHttpTransportResponse(200, "application/json", {"jsonrpc": "2.0", "id": payload["id"], "result": {"timestamp": "0x68160b80"}})
        return SolanaHttpTransportResponse(
            200,
            "application/json",
            {
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": [
                    {
                        "transactionHash": "0xtx3",
                        "blockNumber": "0x64",
                        "logIndex": "0x2",
                        "data": "0x"
                        + f"{100 * 10**6:064x}"
                        + f"{0:064x}"
                        + f"{0:064x}"
                        + f"{10**18:064x}",
                    }
                ],
            },
        )

    registry = AcquisitionConfig.model_validate(settings.acquisition).evm_sources
    source = EvmPoolSwapTradeSource(settings, registry["base_pool"], transport=transport)

    records = source.fetch_trades(last_cursor="98")

    assert len(records) == 1
    assert records[0].payload["route_hint"] == "evm_aerodrome_swap_watch"


def test_evm_pool_swap_trade_source_decodes_uniswap_v3_style_swap() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "acquisition": {
                "evm_sources": {
                    "eth_pool": {
                        "enabled": True,
                        "source_type": "pool_swap_trade",
                        "chain": "ethereum",
                        "token": "UNI",
                        "pool_address": "0x00000000000000000000000000000000000000dd",
                        "pool_protocol": "uniswap_v3",
                        "token_contract": "0x00000000000000000000000000000000000000bb",
                        "quote_contract": "0x00000000000000000000000000000000000000cc",
                        "token_is_token0": True,
                        "quote_decimals": 6,
                        "token_decimals": 18,
                    }
                }
            },
            "venues": {
                "native_asset_rpc": {
                    "ethereum": {
                        "url": "https://rpc.ethereum.example",
                        "timeout_seconds": 5.0,
                        "max_retries": 2,
                    }
                }
            },
        }
    )

    amount0 = (1 << 256) - 10**18
    amount1 = 100 * 10**6

    def transport(rpc_url: str, payload: dict[str, Any], timeout_seconds: float) -> SolanaHttpTransportResponse:
        _ = rpc_url, timeout_seconds
        if payload["method"] == "eth_blockNumber":
            return SolanaHttpTransportResponse(200, "application/json", {"jsonrpc": "2.0", "id": payload["id"], "result": "0x64"})
        if payload["method"] == "eth_getBlockByNumber":
            return SolanaHttpTransportResponse(200, "application/json", {"jsonrpc": "2.0", "id": payload["id"], "result": {"timestamp": "0x68160b80"}})
        return SolanaHttpTransportResponse(
            200,
            "application/json",
            {
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": [
                    {
                        "transactionHash": "0xtx4",
                        "blockNumber": "0x64",
                        "logIndex": "0x3",
                        "data": "0x"
                        + f"{amount0:064x}"
                        + f"{amount1:064x}"
                        + f"{0:064x}"
                        + f"{0:064x}"
                        + f"{0:064x}",
                    }
                ],
            },
        )

    registry = AcquisitionConfig.model_validate(settings.acquisition).evm_sources
    source = EvmPoolSwapTradeSource(settings, registry["eth_pool"], transport=transport)

    records = source.fetch_trades(last_cursor="98")

    assert len(records) == 1
    assert records[0].payload["side"] == "buy"
    assert records[0].payload["token_amount"] == 1.0
    assert records[0].payload["quote_amount_usd"] == 100.0
    assert records[0].payload["route_hint"] == "evm_uniswap_v3_swap_watch"
    assert records[0].payload["route_diagnostics"] == {
        "sqrt_price_x96": 0,
        "liquidity": 0,
        "tick": 0,
    }


def test_evm_quote_source_fetches_normalized_quote() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "acquisition": {
                "evm_sources": {
                    "base_quote": {
                        "enabled": True,
                        "source_type": "quote",
                        "chain": "base",
                        "chain_id": 8453,
                        "token": "AERO",
                        "token_contract": "0x00000000000000000000000000000000000000bb",
                        "quote_contract": "0x00000000000000000000000000000000000000cc",
                        "quote_notional_usd": 5000.0,
                    }
                }
            }
        }
    )

    def quote_transport(url: str, headers: dict[str, str], timeout_seconds: float) -> dict[str, Any]:
        _ = url, headers, timeout_seconds
        return {"buyAmount": str(2000 * 10**18), "route": {"fills": [{}, {}]}}

    def price_transport(url: str, headers: dict[str, str], timeout_seconds: float) -> dict[str, Any]:
        _ = url, headers, timeout_seconds
        return {
            "pairs": [
                {
                    "chainId": "base",
                    "priceUsd": "2.45",
                    "pairAddress": "0xpair",
                    "dexId": "aerodrome",
                    "baseToken": {"address": "0x00000000000000000000000000000000000000bb"},
                    "quoteToken": {"address": "0x00000000000000000000000000000000000000cc"},
                    "volume": {"m5": 4200.0},
                    "txns": {"m5": {"buys": 3, "sells": 1}},
                    "liquidity": {"usd": 1200000.0},
                }
            ]
        }

    registry = AcquisitionConfig.model_validate(settings.acquisition).evm_sources
    source = EvmQuoteSource(
        settings,
        registry["base_quote"],
        quote_transport=lambda url, headers, timeout_seconds, method, body: quote_transport(url, headers, timeout_seconds),
        price_transport=price_transport,
    )

    payloads = source.fetch_quotes()

    assert len(payloads) == 1
    assert payloads[0]["route_summary"]["provider"] == "0x"
    assert payloads[0]["quote_notional_usd"] == 5000.0
    assert payloads[0]["expected_out_usd"] == 4900.0


def test_evm_quote_source_uses_bearer_auth_for_non_zeroex_provider() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "acquisition": {
                "evm_sources": {
                    "base_quote": {
                        "enabled": True,
                        "source_type": "quote",
                        "chain": "base",
                        "chain_id": 8453,
                        "token": "AERO",
                        "token_contract": "0x00000000000000000000000000000000000000bb",
                        "quote_contract": "0x00000000000000000000000000000000000000cc",
                        "quote_notional_usd": 5000.0,
                        "api_provider": "odos",
                    }
                }
            },
            "live": {
                **AppSettings.load().live.model_dump(),
                "credentials": {
                    "dex_providers": {
                        "odos": {
                            "api_key": "odos-key",
                        }
                    }
                },
            },
        }
    )
    captured_headers: list[dict[str, str]] = []

    def quote_transport(url: str, headers: dict[str, str], timeout_seconds: float) -> dict[str, Any]:
        _ = url, timeout_seconds
        captured_headers.append(headers)
        return {"outAmounts": [str(2000 * 10**18)], "pathId": "odos-path-auth"}

    def price_transport(url: str, headers: dict[str, str], timeout_seconds: float) -> dict[str, Any]:
        _ = url, headers, timeout_seconds
        return {
            "pairs": [
                {
                    "chainId": "base",
                    "priceUsd": "2.45",
                    "pairAddress": "0xpair",
                    "dexId": "aerodrome",
                    "baseToken": {"address": "0x00000000000000000000000000000000000000bb"},
                    "quoteToken": {"address": "0x00000000000000000000000000000000000000cc"},
                    "volume": {"m5": 4200.0},
                    "txns": {"m5": {"buys": 3, "sells": 1}},
                    "liquidity": {"usd": 1200000.0},
                }
            ]
        }

    registry = AcquisitionConfig.model_validate(settings.acquisition).evm_sources
    source = EvmQuoteSource(
        settings,
        registry["base_quote"],
        quote_transport=lambda url, headers, timeout_seconds, method, body: quote_transport(url, headers, timeout_seconds),
        price_transport=price_transport,
    )

    payloads = source.fetch_quotes()

    assert len(payloads) == 1
    assert captured_headers == [{"Authorization": "Bearer odos-key"}]


def test_evm_quote_source_builds_odos_post_request_and_parses_response() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "acquisition": {
                "evm_sources": {
                    "base_quote": {
                        "enabled": True,
                        "source_type": "quote",
                        "chain": "base",
                        "chain_id": 8453,
                        "token": "AERO",
                        "token_contract": "0x00000000000000000000000000000000000000bb",
                        "quote_contract": "0x00000000000000000000000000000000000000cc",
                        "quote_notional_usd": 5000.0,
                        "quote_api_url": "https://api.odos.xyz/sor/quote/v2",
                        "api_provider": "odos",
                        "wallet_address": "0x00000000000000000000000000000000000000aa",
                    }
                }
            }
        }
    )
    captured_requests: list[dict[str, Any]] = []

    def quote_transport(
        url: str,
        headers: dict[str, str],
        timeout_seconds: float,
        method: str,
        body: dict[str, Any] | None,
    ) -> dict[str, Any]:
        _ = headers, timeout_seconds
        captured_requests.append({"url": url, "method": method, "body": body})
        return {
            "outAmounts": [str(2000 * 10**18)],
            "pathId": "odos-path-1",
            "gasEstimate": 123456,
            "pathViz": [{"name": "route-1"}, {"name": "route-2"}],
            "inTokens": [{"tokenAddress": "0x00000000000000000000000000000000000000cc"}],
            "outTokens": [{"tokenAddress": "0x00000000000000000000000000000000000000bb"}],
        }

    def price_transport(url: str, headers: dict[str, str], timeout_seconds: float) -> dict[str, Any]:
        _ = url, headers, timeout_seconds
        return {
            "pairs": [
                {
                    "chainId": "base",
                    "priceUsd": "2.45",
                    "pairAddress": "0xpair",
                    "dexId": "aerodrome",
                    "baseToken": {"address": "0x00000000000000000000000000000000000000bb"},
                    "quoteToken": {"address": "0x00000000000000000000000000000000000000cc"},
                    "volume": {"m5": 4200.0},
                    "txns": {"m5": {"buys": 3, "sells": 1}},
                    "liquidity": {"usd": 1200000.0},
                }
            ]
        }

    registry = AcquisitionConfig.model_validate(settings.acquisition).evm_sources
    source = EvmQuoteSource(
        settings,
        registry["base_quote"],
        quote_transport=quote_transport,
        price_transport=price_transport,
    )

    payloads = source.fetch_quotes()

    assert len(payloads) == 1
    assert captured_requests == [
        {
            "url": "https://api.odos.xyz/sor/quote/v2",
            "method": "POST",
            "body": {
                "chainId": 8453,
                "inputTokens": [
                    {
                        "tokenAddress": "0x00000000000000000000000000000000000000cc",
                        "amount": str(5000 * 10**6),
                    }
                ],
                "outputTokens": [
                    {
                        "tokenAddress": "0x00000000000000000000000000000000000000bb",
                        "proportion": 1,
                    }
                ],
                "slippageLimitPercent": 1.0,
                "userAddr": "0x00000000000000000000000000000000000000aa",
                "disableRFQs": True,
                "compact": True,
            },
        }
    ]
    assert payloads[0]["route_summary"]["provider"] == "odos"
    assert payloads[0]["route_summary"]["path_id"] == "odos-path-1"
    assert payloads[0]["route_summary"]["path_segments"] == 2
    assert payloads[0]["route_summary"]["input_count"] == 1
    assert payloads[0]["route_summary"]["output_count"] == 1
    assert payloads[0]["route_summary"]["volume_5m_usd"] == 4200.0
    assert payloads[0]["route_summary"]["buy_pressure"] == 0.75
    assert payloads[0]["expected_out_usd"] == 4900.0


def test_evm_quote_source_prefers_most_active_dexscreener_pair_over_quote_match() -> None:
    settings = AppSettings.load().model_copy(
        update={
            "acquisition": {
                "evm_sources": {
                    "arb_quote": {
                        "enabled": True,
                        "source_type": "quote",
                        "chain": "arbitrum",
                        "chain_id": 42161,
                        "token": "ARB",
                        "token_contract": "0x00000000000000000000000000000000000000bb",
                        "quote_contract": "0x00000000000000000000000000000000000000cc",
                        "quote_notional_usd": 5000.0,
                        "quote_api_url": "https://api.odos.xyz/sor/quote/v2",
                        "api_provider": "odos",
                        "wallet_address": "0x00000000000000000000000000000000000000aa",
                    }
                }
            }
        }
    )

    def quote_transport(
        url: str,
        headers: dict[str, str],
        timeout_seconds: float,
        method: str,
        body: dict[str, Any] | None,
    ) -> dict[str, Any]:
        _ = url, headers, timeout_seconds, method, body
        return {
            "outAmounts": [str(2000 * 10**18)],
            "pathId": "odos-path-1",
            "gasEstimate": 123456,
            "pathViz": [{"name": "route-1"}],
            "inTokens": [{"tokenAddress": "0x00000000000000000000000000000000000000cc"}],
            "outTokens": [{"tokenAddress": "0x00000000000000000000000000000000000000bb"}],
        }

    def price_transport(url: str, headers: dict[str, str], timeout_seconds: float) -> dict[str, Any]:
        _ = url, headers, timeout_seconds
        return {
            "pairs": [
                {
                    "chainId": "arbitrum",
                    "pairAddress": "0xsmall-usdc",
                    "dexId": "uniswap",
                    "priceUsd": "2.45",
                    "baseToken": {"address": "0x00000000000000000000000000000000000000bb"},
                    "quoteToken": {"address": "0x00000000000000000000000000000000000000cc"},
                    "volume": {"m5": 130.81},
                    "txns": {"m5": {"buys": 0, "sells": 2}},
                    "liquidity": {"usd": 636796.13},
                },
                {
                    "chainId": "arbitrum",
                    "pairAddress": "0xdeep-weth",
                    "dexId": "uniswap",
                    "priceUsd": "2.45",
                    "baseToken": {"address": "0x00000000000000000000000000000000000000bb"},
                    "quoteToken": {"address": "0x00000000000000000000000000000000000000dd"},
                    "volume": {"m5": 39013.75},
                    "txns": {"m5": {"buys": 17, "sells": 25}},
                    "liquidity": {"usd": 2234718.54},
                },
            ]
        }

    registry = AcquisitionConfig.model_validate(settings.acquisition).evm_sources
    source = EvmQuoteSource(
        settings,
        registry["arb_quote"],
        quote_transport=quote_transport,
        price_transport=price_transport,
    )

    payloads = source.fetch_quotes()

    assert len(payloads) == 1
    assert payloads[0]["route_summary"]["market_pair_address"] == "0xdeep-weth"
    assert payloads[0]["route_summary"]["volume_5m_usd"] == 39013.75
    assert payloads[0]["route_summary"]["buy_pressure"] == 0.404762


# ── MeasurementProfileRegistry ─────────────────────────────────────────────


def test_measurement_profile_registry_register_and_get() -> None:
    from sentinel.onchain_live_sources import MeasurementProfileRegistry
    from core.schemas import MeasurementProfile
    from datetime import UTC, datetime

    registry = MeasurementProfileRegistry()
    profile = MeasurementProfile(
        profile_id="disc:evt-1",
        chain="solana",
        token="BONK",
        discovery_event_type="alpha.launch_candidate",
        discovery_event_id="evt-1",
        registered_at=datetime.now(UTC),
        token_mint="bonk-mint",
        quote_mint="usdc-mint",
        chain_type="solana",
    )

    registry.register(profile)

    fetched = registry.get("solana", "BONK")
    assert fetched is not None
    assert fetched.profile_id == "disc:evt-1"
    assert fetched.token_mint == "bonk-mint"

    missing = registry.get("solana", "UNKNOWN")
    assert missing is None


def test_measurement_profile_registry_skips_duplicate_registration() -> None:
    from sentinel.onchain_live_sources import MeasurementProfileRegistry
    from core.schemas import MeasurementProfile
    from datetime import UTC, datetime

    registry = MeasurementProfileRegistry()
    profile = MeasurementProfile(
        profile_id="disc:evt-1",
        chain="solana",
        token="BONK",
        discovery_event_type="alpha.launch_candidate",
        discovery_event_id="evt-1",
        registered_at=datetime.now(UTC),
        chain_type="solana",
    )

    registry.register(profile)
    registry.register(profile)  # same profile_id — no-op

    assert registry.count() == 1


def test_measurement_profile_registry_ignores_stale_profiles() -> None:
    from sentinel.onchain_live_sources import MeasurementProfileRegistry
    from core.schemas import MeasurementProfile
    from datetime import UTC, datetime, timedelta

    registry = MeasurementProfileRegistry()
    stale = MeasurementProfile(
        profile_id="disc:stale",
        chain="solana",
        token="STALE",
        discovery_event_type="alpha.launch_candidate",
        discovery_event_id="stale-evt",
        registered_at=datetime.now(UTC) - timedelta(hours=2),
        ttl_seconds=3600.0,
        chain_type="solana",
    )
    registry.register(stale)

    assert registry.get("solana", "STALE") is None
    assert registry.count() == 0


def test_measurement_profile_registry_active_profiles_excludes_stale() -> None:
    from sentinel.onchain_live_sources import MeasurementProfileRegistry
    from core.schemas import MeasurementProfile
    from datetime import UTC, datetime, timedelta

    registry = MeasurementProfileRegistry()
    fresh = MeasurementProfile(
        profile_id="disc:fresh",
        chain="solana",
        token="FRESH",
        discovery_event_type="alpha.launch_candidate",
        discovery_event_id="fresh-evt",
        registered_at=datetime.now(UTC),
        chain_type="solana",
    )
    stale = MeasurementProfile(
        profile_id="disc:stale",
        chain="solana",
        token="STALE",
        discovery_event_type="alpha.launch_candidate",
        discovery_event_id="stale-evt",
        registered_at=datetime.now(UTC) - timedelta(hours=2),
        ttl_seconds=3600.0,
        chain_type="solana",
    )
    registry.register(fresh)
    registry.register(stale)

    active = registry.active_profiles()
    assert len(active) == 1
    assert active[0].token == "FRESH"


def test_measurement_profile_registry_cleanup_removes_stale() -> None:
    from sentinel.onchain_live_sources import MeasurementProfileRegistry
    from core.schemas import MeasurementProfile
    from datetime import UTC, datetime, timedelta

    registry = MeasurementProfileRegistry()
    stale = MeasurementProfile(
        profile_id="disc:stale",
        chain="solana",
        token="STALE",
        discovery_event_type="alpha.launch_candidate",
        discovery_event_id="stale-evt",
        registered_at=datetime.now(UTC) - timedelta(hours=2),
        ttl_seconds=3600.0,
        chain_type="solana",
    )
    registry.register(stale)
    registry.cleanup()

    assert registry.count() == 0


# ── resolve_token_addresses_for_discovery ──────────────────────────────────


def test_resolve_token_addresses_for_discovery_solana() -> None:
    from sentinel.onchain_live_sources import resolve_token_addresses_for_discovery

    def http_get(url: str, headers: dict[str, str], timeout: float) -> dict[str, object]:
        _ = headers, timeout
        assert "dexscreener" in url
        return {
            "pairs": [
                {
                    "chainId": "solana",
                    "dexId": "raydium",
                    "pairAddress": "pair-xxx",
                    "baseToken": {"symbol": "BONK", "address": "bonk-mint-abc"},
                    "quoteToken": {"symbol": "USDC", "address": "usdc-mint-xyz"},
                    "priceUsd": "0.000012",
                }
            ]
        }

    result = resolve_token_addresses_for_discovery("solana", "BONK", http_get=http_get)

    assert result["chain_type"] == "solana"
    assert result["token_mint"] == "bonk-mint-abc"
    assert result["quote_mint"] == "usdc-mint-xyz"
    assert result["pool_address"] == "pair-xxx"
    assert result["dex"] == "raydium"


def test_resolve_token_addresses_for_discovery_evm() -> None:
    from sentinel.onchain_live_sources import resolve_token_addresses_for_discovery

    def http_get(url: str, headers: dict[str, str], timeout: float) -> dict[str, object]:
        _ = headers, timeout
        return {
            "pairs": [
                {
                    "chainId": "base",
                    "dexId": "aerodrome",
                    "pairAddress": "0xpair",
                    "baseToken": {"symbol": "AERO", "address": "0xaero-contract"},
                    "quoteToken": {"symbol": "USDC", "address": "0xusdc-contract"},
                    "priceUsd": "2.45",
                }
            ]
        }

    result = resolve_token_addresses_for_discovery("base", "AERO", http_get=http_get)

    assert result["chain_type"] == "evm"
    assert result["token_contract"] == "0xaero-contract"
    assert result["quote_contract"] == "0xusdc-contract"
    assert result["pool_address"] == "0xpair"


def test_resolve_token_addresses_for_discovery_falls_back_to_first_chain_pair() -> None:
    from sentinel.onchain_live_sources import resolve_token_addresses_for_discovery

    def http_get(url: str, headers: dict[str, str], timeout: float) -> dict[str, object]:
        _ = headers, timeout
        return {
            "pairs": [
                {
                    "chainId": "solana",
                    "dexId": "raydium",
                    "pairAddress": "pair-bonk-usdc",
                    "baseToken": {"symbol": "BONK", "address": "bonk-mint"},
                    "quoteToken": {"symbol": "USDC", "address": "usdc-mint"},
                }
            ]
        }

    result = resolve_token_addresses_for_discovery("solana", "BONK", http_get=http_get)
    assert result["chain_type"] == "solana"
    assert result["token_mint"] == "bonk-mint"


def test_resolve_token_addresses_for_discovery_returns_empty_on_failure() -> None:
    from sentinel.onchain_live_sources import resolve_token_addresses_for_discovery

    def http_get(url: str, headers: dict[str, str], timeout: float) -> dict[str, object]:
        raise RuntimeError("network error")

    result = resolve_token_addresses_for_discovery("solana", "UNKNOWN", http_get=http_get)
    assert result == {}


def test_resolve_token_addresses_for_discovery_returns_empty_on_no_pairs() -> None:
    from sentinel.onchain_live_sources import resolve_token_addresses_for_discovery

    def http_get(url: str, headers: dict[str, str], timeout: float) -> dict[str, object]:
        return {"pairs": []}

    result = resolve_token_addresses_for_discovery("solana", "NOTHING", http_get=http_get)
    assert result == {}


# ── _build_source_from_profile ─────────────────────────────────────────────


def test_build_source_from_profile_solana() -> None:
    from sentinel.onchain_live_sources import _build_source_from_profile, SolanaWalletTradeSource
    from core.schemas import MeasurementProfile
    from datetime import UTC, datetime

    profile = MeasurementProfile(
        profile_id="disc:test",
        chain="solana",
        token="TEST",
        discovery_event_type="alpha.launch_candidate",
        discovery_event_id="test-evt",
        registered_at=datetime.now(UTC),
        token_mint="test-mint",
        quote_mint="usdc-mint",
        pool_address="pool-1",
        chain_type="solana",
    )

    source = _build_source_from_profile(AppSettings.load(), profile)
    assert source is not None
    assert isinstance(source, SolanaWalletTradeSource)
    assert source.config.token == "TEST"
    assert source.config.token_mint == "test-mint"


def test_build_source_from_profile_evm() -> None:
    from sentinel.onchain_live_sources import _build_source_from_profile, EvmPoolSwapTradeSource
    from core.schemas import MeasurementProfile
    from datetime import UTC, datetime

    profile = MeasurementProfile(
        profile_id="disc:test-evm",
        chain="base",
        token="AERO",
        discovery_event_type="alpha.launch_candidate",
        discovery_event_id="test-evt",
        registered_at=datetime.now(UTC),
        token_contract="0xaero-ct",
        quote_contract="0xusdc-ct",
        pool_address="0xpool",
        chain_type="evm",
    )

    source = _build_source_from_profile(AppSettings.load(), profile)
    assert source is not None
    assert isinstance(source, EvmPoolSwapTradeSource)
    assert source.config.token == "AERO"


def test_build_source_from_profile_returns_none_when_incomplete() -> None:
    from sentinel.onchain_live_sources import _build_source_from_profile
    from core.schemas import MeasurementProfile
    from datetime import UTC, datetime

    profile = MeasurementProfile(
        profile_id="disc:incomplete",
        chain="solana",
        token="INCOMPLETE",
        discovery_event_type="alpha.launch_candidate",
        discovery_event_id="inc-evt",
        registered_at=datetime.now(UTC),
        chain_type="solana",
        # missing token_mint, quote_mint
    )

    source = _build_source_from_profile(AppSettings.load(), profile)
    assert source is None


def test_build_source_from_profile_returns_none_for_unsupported_chain_type() -> None:
    from sentinel.onchain_live_sources import _build_source_from_profile
    from core.schemas import MeasurementProfile
    from datetime import UTC, datetime

    profile = MeasurementProfile(
        profile_id="disc:unknown",
        chain="unknown",
        token="XXX",
        discovery_event_type="alpha.launch_candidate",
        discovery_event_id="unknown-evt",
        registered_at=datetime.now(UTC),
        chain_type=None,
    )

    source = _build_source_from_profile(AppSettings.load(), profile)
    assert source is None


# ── build_live_sources with registry ───────────────────────────────────────


def test_build_live_sources_includes_registry_profiles() -> None:
    from sentinel.onchain_live_sources import MeasurementProfileRegistry, _build_source_from_profile
    from core.schemas import MeasurementProfile
    from datetime import UTC, datetime

    settings = AppSettings.load()
    registry = MeasurementProfileRegistry()

    profile = MeasurementProfile(
        profile_id="disc:reg-test",
        chain="solana",
        token="REGTOKEN",
        discovery_event_type="alpha.launch_candidate",
        discovery_event_id="reg-evt",
        registered_at=datetime.now(UTC),
        token_mint="reg-mint",
        quote_mint="usdc-mint",
        pool_address="pool-reg",
        chain_type="solana",
    )
    registry.register(profile)

    sources = build_live_sources(settings, registry=registry)

    profile_sources = [s for s in sources if "profile_" in s.config.source_name]
    assert len(profile_sources) >= 1
    assert profile_sources[0].config.token == "REGTOKEN"


def test_build_live_sources_with_empty_registry_returns_static_sources_only() -> None:
    from sentinel.onchain_live_sources import MeasurementProfileRegistry

    settings = AppSettings.load().model_copy(
        update={
            "acquisition": {
                "solana_wallet_trade": {
                    "enabled": True,
                    "wallet_address": "wallet-1",
                    "token": "BONK",
                    "token_mint": "token-mint",
                    "quote_mint": "quote-mint",
                },
            }
        }
    )
    registry = MeasurementProfileRegistry()

    sources = build_live_sources(settings, registry=registry)

    assert len(sources) == 1
    assert isinstance(sources[0], SolanaWalletTradeSource)


# ── Redis-backed MeasurementProfileRegistry ────────────────────────────────


class _FakeRedis:
    """Minimal Redis mock for testing persistence layer."""

    def __init__(self) -> None:
        self._data: dict[str, bytes] = {}

    def setex(self, key: str, seconds: int, value: str) -> None:
        _ = seconds
        self._data[key] = value.encode("utf-8")

    def get(self, key: str) -> str | None:
        raw = self._data.get(key)
        return raw.decode("utf-8") if raw is not None else None

    def delete(self, *keys: str) -> int:
        count = 0
        for key in keys:
            if key in self._data:
                del self._data[key]
                count += 1
        return count

    def scan(self, cursor: int, *, match: str, count: int) -> tuple[int, list[str]]:
        _ = cursor, count
        import fnmatch

        matching = [k for k in self._data if fnmatch.fnmatch(k, match)]
        return 0, matching


def test_registry_with_redis_persists_profile_on_register() -> None:
    from sentinel.onchain_live_sources import MeasurementProfileRegistry
    from core.schemas import MeasurementProfile
    from datetime import UTC, datetime

    fake = _FakeRedis()
    registry = MeasurementProfileRegistry(redis_client=fake)  # type: ignore[arg-type]

    profile = MeasurementProfile(
        profile_id="disc:evt-1",
        chain="solana",
        token="TEST",
        discovery_event_type="alpha.launch_candidate",
        discovery_event_id="evt-1",
        registered_at=datetime.now(UTC),
        token_mint="test-mint",
        quote_mint="usdc-mint",
        chain_type="solana",
    )
    registry.register(profile)

    key = "measurement:profile:solana:TEST"
    raw = fake.get(key)
    assert raw is not None
    assert '"token":"TEST"' in raw
    assert '"chain":"solana"' in raw


def test_registry_with_redis_loads_profiles_on_lazy_init() -> None:
    from sentinel.onchain_live_sources import MeasurementProfileRegistry
    from core.schemas import MeasurementProfile
    from datetime import UTC, datetime

    fake = _FakeRedis()
    key = "measurement:profile:solana:TEST"

    # Pre-populate Redis with a profile (simulating another worker's write)
    profile = MeasurementProfile(
        profile_id="disc:evt-2",
        chain="solana",
        token="TEST",
        discovery_event_type="alpha.launch_candidate",
        discovery_event_id="evt-2",
        registered_at=datetime.now(UTC),
        token_mint="test-mint",
        quote_mint="usdc-mint",
        chain_type="solana",
    )
    fake.setex(key, 3600, profile.model_dump_json())

    # Create a fresh registry — it should lazy-load from Redis on first access
    registry = MeasurementProfileRegistry(redis_client=fake)  # type: ignore[arg-type]

    fetched = registry.get("solana", "TEST")
    assert fetched is not None
    assert fetched.profile_id == "disc:evt-2"
    assert fetched.token_mint == "test-mint"
    assert fetched.chain_type == "solana"


def test_registry_with_redis_skips_stale_profiles_on_load() -> None:
    from sentinel.onchain_live_sources import MeasurementProfileRegistry
    from core.schemas import MeasurementProfile
    from datetime import UTC, datetime, timedelta

    fake = _FakeRedis()

    # Stale profile (TTL expired relative to registered_at)
    stale = MeasurementProfile(
        profile_id="disc:stale",
        chain="solana",
        token="STALE",
        discovery_event_type="alpha.launch_candidate",
        discovery_event_id="stale-evt",
        registered_at=datetime.now(UTC) - timedelta(hours=2),
        ttl_seconds=3600.0,
        chain_type="solana",
    )
    fake.setex("measurement:profile:solana:STALE", 3600, stale.model_dump_json())

    registry = MeasurementProfileRegistry(redis_client=fake)  # type: ignore[arg-type]

    assert registry.get("solana", "STALE") is None
    # stale should also be removed from Redis
    assert fake.get("measurement:profile:solana:STALE") is None


def test_registry_with_redis_cleanup_removes_stale_from_redis() -> None:
    from sentinel.onchain_live_sources import MeasurementProfileRegistry
    from core.schemas import MeasurementProfile
    from datetime import UTC, datetime, timedelta

    fake = _FakeRedis()
    registry = MeasurementProfileRegistry(redis_client=fake)  # type: ignore[arg-type]

    stale = MeasurementProfile(
        profile_id="disc:stale",
        chain="solana",
        token="STALE",
        discovery_event_type="alpha.launch_candidate",
        discovery_event_id="stale-evt",
        registered_at=datetime.now(UTC) - timedelta(hours=2),
        ttl_seconds=3600.0,
        chain_type="solana",
    )
    registry.register(stale)
    assert fake.get("measurement:profile:solana:STALE") is not None

    registry.cleanup()
    assert registry.count() == 0
    assert fake.get("measurement:profile:solana:STALE") is None


def test_in_memory_registry_still_works_without_redis() -> None:
    from sentinel.onchain_live_sources import MeasurementProfileRegistry
    from core.schemas import MeasurementProfile
    from datetime import UTC, datetime

    registry = MeasurementProfileRegistry()  # no redis_client

    profile = MeasurementProfile(
        profile_id="disc:no-redis",
        chain="solana",
        token="MEMORY",
        discovery_event_type="alpha.launch_candidate",
        discovery_event_id="no-redis-evt",
        registered_at=datetime.now(UTC),
        chain_type="solana",
    )
    registry.register(profile)

    assert registry.count() == 1
    assert registry.get("solana", "MEMORY") is not None