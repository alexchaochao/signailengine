from __future__ import annotations

import json
import logging

from infra.logging import JsonFormatter, configure_logging


def test_configure_logging_clamps_http_client_noise() -> None:
    configure_logging("INFO")

    assert logging.getLogger("httpx").level == logging.WARNING
    assert logging.getLogger("httpcore").level == logging.WARNING


def test_json_formatter_includes_exception_text_and_extra_fields() -> None:
    formatter = JsonFormatter()
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        record = logging.getLogger("signalengine.test").makeRecord(
            "signalengine.test",
            logging.ERROR,
            __file__,
            0,
            "failure",
            (),
            __import__("sys").exc_info(),
            extra={
                "service": "catalyst_alpha_live",
                "provider": "binance_cms_api",
                "source_url": "https://example.invalid/feed",
                "error_type": "RuntimeError",
            },
        )

    payload = json.loads(formatter.format(record))
    assert payload["message"] == "failure"
    assert "RuntimeError: boom" in payload["exception"]
    assert payload["provider"] == "binance_cms_api"
    assert payload["source_url"] == "https://example.invalid/feed"
    assert payload["error_type"] == "RuntimeError"