from __future__ import annotations

import logging

from infra.logging import configure_logging


def test_configure_logging_clamps_http_client_noise() -> None:
    configure_logging("INFO")

    assert logging.getLogger("httpx").level == logging.WARNING
    assert logging.getLogger("httpcore").level == logging.WARNING