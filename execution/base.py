from __future__ import annotations

from abc import ABC, abstractmethod

from core.schemas import (
    ExecutionIntent,
    ExecutionQuote,
    ExecutionReport,
    PreparedExecution,
    RiskDecision,
)


class ExecutionAdapter(ABC):
    adapter_name: str

    def prepare(self, intent: ExecutionIntent, risk: RiskDecision) -> PreparedExecution:
        quote = self.quote(intent, risk)
        return PreparedExecution(
            intent=intent,
            quote=quote,
            adapter_name=self.adapter_name,
            requested_notional_usd=risk.adjusted_notional_usd,
            simulation=True,
        )

    @abstractmethod
    def quote(self, intent: ExecutionIntent, risk: RiskDecision) -> ExecutionQuote:
        raise NotImplementedError

    @abstractmethod
    def execute(self, prepared: PreparedExecution) -> ExecutionReport:
        raise NotImplementedError