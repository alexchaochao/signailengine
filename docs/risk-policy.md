# Risk Policy Baseline

## Purpose

This document defines the minimum policy surface that the Phase 1 and later risk engine must enforce.

---

## Environment Policy

### Local

- execution mode: paper only
- live trading: disabled
- intended use: development and smoke tests

### Paper

- execution mode: simulated order flow
- live trading: disabled
- intended use: end-to-end strategy validation

### Live

- execution mode: unavailable in current phase
- live trading: disabled until replay, alerts, reconciliation, and kill switch exist

---

## Portfolio Limits

- max per-token exposure: 0.10
- max chain exposure: 0.40
- max concurrent positions: 5
- max daily loss: 0.03
- default cooldown after exit: 30 minutes

---

## Trade Admission Requirements

A trade may proceed only if all of the following are true:

1. the token is in the allowlist
2. estimated liquidity is at least 100000 USD
3. estimated 5 minute volume is at least 25000 USD
4. expected slippage is within the configured cap
5. the portfolio remains within exposure limits after sizing
6. the system is in `local` or `paper` mode

---

## Live Trading Guard

Live trading must remain disabled until these controls are present:

1. replay framework
2. risk gate implementation
3. execution reconciliation
4. global kill switch
5. alerting for infra and execution failures
6. position and balance validation