# Alibaba Cloud Production Design

Updated: 2026-05-05

## Goal

Move SignalEngine from local single-host orchestration into a production layout on Alibaba Cloud without changing the core event pipeline contract.

This document covers:

- process supervision
- external Redis and PostgreSQL
- configuration and secret injection
- deployment topology
- recovery and rollback

This document does not cover feature work. Runtime behavior should remain the same as the current worker modes in [core/worker.py](../core/worker.py) and the local launcher in [run_full_stack.sh](../run_full_stack.sh).

## Recommended First Production Shape

Use a small ECS-based deployment first instead of jumping directly to Kubernetes.

Recommended initial components:

- 1 ECS instance for stateless worker processes
- ApsaraDB RDS for PostgreSQL
- ApsaraDB for Redis or Tair for Redis Streams
- Alibaba Cloud OSS for replay datasets, exported reports, and optional backups
- Alibaba Cloud Simple Log Service for centralized logs
- Managed Prometheus and Grafana for metrics dashboards and alert routing

Reasoning:

- the current system already runs as separate long-lived CLI worker modes
- ECS plus `systemd` is the shortest path from the existing local process model
- Redis and PostgreSQL are already externalizable via config
- this avoids introducing Kubernetes operational complexity before live trading is enabled

## Process Model

The current local runtime launches these long-lived processes:

- pipeline worker
- on-chain feature live sync
- launch alpha live sync
- catalyst alpha live sync
- flow measurement live sync
- Telegram publisher
- wallet intelligence sync

In production, each long-lived mode should run as its own `systemd` unit.

Recommended units:

- `signalengine-pipeline.service`
- `signalengine-onchain-feature.service`
- `signalengine-launch-alpha.service`
- `signalengine-catalyst-alpha.service`
- `signalengine-flow-measurement.service`
- `signalengine-telegram-publisher.service`
- `signalengine-wallet-intelligence.service`

Each unit should:

- run exactly one worker mode
- restart automatically on failure
- write logs to stdout and stderr for journald collection
- load the same environment file format used locally
- expose a stable working directory and virtualenv path

## Managed Infrastructure

### PostgreSQL

Use ApsaraDB RDS for PostgreSQL.

Requirements:

- private VPC access only
- automatic backups enabled
- point-in-time recovery enabled
- separate production database and optional staging database
- connection limit sized for all concurrent workers plus maintenance jobs

### Redis

Use ApsaraDB for Redis or Tair with private VPC access.

Requirements:

- Redis Streams support
- persistence enabled
- memory sizing based on expected stream retention and consumer lag
- slowlog and connection monitoring enabled

### Object Storage

Use OSS for:

- replay input datasets
- exported comparison reports
- optional daily logical backups

## Network Topology

Recommended initial topology:

- one VPC
- one private subnet for ECS workers
- one private subnet or managed private endpoint path for RDS and Redis
- one bastion or controlled SSH entry path for operations
- no public database endpoints

Optional public ingress:

- only if a webhook collector or external callback endpoint is introduced later
- keep current worker-only deployment private when possible

## Configuration Injection

Production configuration should not rely on editing [infra/settings.yaml](../infra/settings.yaml) directly on the server.

Recommended split:

- repository-shipped safe defaults stay in YAML
- environment-specific overrides live in a deployment-managed env file
- secrets are injected from Alibaba Cloud KMS or a secure secret store into the env file generation step

Recommended runtime files on ECS:

- `/opt/signalengine/app` for checked-out code
- `/opt/signalengine/venv` for the Python environment
- `/etc/signalengine/signalengine.env` for non-secret and secret environment variables

The env file should contain:

- Redis URL
- PostgreSQL URL
- Telegram credentials when enabled
- live wallet and venue credentials only when the rollout phase allows them
- environment name such as `paper` or `live`

## Deployment Workflow

Recommended first deployment workflow:

1. Build and test in CI.
2. Produce a versioned artifact or a pinned git revision.
3. Copy artifact to ECS.
4. Create or update virtualenv dependencies.
5. Install or update `systemd` unit files.
6. Run `signalengine-worker --healthcheck` against production config.
7. Restart one unit at a time.
8. Verify logs, Redis connectivity, PostgreSQL connectivity, and metrics targets.

Avoid:

- pushing directly from a developer laptop into the production host
- running all worker modes inside a single `nohup` shell script in production
- storing secrets in the repository or inside YAML defaults

## Rollout Strategy

The production rollout should remain paper-first.

Recommended stages:

1. data collection only
2. paper pipeline plus persistence
3. notifications enabled
4. replay and attribution regression gate
5. guarded live execution for one venue path

Live execution should remain blocked until:

- replay baselines are stable
- reconciliation is verified against production-like conditions
- alerts are actionable and tested
- kill switch operation is documented and exercised

## Recovery and Failure Handling

### Worker Failure

Primary mechanism:

- `Restart=always` in `systemd`
- short restart delay
- journald plus centralized log collection

### Host Failure

Primary mechanism:

- redeploy the same artifact onto a fresh ECS instance
- reconnect to managed Redis and PostgreSQL
- replay or resume from persisted checkpoints

### Redis or PostgreSQL Outage

Required operator actions:

- pause dependent worker units if the outage is prolonged
- validate backlog growth and consumer lag before restart
- replay dead letters only after root cause is resolved

### Deployment Rollback

Rollback should be artifact-based:

- keep the prior release available on disk or in artifact storage
- switch the active symlink or version path
- restart affected `systemd` units one by one

## Operational Readiness Checklist

Before using Alibaba Cloud production for anything beyond paper mode, confirm all of the following:

- managed Redis and PostgreSQL are private and backed up
- all worker units have restart policies and log collection
- production config is injected externally, not edited by hand in repo files
- healthcheck command is wired into deployment validation
- dashboards exist for worker health, stream lag, risk rejections, and delivery failures
- rollback instructions are documented and tested
- kill switch ownership is explicit

## Recommended Next Follow-Up

The next implementation task after this document is observability hardening:

- worker health checks
- Redis stream backlog monitoring
- Telegram delivery failure alerts
- production thresholds for adapter failures, live source failures, and risk rejection spikes