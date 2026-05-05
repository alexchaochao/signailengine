# Configuration Strategy

## Sources and Precedence

Configuration precedence from highest to lowest:

1. environment variables prefixed with `SIGNALENGINE_`
2. process-level secret injection outside the repository
3. defaults from [infra/settings.yaml](../infra/settings.yaml)

The project uses nested environment overrides via double underscore.

Examples:

- `SIGNALENGINE_RUNTIME__ENVIRONMENT=paper`
- `SIGNALENGINE_REDIS__URL=redis://localhost:6379/1`
- `SIGNALENGINE_POSTGRES__URL=postgresql+psycopg://signalengine:signalengine@localhost:5432/signalengine`

---

## Environments

- `local`: local development and smoke tests
- `paper`: paper-trading deployment mode
- `live`: reserved, blocked by policy until later phases

---

## Settings Groups

### Runtime

- application name
- environment name
- log level

### Redis

- URL
- stream names
- consumer group defaults

### Postgres

- database URL
- echo toggle
- pool sizing

### Observability

- metrics host
- metrics port
- service namespace

### Risk

- max token exposure
- max chain exposure
- max concurrent positions
- max daily loss
- cooldown minutes
- live trading enabled flag

### Venues

- DEX adapter name
- CEX adapter name
- paper execution enabled flag

---

## Operational Rules

- source-controlled config must contain safe defaults only
- live credentials must never be stored in the repository
- `live_trading_enabled` must default to `false`
- any new required setting must be added to `.env.example` and documented here