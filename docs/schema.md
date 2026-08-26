# Report Schema

All serialized reports include schema version fields:

- `midge-destroyer.ledger/v1` for final ledgers.
- `midge-destroyer.report/v2` for scenario-level reports, including lifecycle,
  timeout, and post-timeout recovery-verification fields. The report aggregator
  continues to accept v1 artifacts.
- `midge-destroyer.suite-report/v1` for suite aggregate.

## Determinism

All scenario plans are derived from `seed + scale` and use deterministic generation paths.
