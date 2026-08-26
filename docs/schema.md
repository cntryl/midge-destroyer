# Report Schema

All serialized reports include schema version fields:

- `midge-destroyer.ledger/v1` for final ledgers.
- `midge-destroyer.report/v1` for scenario-level report.
- `midge-destroyer.suite-report/v1` for suite aggregate.

## Determinism

All scenario plans are derived from `seed + scale` and use deterministic generation paths.
