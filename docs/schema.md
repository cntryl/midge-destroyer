# Report Schema

All serialized reports include schema version fields:

- `midge-destroyer.ledger/v1` for final ledgers.
- `midge-destroyer.lifecycle/v2` for the separate worker lifecycle/error channel.
- `midge-destroyer.report/v3` for scenario reports with recovery budgets,
  per-fault recovery events, infrastructure attribution, and the verdicts
  `pass`, `wobble`, `bend`, `break`, `infrastructure_error`, and `skipped`.
- `midge-destroyer.suite-manifest/v3` for one execution-scoped suite manifest
  whose scenario artifact paths are nested beneath the suite directory.

The report aggregator reads only v3 suite manifests. Standalone scenario reports
from older or unrelated executions are intentionally ignored.

## Determinism

All scenario plans are derived from `seed + scale` and use deterministic generation paths.
