# midge-destroyer

Midge-specific adversarial correctness and recovery harness.

## Scope

- Local, Sqrzl-simulated, S3, Azure Blob, and GCS backends.
- Black-box primary execution:
  - deterministic scenario generation
  - external worker subprocesses
  - append-only expected ledgers and replay validation
- Optional failpoint tier (`--features failpoint-tier`) with durable sentinels.

## CLI

- `destroyer run <scenario> --cloud <local|sqrzl|s3|azure|gcs> --seed <u64> --scale <small|medium|large|xlarge>`
- `destroyer suite <smoke|standard|soak> --report-json`
- `destroyer report --report-json`
- `destroyer frontier <scenario|all> --max-scale <small|medium|large|xlarge>`

Executable black-box scenarios include `recovery-crash-loop`, `ack-kill-window`,
`cloud-cache-loss`, `manifest-race`, and `sst-corruption`.
`cloud-cache-loss` requires `s3`, `azure`, `gcs`, or another cloud backend.

Exact engine-cut scenarios require `--features failpoint-tier`:
`wal-sync-ack-cut`, `manifest-sync-failure`, `compaction-commit-cut`,
`wal-prune-cut`, `lease-renewal-failure`, and `flush-barrier`. These refuse to
run when the feature is absent.

## Semantics (Midge-specific)

- Operations are `Put`, `Delete`, and durability-mode mutations.
- Outcomes are tracked as:
  - `dispatched`, `acked`, `failed`, `unknown`, `duplicate`, `missing`.
- Replay behavior is deterministic and seed-based.
- Every scenario run records artifacts:
  - seed and scenario metadata
  - command stream
  - worker logs/reports
  - DB directories
  - final ledger and verifier results.

## Cloud mode

`--cloud sqrzl` is included for parity testing and chaos injection.
This mode is manual by default and only runs when `MIDGE_DESTROYER_CLOUD_SMOKE=1`.

Cloud blob storage is always the `ghcr.io/sqrzl/sqrzl-emulator:latest`
emulator. Selecting `s3`,
`azure`, or `gcs` chooses the Sqrzl protocol surface used by Midge. The
controller starts the matching Compose project, runs the command, and brings
it down even when the harness returns an error:

```sh
cargo run --bin midge-destroyer -- run smoke-local --cloud s3 --scale small --seed 1

cargo run --bin midge-destroyer -- run smoke-local --cloud azure --scale small --seed 1

cargo run --bin midge-destroyer -- run smoke-local --cloud gcs --scale small --seed 1
```

The Compose defaults can be overridden with the corresponding
`MIDGE_DESTROYER_S3_*`, `MIDGE_DESTROYER_AZURE_*`, and
`MIDGE_DESTROYER_GCS_*` environment variables.

## Plan expectations

- Fast, deterministic local smoke runs in CI.
- Long cloud/recovery workflows remain opt-in and manual.
