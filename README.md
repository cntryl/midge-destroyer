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
- `destroyer suite <smoke|standard|soak> --cloud <backend> --seed <u64> --report-json`
- `destroyer report --report-json`
- `destroyer frontier <scenario|all> --max-scale <small|medium|large|xlarge>`

Executable black-box scenarios include `recovery-crash-loop`,
`lease-takeover-latency`, `uuid-compaction-pressure`,
`scan-compaction-starvation`, `snapshot-pinned-gc-pressure`,
`multi-cf-hot-cold-interference`, `delete-space-amplification`,
`cold-cache-read-storm`, `ack-kill-window`, `cloud-cache-loss`,
`manifest-race`, `sst-corruption`, `wal-truncation-race`,
`stale-cache-recovery`, and `sqrzl-visibility`.
`cloud-cache-loss`, `cold-cache-read-storm`, `wal-truncation-race`, and `stale-cache-recovery`
require `s3`, `azure`, `gcs`, or another cloud backend.
The current `wal-truncation-race` injector removes cloud WAL cache files;
it does not truncate arbitrary durable records or simulate an exact engine cut.

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

Cloud protocol campaigns use the pinned Sqrzl emulator digest in the Compose files. Selecting `s3`,
`azure`, or `gcs` chooses the Sqrzl protocol surface used by Midge. The
controller starts the matching Compose project, runs the command, and brings
it down even when the harness returns an error:

```sh
cargo run --bin midge-destroyer -- run smoke-local --cloud s3 --scale small --seed 1

cargo run --bin midge-destroyer -- run smoke-local --cloud azure --scale small --seed 1

cargo run --bin midge-destroyer -- run smoke-local --cloud gcs --scale small --seed 1
```

Each command gets a unique Compose project and loopback-only dynamic API and
health ports. The resolved API endpoint is passed explicitly to every worker.
The execution directory retains the bind-mounted Sqrzl blobs, source and
resolved Compose configuration, endpoints, health probes, service state, logs,
and teardown result. Suites probe health before and after every scenario and
restart an unhealthy emulator before continuing.

Standard and soak suites run their full applicable catalog in stable order.
Failpoint scenarios appear as `skipped` unless the binary was built with
`--features failpoint-tier`; only an explicit `--max-scenarios` truncates a
suite. Aggregation reads the new execution-scoped `suite-manifest.json` files,
so stale standalone reports cannot be mixed into a result.

## Plan expectations

- Pull requests run lint, harness tests, target/provenance tests and real local recovery smoke.
- Recurring campaigns run separately from pull request CI and retain execution-scoped evidence.

## Engine targets and continuous campaigns

The default Cargo dependency is the published `cntryl-midge = "=0.3.1"` crate.
No sibling checkout is required. Use standard Cargo dependency sources:
registry versions for releases, a Git revision for branch builds, or an explicit
local path override for engine development.

`campaign.py` copies the harness into a new directory outside the checkout,
uses `cargo add` to select the dependency, verifies Cargo metadata and the
lockfile, then builds both controller and worker. The source checkout and its
lockfile stay unchanged. `latest` selects the newest non-yanked stable release.
`main` and `develop` resolve once to full commit SHAs, rather than floating while
a campaign runs. `git:<SHA>` replays that exact commit. An explicit released
version pins its crates.io checksum. Unsupported API changes fail the build;
the helper does not silently choose another engine or remove scenario checks.

```sh
python3 scripts/campaign.py prepare --target latest --output /tmp/destroyer-latest
python3 scripts/campaign.py run --output /tmp/destroyer-latest --backend local --preset standard --seeds 2 --seed-start 100

python3 scripts/campaign.py prepare --target develop --output /tmp/destroyer-develop
python3 scripts/campaign.py prepare --target main --output /tmp/destroyer-main
python3 scripts/campaign.py prepare --target 0.3.1 --output /tmp/destroyer-031
```

Add `--instrumented` to `prepare` for the optional failpoint tier. Reports record
the engine source, version, checksum or Git SHA, enabled features, harness
revision and dirty state, Rust tools, and emulator digest. The workflow also
retains its resolved campaign plan. A dirty local harness run is identified as
such; scheduled qualification uses a clean checkout. Seeds reproduce operation
and fault plans, not the operating system's thread schedule.

For an explicit local source override, Cargo supports:

```sh
cargo build --bins --config 'patch.crates-io.cntryl-midge.path="/absolute/path/to/midge"'
```

The local crate version must satisfy the manifest requirement. Local override
runs are development evidence; a published-release campaign rejects a path
patch and requires the selected registry checksum.

The `Continuous Midge campaigns` workflow runs on these UTC schedules:

| Cadence | Target | Workload |
| --- | --- | --- |
| Hourly at minute 17 | Latest published stable release | Two independent local standard campaigns |
| Daily at 02:43 | Latest published stable release | S3, Azure and GCS soak campaigns, plus a separate local failpoint campaign |
| Every six hours at minute 13 | Main and develop | Independent local standard campaigns pinned to branch commits |
| Manual | Latest, release version, branch or exact Git SHA | Backend, preset, 1-4 seeds and optional failpoint tier |

Scheduled seeds vary with workflow run ID and attempt, and each invocation is
recorded before execution. Jobs have a 90-minute bound; each seed has a
60-minute bound. Campaign lanes do not cancel an active predecessor, and a
failed seed does not suppress later seeds. Scheduled suites run the complete
applicable catalog without truncation. A manual local run can use
`--max-scenarios` for development; that truncated run is not full qualification.
For capacity exploration, use the prepared controller's existing `frontier`
command with an explicit seed range and scale. Capacity boundaries alone do
not establish a correctness defect.

The runner reads actual suite verdicts, because the suite CLI exit code alone
does not express scenario failures. `break` and `infrastructure_error` fail the
campaign. `wobble` and `bend` remain warning candidates. Empty, entirely skipped,
unknown or missing reports fail closed. Optional cuts can be skipped in a
black-box build and are exercised by the separate instrumented campaign.
Cloud protocol names refer to isolated Sqrzl qualification, not live-provider
or production-capacity proof.

Artifacts are retained for 30 days on success, failure and cancellation:
provenance, lockfile, Cargo metadata, commands, controller/worker logs, expected
ledgers, database and emulator state, suite manifests and `triage.md`.
Compilation failures retain target/provenance evidence and job logs. Uploads
exclude compiled dependency directories. Workflow summaries link observations
to their scenario artifacts. No workflow automatically files a Midge issue.

## From campaigns to the next release roadmap

Treat `triage.md` as an investigation queue. Reproduce the recorded target,
harness revision, tier, emulator digest, seed and preset. Separate emulator and
harness failures from engine behavior. Verify the expected ledger against the
public contract, minimize the failure and add a regression test. Check open
Midge issues before filing a confirmed defect with priority and subsystem labels.
Add confirmed issue links to the next release roadmap. Keep recovery warnings,
capacity boundaries and skipped coverage separate from confirmed defects.
