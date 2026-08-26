# Fault Model v1

## Black-box faults

- `ProcessKill`: worker process termination.
- `ForcedReopen`: controlled close + reopen sequence.
- `StaleCacheCleanup`: removal of cached/local state.
- `DroppedWrite`: partial write surface via direct artifact manipulation.
- `WalTruncationRace`: intentional truncation of WAL artifacts.
- `ManifestInterruption`: manifest edits interrupted.
- `SstCorruption`: local SST file mutation.
- `CompactionRace`: race against compaction ordering.
- `LeaseStalenessWindow`: stale lease simulation.
- `ProviderLatencySpike`: synthetic delay at fault boundary.
- `RegionPartition`: I/O partition simulation.
- `StrictAsyncDurabilityFlip`: dynamic write policy alternation.

## Optional failpoint faults

- Exact WAL path faulting.
- Manifest checkpoint interruptions.
- Flush/compaction barrier faulting.
- Lease renewal cut points.
- Migration boundary truncation.

Every fault defines whether it is expected to preserve safety or induce temporary unavailability.
