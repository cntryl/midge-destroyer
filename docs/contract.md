# Midge Destroyer Contract v1

## Mutation semantics

- Keys are byte strings and values are byte strings.
- Mutations are executed through Midge public engine API only.
- `dispatched` means command was in expected ledger before worker result is known.
- `acked` means command committed and persisted according to chosen write options.
- `failed` means worker committed an explicit error.
- `unknown` means worker died before reporting final outcome.
- `duplicate` means the same operation_id was observed more than once.
- `missing` means expected operation had no observable worker result.

## Expected safety goals

- No duplicate keys with identical operation_id should be treated as independent success events.
- Recovery paths that are restartable must preserve previously acknowledged state.
- Ledger replay is append-only and authoritative for expected run behavior.
- Artifact capture is mandatory per scenario for deterministic replay.
