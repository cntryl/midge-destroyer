# Lease profiles

`midge-destroyer` keeps Midge's conservative lease behavior as the default and
lets crash/recovery campaigns opt into a bounded-failover profile.

| Profile | TTL | Clock-skew tolerance | Warning | Soft deadline |
| --- | ---: | ---: | ---: | ---: |
| `conservative` | 30s | 15s | 40s | 50s |
| `bounded-failover` | 30s | 5s | 32s | 40s |

The bounded profile is an availability experiment, not a fencing bypass. Epoch
checks, conditional lease writes, and fail-closed renewal behavior remain
unchanged. Correct recovery between the warning and soft deadline is a `wobble`;
correct recovery after the soft deadline but before the hard observation
deadline is a `bend`. The hard deadline is the larger of `--recovery-timeout`
and twice the soft deadline. Missing or stale-holder mutations remain `break`
verdicts.

Example:

```text
destroyer run lease-takeover-latency --cloud s3 --seed 42 \
  --scale large --lease-profile bounded-failover
```
