"""Resolve scheduled or manual campaigns without floating engine targets."""
import json
import os
from pathlib import Path

from campaign import resolve_target


def plan(event, schedule, requested, backend, preset, seeds, seed_start, instrumented, run_id, attempt):
    selections = []
    if event == "workflow_dispatch":
        if backend not in {"local", "sqrzl", "s3", "azure", "gcs"} or preset not in {"smoke", "standard", "soak"}:
            raise ValueError("invalid backend or preset")
        if not 1 <= seeds <= 4 or seed_start < 0 or seed_start + seeds > 2**64:
            raise ValueError("manual campaigns need 1-4 seeds in the u64 range")
        selections.append((requested, backend, preset, seeds, seed_start, instrumented, "manual"))
    elif event == "schedule":
        start = run_id * 64 + attempt * 16
        if schedule == "17 * * * *":
            for slot in range(2):
                selections.append(("latest", "local", "standard", 1, start + slot, False, "hourly-" + str(slot)))
        elif schedule == "43 2 * * *":
            for slot, provider in enumerate(["s3", "azure", "gcs"]):
                selections.append(("latest", provider, "soak", 1, start + slot, False, "daily-" + provider))
            selections.append(("latest", "local", "standard", 1, start + 3, True, "daily-failpoints"))
        elif schedule == "13 */6 * * *":
            for slot, target in enumerate(["main", "develop"]):
                selections.append((target, "local", "standard", 1, start + slot, False, "branch-" + target))
        else:
            raise ValueError("unrecognized schedule")
    else:
        raise ValueError("unsupported campaign event")
    resolved = {}
    entries = []
    for target, provider, selected_preset, count, first, tier, lane in selections:
        if target not in resolved:
            value = resolve_target(target)
            resolved[target] = "git:" + value["revision"] if value["kind"] == "git" else value["version"]
        entries.append({"requested": target, "resolved": resolved[target], "backend": provider,
                        "preset": selected_preset, "seeds": count, "seed_start": first,
                        "instrumented": tier, "lane": lane})
    return {"include": entries}


if __name__ == "__main__":
    matrix = plan(os.environ["EVENT_NAME"], os.environ.get("EVENT_SCHEDULE", ""),
                  os.environ.get("REQUESTED_TARGET", "latest"), os.environ.get("BACKEND", "local"),
                  os.environ.get("PRESET", "standard"), int(os.environ.get("SEEDS") or "2"),
                  int(os.environ.get("SEED_START") or "1"), os.environ.get("INSTRUMENTED", "false") == "true",
                  int(os.environ["RUN_ID"]), int(os.environ["RUN_ATTEMPT"]))
    Path("campaign-plan.json").write_text(json.dumps(matrix, indent=2) + "\n")
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write("matrix=" + json.dumps(matrix) + "\n")
