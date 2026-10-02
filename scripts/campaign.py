#!/usr/bin/env python3
"""Build an exact Midge target and retain continuous campaign evidence."""
import argparse
import datetime
import json
import os
from pathlib import Path
import re
import signal
import shutil
import subprocess
import sys
import urllib.request

REPOSITORY = "https://github.com/cntryl/midge.git"
VERSION = re.compile(r"\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?")
CRATES_SOURCE = "registry+https://github.com/rust-lang/crates.io-index"
VERDICTS = {"pass", "wobble", "bend", "break", "infrastructure_error", "skipped"}


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n")


def command(args, cwd=None):
    return subprocess.check_output(args, cwd=cwd, text=True).strip()


def registry():
    request = urllib.request.Request(
        "https://crates.io/api/v1/crates/cntryl-midge",
        headers={"User-Agent": "midge-destroyer continuous qualification"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def resolve_target(target, registry_data=None, git_resolver=None):
    if target.startswith("git:") and re.fullmatch(r"[0-9a-f]{40}", target[4:]):
        return {"kind": "git", "requested": target, "revision": target[4:]}
    if target in {"main", "develop"}:
        resolve = git_resolver or (lambda branch: command(
            ["git", "ls-remote", REPOSITORY, "refs/heads/" + branch]
        ))
        output = resolve(target).split()
        if len(output) != 2 or output[1] != "refs/heads/" + target:
            raise ValueError("branch resolution did not return the requested ref")
        if not re.fullmatch(r"[0-9a-f]{40}", output[0]):
            raise ValueError("branch resolution did not return a commit SHA")
        return {"kind": "git", "requested": target, "revision": output[0]}
    if target != "latest" and not VERSION.fullmatch(target):
        raise ValueError("target must be latest, main, develop, a released version, or git:<SHA>")
    data = registry_data if registry_data is not None else registry()
    versions = data["versions"]
    if target == "latest":
        stable = [v for v in versions if not v["yanked"] and re.fullmatch(r"\d+\.\d+\.\d+", v["num"])]
        if not stable:
            raise ValueError("registry contains no non-yanked stable Midge release")
        selected = max(stable, key=lambda v: tuple(map(int, v["num"].split("."))))
    else:
        selected = next((v for v in versions if v["num"] == target and not v["yanked"]), None)
        if selected is None:
            raise ValueError("requested release is absent or yanked")
    return {"kind": "registry", "requested": target, "version": selected["num"], "checksum": selected["checksum"]}


def lock_package(lock_text):
    matches = []
    for block in lock_text.split("[[package]]")[1:]:
        fields = dict(re.findall(r'^([a-z]+) = "([^"]*)"$', block, re.MULTILINE))
        if fields.get("name") == "cntryl-midge":
            matches.append(fields)
    if len(matches) != 1:
        raise ValueError("lockfile must resolve exactly one cntryl-midge package")
    return matches[0]


def verify_resolution(target, metadata, lock_text):
    packages = [p for p in metadata["packages"] if p["name"] == "cntryl-midge"]
    if len(packages) != 1:
        raise ValueError("metadata must resolve exactly one cntryl-midge package")
    package = packages[0]
    locked = lock_package(lock_text)
    if locked["version"] != package["version"] or locked.get("source") != package.get("source"):
        raise ValueError("Cargo metadata and lockfile disagree")
    if target["kind"] == "registry":
        if package.get("source") != CRATES_SOURCE or package["version"] != target["version"]:
            raise ValueError("release target resolved a path, patch, or different version")
        if locked.get("checksum") != target["checksum"]:
            raise ValueError("registry checksum does not match the selected published artifact")
    elif package.get("source") != "git+" + REPOSITORY + "?rev=" + target["revision"] + "#" + target["revision"]:
        raise ValueError("Git target does not resolve the pinned Midge commit")
    features = next((n["features"] for n in (metadata.get("resolve") or {}).get("nodes", []) if n["id"] == package.get("id")), [])
    return {"version": package["version"], "source": package["source"], "checksum": locked.get("checksum"), "engine_features": sorted(features)}


def prepare(args):
    root = Path(__file__).resolve().parents[1]
    output = Path(args.output).resolve()
    if output == root or root in output.parents:
        raise ValueError("build output must be outside the source checkout")
    if output.exists():
        raise ValueError("use a new campaign output directory")
    output.mkdir(parents=True)
    target = resolve_target(args.target)
    write_json(output / "target.json", target)
    build = output / "build"
    shutil.copytree(root, build, ignore=shutil.ignore_patterns(".git", "target", "artifacts", "__pycache__"))
    add = ["cargo", "add", "cntryl-midge", "--manifest-path", str(build / "Cargo.toml")]
    if target["kind"] == "registry":
        add[2] += "@=" + target["version"]
        add += ["--registry", "crates-io"]
    else:
        add += ["--git", REPOSITORY, "--rev", target["revision"]]
    subprocess.run(add, check=True)
    features = ["--features", "failpoint-tier"] if args.instrumented else []
    metadata = json.loads(command(["cargo", "metadata", "--format-version", "1"] + features, cwd=build))
    lock_text = (build / "Cargo.lock").read_text()
    resolved = verify_resolution(target, metadata, lock_text)
    provenance = {
        "schema_version": "midge-destroyer.campaign/v1",
        "requested_target": args.requested_target or args.target,
        "target": target,
        "resolved": resolved,
        "tier": "failpoint" if args.instrumented else "black-box",
        "harness_revision": command(["git", "rev-parse", "HEAD"], cwd=root),
        "harness_dirty": bool(command(["git", "status", "--porcelain"], cwd=root)),
        "rustc": command(["rustc", "--version"]),
        "cargo": command(["cargo", "--version"]),
        "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "emulator_images": sorted(set(re.findall(r"image:\s*(\S+)", "\n".join(p.read_text() for p in build.glob("compose.*.yml"))))),
    }
    write_json(output / "provenance.json", provenance)
    write_json(output / "cargo-metadata.json", metadata)
    shutil.copy2(build / "Cargo.lock", output / "Cargo.lock")
    if not args.no_build:
        subprocess.run(["cargo", "build", "--release", "--locked", "--bins"] + features, cwd=build, check=True)


def inspect_suite(report):
    if report.get("schema_version") != "midge-destroyer.suite-manifest/v3":
        raise ValueError("missing or unsupported execution-scoped suite manifest")
    results = report.get("results", [])
    if not results or report.get("scenario_count") != len(results):
        raise ValueError("suite is empty or scenario coverage is incomplete")
    if any(r.get("verdict") not in VERDICTS for r in results):
        raise ValueError("suite contains an unknown verdict")
    if not any(r["verdict"] != "skipped" for r in results):
        raise ValueError("all scenarios were skipped")
    candidates = [r for r in results if r["verdict"] in {"wobble", "bend", "break", "infrastructure_error"}]
    failed = any(r["verdict"] in {"break", "infrastructure_error"} for r in results)
    return failed, candidates


def run(args):
    output = Path(args.output).resolve()
    if (output / "campaign-report.json").exists():
        raise ValueError("campaign already ran; prepare a new output directory to preserve its evidence")
    provenance = json.loads((output / "provenance.json").read_text())
    binary = output / "build/target/release/midge-destroyer"
    runs = []
    any_failure = False
    for offset in range(args.seeds):
        seed = args.seed_start + offset
        artifacts = output / ("seed-" + str(seed))
        invocation = [str(binary), "--artifacts-root", str(artifacts), "suite", args.preset,
                      "--cloud", args.backend, "--seed", str(seed), "--report-json"]
        if args.max_scenarios is not None:
            invocation += ["--max-scenarios", str(args.max_scenarios)]
        record = {"seed": seed, "backend": args.backend, "preset": args.preset, "command": invocation, "candidates": []}
        write_json(output / ("invocation-" + str(seed) + ".json"), record)
        with (output / ("stdout-" + str(seed) + ".log")).open("w") as stdout, (output / ("stderr-" + str(seed) + ".log")).open("w") as stderr:
            try:
                process = subprocess.Popen(invocation, stdout=stdout, stderr=stderr, start_new_session=True)
                try:
                    record["exit_code"] = process.wait(timeout=args.timeout_seconds)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()
                    raise
                manifests = list(artifacts.glob("*/suite-manifest.json"))
                if len(manifests) != 1:
                    raise ValueError("expected exactly one fresh suite manifest")
                report = json.loads(manifests[0].read_text())
                if report.get("seed") != seed or report.get("backend", "").lower() != args.backend or report.get("preset", "").lower() != args.preset:
                    raise ValueError("suite manifest does not match the invoked seed, backend and preset")
                failed, record["candidates"] = inspect_suite(report)
                record["failed"] = failed or record["exit_code"] != 0
                record["manifest"] = str(manifests[0])
            except (subprocess.TimeoutExpired, ValueError, OSError, json.JSONDecodeError) as error:
                record["failed"] = True
                record["harness_error"] = str(error)
        any_failure |= record["failed"]
        runs.append(record)
        write_json(output / "campaign-report.json", {"provenance": provenance, "runs": runs, "failed": any_failure})
        write_triage(output, provenance, runs)
    return 1 if any_failure else 0


def cleanup(output):
    for config in Path(output).rglob("compose.resolved.yml"):
        match = re.search(r"^name:\s*(midge-destroyer-[a-z0-9-]+)\s*$", config.read_text(), re.MULTILINE)
        if match is None:
            continue
        base = ["docker", "compose", "-p", match[1], "-f", str(config)]
        with config.with_name("campaign-cleanup.log").open("a") as log:
            try:
                subprocess.run(base + ["logs", "--no-color"], stdout=log, stderr=log, timeout=20)
                subprocess.run(base + ["down"], stdout=log, stderr=log, timeout=30, check=True)
            except (OSError, subprocess.SubprocessError) as error:
                log.write("Cleanup failed: " + str(error) + "\n")
                raise


def write_triage(output, provenance, runs):
    target = provenance["target"]
    replay_target = "git:" + target["revision"] if target["kind"] == "git" else target["version"]
    lines = ["# Campaign triage candidates", "", "These observations are not confirmed Midge defects.", "",
             "Target: `" + provenance["requested_target"] + "`; resolved: `" + json.dumps(provenance["resolved"], sort_keys=True) + "`.",
             "Tier: `" + provenance["tier"] + "`; harness: `" + provenance["harness_revision"] + "`.", "",
             "Prepare replay with `python3 scripts/campaign.py prepare --target " + replay_target + " --output /tmp/destroyer-replay" + (" --instrumented" if provenance["tier"] == "failpoint" else "") + "`.", ""]
    for run in runs:
        lines += ["## Seed " + str(run["seed"]), "", "Replay the recorded invocation JSON after preparing the same target, tier and emulator digest.", ""]
        lines += ["`python3 scripts/campaign.py run --output /tmp/destroyer-replay --backend " + run["backend"] + " --preset " + run["preset"] + " --seeds 1 --seed-start " + str(run["seed"]) + "`", ""]
        if run.get("harness_error"):
            lines += ["Harness failure: " + run["harness_error"], ""]
        for candidate in run["candidates"]:
            lines += ["- `" + candidate["scenario"] + "`: `" + candidate["verdict"] + "`; " + (candidate.get("invariant_violated") or "inspect recovery and infrastructure evidence")]
        lines += [""]
    lines += ["Before filing: reproduce the exact target, inspect its contract and expected ledger, distinguish infrastructure from engine failure, minimize the scenario and check existing issues.",
              "Confirmed Midge issues must include the failure scenario, regression test, priority and subsystem labels. Add confirmed issue links to the next release roadmap; preserve warnings separately."]
    (output / "triage.md").write_text("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    build = commands.add_parser("prepare")
    build.add_argument("--target", default="latest")
    build.add_argument("--output", required=True)
    build.add_argument("--instrumented", action="store_true")
    build.add_argument("--no-build", action="store_true")
    build.add_argument("--requested-target", help="Original label when a workflow already pinned its target")
    resolve = commands.add_parser("resolve")
    resolve.add_argument("--target", default="latest")
    clean = commands.add_parser("cleanup")
    clean.add_argument("--output", required=True)
    campaign = commands.add_parser("run")
    campaign.add_argument("--output", required=True)
    campaign.add_argument("--backend", choices=["local", "sqrzl", "s3", "azure", "gcs"], default="local")
    campaign.add_argument("--preset", choices=["smoke", "standard", "soak"], default="standard")
    campaign.add_argument("--seeds", type=int, default=1)
    campaign.add_argument("--seed-start", type=int, default=1)
    campaign.add_argument("--timeout-seconds", type=int, default=2400)
    campaign.add_argument("--max-scenarios", type=int)
    args = parser.parse_args()
    if args.action == "run" and (args.seeds < 1 or args.seeds > 16 or args.seed_start < 0 or args.seed_start + args.seeds > 2**64 or args.timeout_seconds < 1 or (args.max_scenarios is not None and args.max_scenarios < 1)):
        parser.error("invalid seed, timeout or scenario bound")
    try:
        if args.action == "resolve":
            target = resolve_target(args.target)
            print("git:" + target["revision"] if target["kind"] == "git" else target["version"])
            return 0
        if args.action == "cleanup":
            cleanup(args.output)
            return 0
        return prepare(args) if args.action == "prepare" else run(args)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
