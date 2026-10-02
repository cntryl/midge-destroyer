import argparse
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import plan_campaigns

SPEC = importlib.util.spec_from_file_location("campaign", Path(__file__).with_name("campaign.py"))
campaign = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(campaign)


def release(number, yanked=False):
    return {"num": number, "yanked": yanked, "checksum": "published-checksum"}


def report(verdict):
    return {"schema_version": "midge-destroyer.suite-manifest/v3", "scenario_count": 1,
            "results": [{"scenario": "sentinel", "verdict": verdict}]}


class CampaignTests(unittest.TestCase):
    def test_should_resolve_hourly_target_once_and_rotate_seeds_between_runs(self):
        # Arrange
        target = {"kind": "registry", "version": "0.3.1"}
        arguments = ["schedule", "17 * * * *", "latest", "local", "standard", 2, 1, False]
        # Act
        with patch.object(plan_campaigns, "resolve_target", return_value=target) as resolve:
            first = plan_campaigns.plan(*arguments, 100, 1)
            self.assertEqual(resolve.call_count, 1)
        with patch.object(plan_campaigns, "resolve_target", return_value=target):
            second = plan_campaigns.plan(*arguments, 101, 1)
        # Assert
        self.assertEqual(len(first["include"]), 2)
        self.assertEqual({r["resolved"] for r in first["include"]}, {"0.3.1"})
        self.assertTrue({r["seed_start"] for r in first["include"]}.isdisjoint({r["seed_start"] for r in second["include"]}))

    def test_should_separate_provider_soak_and_instrumented_tiers(self):
        # Arrange / Act
        with patch.object(plan_campaigns, "resolve_target", return_value={"kind": "registry", "version": "0.3.1"}):
            nightly = plan_campaigns.plan("schedule", "43 2 * * *", "latest", "local", "standard", 1, 1, False, 100, 1)
        # Assert
        self.assertEqual({r["backend"] for r in nightly["include"] if not r["instrumented"]}, {"s3", "azure", "gcs"})
        self.assertEqual(len([r for r in nightly["include"] if r["instrumented"]]), 1)
        with self.assertRaises(ValueError):
            plan_campaigns.plan("workflow_dispatch", "", "latest", "local", "soak", 100, 1, False, 100, 1)

    def test_should_select_latest_stable_when_newer_versions_are_yanked_or_prerelease(self):
        # Arrange
        data = {"versions": [release("0.4.0", True), release("0.5.0-rc.1"), release("0.3.10"), release("0.3.9")]}
        # Act / Assert
        self.assertEqual(campaign.resolve_target("latest", data)["version"], "0.3.10")
        with self.assertRaises(ValueError):
            campaign.resolve_target("0.4.0", data)

    def test_should_pin_requested_branch_when_resolving_git_target(self):
        # Arrange
        sha = "a" * 40
        # Act
        target = campaign.resolve_target("develop", git_resolver=lambda branch: sha + "\trefs/heads/" + branch)
        # Assert
        self.assertEqual(target["revision"], sha)
        self.assertEqual(campaign.resolve_target("git:" + sha)["revision"], sha)
        with self.assertRaises(ValueError):
            campaign.resolve_target("develop", git_resolver=lambda _: sha + "\trefs/heads/main")
        with self.assertRaises(ValueError):
            campaign.resolve_target("develop; echo unsafe")

    def test_should_reject_path_patch_and_checksum_mismatch_when_qualifying_release(self):
        # Arrange
        target = campaign.resolve_target("0.3.1", {"versions": [release("0.3.1")]})
        package = {"name": "cntryl-midge", "version": "0.3.1", "source": campaign.CRATES_SOURCE}
        lock = '\n[[package]]\nname = "cntryl-midge"\nversion = "0.3.1"\nsource = "' + campaign.CRATES_SOURCE + '"\nchecksum = "published-checksum"\n'
        # Act / Assert
        self.assertEqual(campaign.verify_resolution(target, {"packages": [package]}, lock)["checksum"], "published-checksum")
        with self.assertRaises(ValueError):
            campaign.verify_resolution(target, {"packages": [dict(package, source=None)]}, lock)
        with self.assertRaises(ValueError):
            campaign.verify_resolution(target, {"packages": [package]}, lock.replace("published-checksum", "other-checksum"))

    def test_should_reject_different_git_revision_when_metadata_looks_compatible(self):
        # Arrange
        sha = "a" * 40
        target = campaign.resolve_target("git:" + sha)
        source = "git+" + campaign.REPOSITORY + "?rev=" + sha + "#" + "b" * 40
        package = {"name": "cntryl-midge", "version": "0.3.1", "source": source}
        lock = '\n[[package]]\nname = "cntryl-midge"\nversion = "0.3.1"\nsource = "' + source + '"\n'
        # Act / Assert
        with self.assertRaises(ValueError):
            campaign.verify_resolution(target, {"packages": [package]}, lock)

    def test_should_fail_campaign_when_suite_reports_break_despite_successful_exit(self):
        # Arrange
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            provenance = {"requested_target": "0.3.1", "target": {"kind": "registry", "version": "0.3.1"},
                          "resolved": {}, "tier": "black-box", "harness_revision": "a" * 40}
            campaign.write_json(output / "provenance.json", provenance)
            binary = output / "build/target/release/midge-destroyer"
            binary.parent.mkdir(parents=True)
            binary.write_text("#!" + sys.executable + "\n" + '''import json, pathlib, sys
root = pathlib.Path(sys.argv[sys.argv.index('--artifacts-root')+1])
seed = int(sys.argv[sys.argv.index('--seed')+1])
root = root / 'execution'
root.mkdir(parents=True)
result = {'scenario':'sentinel', 'verdict':'break' if seed == 41 else 'pass', 'invariant_violated':'missing state' if seed == 41 else None}
(root/'suite-manifest.json').write_text(json.dumps({'schema_version':'midge-destroyer.suite-manifest/v3','scenario_count':1,'results':[result],'seed':seed,'backend':'Local','preset':'Standard'}))
''')
            binary.chmod(0o755)
            args = argparse.Namespace(output=str(output), seeds=2, seed_start=41, backend="local", preset="standard", max_scenarios=None, timeout_seconds=5)
            # Act
            exit_code = campaign.run(args)
            evidence = json.loads((output / "campaign-report.json").read_text())
            # Assert
            self.assertEqual(exit_code, 1)
            self.assertEqual(len(evidence["runs"]), 2)
            self.assertEqual(evidence["runs"][0]["exit_code"], 0)
            self.assertTrue(evidence["runs"][0]["failed"])
            self.assertFalse(evidence["runs"][1]["failed"])
            self.assertIn("missing state", (output / "triage.md").read_text())

    def test_should_reject_empty_unknown_and_all_skipped_coverage(self):
        # Arrange / Act / Assert
        for value in [{}, dict(report("pass"), scenario_count=2), report("skipped"), report("unrecognized")]:
            with self.assertRaises(ValueError):
                campaign.inspect_suite(value)
        self.assertTrue(campaign.inspect_suite(report("infrastructure_error"))[0])
        self.assertFalse(campaign.inspect_suite(report("wobble"))[0])
        self.assertEqual(campaign.inspect_suite(report("wobble"))[1][0]["verdict"], "wobble")


if __name__ == "__main__":
    unittest.main()
