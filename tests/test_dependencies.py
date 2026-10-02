"""Tests for job dependencies: WAITING/SKIPPED lifecycle, cascades, validation."""

import shutil
import tempfile
import unittest

from backend.common import constants as C
from backend.common.config import ClusterConfig
from backend.common.logbus import LogBus
from backend.common.storage import Storage
from backend.master.dependencies import DependencyResolver
from backend.master.job_manager import JobManager

PAYLOAD = {
    "name": "t", "mapper": "wordcount_mapper", "reducer": "count_reducer",
    "num_map_tasks": 2, "num_reduce_tasks": 1, "input_rows": 100, "params": {},
}


class TestDependencies(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.logbus = LogBus(self.storage)
        self.jm = JobManager(self.storage, ClusterConfig(), self.logbus)
        self.deps = DependencyResolver(self.jm, self.logbus)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ------------------------------------------------------------------
    def submit(self, name, depends_on=None):
        """Mirror the API flow: validate -> submit -> evaluate."""
        payload = dict(PAYLOAD, name=name)
        payload["depends_on"] = self.deps.validate(None, depends_on)
        job = self.jm.submit(payload)
        self.deps.evaluate()
        return self.jm.get_job(job.job_id)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def test_submit_with_deps_waits(self):
        a = self.submit("a")
        b = self.submit("b", [a.job_id])
        self.assertEqual(b.status, C.JOB_WAITING)
        self.assertEqual(b.depends_on, [a.job_id])
        self.assertEqual(b.started_ms, 0)
        # tasks are planned up front so the UI shows the full picture
        self.assertTrue(self.jm.tasks_for(b.job_id))

    def test_submit_without_deps_starts_immediately(self):
        a = self.submit("a")
        self.assertEqual(a.status, C.JOB_MAP)
        self.assertTrue(a.started_ms)

    def test_starts_when_all_deps_succeed(self):
        a = self.submit("a")
        b = self.submit("b", [a.job_id])
        self.jm.set_job_status(self.jm.get_job(a.job_id), C.JOB_SUCCEEDED)
        self.deps.evaluate()
        b = self.jm.get_job(b.job_id)
        self.assertEqual(b.status, C.JOB_MAP)
        self.assertTrue(b.started_ms)

    def test_waits_until_every_dep_succeeds(self):
        a1 = self.submit("a1")
        a2 = self.submit("a2")
        b = self.submit("b", [a1.job_id, a2.job_id])
        self.jm.set_job_status(self.jm.get_job(a1.job_id), C.JOB_SUCCEEDED)
        self.deps.evaluate()
        self.assertEqual(self.jm.get_job(b.job_id).status, C.JOB_WAITING)
        self.jm.set_job_status(self.jm.get_job(a2.job_id), C.JOB_SUCCEEDED)
        self.deps.evaluate()
        self.assertEqual(self.jm.get_job(b.job_id).status, C.JOB_MAP)

    def test_skips_when_dep_fails(self):
        a = self.submit("a")
        b = self.submit("b", [a.job_id])
        self.jm.fail(self.jm.get_job(a.job_id), "boom")
        self.deps.evaluate()
        b = self.jm.get_job(b.job_id)
        self.assertEqual(b.status, C.JOB_SKIPPED)
        self.assertTrue(b.finished_ms)
        self.assertIn(a.job_id, b.stats["skip_reason"])

    def test_skips_when_dep_cancelled(self):
        a = self.submit("a")
        b = self.submit("b", [a.job_id])
        self.jm.cancel(self.jm.get_job(a.job_id))
        self.deps.evaluate()
        self.assertEqual(self.jm.get_job(b.job_id).status, C.JOB_SKIPPED)

    def test_doomed_dep_wins_over_still_running_dep(self):
        a1 = self.submit("a1")  # stays MAP (running)
        a2 = self.submit("a2")
        b = self.submit("b", [a1.job_id, a2.job_id])
        self.jm.fail(self.jm.get_job(a2.job_id), "boom")
        self.deps.evaluate()
        # a1 is still running, but a2 can never succeed -> skip immediately
        self.assertEqual(self.jm.get_job(b.job_id).status, C.JOB_SKIPPED)

    # ------------------------------------------------------------------
    # Graph shapes
    # ------------------------------------------------------------------
    def test_failure_cascades_through_chain(self):
        a = self.submit("a")
        b = self.submit("b", [a.job_id])
        c = self.submit("c", [b.job_id])
        self.jm.fail(self.jm.get_job(a.job_id), "boom")
        self.deps.evaluate()
        self.assertEqual(self.jm.get_job(b.job_id).status, C.JOB_SKIPPED)
        self.assertEqual(self.jm.get_job(c.job_id).status, C.JOB_SKIPPED)

    def test_fan_out_all_dependents_release_together(self):
        a = self.submit("a")
        b = self.submit("b", [a.job_id])
        c = self.submit("c", [a.job_id])
        self.jm.set_job_status(self.jm.get_job(a.job_id), C.JOB_SUCCEEDED)
        self.deps.evaluate()
        self.assertEqual(self.jm.get_job(b.job_id).status, C.JOB_MAP)
        self.assertEqual(self.jm.get_job(c.job_id).status, C.JOB_MAP)

    def test_diamond_releases_after_both_branches(self):
        a = self.submit("a")
        b = self.submit("b", [a.job_id])
        c = self.submit("c", [a.job_id])
        d = self.submit("d", [b.job_id, c.job_id])
        self.jm.set_job_status(self.jm.get_job(a.job_id), C.JOB_SUCCEEDED)
        self.deps.evaluate()
        self.assertEqual(self.jm.get_job(d.job_id).status, C.JOB_WAITING)
        self.jm.set_job_status(self.jm.get_job(b.job_id), C.JOB_SUCCEEDED)
        self.jm.set_job_status(self.jm.get_job(c.job_id), C.JOB_SUCCEEDED)
        self.deps.evaluate()
        self.assertEqual(self.jm.get_job(d.job_id).status, C.JOB_MAP)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------
    def test_rejects_unknown_dependency(self):
        with self.assertRaises(ValueError):
            self.deps.validate(None, ["job-does-not-exist"])

    def test_rejects_self_dependency(self):
        a = self.submit("a")
        with self.assertRaises(ValueError):
            self.deps.validate(a.job_id, [a.job_id])

    def test_rejects_direct_cycle(self):
        a = self.submit("a")
        b = self.submit("b", [a.job_id])
        with self.assertRaises(ValueError):
            self.deps.validate(a.job_id, [b.job_id])

    def test_rejects_indirect_cycle(self):
        a = self.submit("a")
        b = self.submit("b", [a.job_id])
        c = self.submit("c", [b.job_id])
        with self.assertRaises(ValueError):
            self.deps.validate(a.job_id, [c.job_id])

    def test_validate_normalizes(self):
        a = self.submit("a")
        self.assertEqual(
            self.deps.validate(None, [a.job_id, a.job_id, "  ", ""]),
            [a.job_id],
        )
        self.assertEqual(self.deps.validate(None, None), [])
        self.assertEqual(self.deps.validate(None, f" {a.job_id} ,"), [a.job_id])

    # ------------------------------------------------------------------
    # Mid-flight edits
    # ------------------------------------------------------------------
    def test_removing_dep_releases_job(self):
        a = self.submit("a")  # stays running
        b = self.submit("b", [a.job_id])
        self.assertEqual(self.jm.get_job(b.job_id).status, C.JOB_WAITING)
        new = self.deps.validate(b.job_id, [])
        self.jm.apply_job(b.job_id, lambda j: setattr(j, "depends_on", new))
        self.deps.evaluate()
        self.assertEqual(self.jm.get_job(b.job_id).status, C.JOB_MAP)

    def test_swapping_dep_reblocks_on_new_dep(self):
        a1 = self.submit("a1")
        a2 = self.submit("a2")
        b = self.submit("b", [a1.job_id])
        self.jm.set_job_status(self.jm.get_job(a1.job_id), C.JOB_SUCCEEDED)
        # swap a1 (done) for a2 (running) before the resolver runs
        new = self.deps.validate(b.job_id, [a2.job_id])
        self.jm.apply_job(b.job_id, lambda j: setattr(j, "depends_on", new))
        self.deps.evaluate()
        self.assertEqual(self.jm.get_job(b.job_id).status, C.JOB_WAITING)

    def test_adding_failed_dep_skips_immediately(self):
        a1 = self.submit("a1")
        a2 = self.submit("a2")
        b = self.submit("b", [a1.job_id])
        self.jm.fail(self.jm.get_job(a2.job_id), "boom")
        new = self.deps.validate(b.job_id, [a1.job_id, a2.job_id])
        self.jm.apply_job(b.job_id, lambda j: setattr(j, "depends_on", new))
        self.deps.evaluate()
        self.assertEqual(self.jm.get_job(b.job_id).status, C.JOB_SKIPPED)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def test_waiting_job_survives_master_restart(self):
        a = self.submit("a")
        b = self.submit("b", [a.job_id])
        # simulate a master restart: fresh manager + resolver over the same store
        jm2 = JobManager(self.storage, ClusterConfig(), self.logbus)
        deps2 = DependencyResolver(jm2, self.logbus)
        b2 = jm2.get_job(b.job_id)
        self.assertEqual(b2.status, C.JOB_WAITING)
        self.assertEqual(b2.depends_on, [a.job_id])
        jm2.set_job_status(jm2.get_job(a.job_id), C.JOB_SUCCEEDED)
        deps2.evaluate()
        self.assertEqual(jm2.get_job(b.job_id).status, C.JOB_MAP)

    # ------------------------------------------------------------------
    # Derived views
    # ------------------------------------------------------------------
    def test_summary_exposes_dependency_view(self):
        a = self.submit("a")
        b = self.submit("b", [a.job_id])
        summary = self.jm.job_summary(self.jm.get_job(b.job_id))
        self.assertEqual(summary["depends_on"], [a.job_id])
        self.assertEqual(summary["dependency_status"][0]["status"], C.JOB_MAP)
        self.assertFalse(summary["dependency_status"][0]["met"])
        upstream = self.jm.job_summary(self.jm.get_job(a.job_id))
        self.assertEqual(upstream["dependents"][0]["job_id"], b.job_id)


if __name__ == "__main__":
    unittest.main()
