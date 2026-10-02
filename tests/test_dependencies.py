"""Tests for job dependency gating and cascading skip behaviour."""

import shutil
import tempfile
import unittest

from backend.common import constants as C
from backend.common.config import ClusterConfig
from backend.common.logbus import LogBus
from backend.common.storage import Storage
from backend.master.job_manager import JobManager

SPEC = {
    "mapper": "wordcount_mapper",
    "reducer": "count_reducer",
    "num_map_tasks": 2,
    "num_reduce_tasks": 1,
    "input_rows": 40,
    "params": {},
}


class TestJobDependencies(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.jm = JobManager(Storage(self.tmp), ClusterConfig(), LogBus(Storage(self.tmp)))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def submit(self, name, **extra):
        return self.jm.submit({"name": name, **SPEC, **extra})

    def test_waiting_job_starts_after_all_upstreams_succeed(self):
        a = self.submit("clean-a")
        b = self.submit("clean-b")
        stats = self.submit("stats", depends_on=[a.job_id, b.job_id])

        self.assertEqual(stats.status, C.JOB_WAITING)
        self.assertEqual(self.jm.tasks_for(stats.job_id), [])
        self.assertEqual(stats.depends_on, [a.job_id, b.job_id])

        self.jm.set_job_status(a, C.JOB_SUCCEEDED)
        stats = self.jm.get_job(stats.job_id)
        self.assertEqual(stats.status, C.JOB_WAITING)

        self.jm.set_job_status(b, C.JOB_SUCCEEDED)
        stats = self.jm.get_job(stats.job_id)
        self.assertEqual(stats.status, C.JOB_MAP)
        self.assertTrue(self.jm.tasks_for(stats.job_id))

    def test_failed_or_cancelled_upstream_skips_descendants(self):
        failed = self.submit("failed-clean")
        cancelled = self.submit("cancelled-clean")
        child1 = self.submit("child-failed", depends_on=[failed.job_id])
        child2 = self.submit("child-cancelled", depends_on=[cancelled.job_id])
        grandchild = self.submit("grandchild", depends_on=[child1.job_id])

        self.jm.set_job_status(failed, C.JOB_FAILED)
        self.jm.cancel(cancelled)

        child1 = self.jm.get_job(child1.job_id)
        child2 = self.jm.get_job(child2.job_id)
        grandchild = self.jm.get_job(grandchild.job_id)
        self.assertEqual(child1.status, C.JOB_SKIPPED)
        self.assertIn(C.JOB_FAILED, child1.skip_reason)
        self.assertEqual(child2.status, C.JOB_SKIPPED)
        self.assertEqual(grandchild.status, C.JOB_SKIPPED)
        self.assertIn(C.JOB_SKIPPED, grandchild.skip_reason)

    def test_already_succeeded_dependency_starts_immediately(self):
        a = self.submit("done")
        self.jm.set_job_status(a, C.JOB_SUCCEEDED)
        b = self.submit("after-done", depends_on=[a.job_id])
        self.assertEqual(b.status, C.JOB_MAP)

    def test_fan_out_starts_each_downstream(self):
        a = self.submit("shared")
        b = self.submit("b", depends_on=[a.job_id])
        c = self.submit("c", depends_on=[a.job_id])
        self.assertEqual(b.status, C.JOB_WAITING)
        self.assertEqual(c.status, C.JOB_WAITING)

        self.jm.set_job_status(a, C.JOB_SUCCEEDED)
        self.assertEqual(self.jm.get_job(b.job_id).status, C.JOB_MAP)
        self.assertEqual(self.jm.get_job(c.job_id).status, C.JOB_MAP)

    def test_update_dependencies_while_waiting(self):
        a = self.submit("a")
        b = self.submit("b")
        c = self.submit("c", depends_on=[a.job_id])

        c = self.jm.update_dependencies(c.job_id, [b.job_id])
        self.assertEqual(c.depends_on, [b.job_id])
        self.jm.set_job_status(a, C.JOB_SUCCEEDED)
        self.assertEqual(self.jm.get_job(c.job_id).status, C.JOB_WAITING)

        self.jm.set_job_status(b, C.JOB_SUCCEEDED)
        self.assertEqual(self.jm.get_job(c.job_id).status, C.JOB_MAP)

    def test_dependency_cycle_and_unknown_reference_are_rejected(self):
        a = self.submit("a")
        b = self.submit("b", depends_on=[a.job_id])
        with self.assertRaises(ValueError):
            self.submit("cycle", depends_on=["missing-job"])
        c = self.submit("c", depends_on=[b.job_id])
        with self.assertRaises(ValueError):
            self.jm.update_dependencies(a.job_id, [b.job_id])
        with self.assertRaises(ValueError):
            self.jm.update_dependencies(a.job_id, [c.job_id])
        with self.assertRaises(ValueError):
            self.jm.update_dependencies(a.job_id, [a.job_id])

    def test_waiting_state_persists_until_dependency_succeeds(self):
        a = self.submit("a")
        b = self.submit("b", depends_on=[a.job_id])

        restarted = JobManager(Storage(self.tmp), ClusterConfig(), LogBus(Storage(self.tmp)))
        b = restarted.get_job(b.job_id)
        self.assertEqual(b.status, C.JOB_WAITING)
        self.assertEqual(restarted.tasks_for(b.job_id), [])

        restarted.set_job_status(restarted.get_job(a.job_id), C.JOB_SUCCEEDED)
        self.assertEqual(restarted.get_job(b.job_id).status, C.JOB_MAP)

    def test_running_job_dependencies_are_immutable(self):
        a = self.submit("running")
        b = self.submit("b")
        with self.assertRaises(ValueError):
            self.jm.update_dependencies(a.job_id, [b.job_id])


if __name__ == "__main__":
    unittest.main()
