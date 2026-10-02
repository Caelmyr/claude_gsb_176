"""Headless integration test: dependencies wired through the real Scheduler.

The HTTP layer needs Flask, which the test environment may not have; the
Scheduler and its collaborators do not.  Driving ``Scheduler.tick`` plus
simulated worker completions exercises the same master-side flow the API
triggers: WAITING -> MAP -> SHUFFLE -> REDUCE -> SUCCEEDED, and the
failure/cancel cascades into SKIPPED.
"""

import shutil
import tempfile
import time
import unittest

from backend.common import constants as C
from backend.common.config import ClusterConfig
from backend.common.logbus import LogBus
from backend.common.storage import Storage
from backend.master.dependencies import DependencyResolver
from backend.master.fault_tolerance import FaultTolerance
from backend.master.job_manager import JobManager
from backend.master.metrics import Metrics
from backend.master.registry import WorkerRegistry
from backend.master.scheduler import Scheduler
from backend.master.shuffle import ShuffleCoordinator

PAYLOAD = {
    "mapper": "wordcount_mapper", "reducer": "count_reducer",
    "num_map_tasks": 2, "num_reduce_tasks": 1, "input_rows": 100, "params": {},
}


class TestDependencyFlow(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.logbus = LogBus(self.storage)
        self.config = ClusterConfig(max_attempts=1)  # first task failure dooms the job
        self.jm = JobManager(self.storage, self.config, self.logbus)
        self.registry = WorkerRegistry(self.storage, self.config)
        self.shuffle = ShuffleCoordinator(self.storage, self.jm, self.registry, self.logbus)
        self.ft = FaultTolerance(self.storage, self.jm, self.config, self.logbus)
        self.metrics = Metrics(self.storage)
        self.deps = DependencyResolver(self.jm, self.logbus)
        self.scheduler = Scheduler(
            self.storage, self.jm, self.registry, self.shuffle,
            self.ft, self.metrics, self.config, self.logbus, deps=self.deps,
        )

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ------------------------------------------------------------------
    def submit(self, name, depends_on=None):
        payload = dict(PAYLOAD, name=name)
        payload["depends_on"] = self.deps.validate(None, depends_on)
        job = self.jm.submit(payload)
        self.deps.evaluate()
        return job.job_id

    def complete_stage(self, job_id, kind):
        """Simulate workers reporting success for every task of ``kind``."""
        for task in self.jm.tasks_for(job_id, kind):
            if task.status != C.TASK_SUCCEEDED:
                self.scheduler.on_task_complete({
                    "job_id": job_id, "task_id": task.task_id, "worker_id": "w-test",
                    "status": C.TASK_SUCCEEDED, "records_processed": 10,
                    "records_emitted": 10, "duration_ms": 1,
                    "partition_sizes": {}, "results": [],
                })

    def run_job_to_success(self, job_id):
        self.complete_stage(job_id, C.TASK_MAP)
        self.scheduler.tick()          # MAP -> SHUFFLE
        time.sleep(0.45)               # SHUFFLE_HOLD_MS
        self.scheduler.tick()          # SHUFFLE -> REDUCE
        self.complete_stage(job_id, C.TASK_REDUCE)
        self.scheduler.tick()          # REDUCE -> SUCCEEDED (+ dep evaluation)

    # ------------------------------------------------------------------
    def test_chain_releases_through_scheduler_ticks(self):
        a = self.submit("a")
        b = self.submit("b", [a])
        self.assertEqual(self.jm.get_job(b).status, C.JOB_WAITING)

        self.run_job_to_success(a)
        self.assertEqual(self.jm.get_job(a).status, C.JOB_SUCCEEDED)
        # _finish_success evaluated dependencies synchronously
        self.assertEqual(self.jm.get_job(b).status, C.JOB_MAP)

        self.run_job_to_success(b)
        self.assertEqual(self.jm.get_job(b).status, C.JOB_SUCCEEDED)

    def test_job_failure_skips_dependent_on_next_tick(self):
        a = self.submit("a")
        b = self.submit("b", [a])
        task = self.jm.tasks_for(a, C.TASK_MAP)[0]
        # max_attempts=1: first failure retries, second exhausts the budget.
        for _ in range(2):
            self.scheduler.on_task_complete({
                "job_id": a, "task_id": task.task_id, "worker_id": "w-test",
                "status": C.TASK_FAILED, "error": "boom",
            })
        # the job failed inside on_task_complete, which also ran the resolver;
        # the dependent is already SKIPPED.
        self.assertEqual(self.jm.get_job(a).status, C.JOB_FAILED)
        self.assertEqual(self.jm.get_job(b).status, C.JOB_SKIPPED)
        reason = self.jm.get_job(b).stats["skip_reason"]
        self.assertIn(a, reason)

    def test_cancel_skips_dependent(self):
        a = self.submit("a")
        b = self.submit("b", [a])
        self.jm.cancel(self.jm.get_job(a))
        self.deps.evaluate()  # the cancel route does this synchronously
        self.assertEqual(self.jm.get_job(b).status, C.JOB_SKIPPED)

    def test_tick_alone_eventually_settles_dependents(self):
        # Even without the synchronous triggers, the scheduler tick evaluates.
        a = self.submit("a")
        b = self.submit("b", [a])
        self.jm.set_job_status(self.jm.get_job(a), C.JOB_SUCCEEDED)
        self.scheduler.tick()
        self.assertEqual(self.jm.get_job(b).status, C.JOB_MAP)


if __name__ == "__main__":
    unittest.main()
