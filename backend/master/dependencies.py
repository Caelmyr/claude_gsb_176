"""Job dependency resolution: validation, start/skip decisions, cascading.

A job submitted with ``depends_on: [job_id, ...]`` enters the lifecycle as
``WAITING`` instead of going straight to ``MAP``.  This module owns the two
questions the rest of the Master asks:

* **validate** — may this job carry this dependency set?  Every dependency
  must exist, a job may not depend on itself, and the resulting graph must
  stay acyclic (a cycle would deadlock every job on it).  The same check runs
  at submission time and when dependencies are edited mid-flight.
* **evaluate** — for each ``WAITING`` job, decide *start*, *skip* or *keep
  waiting*:

  - every dependency ``SUCCEEDED``            -> start (transition to ``MAP``);
  - any dependency ``FAILED``/``CANCELLED``/``SKIPPED`` (or vanished)
                                              -> skip (terminal ``SKIPPED``);
  - otherwise                                 -> keep waiting.

  Evaluation runs to a fixpoint inside one call, so a failure cascades through
  a whole chain (A fails -> B skipped -> C skipped) atomically, and a success
  releases an entire ready layer at once.  It is invoked from the scheduler
  tick and synchronously after every terminal transition (success, failure,
  cancel) and after every dependency edit, so the UI never shows a stale
  "waiting" job whose upstream already settled.
"""

from __future__ import annotations

from typing import Optional

from backend.common import constants as C
from backend.common.jsonutil import now_ms
from backend.common.logbus import LogBus
from backend.common.models import Job
from backend.master.job_manager import JobManager

# Dependency outcomes that can never turn into a success again.
_DOOMED_STATES = (C.JOB_FAILED, C.JOB_CANCELLED, C.JOB_SKIPPED)


class DependencyResolver:
    def __init__(self, job_manager: JobManager, logbus: LogBus) -> None:
        self.job_manager = job_manager
        self.logbus = logbus

    # ------------------------------------------------------------------
    # Validation (submission & mid-flight edits)
    # ------------------------------------------------------------------
    def validate(self, job_id: Optional[str], dep_ids) -> list[str]:
        """Normalize and vet a dependency list; raise ValueError if invalid.

        ``job_id`` is the job that would carry the dependencies (``None`` for a
        not-yet-created submission — a fresh id cannot be part of a cycle).
        Returns the deduplicated, order-preserving list of dependency ids.
        """
        normalized = self._normalize(dep_ids)

        jobs = {j.job_id: j for j in self.job_manager.list_jobs()}
        for dep in normalized:
            if dep == job_id:
                raise ValueError(f"job cannot depend on itself: {dep}")
            if dep not in jobs:
                raise ValueError(f"unknown dependency job: {dep}")

        if job_id is not None and normalized:
            graph = {jid: list(j.depends_on) for jid, j in jobs.items()}
            graph[job_id] = normalized
            if self._creates_cycle(graph, job_id):
                raise ValueError(
                    f"dependency cycle detected through job {job_id}"
                )
        return normalized

    @staticmethod
    def _normalize(dep_ids) -> list[str]:
        if dep_ids is None:
            return []
        if isinstance(dep_ids, str):  # tolerate "a,b" from hand-rolled clients
            dep_ids = dep_ids.split(",")
        if not isinstance(dep_ids, (list, tuple)):
            raise ValueError("depends_on must be a list of job ids")
        out: list[str] = []
        for raw in dep_ids:
            dep = str(raw).strip()
            if dep and dep not in out:
                out.append(dep)
        return out

    @staticmethod
    def _creates_cycle(graph: dict[str, list[str]], job_id: str) -> bool:
        """True if following depends_on edges from ``job_id`` leads back to it."""
        stack = list(graph.get(job_id, []))
        seen: set[str] = set()
        while stack:
            cur = stack.pop()
            if cur == job_id:
                return True
            if cur in seen:
                continue
            seen.add(cur)
            stack.extend(graph.get(cur, []))
        return False

    # ------------------------------------------------------------------
    # Evaluation (scheduler tick + terminal-transition triggers)
    # ------------------------------------------------------------------
    def evaluate(self) -> None:
        """Start or skip every WAITING job whose dependencies have settled.

        Loops to a fixpoint so cascades (skip -> downstream skip, or a whole
        ready layer starting together) complete within a single call.
        """
        while self._evaluate_pass():
            pass

    def _evaluate_pass(self) -> bool:
        moved = False
        for job in self.job_manager.list_jobs():
            if job.status != C.JOB_WAITING:
                continue
            decision, reason = self._decide(job)
            if decision == "start":
                self.job_manager.set_job_status(job, C.JOB_MAP)
                self.logbus.info(
                    job.job_id, "all dependencies succeeded; job started",
                    task_id="deps",
                )
                moved = True
            elif decision == "skip":
                self._skip(job, reason)
                moved = True
        return moved

    def _decide(self, job: Job) -> tuple[str, str]:
        """Return ("start" | "skip" | "wait", reason) for a WAITING job.

        Every dependency is inspected: a doomed dependency settles the job
        immediately even if others are still running (the job can never
        start, so waiting would just defer the inevitable skip).
        """
        doomed: Optional[str] = None
        all_succeeded = True
        for dep_id in job.depends_on:
            dep = self.job_manager.get_job(dep_id)
            if dep is None:
                return "skip", f"dependency {dep_id} no longer exists"
            if dep.status in _DOOMED_STATES:
                if doomed is None:
                    doomed = (f"dependency {dep.name} ({dep_id}) "
                              f"ended with {dep.status}")
            elif dep.status != C.JOB_SUCCEEDED:
                all_succeeded = False
        if doomed is not None:
            return "skip", doomed
        if not all_succeeded:
            return "wait", ""
        return "start", ""  # empty dependency list also releases immediately

    def _skip(self, job: Job, reason: str) -> None:
        def apply(j: Job) -> None:
            j.status = C.JOB_SKIPPED
            if not j.finished_ms:
                j.finished_ms = now_ms()
            j.stats["skip_reason"] = reason

        self.job_manager.apply_job(job.job_id, apply)
        self.logbus.warn(job.job_id, f"job skipped: {reason}", task_id="deps")
