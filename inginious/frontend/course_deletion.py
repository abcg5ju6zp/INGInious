# -*- coding: utf-8 -*-
#
# This file is part of INGInious. See the LICENSE and the COPYRIGHTS files for
# more information about the licensing of this file.

""" Safe course deletion: draining state machine, write gate, operation locks.

A course deletion is durable and crash-proof: a :class:`CourseDeletion` tombstone
stored in MongoDB is both the stable identity of the deletion and the write gate
consulted by every process (webapp request threads, the LTI publishers, the
independent ``inginious-webdav`` process). A daemon coordinator thread
(:class:`CourseDeletionManager`) drives the tombstone through the phases

    requested -> draining -> wiping_db -> deleting_fs -> finalizing -> (removed)

and resumes unfinished tombstones on webapp startup. Blocking sub-conditions
(``awaiting_jobs`` / ``plugin_refused`` / ``fs_error``) never move the phase
backwards, so a blocked course stays closed until the deletion completes.
"""

import contextlib
import json
import logging
import os
import socket
import threading
import time
import uuid
from datetime import datetime, timezone, timedelta

from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from inginious.common.exceptions import CourseOperationInProgressException
from inginious.common.filesystems import get_fs_provider, invalidate_cache
# Gate primitives live in a dependency-light module; re-exported here for the
# callers that historically imported them from the manager module.
from inginious.frontend.course_deletion_gate import (
    is_course_draining, assert_course_writable, invalidate_gate_cache, acquire_operation_lock,
    renew_operation_lock, release_operation_lock, DELETE_LOCK_TTL)
from inginious.frontend.models import (CourseDeletion, Submission, CourseClass, Audience,
                                      Group, UserTask, User, Session, LISOutcome, LTIGrade, LaunchData)
from inginious.frontend.models.course_deletion import (PHASE_REQUESTED, PHASE_DRAINING, PHASE_WIPING_DB,
                                                       PHASE_DELETING_FS, PHASE_FINALIZING, BLOCKED_AWAITING_JOBS,
                                                       BLOCKED_PLUGIN_REFUSED, BLOCKED_FS_ERROR,
                                                       PIN_ACTIVE, PIN_DONE, PIN_SKIPPED, PIN_BLOCKED)
from inginious.frontend.plugins import plugin_manager

__all__ = ["CourseDeletionManager", "is_course_draining", "assert_course_writable", "invalidate_gate_cache",
           "acquire_operation_lock", "renew_operation_lock", "release_operation_lock",
           "job_belongs_to_course", "filter_course_jobs", "compute_running_deadline", "lti_launch_matches",
           "DB_STEPS"]

# Default tunables (overridable through the ``course_deletion`` configuration entry)
TICK_INTERVAL = 2.0          # coordinator period, seconds
LEASE_SECONDS = 15.0         # tombstone worker ownership lease
SETTLE_SECONDS = 3.0         # delay between gate publication and DB wipe
JOB_GRACE_SECONDS = 60.0     # grace after a running job is killed
RUNNING_HARD_CAP = 1800.0    # max wait for a running job, even without a task timeout
WAITING_ROW_GRACE = 15.0     # grace before force-erroring orphan waiting submissions
PLUGIN_CALL_BUDGET = 5.0     # a single cooperative plugin hook call budget
PLUGIN_TOTAL_BUDGET = 600.0  # overall plugin drain budget before plugin_refused
FS_AUTO_RETRY_DELAY = 30.0   # auto-retry delay after a filesystem deletion failure
FAST_PATH_BUDGET = 5.0       # request-thread fast path budget

# Ordered, idempotent database cleanup steps; persisted in ``db_steps`` so a
# crashed worker replays only what is missing.
DB_STEPS = ["s1_gridfs", "s2_recheck", "s3_userdata", "s4_course_class", "s5_users", "s6_lti"]

_logger = logging.getLogger("inginious.webapp.course_deletion")


def _now():
    return datetime.now(tz=timezone.utc)


# ---------------------------------------------------------------------------------------
# Pure helpers (kept free of any MongoDB access so they can be unit-tested directly)
# ---------------------------------------------------------------------------------------

def job_belongs_to_course(info, courseid):
    """ True iff a backend queue ``info`` field (``"courseid/taskid"`` or an
    environment name / None for taskless jobs) belongs to *exactly* ``courseid``.

    A real course job always carries a ``courseid/taskid`` pair, so the slash
    is required: an environment name that happens to equal a course id (e.g. a
    taskless "docker" job) must not match. """
    if not info or not isinstance(info, str) or "/" not in info:
        return False
    return info.split("/", 1)[0] == courseid


def filter_course_jobs(jobs_running, jobs_waiting, courseid):
    """ Splits a :meth:`Client.get_job_queue_snapshot` snapshot into the
    (running, waiting) entries that belong to ``courseid``. """
    running = [e for e in (jobs_running or []) if job_belongs_to_course(e[3], courseid)]
    waiting = [e for e in (jobs_waiting or []) if job_belongs_to_course(e[2], courseid)]
    return running, waiting


def compute_running_deadline(first_seen, started_at, max_time, now,
                             grace=JOB_GRACE_SECONDS, hard_cap=RUNNING_HARD_CAP):
    """ Absolute soft deadline for a running job.

    The job is waited for until its declared task timeout (plus a removal grace),
    bounded by ``hard_cap`` measured from the first time it was observed. """
    capped = first_seen + timedelta(seconds=hard_cap)
    natural = None
    if started_at and max_time and max_time > 0:
        natural = started_at + timedelta(seconds=max_time + grace)
    return min(natural, capped) if natural is not None else capped


def lti_launch_matches(value, courseid):
    """ Best-effort matcher for a course reference inside an opaque pylti1p3
    ``lti_launch`` cached value. The course id only appears in LTI route URLs. """
    if not value:
        return False
    try:
        serialized = json.dumps(value, default=str)
    except (TypeError, ValueError):
        return False
    # A boundary after the course id ("/", "?", end of JSON string) is required
    # so that course "cs10" does not match a "cs101" launch URL.
    markers = []
    for base in ("/lti/{}", "/lti1.3/launch/{}", "/lti1.3/deeplink/{}"):
        prefix = base.format(courseid)
        markers += [prefix + "/", prefix + "?", prefix + '"']
    return any(marker in serialized for marker in markers)


# ---------------------------------------------------------------------------------------
# Tombstone wrapper (raw pymongo updates to coexist with concurrent admin actions)
# ---------------------------------------------------------------------------------------

class _Tombstone:
    def __init__(self, doc):
        self.doc = doc

    @classmethod
    def load(cls, courseid):
        doc = CourseDeletion._get_collection().find_one({"_id": courseid})
        return cls(doc) if doc is not None else None

    def reload(self):
        self.doc = CourseDeletion._get_collection().find_one({"_id": self.doc["_id"]})
        return self

    @property
    def id(self):
        return self.doc["_id"]

    def __getitem__(self, key):
        return self.doc.get(key)

    def get(self, key, default=None):
        return self.doc.get(key, default)

    def update(self, now=None, **fields):
        now = now or _now()
        CourseDeletion._get_collection().update_one({"_id": self.id}, {"$set": dict(fields, updated_at=now)})
        self.doc.update(fields)
        self.doc["updated_at"] = now

    def unset(self, *fields):
        CourseDeletion._get_collection().update_one({"_id": self.id}, {"$unset": {f: "" for f in fields}})
        for f in fields:
            self.doc.pop(f, None)

    def add_fact(self, fact):
        CourseDeletion._get_collection().update_one({"_id": self.id}, {"$addToSet": {"facts": fact}})
        self.doc.setdefault("facts", [])
        if fact not in self.doc["facts"]:
            self.doc["facts"].append(fact)

    def pin(self, name):
        return (self.doc.get("pins") or {}).get(name)

    def set_pin(self, name, status, detail=None, now=None):
        now = now or _now()
        pin_doc = {"name": name, "status": status, "recorded_at": now, "detail": detail or {}}
        CourseDeletion._get_collection().update_one({"_id": self.id}, {"$set": {"pins.{}".format(name): pin_doc}})
        self.doc.setdefault("pins", {})[name] = pin_doc

    def append_db_step(self, step):
        CourseDeletion._get_collection().update_one({"_id": self.id}, {"$addToSet": {"db_steps": step}})
        self.doc.setdefault("db_steps", [])
        if step not in self.doc["db_steps"]:
            self.doc["db_steps"].append(step)

    def block(self, state, detail=None, now=None):
        """ Enter a blocked sub-condition (kept until ``unblock``). """
        now = now or _now()
        blocked_since = (self.doc.get("blocked_since") if self.doc.get("blocked") == state else None) or now
        self.update(blocked=state, blocked_since=blocked_since, blocked_detail=detail or {}, now=now)

    def unblock(self):
        if self.doc.get("blocked"):
            self.unset("blocked", "blocked_since", "blocked_detail")


# ---------------------------------------------------------------------------------------
# Manager + coordinator thread
# ---------------------------------------------------------------------------------------

class CourseDeletionManager:
    """ Drives course deletions and hands out archive/export operation locks. """

    def __init__(self, client=None, config=None):
        config = config or {}
        self._client = client
        self._tick_interval = config.get("tick_interval", TICK_INTERVAL)
        self._lease_seconds = config.get("lease_seconds", LEASE_SECONDS)
        self._settle_seconds = config.get("settle_seconds", SETTLE_SECONDS)
        self._running_hard_cap = config.get("running_hard_cap", RUNNING_HARD_CAP)
        self._plugin_total_budget = config.get("plugin_total_budget", PLUGIN_TOTAL_BUDGET)
        self._owner = "{}:{}:{}".format(socket.gethostname(), os.getpid(), uuid.uuid4().hex[:8])
        self._stopped = threading.Event()
        self._thread = None
        # courseid -> {"thread": Thread, "responses": list, "started": datetime}
        self._plugin_calls = {}

    # ----- lifecycle ----------------------------------------------------------

    def start(self):
        if self._thread is not None:
            return
        self._stopped.clear()
        self._thread = threading.Thread(target=self._run, name="course-deletion-coordinator", daemon=True)
        self._thread.start()
        _logger.info("Course deletion coordinator started as %s", self._owner)

    def stop(self):
        self._stopped.set()

    def _run(self):
        while not self._stopped.is_set():
            try:
                for tomb in CourseDeletion.objects.only("id"):
                    if self._stopped.is_set():
                        break
                    try:
                        self.tick_course(tomb.id)
                    except Exception:
                        _logger.exception("Error while ticking deletion of %s", tomb.id)
            except Exception:
                _logger.exception("Error while scanning course deletions")
            self._stopped.wait(self._tick_interval)

    # ----- request side -------------------------------------------------------

    def request_deletion(self, course, username):
        """ Idempotently request deletion of ``course``.

        :returns: (tombstone, created): when a tombstone already exists (retry,
                  double click, replayed POST) it is returned unchanged and no
                  second cleanup is ever created.
        """
        courseid = course.get_id()
        existing = CourseDeletion.objects(id=courseid).first()
        if existing is not None:
            return existing, False

        deletion_id = str(uuid.uuid4())
        holder = "delete:" + deletion_id
        if not acquire_operation_lock(courseid, "delete", holder, DELETE_LOCK_TTL):
            raise CourseOperationInProgressException(
                _("An archive or export operation is currently running on this course. Please retry shortly."))

        now = _now()
        descriptor = course.get_descriptor()
        tombstone = CourseDeletion(
            id=courseid, deletion_id=deletion_id, requested_by=username, requested_at=now, updated_at=now,
            descriptor=descriptor, course_name=descriptor.get("name"), phase=PHASE_REQUESTED)
        try:
            tombstone.save(force_insert=True)
        except DuplicateKeyError:
            # Concurrent request won the race: abandon our lock and return its tombstone.
            release_operation_lock(courseid, holder)
            return CourseDeletion.objects(id=courseid).first(), False
        invalidate_gate_cache(courseid)
        _logger.info("Course %s deletion %s requested by %s", courseid, deletion_id, username)
        return tombstone, True

    def get_deletion(self, courseid):
        return CourseDeletion.objects(id=courseid).first()

    def run_now(self, courseid, max_seconds=FAST_PATH_BUDGET):
        """ Run ticks in the calling thread (fast path for courses without
        references). Returns True iff the deletion completed. """
        end = time.monotonic() + max_seconds
        while time.monotonic() < end:
            finalized = self.tick_course(courseid)
            if finalized:
                return True
            if self.get_deletion(courseid) is None:
                return True
            time.sleep(0.1)
        return self.get_deletion(courseid) is None

    # ----- admin actions ------------------------------------------------------

    def continue_waiting(self, courseid):
        """ Clear an awaiting_jobs block: the coordinator keeps observing and
        completes as soon as the surviving jobs disappear. """
        CourseDeletion._get_collection().update_one(
            {"_id": courseid, "blocked": BLOCKED_AWAITING_JOBS},
            {"$unset": {"blocked": "", "blocked_since": "", "blocked_detail": ""}})

    def isolate_plugin(self, courseid, plugin_name):
        """ Mark a non-cooperating plugin as isolated and proceed without it. """
        res = CourseDeletion._get_collection().update_one(
            {"_id": courseid, "blocked": BLOCKED_PLUGIN_REFUSED},
            {"$addToSet": {"isolated_plugins": plugin_name},
             "$set": {"pins.plugins.detail.plugins.{}.status".format(plugin_name): PIN_SKIPPED},
             "$unset": {"blocked": "", "blocked_since": "", "blocked_detail": ""}})
        return res.modified_count > 0

    def retry_fs(self, courseid):
        """ Immediately retry a failed filesystem finalization. """
        res = CourseDeletion._get_collection().update_one(
            {"_id": courseid, "blocked": BLOCKED_FS_ERROR},
            {"$unset": {"blocked": "", "blocked_since": "", "blocked_detail": ""}})
        return res.modified_count > 0

    # ----- archive/export lock ------------------------------------------------

    @contextlib.contextmanager
    def operation_lock(self, courseid, op, lease_seconds=60.0, heartbeat=10.0):
        """ Serialize ``op`` (archive/export) against deletion and each other. """
        if CourseDeletion.objects(id=courseid).only("id").first() is not None:
            raise CourseOperationInProgressException(
                _("This course is being deleted; archive and export are unavailable."))
        holder = "{}:{}".format(op, uuid.uuid4().hex)
        if not acquire_operation_lock(courseid, op, holder, lease_seconds):
            raise CourseOperationInProgressException(
                _("Another operation is currently running on this course. Please retry shortly."))
        stop = threading.Event()

        def _heartbeat():
            while not stop.wait(heartbeat):
                renew_operation_lock(courseid, holder, lease_seconds)

        thread = threading.Thread(target=_heartbeat, name="course-operation-heartbeat", daemon=True)
        thread.start()
        try:
            yield
        finally:
            stop.set()
            release_operation_lock(courseid, holder)

    # ----- worker -------------------------------------------------------------

    def _claim(self, courseid):
        now = _now()
        res = CourseDeletion._get_collection().find_one_and_update(
            {"_id": courseid,
             "$or": [{"owner_id": None}, {"owner_id": self._owner}, {"lease_until": {"$lt": now}}]},
            {"$set": {"owner_id": self._owner, "lease_until": now + timedelta(seconds=self._lease_seconds),
                      "updated_at": now}},
            return_document=ReturnDocument.AFTER)
        return res is not None

    def tick_course(self, courseid):
        """ Run one state-machine tick. Returns True when the tombstone is gone. """
        if not self._claim(courseid):
            return False
        ts = _Tombstone.load(courseid)
        if ts is None:
            return True

        # The coarse delete lock follows the (possibly different) process that
        # currently owns the tombstone, and keeps archive/export out.
        renew_operation_lock(courseid, "delete:" + ts["deletion_id"], DELETE_LOCK_TTL)

        phase = ts["phase"]
        try:
            if phase == PHASE_REQUESTED:
                self._tick_requested(ts)
            elif phase == PHASE_DRAINING:
                self._tick_draining(ts)
            elif phase == PHASE_WIPING_DB:
                self._tick_wiping_db(ts)
            elif phase == PHASE_DELETING_FS:
                self._tick_deleting_fs(ts)
            elif phase == PHASE_FINALIZING:
                self._tick_finalizing(ts)
        except Exception:
            _logger.exception("Deletion %s of %s failed in phase %s", ts["deletion_id"], courseid, phase)
        return _Tombstone.load(courseid) is None

    def _tick_requested(self, ts):
        now = _now()
        ts.update(phase=PHASE_DRAINING, gate_published_at=now)
        invalidate_gate_cache(ts.id)
        _logger.info("Course %s entering draining phase (%s)", ts.id, ts["deletion_id"])

    # ----- draining -----------------------------------------------------------

    def _tick_draining(self, ts):
        courseid = ts.id
        now = _now()
        gate_age = (now - ts["gate_published_at"]).total_seconds()

        self._enumerate_static_refs(ts, now)
        self._drain_sessions(ts, now, gate_age)
        self._drain_lti_launches(ts)
        jobs_blocked = self._drain_jobs(ts, now)
        lti_ready = self._drain_lti_queues(ts, now)
        plugins_blocked = self._drain_plugins(ts, now)

        if jobs_blocked:
            ts.block(BLOCKED_AWAITING_JOBS, jobs_blocked, now=now)
            return
        if plugins_blocked:
            ts.block(BLOCKED_PLUGIN_REFUSED, plugins_blocked, now=now)
            return
        ts.unblock()

        if gate_age >= self._settle_seconds and lti_ready and self._all_pins_complete(ts):
            ts.update(phase=PHASE_WIPING_DB, now=now)
            _logger.info("Course %s drained, starting database cleanup", courseid)

    def _enumerate_static_refs(self, ts, now):
        """ Pin the DB-held references. They are cleaned later; here we only
        enumerate and record the conclusion counts. """
        courseid = ts.id
        refs = {
            "waiting_submissions": Submission.objects(courseid=courseid, status="waiting").count(),
            "course_class": CourseClass.objects(id=courseid).count(),
            "audiences": Audience.objects(courseid=courseid).count(),
            "groups": Group.objects(courseid=courseid).count(),
            "user_tasks": UserTask.objects(courseid=courseid).count(),
        }
        pinned_count, binding_keys = self._count_user_refs(courseid)
        refs["users"] = pinned_count
        refs["lti_binding_keys"] = len(binding_keys)
        for name, detail in (("db_refs", refs), ("users_refs", {"pinned": pinned_count,
                                                                "ltibinding_keys": sorted(binding_keys)})):
            pin = ts.pin(name)
            if pin is None or pin.get("status") != PIN_DONE:
                ts.set_pin(name, PIN_DONE, detail, now)

    def _drain_sessions(self, ts, now, gate_age):
        courseid = ts.id
        if gate_age < self._settle_seconds:
            return
        # No new LTI session can be created once the gate is effective; remove
        # the remaining ones so LTI task-page callbacks lose their carrier.
        Session._get_collection().delete_many({"lti.task": courseid})
        pin = ts.pin("sessions_lti")
        if pin is None or pin.get("status") != PIN_DONE:
            ts.set_pin("sessions_lti", PIN_DONE, {}, now)

    def _drain_lti_launches(self, ts):
        """ Weak reference: lti_launch is not keyed by course id. Best-effort
        removal, never blocks the deletion. """
        courseid = ts.id
        pin = ts.pin("lti_launch")
        if pin is not None and pin.get("status") == PIN_DONE:
            return
        removed = 0
        for entry in LaunchData.objects():
            try:
                if lti_launch_matches(entry.value, courseid):
                    entry.delete()
                    removed += 1
            except Exception:
                _logger.exception("Could not inspect an lti_launch entry while deleting %s", courseid)
        ts.set_pin("lti_launch", PIN_DONE, {"removed": removed})

    def _drain_jobs(self, ts, now):
        """ Kill queued jobs immediately, wait bounded for running ones, then
        kill them too. Returns a block detail dict when jobs survive. """
        courseid = ts.id
        jobs_running = jobs_waiting = None
        if self._client is not None:
            try:
                jobs_running, jobs_waiting = self._client.get_job_queue_snapshot()
            except Exception:
                _logger.exception("Could not get the job queue snapshot while deleting %s", courseid)
                jobs_running = jobs_waiting = None
        running, waiting = filter_course_jobs(jobs_running, jobs_waiting, courseid)

        pin = ts.pin("backend_jobs") or {}
        detail = dict(pin.get("detail") or {})
        detail.setdefault("killed", {})
        detail.setdefault("running", {})

        # Queued jobs: the backend answers "killed" at once.
        for entry in waiting:
            jobid = entry[0]
            if jobid not in detail["killed"] and self._client is not None:
                try:
                    self._client.kill_job(jobid)
                except Exception:
                    _logger.exception("Could not kill queued job %s", jobid)
                detail["killed"][jobid] = now
            detail["killed"].setdefault(jobid, now)

        # Running jobs: bounded wait, then kill, then a removal grace.
        survivors = []
        for entry in running:
            jobid = entry[0]
            started_at = datetime.fromtimestamp(entry[5], tz=timezone.utc) if entry[5] else None
            max_time = entry[6]
            info = detail["running"].setdefault(jobid, {})
            first_seen = info.get("first_seen")
            if not isinstance(first_seen, datetime):
                first_seen = now
                info["first_seen"] = first_seen
            soft_deadline = compute_running_deadline(first_seen, started_at, max_time, now,
                                                     hard_cap=self._running_hard_cap)
            info["soft_deadline"] = soft_deadline
            if now >= soft_deadline:
                if not info.get("killed_at"):
                    if self._client is not None:
                        try:
                            self._client.kill_job(jobid)
                        except Exception:
                            _logger.exception("Could not kill running job %s", jobid)
                    info["killed_at"] = now
                    detail["killed"][jobid] = now
                if now >= info["killed_at"] + timedelta(seconds=JOB_GRACE_SECONDS):
                    survivors.append(jobid)

        # Orphan waiting submission rows: once the backend reports no job for
        # the course for a grace period, fail them like the startup reset does.
        waiting_rows = Submission.objects(courseid=courseid, status="waiting").count()
        snapshot_available = jobs_running is not None
        if snapshot_available and not running and not waiting:
            since = detail.get("rows_absent_since")
            if not isinstance(since, datetime):
                since = now
                detail["rows_absent_since"] = since
            if waiting_rows > 0 and now - since >= timedelta(seconds=WAITING_ROW_GRACE):
                Submission.objects(courseid=courseid, status="waiting").update(
                    status="error", grade=0.0, text="Course deleted", unset__jobid=True)
                waiting_rows = Submission.objects(courseid=courseid, status="waiting").count()
        else:
            detail.pop("rows_absent_since", None)

        clear = snapshot_available and not running and not waiting and waiting_rows == 0
        if clear:
            since = detail.get("clear_since")
            if not isinstance(since, datetime):
                since = now
                detail["clear_since"] = since
            status = PIN_DONE if now - since >= timedelta(seconds=self._tick_interval) else PIN_ACTIVE
        else:
            detail["clear_since"] = None
            status = PIN_BLOCKED if survivors else PIN_ACTIVE
        detail["running_count"] = len(running)
        detail["waiting_count"] = len(waiting)
        detail["waiting_submissions"] = waiting_rows
        ts.set_pin("backend_jobs", status, detail, now)

        if survivors:
            return {"jobs": survivors, "message": _("Running grading jobs did not stop in time.")}
        return None

    def _drain_lti_queues(self, ts, now):
        """ Remove pending LTI grade pushbacks for the course and only report
        completion after the queues stay empty across two observations. """
        courseid = ts.id
        # The hardened publishers also discard such entries themselves; deleting
        # here is idempotent and avoids waiting on external LTI platforms.
        LISOutcome.objects(courseid=courseid).delete()
        LTIGrade.objects(courseid=courseid).delete()
        count = LISOutcome.objects(courseid=courseid).count() + LTIGrade.objects(courseid=courseid).count()

        pin = ts.pin("lti_queues") or {}
        detail = dict(pin.get("detail") or {})
        if count == 0:
            since = detail.get("clear_since")
            if not isinstance(since, datetime):
                since = now
                detail["clear_since"] = since
            ready = now - since >= timedelta(seconds=self._tick_interval)
            ts.set_pin("lti_queues", PIN_DONE if ready else PIN_ACTIVE, detail, now)
            return ready
        detail["clear_since"] = None
        ts.set_pin("lti_queues", PIN_ACTIVE, detail, now)
        return False

    def _drain_plugins(self, ts, now):
        """ Cooperative plugin drain via the ``course_deletion_drain`` hook.

        Plugins answer ``ready`` / ``pending`` / ``refused`` (exceptions are
        swallowed by the plugin manager and count as abandonment). A plugin
        whose answers never converge past the total budget ends up in
        ``plugin_refused`` and may be isolated by an administrator. """
        courseid = ts.id
        isolated = set(ts.get("isolated_plugins") or [])

        pin = ts.pin("plugins") or {}
        detail = dict(pin.get("detail") or {})
        plugin_states = dict(detail.get("plugins") or {})
        deadline = detail.get("deadline")
        if not isinstance(deadline, datetime):
            deadline = now + timedelta(seconds=self._plugin_total_budget)
            detail["deadline"] = deadline

        call = self._plugin_calls.get(courseid)
        if call is None or not call["thread"].is_alive():
            responses = []
            thread = threading.Thread(
                target=lambda: responses.extend(
                    plugin_manager.call_hook("course_deletion_drain", courseid=courseid,
                                             descriptor=ts.get("descriptor"), deadline=deadline,
                                             isolated=sorted(isolated))),
                name="course-deletion-plugin-drain", daemon=True)
            call = {"thread": thread, "responses": responses, "started": now}
            self._plugin_calls[courseid] = call
            thread.start()

        thread = call["thread"]
        thread.join(PLUGIN_CALL_BUDGET)

        overdue = now >= deadline
        if not thread.is_alive():
            for response in call["responses"]:
                if isinstance(response, dict) and response.get("state") in ("ready", "pending", "refused"):
                    name = response.get("plugin") or response.get("name") or "plugin"
                    plugin_states[name] = {"state": response["state"],
                                          "message": str(response.get("message", ""))}
            self._plugin_calls.pop(courseid, None)

        pending, refused = [], []
        for name, info in plugin_states.items():
            if name in isolated:
                info["status"] = PIN_SKIPPED
                continue
            state = info.get("state")
            if state == "ready":
                info["status"] = PIN_DONE
            elif state == "refused":
                info["status"] = PIN_BLOCKED
                refused.append(name)
            else:
                info["status"] = PIN_ACTIVE if not overdue else PIN_BLOCKED
                pending.append(name)
        if thread.is_alive() and overdue:
            pending.append("unknown")

        detail["plugins"] = plugin_states
        all_done = not thread.is_alive() and not pending and not refused
        if all_done:
            ts.set_pin("plugins", PIN_DONE, detail, now)
            self._plugin_calls.pop(courseid, None)
            return None
        ts.set_pin("plugins", PIN_ACTIVE, detail, now)
        if refused or overdue:
            names = sorted(set(refused) | set(pending))
            return {"plugins": names, "message": _("A plugin refused to release its references.")}
        return None

    def _all_pins_complete(self, ts):
        required = ["backend_jobs", "lti_queues", "sessions_lti", "lti_launch", "plugins", "db_refs", "users_refs"]
        pins = ts.doc.get("pins") or {}
        for name in required:
            pin = pins.get(name)
            if pin is None or pin.get("status") not in (PIN_DONE, PIN_SKIPPED):
                return False
        return True

    # ----- database cleanup ---------------------------------------------------

    def _tick_wiping_db(self, ts):
        courseid = ts.id
        now = _now()
        steps = set(ts.get("db_steps") or [])

        if "s1_gridfs" not in steps:
            for submission in Submission.objects(courseid=courseid):
                try:
                    submission.input.delete()
                    submission.archive.delete()
                except Exception:
                    _logger.exception("Could not delete GridFS blobs of submission %s", submission.id)
            ts.append_db_step("s1_gridfs")

        if "s2_recheck" not in steps:
            waiting_rows = Submission.objects(courseid=courseid, status="waiting").count()
            running, waiting = (None, None)
            if self._client is not None:
                try:
                    running, waiting = self._client.get_job_queue_snapshot()
                except Exception:
                    _logger.exception("Could not recheck the job queue while deleting %s", courseid)
                    running = waiting = None
            jobs_r, jobs_w = filter_course_jobs(running, waiting, courseid)
            if waiting_rows > 0 or jobs_r or jobs_w or running is None:
                ts.block(BLOCKED_AWAITING_JOBS,
                         {"waiting_submissions": waiting_rows, "running": len(jobs_r), "waiting": len(jobs_w)},
                         now=now)
                return
            ts.unblock()
            ts.append_db_step("s2_recheck")

        if "s3_userdata" not in steps:
            Audience.objects(courseid=courseid).delete()
            Group.objects(courseid=courseid).delete()
            UserTask.objects(courseid=courseid).delete()
            Submission.objects(courseid=courseid).delete()
            ts.append_db_step("s3_userdata")

        if "s4_course_class" not in steps:
            CourseClass.objects(id=courseid).delete()
            ts.append_db_step("s4_course_class")

        if "s5_users" not in steps:
            self._cleanup_user_refs(courseid)
            ts.append_db_step("s5_users")

        if "s6_lti" not in steps:
            LISOutcome.objects(courseid=courseid).delete()
            LTIGrade.objects(courseid=courseid).delete()
            Session._get_collection().delete_many({"lti.task": courseid})
            self._drain_lti_launches(_Tombstone.load(courseid))
            ts.append_db_step("s6_lti")

        ts.update(phase=PHASE_DELETING_FS, now=now)
        _logger.info("Course %s database cleaned, removing directory", courseid)

    def _count_user_refs(self, courseid):
        coll = User._get_collection()
        pinned = coll.count_documents({"pinned_courses": courseid})
        keys = set()
        for user in coll.find({"ltibindings": {"$exists": True}}, {"ltibindings": 1}):
            for key in (user.get("ltibindings") or {}):
                if key == courseid or key.startswith(courseid + "/"):
                    keys.add(key)
        return pinned, keys

    def _cleanup_user_refs(self, courseid):
        coll = User._get_collection()
        coll.update_many({"pinned_courses": courseid}, {"$pull": {"pinned_courses": courseid}})
        _, keys = self._count_user_refs(courseid)
        for key in keys:
            coll.update_many({}, {"$unset": {"ltibindings." + key: ""}})

    # ----- filesystem ---------------------------------------------------------

    def _tick_deleting_fs(self, ts):
        courseid = ts.id
        now = _now()
        if ts["blocked"] == BLOCKED_FS_ERROR and \
                now - (ts.get("blocked_since") or now) < timedelta(seconds=FS_AUTO_RETRY_DELAY):
            return
        ts.unblock()

        course_fs = get_fs_provider().from_subfolder(courseid)
        invalidate_cache(course_fs)
        try:
            if not course_fs.exists():
                ts.add_fact("fs_externally_removed")
            else:
                attempts = (ts.get("fs_attempts") or 0) + 1
                CourseDeletion._get_collection().update_one({"_id": courseid}, {"$set": {"fs_attempts": attempts}})
                course_fs.delete()
                if course_fs.exists():
                    raise OSError("course directory still present after deletion")
        except FileNotFoundError:
            ts.add_fact("fs_externally_removed")
        except OSError as ex:
            _logger.exception("Could not delete the directory of course %s", courseid)
            CourseDeletion._get_collection().update_one(
                {"_id": courseid}, {"$set": {"fs_last_error": repr(ex)}})
            ts.block(BLOCKED_FS_ERROR, {"error": repr(ex)}, now=now)
            return
        ts.update(phase=PHASE_FINALIZING, now=now)
        _logger.info("Course %s directory removed, finalizing", courseid)

    def _tick_finalizing(self, ts):
        courseid = ts.id
        now = _now()
        if not ts.get("event_published_at"):
            try:
                plugin_manager.call_hook("course_deleted", courseid=courseid,
                                         descriptor=ts.get("descriptor"),
                                         isolated=sorted(ts.get("isolated_plugins") or []))
            except Exception:
                _logger.exception("Error while publishing course_deleted for %s", courseid)
            ts.update(event_published_at=now, now=now)
        holder = "delete:" + ts["deletion_id"]
        CourseDeletion._get_collection().delete_one({"_id": courseid, "deletion_id": ts["deletion_id"]})
        release_operation_lock(courseid, holder)
        invalidate_gate_cache(courseid)
        self._plugin_calls.pop(courseid, None)
        _logger.info("Course %s fully deleted (deletion %s)", courseid, ts["deletion_id"])
