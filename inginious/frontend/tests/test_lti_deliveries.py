# -*- coding: utf-8 -*-
#
# This file is part of INGInious. See the LICENSE and the COPYRIGHTS files for
# more information about the licensing of this file.

"""
Tests for the LTI grade delivery state machine (leases, retries, isolation,
supersession, requeue, self-migration) and for the 1.1/1.3 failure classification.

The state machine is tested against an in-memory repository that reproduces the
findAndModify CAS semantics of the mongoengine repository.
"""

import copy
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
import requests
from bson import ObjectId
from unittest.mock import patch

from inginious.frontend.lti import LTIScorePublisher
from inginious.frontend.lti.delivery import (
    LocalContextError, RemoteTemporaryError, RemoteRejectedError,
    MAX_ATTEMPTS, LEASE_TTL, SEND_DEADLINE, NEW_DELIVERY_GRACE, backoff_delay,
    CATEGORY_LOCAL_CONTEXT, CATEGORY_REMOTE_TEMPORARY, CATEGORY_REMOTE_REJECTED)
from inginious.frontend.models.lti_delivery import (
    STATUS_PENDING, STATUS_PROCESSING, STATUS_SUCCEEDED, STATUS_FAILED, STATUS_SUPERSEDED)
from inginious.frontend.lti.v1_1 import LTIOutcomeManager
from inginious.frontend.lti.v1_3 import LTIGradeManager
from lti.utils import InvalidLTIConfigError
from pylti1p3.exception import LtiServiceException


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------

class FakeDeliveryRepository:
    """ In-memory twin of MongoDeliveryRepository, replicating its CAS conditions. """

    def __init__(self, clock):
        self._clock = clock
        self._docs = []
        import threading
        self._lock = threading.RLock()

    def _snap(self, rec):
        return copy.copy(rec)

    def get(self, doc_id):
        with self._lock:
            for rec in self._docs:
                if rec.id == doc_id:
                    return self._snap(rec)
        return None

    def all(self):
        with self._lock:
            return [self._snap(r) for r in self._docs]

    # setup

    def ensure_indexes(self):
        pass

    def migrate_legacy(self, now):
        with self._lock:
            for rec in self._docs:
                if rec.status is None:
                    rec.status = STATUS_PENDING
                    rec.attempts = rec.nb_attempt or 0
                    rec.next_attempt_at = now
                    rec.created_at = now
                    rec.updated_at = now

    # worker

    def reclaim_expired(self, now, owner):
        with self._lock:
            candidates = [r for r in self._docs
                          if r.status == STATUS_PROCESSING and r.lease_until is not None and r.lease_until <= now]
            if not candidates:
                return None
            rec = min(candidates, key=lambda r: (r.lease_until, r.id))
            rec.lease_owner = owner
            rec.lease_until = now + timedelta(seconds=LEASE_TTL)
            rec.updated_at = now
            rec.attempts += 1
            return self._snap(rec)

    def iter_due_pending(self, now, limit=50):
        with self._lock:
            due = [r for r in self._docs
                   if r.status == STATUS_PENDING and r.next_attempt_at is not None and r.next_attempt_at <= now]
            due.sort(key=lambda r: (r.next_attempt_at, r.id))
            return [self._snap(r) for r in due[:limit]]

    def has_processing_same_key(self, username, courseid, taskid):
        with self._lock:
            return any(r.status == STATUS_PROCESSING and r.username == username
                       and r.courseid == courseid and r.taskid == taskid for r in self._docs)

    def claim_pending(self, doc_id, now, owner):
        with self._lock:
            for rec in self._docs:
                if rec.id == doc_id and rec.status == STATUS_PENDING:
                    rec.status = STATUS_PROCESSING
                    rec.lease_owner = owner
                    rec.lease_until = now + timedelta(seconds=LEASE_TTL)
                    rec.updated_at = now
                    rec.attempts += 1
                    return self._snap(rec)
        return None

    def is_superseded(self, doc_id):
        with self._lock:
            for rec in self._docs:
                if rec.id == doc_id:
                    return rec.superseded_by is not None
        return False

    def _settle(self, rec, status, now, **changes):
        rec.status = status
        rec.finished_at = changes.pop("finished_at", None)
        rec.lease_owner = None
        rec.lease_until = None
        for key, value in changes.items():
            setattr(rec, key, value)
        rec.updated_at = now

    def settle_success(self, doc_id, owner, now, grade):
        with self._lock:
            for rec in self._docs:
                if rec.id == doc_id and rec.status == STATUS_PROCESSING and rec.lease_owner == owner \
                        and rec.superseded_by is None:
                    self._settle(rec, STATUS_SUCCEEDED, now, finished_at=now,
                                 error_category=None, error_summary=None, sent_grade=grade)
                    return True
        return False

    def settle_retry(self, doc_id, owner, now, category, summary, next_attempt_at):
        with self._lock:
            for rec in self._docs:
                if rec.id == doc_id and rec.status == STATUS_PROCESSING and rec.lease_owner == owner \
                        and rec.superseded_by is None:
                    self._settle(rec, STATUS_PENDING, now, next_attempt_at=next_attempt_at,
                                 error_category=category, error_summary=summary)
                    return True
        return False

    def settle_failed(self, doc_id, owner, now, category, summary):
        with self._lock:
            for rec in self._docs:
                if rec.id == doc_id and rec.status == STATUS_PROCESSING and rec.lease_owner == owner \
                        and rec.superseded_by is None:
                    self._settle(rec, STATUS_FAILED, now, finished_at=now,
                                 error_category=category, error_summary=summary)
                    return True
        return False

    def close_superseded(self, doc_id, owner, now):
        with self._lock:
            for rec in self._docs:
                if rec.id == doc_id and rec.status == STATUS_PROCESSING and rec.lease_owner == owner \
                        and rec.superseded_by is not None:
                    self._settle(rec, STATUS_SUPERSEDED, now, finished_at=now)
                    return True
        return False

    # enqueue

    def insert_delivery(self, fields, next_attempt_at):
        with self._lock:
            now = self._clock()
            rec = SimpleNamespace(
                id=ObjectId(), courseid=fields["courseid"], taskid=fields["taskid"],
                username=fields["username"],
                status=STATUS_PENDING, error_category=None, error_summary=None, attempts=0,
                next_attempt_at=next_attempt_at, lease_owner=None, lease_until=None,
                superseded_by=None, sent_grade=None, created_at=now, updated_at=now,
                finished_at=None, nb_attempt=None)
            for key, value in fields.items():
                setattr(rec, key, value)
            self._docs.append(rec)
            return self._snap(rec)

    def supersede_active(self, username, courseid, taskid, new_id, now):
        with self._lock:
            for rec in self._docs:
                if rec.id == new_id or rec.username != username or \
                        rec.courseid != courseid or rec.taskid != taskid:
                    continue
                if rec.status == STATUS_PENDING:
                    rec.status = STATUS_SUPERSEDED
                    rec.superseded_by = new_id
                    rec.finished_at = now
                    rec.updated_at = now
                elif rec.status == STATUS_PROCESSING:
                    rec.superseded_by = new_id
                    rec.updated_at = now

    # teacher

    def requeue_failed(self, courseid, doc_id, now):
        with self._lock:
            for rec in self._docs:
                if rec.id == doc_id and rec.courseid == courseid and rec.status == STATUS_FAILED:
                    rec.status = STATUS_PENDING
                    rec.attempts = 0
                    rec.next_attempt_at = now + timedelta(seconds=NEW_DELIVERY_GRACE)
                    rec.updated_at = now
                    rec.error_category = None
                    rec.error_summary = None
                    rec.lease_owner = None
                    rec.lease_until = None
                    rec.superseded_by = None
                    rec.finished_at = None
                    self.supersede_active(rec.username, courseid, rec.taskid, rec.id, now)
                    return True
        return False

    def list_for_course(self, courseid, status=None, taskid=None, username=None, limit=200):
        with self._lock:
            out = []
            for rec in self._docs:
                if rec.courseid != courseid:
                    continue
                if status and rec.status != status:
                    continue
                if taskid and rec.taskid != taskid:
                    continue
                if username and rec.username != username:
                    continue
                out.append(self._snap(rec))
            out.sort(key=lambda r: r.created_at, reverse=True)
            return out[:limit]


class MutableClock:
    def __init__(self):
        self.now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += timedelta(seconds=seconds)


class TestPublisher(LTIScorePublisher):
    """ Minimal concrete publisher whose send/prepare behavior is test-controlled. """
    __test__ = False  # not a pytest class
    _submission_tags = {"outcome_result_id": "outcome_result_id"}

    def __init__(self, repo, clock, user_manager, worker_id="w1", prepare_hook=None, send_hook=None):
        self._logger = logging.getLogger("inginious.tests.publisher")
        self._prepare_hook = prepare_hook
        self._send_hook = send_hook
        self.sent = []  # (entry id, grade) actually sent to the "platform"
        super().__init__(None, user_manager, repository=repo, clock=clock, worker_id=worker_id,
                         send_runner=lambda fn, ctx: fn(ctx), autostart=False)

    def prepare(self, entry, grade):
        context = {"entry_id": entry.id, "username": entry.username,
                   "courseid": entry.courseid, "taskid": entry.taskid, "grade": grade}
        if self._prepare_hook is not None:
            self._prepare_hook(self, entry, context)
        return context

    def send(self, context):
        self.sent.append((context["entry_id"], context["grade"]))
        if self._send_hook is not None:
            self._send_hook(self, context)


def make_user_manager(grades=None, failing_users=()):
    grades = grades or {}

    def get_task_cache(username, courseid, taskid):
        if username in failing_users:
            raise RuntimeError("task cache unavailable")
        return {"grade": grades.get((username, courseid, taskid), 100.0)}

    return SimpleNamespace(get_task_cache=get_task_cache)


def make_legacy_rec(**overrides):
    rec = SimpleNamespace(
        id=ObjectId(), courseid="c", taskid="t", username="u",
        status=None, error_category=None, error_summary=None, attempts=0,
        next_attempt_at=None, lease_owner=None, lease_until=None,
        superseded_by=None, sent_grade=None, created_at=None, updated_at=None,
        finished_at=None, nb_attempt=overrides.pop("nb_attempt", None))
    rec.__dict__.update(overrides)
    return rec


@pytest.fixture
def harness():
    clock = MutableClock()
    repo = FakeDeliveryRepository(clock)
    user_manager = make_user_manager()
    publisher = TestPublisher(repo, clock, user_manager)
    return SimpleNamespace(clock=clock, repo=repo, um=user_manager, pub=publisher)


def insert(repo, clock, username="u", courseid="c", taskid="t", status=STATUS_PENDING,
           due=True, attempts=0, superseded_by=None, lease_owner=None, lease_until=None,
           **extra):
    now = clock()
    rec = SimpleNamespace(
        id=ObjectId(), courseid=courseid, taskid=taskid, username=username,
        status=status, error_category=extra.pop("error_category", None),
        error_summary=extra.pop("error_summary", None), attempts=attempts,
        next_attempt_at=now if due else now + timedelta(hours=1),
        lease_owner=lease_owner, lease_until=lease_until,
        superseded_by=superseded_by, sent_grade=None, created_at=now, updated_at=now,
        finished_at=None, nb_attempt=None, outcome_result_id="rid")
    rec.__dict__.update(extra)
    repo._docs.append(rec)
    return rec


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

def test_send_deadline_shorter_than_lease():
    assert SEND_DEADLINE < LEASE_TTL


def test_backoff_sequence_and_cap():
    assert [backoff_delay(a) for a in range(1, 5)] == [30, 60, 120, 240]
    assert backoff_delay(8) == 3600


# ---------------------------------------------------------------------------
# Basic state machine, isolation and backoff
# ---------------------------------------------------------------------------

def test_due_pending_succeeds_and_is_evidenced(harness):
    rec = insert(harness.repo, harness.clock)
    assert harness.pub._tick() is True

    stored = harness.repo.get(rec.id)
    assert stored.status == STATUS_SUCCEEDED
    assert stored.attempts == 1
    assert stored.sent_grade == 100.0
    assert stored.finished_at == harness.clock.now
    assert stored.lease_owner is None
    assert harness.pub.sent == [(rec.id, 100.0)]


def test_not_due_not_claimed_then_due(harness):
    rec = insert(harness.repo, harness.clock, due=False)
    assert harness.pub._tick() is False
    assert harness.repo.get(rec.id).status == STATUS_PENDING

    harness.clock.advance(3601)
    assert harness.pub._tick() is True
    assert harness.repo.get(rec.id).status == STATUS_SUCCEEDED


def test_fifo_order(harness):
    first = insert(harness.repo, harness.clock, username="a")
    harness.clock.advance(1)
    second = insert(harness.repo, harness.clock, username="b")

    harness.pub._tick()
    assert harness.pub.sent[0][0] == first.id
    harness.pub._tick()
    assert harness.pub.sent[1][0] == second.id


def test_grade_read_failure_is_isolated(harness):
    bad = insert(harness.repo, harness.clock, username="bad")
    harness.um.get_task_cache = make_user_manager(failing_users=("bad",)).get_task_cache
    good = insert(harness.repo, harness.clock, username="good")

    # both due; order depends on (next_attempt_at, id): iterate ticks until the good one is sent
    sent_ids = set()
    for _ in range(2):
        harness.pub._tick()
        sent_ids = {sid for sid, _ in harness.pub.sent}

    assert good.id in sent_ids
    bad_stored = harness.repo.get(bad.id)
    assert bad_stored.status == STATUS_PENDING
    assert bad_stored.attempts == 1
    assert bad_stored.error_category == CATEGORY_LOCAL_CONTEXT
    assert bad_stored.next_attempt_at > harness.clock.now


def test_prepare_local_context_retries(harness):
    rec = insert(harness.repo, harness.clock)

    def fail_prepare(pub, entry, ctx):
        raise LocalContextError("course could not be loaded")

    harness.pub._prepare_hook = fail_prepare
    harness.pub._tick()

    stored = harness.repo.get(rec.id)
    assert stored.status == STATUS_PENDING
    assert stored.error_category == CATEGORY_LOCAL_CONTEXT
    assert "course could not be loaded" in stored.error_summary
    assert harness.pub.sent == []


def test_temporary_failures_back_off_then_become_terminal(harness):
    rec = insert(harness.repo, harness.clock)

    def always_temp(pub, ctx):
        raise RemoteTemporaryError("network error while contacting the LTI platform")

    harness.pub._send_hook = always_temp

    expected_delays = [30, 60, 120, 240]
    for attempt in range(1, MAX_ATTEMPTS + 1):
        start = harness.clock.now
        harness.pub._tick()
        stored = harness.repo.get(rec.id)
        assert stored.attempts == attempt
        if attempt < MAX_ATTEMPTS:
            assert stored.status == STATUS_PENDING
            assert stored.next_attempt_at == start + timedelta(seconds=expected_delays[attempt - 1])
            assert stored.error_category == CATEGORY_REMOTE_TEMPORARY
            harness.clock.advance(expected_delays[attempt - 1])
        else:
            assert stored.status == STATUS_FAILED
            assert stored.finished_at is not None

    # evidence is retained, nothing is deleted
    assert len(harness.repo.all()) == 1
    # failed delivery no longer occupies the active queue
    assert harness.pub._claim(harness.clock()) is None


def test_rejected_is_immediately_terminal(harness):
    rec = insert(harness.repo, harness.clock)
    harness.pub._send_hook = lambda pub, ctx: (_ for _ in ()).throw(
        RemoteRejectedError("platform rejected the grade (HTTP 401)"))

    harness.pub._tick()
    stored = harness.repo.get(rec.id)
    assert stored.status == STATUS_FAILED
    assert stored.attempts == 1
    assert stored.error_category == CATEGORY_REMOTE_REJECTED
    assert "HTTP 401" in stored.error_summary
    assert harness.pub._claim(harness.clock()) is None


def test_terminal_failure_does_not_block_queue(harness):
    insert(harness.repo, harness.clock, username="rejected")
    other = insert(harness.repo, harness.clock, username="other")

    rejected_ids = set()

    def hook(pub, ctx):
        rejected_ids.add(ctx["entry_id"])
        if ctx["username"] == "rejected":
            raise RemoteRejectedError("platform rejected the grade (code_major=failure)")

    harness.pub._send_hook = hook
    harness.pub._tick()
    harness.pub._tick()

    assert other.id in {sid for sid, _ in harness.pub.sent}
    rejected_rec = next(r for r in harness.repo.all() if r.username == "rejected")
    assert rejected_rec.status == STATUS_FAILED


def test_unexpected_exception_keeps_safe_summary(harness):
    rec = insert(harness.repo, harness.clock)

    def boom(pub, ctx):
        raise RuntimeError("oauth header with secret=SUPERSECRET url=http://x.example/post")

    harness.pub._send_hook = boom
    harness.pub._tick()

    stored = harness.repo.get(rec.id)
    assert stored.status == STATUS_PENDING
    assert stored.error_category == CATEGORY_REMOTE_TEMPORARY
    assert stored.error_summary == "internal error during delivery"
    assert "SUPERSECRET" not in (stored.error_summary or "")


def test_success_after_failure_clears_error(harness):
    rec = insert(harness.repo, harness.clock)
    harness.pub._send_hook = lambda pub, ctx: (_ for _ in ()).throw(
        RemoteTemporaryError("network error while contacting the LTI platform"))
    harness.pub._tick()

    harness.clock.advance(30)
    harness.pub._send_hook = None
    harness.pub._tick()

    stored = harness.repo.get(rec.id)
    assert stored.status == STATUS_SUCCEEDED
    assert stored.error_category is None
    assert stored.error_summary is None
    assert stored.sent_grade == 100.0


def test_run_loop_survives_tick_exception_and_stops(harness):
    calls = []

    def boom():
        calls.append(1)
        harness.pub.stop()
        raise RuntimeError("tick-level database outage")

    harness.pub._tick = boom
    harness.pub.run()  # must return promptly after stop, despite the exception
    assert calls == [1]


# ---------------------------------------------------------------------------
# Leases and recovery
# ---------------------------------------------------------------------------

def test_claim_is_atomic(harness):
    rec = insert(harness.repo, harness.clock)
    now = harness.clock.now

    assert harness.repo.claim_pending(rec.id, now, "w1") is not None
    assert harness.repo.claim_pending(rec.id, now, "w2") is None


def test_unexpired_lease_is_not_reclaimed_and_blocks_same_key(harness):
    insert(harness.repo, harness.clock, status=STATUS_PROCESSING,
           attempts=1, lease_owner="w1",
           lease_until=harness.clock.now + timedelta(seconds=100))
    insert(harness.repo, harness.clock, username="u")  # same key, pending, due

    assert harness.repo.reclaim_expired(harness.clock.now, "w2") is None
    # the newer delivery is behind the ordering barrier
    assert harness.pub._claim(harness.clock.now) is None

    # ... but another key is not blocked
    other = insert(harness.repo, harness.clock, username="someoneelse")
    claimed = harness.pub._claim(harness.clock.now)
    assert claimed.id == other.id


def test_expired_lease_is_taken_over(harness):
    rec = insert(harness.repo, harness.clock, status=STATUS_PROCESSING, attempts=1,
                 lease_owner="dead-w1", lease_until=harness.clock.now - timedelta(seconds=1))

    other_pub = TestPublisher(harness.repo, harness.clock, harness.um, worker_id="w2")
    claimed = other_pub._claim(harness.clock.now)
    assert claimed.id == rec.id
    assert claimed.lease_owner == "w2"
    assert claimed.attempts == 2


def test_terminal_deliveries_never_claimed(harness):
    insert(harness.repo, harness.clock, status=STATUS_SUCCEEDED, attempts=1)
    insert(harness.repo, harness.clock, status=STATUS_FAILED, attempts=5,
           username="f", error_category=CATEGORY_REMOTE_TEMPORARY)
    insert(harness.repo, harness.clock, status=STATUS_SUPERSEDED, username="s")

    harness.clock.advance(100000)
    assert harness.pub._tick() is False


def test_restart_recovers_unconfirmed_and_pending(harness):
    # "processing" delivery whose owner died without confirming, plus a waiting one
    inflight = insert(harness.repo, harness.clock, status=STATUS_PROCESSING, attempts=1,
                      lease_owner="dead-w1", lease_until=harness.clock.now - timedelta(seconds=10))
    waiting = insert(harness.repo, harness.clock, username="u2")

    restarted = TestPublisher(harness.repo, harness.clock, harness.um, worker_id="w-new")
    doc_count = len(harness.repo.all())

    restarted._tick()
    restarted._tick()

    sent_ids = {sid for sid, _ in restarted.sent}
    assert inflight.id in sent_ids
    assert waiting.id in sent_ids
    # no second conclusion document was ever created
    assert len(harness.repo.all()) == doc_count
    assert harness.repo.get(inflight.id).status == STATUS_SUCCEEDED
    assert harness.repo.get(waiting.id).status == STATUS_SUCCEEDED


def test_reclaimed_superseded_is_closed_without_send(harness):
    marker = ObjectId()
    rec = insert(harness.repo, harness.clock, status=STATUS_PROCESSING, attempts=1,
                 lease_owner="dead-w1", lease_until=harness.clock.now - timedelta(seconds=1),
                 superseded_by=marker)

    harness.pub._tick()
    assert harness.pub.sent == []
    stored = harness.repo.get(rec.id)
    assert stored.status == STATUS_SUPERSEDED
    assert stored.superseded_by == marker
    assert stored.lease_owner is None


def test_stale_owner_cannot_settle_after_takeover(harness):
    rec = insert(harness.repo, harness.clock, status=STATUS_PROCESSING, attempts=1,
                 lease_owner="w1", lease_until=harness.clock.now + timedelta(seconds=100))

    # w1's lease expires and w2 takes over while w1 was away
    harness.clock.advance(200)
    w2 = TestPublisher(harness.repo, harness.clock, harness.um, worker_id="w2")
    claimed = w2._claim(harness.clock.now)
    assert claimed.lease_owner == "w2"

    # w1 comes back: its success/close CAS must not touch the delivery
    now = harness.clock.now
    assert harness.repo.settle_success(rec.id, "w1", now, 50.0) is False
    assert harness.repo.close_superseded(rec.id, "w1", now) is False
    stored = harness.repo.get(rec.id)
    assert stored.status == STATUS_PROCESSING
    assert stored.lease_owner == "w2"
    assert stored.sent_grade is None


# ---------------------------------------------------------------------------
# Supersession
# ---------------------------------------------------------------------------

def submission(courseid="c", taskid="t", users=("u",)):
    return {"username": list(users), "courseid": courseid, "taskid": taskid,
            "outcome_result_id": "rid-{}".format(users[0])}


def test_add_supersedes_old_pending_and_new_delivery_wins(harness):
    old = insert(harness.repo, harness.clock)
    harness.pub.add(submission())

    old_stored = harness.repo.get(old.id)
    assert old_stored.status == STATUS_SUPERSEDED
    assert old_stored.superseded_by is not None
    assert old_stored.finished_at is not None

    new = [r for r in harness.repo.all() if r.status == STATUS_PENDING][0]
    assert new.id == old_stored.superseded_by

    # not due during the grace period
    assert harness.pub._tick() is False
    harness.clock.advance(NEW_DELIVERY_GRACE)
    harness.pub._tick()

    assert harness.pub.sent == [(new.id, 100.0)]  # old grade was never sent


def test_add_marks_processing_without_releasing_barrier(harness):
    old = insert(harness.repo, harness.clock, status=STATUS_PROCESSING, attempts=1,
                 lease_owner="w1", lease_until=harness.clock.now + timedelta(seconds=200))
    harness.pub.add(submission())

    old_stored = harness.repo.get(old.id)
    assert old_stored.status == STATUS_PROCESSING  # deliberately not changed
    assert old_stored.superseded_by is not None

    # new delivery must wait: barrier still active
    assert harness.pub._tick() is False

    # lease expires: the old delivery is reclaimed and closed WITHOUT sending,
    # then the new one goes out
    harness.clock.advance(201)
    harness.pub._tick()
    assert harness.pub.sent == []
    assert harness.repo.get(old.id).status == STATUS_SUPERSEDED

    harness.pub._tick()
    assert len(harness.pub.sent) == 1
    new = harness.repo.get(old_stored.superseded_by)
    assert harness.pub.sent[0][0] == new.id


def test_d1_preflight_aborts_send_when_superseded_during_prepare(harness):
    rec = insert(harness.repo, harness.clock)  # claimed by the worker within this tick

    def mark_during_prepare(pub, entry, ctx):
        # a newer submission arrives while the local context is being read
        new = pub._repo.insert_delivery(
            {"username": "u", "courseid": "c", "taskid": "t", "outcome_result_id": "rid2"},
            pub._clock() + timedelta(seconds=NEW_DELIVERY_GRACE))
        pub._repo.supersede_active("u", "c", "t", new.id, pub._clock())

    harness.pub._prepare_hook = mark_during_prepare
    harness.pub._tick()

    assert harness.pub.sent == []
    assert harness.repo.get(rec.id).status == STATUS_SUPERSEDED


def test_d2_success_discarded_when_superseded_during_send(harness):
    rec = insert(harness.repo, harness.clock)

    def mark_during_send(pub, ctx):
        new = pub._repo.insert_delivery(
            {"username": "u", "courseid": "c", "taskid": "t", "outcome_result_id": "rid2"},
            pub._clock() + timedelta(seconds=NEW_DELIVERY_GRACE))
        pub._repo.supersede_active("u", "c", "t", new.id, pub._clock())

    harness.pub._send_hook = mark_during_send
    harness.pub._tick()

    # the remote call happened (already in flight) but no local success is recorded
    assert harness.pub.sent == [(rec.id, 100.0)]
    assert harness.repo.get(rec.id).status == STATUS_SUPERSEDED


def test_barrier_lifts_as_soon_as_inflight_delivery_closes(harness):
    old = insert(harness.repo, harness.clock, status=STATUS_PROCESSING, attempts=1,
                 lease_owner="w1", lease_until=harness.clock.now + timedelta(seconds=200))
    # new delivery present (as if inserted and marked, but marker cleared by w1 winning race)
    new = insert(harness.repo, harness.clock, username="u")

    assert harness.pub._claim(harness.clock.now) is None
    assert harness.repo.settle_success(old.id, "w1", harness.clock.now, 90.0)
    claimed = harness.pub._claim(harness.clock.now)
    assert claimed is not None and claimed.id == new.id


def test_crash_between_insert_and_mark_keeps_final_grade_new(harness):
    # old pending is due; new delivery exists with grace but the supersede markers
    # were never written (process died in between)
    old = insert(harness.repo, harness.clock, username="u")
    fields = {"username": "u", "courseid": "c", "taskid": "t", "outcome_result_id": "rid-new"}
    new = harness.repo.insert_delivery(fields, harness.clock.now + timedelta(seconds=NEW_DELIVERY_GRACE))

    harness.um.get_task_cache = make_user_manager(
        grades={("u", "c", "t"): 90.0}).get_task_cache

    # tick 1: old delivery due and unblocked -> old grade sent first
    harness.pub._tick()
    assert harness.pub.sent[-1][0] == old.id
    # after grace, new delivery goes out with the latest grade
    harness.clock.advance(NEW_DELIVERY_GRACE)
    harness.pub._tick()
    assert harness.pub.sent[-1] == (new.id, 90.0)
    assert harness.pub.sent[0][0] == old.id  # platform order: old then new


def test_group_submission_creates_one_delivery_per_user(harness):
    harness.pub.add(submission(users=("alice", "bob")))
    docs = harness.repo.all()
    assert {r.username for r in docs} == {"alice", "bob"}

    harness.clock.advance(NEW_DELIVERY_GRACE)
    harness.pub._tick()
    harness.pub._tick()
    assert {sid for sid, _ in harness.pub.sent} == {r.id for r in docs}


def test_consecutive_adds_leave_single_active_head(harness):
    harness.pub.add(submission())
    harness.clock.advance(1)
    harness.pub.add(submission())
    harness.clock.advance(1)
    harness.pub.add(submission())

    pending = [r for r in harness.repo.all() if r.status == STATUS_PENDING]
    superseded = [r for r in harness.repo.all() if r.status == STATUS_SUPERSEDED]
    assert len(pending) == 1
    assert len(superseded) == 2
    chain = {r.superseded_by for r in superseded}
    assert pending[0].id in chain


# ---------------------------------------------------------------------------
# Requeue
# ---------------------------------------------------------------------------

def test_requeue_failed_delivery_resets_it(harness):
    rec = insert(harness.repo, harness.clock, status=STATUS_FAILED, attempts=MAX_ATTEMPTS,
                 error_category=CATEGORY_REMOTE_TEMPORARY,
                 error_summary="network error while contacting the LTI platform")

    assert harness.pub.requeue("c", str(rec.id)) is True
    stored = harness.repo.get(rec.id)
    assert stored.status == STATUS_PENDING
    assert stored.attempts == 0
    assert stored.error_category is None
    assert stored.error_summary is None
    assert stored.superseded_by is None
    assert stored.finished_at is None
    assert stored.next_attempt_at == harness.clock.now + timedelta(seconds=NEW_DELIVERY_GRACE)
    # stable identity preserved
    assert stored.id == rec.id


def test_requeue_rejects_active_other_course_and_bad_id(harness):
    pending = insert(harness.repo, harness.clock, status=STATUS_PENDING)
    other_course = insert(harness.repo, harness.clock, courseid="other", status=STATUS_FAILED,
                          username="x")

    assert harness.pub.requeue("c", pending.id) is False
    assert harness.pub.requeue("c", other_course.id) is False
    assert harness.pub.requeue("c", "not-an-object-id") is False


def test_requeue_supersedes_other_active_deliveries(harness):
    failed = insert(harness.repo, harness.clock, status=STATUS_FAILED, attempts=5,
                    error_category=CATEGORY_REMOTE_REJECTED)
    stray = insert(harness.repo, harness.clock, username="u")  # same key, pending

    assert harness.pub.requeue("c", failed.id) is True
    stray_stored = harness.repo.get(stray.id)
    assert stray_stored.status == STATUS_SUPERSEDED
    assert stray_stored.superseded_by == failed.id


# ---------------------------------------------------------------------------
# Legacy migration
# ---------------------------------------------------------------------------

def test_legacy_documents_are_self_migrated(harness):
    legacy = make_legacy_rec(nb_attempt=3)
    harness.repo._docs.append(legacy)

    harness.repo.migrate_legacy(harness.clock.now)
    stored = harness.repo.get(legacy.id)
    assert stored.status == STATUS_PENDING
    assert stored.attempts == 3
    assert stored.next_attempt_at == harness.clock.now
    assert stored.created_at is not None

    # idempotent
    harness.clock.advance(50)
    harness.repo.migrate_legacy(harness.clock.now)
    again = harness.repo.get(legacy.id)
    assert again.status == STATUS_PENDING
    assert again.attempts == 3


# ---------------------------------------------------------------------------
# Compatibility
# ---------------------------------------------------------------------------

def test_add_without_tags_is_a_noop(harness):
    harness.pub.add({"username": ["u"], "courseid": "c", "taskid": "t"})
    assert harness.repo.all() == []


def test_tag_submission_skips_none_values(harness):
    obj = {}
    harness.pub.tag_submission(obj, {"outcome_result_id": None})
    assert obj == {}
    harness.pub.tag_submission(obj, {"outcome_result_id": "rid"})
    assert obj == {"outcome_result_id": "rid"}


# ---------------------------------------------------------------------------
# LTI 1.1 classification
# ---------------------------------------------------------------------------

class FakeResponse:
    def __init__(self, code_major="success", response_code=200):
        self.code_major = code_major
        self.response_code = response_code


class FakeOutcomeRequest:
    next_response = FakeResponse()
    next_exc = None

    def __init__(self, params):
        self.params = params

    def post_replace_result(self, grade):
        if FakeOutcomeRequest.next_exc is not None:
            raise FakeOutcomeRequest.next_exc
        return FakeOutcomeRequest.next_response


@pytest.fixture
def manager_1_1():
    mgr = object.__new__(LTIOutcomeManager)
    mgr._logger = logging.getLogger("inginious.tests.lti11")
    return mgr


def outcome_entry():
    return SimpleNamespace(courseid="c", id=ObjectId(), outcome_consumer_key="key",
                           outcome_result_id="rid", outcome_service_url="http://tc/outcome")


def test_v11_prepare_clips_grade(manager_1_1):
    fake_course = SimpleNamespace(lti_keys=lambda: {"key": "secret"})
    with patch("inginious.frontend.lti.v1_1.Course") as course_cls:
        course_cls.get.return_value = fake_course
        assert manager_1_1.prepare(outcome_entry(), 50.0)["grade"] == pytest.approx(0.5)
        assert manager_1_1.prepare(outcome_entry(), 150.0)["grade"] == pytest.approx(1.0)
        assert manager_1_1.prepare(outcome_entry(), -5.0)["grade"] == pytest.approx(0.0)


def test_v11_prepare_missing_course_and_key(manager_1_1):
    with patch("inginious.frontend.lti.v1_1.Course") as course_cls:
        course_cls.get.side_effect = Exception("no such course")
        with pytest.raises(LocalContextError):
            manager_1_1.prepare(outcome_entry(), 50.0)

        course_cls.get.side_effect = None
        course_cls.get.return_value = SimpleNamespace(lti_keys=lambda: {})
        with pytest.raises(LocalContextError):
            manager_1_1.prepare(outcome_entry(), 50.0)


def test_v11_send_classification(manager_1_1):
    ctx = {"consumer_key": "key", "consumer_secret": "secret",
           "lis_outcome_service_url": "http://tc/outcome", "lis_result_sourcedid": "rid",
           "grade": 0.5}

    cases = [
        (FakeResponse("success", 200), None, None),
        (FakeResponse("failure", 200), RemoteRejectedError, "code_major=failure"),
        (FakeResponse("unsupported", 200), RemoteRejectedError, "code_major=unsupported"),
        (FakeResponse("processing", 200), RemoteTemporaryError, "processing"),
        (FakeResponse(None, 200), RemoteTemporaryError, "unparseable"),
        (FakeResponse(None, 503), RemoteTemporaryError, "HTTP 503"),
        (FakeResponse(None, 429), RemoteTemporaryError, "HTTP 429"),
        (FakeResponse(None, 408), RemoteTemporaryError, "HTTP 408"),
        (FakeResponse(None, 404), RemoteRejectedError, "HTTP 404"),
        (FakeResponse(None, 401), RemoteRejectedError, "HTTP 401"),
    ]

    with patch("inginious.frontend.lti.v1_1.OutcomeRequest", FakeOutcomeRequest):
        for response, expected, fragment in cases:
            FakeOutcomeRequest.next_response = response
            FakeOutcomeRequest.next_exc = None
            if expected is None:
                assert manager_1_1.send(ctx) is None
            else:
                with pytest.raises(expected) as exc_info:
                    manager_1_1.send(ctx)
                assert fragment in str(exc_info.value)


def test_v11_network_error_is_temporary_and_safe(manager_1_1):
    ctx = {"consumer_key": "key", "consumer_secret": "secret",
           "lis_outcome_service_url": "http://tc/outcome", "lis_result_sourcedid": "rid",
           "grade": 0.5}
    with patch("inginious.frontend.lti.v1_1.OutcomeRequest", FakeOutcomeRequest):
        FakeOutcomeRequest.next_exc = requests.ConnectionError(
            "http://tc/outcome?oauth_secret=SUPERSECRET boom")
        with pytest.raises(RemoteTemporaryError) as exc_info:
            manager_1_1.send(ctx)
        assert "SUPERSECRET" not in str(exc_info.value)
        assert "http" not in str(exc_info.value).lower()

        FakeOutcomeRequest.next_exc = InvalidLTIConfigError("bad config")
        with pytest.raises(LocalContextError):
            manager_1_1.send(ctx)


# ---------------------------------------------------------------------------
# LTI 1.3 classification
# ---------------------------------------------------------------------------

@pytest.fixture
def manager_1_3():
    mgr = object.__new__(LTIGradeManager)
    mgr._logger = logging.getLogger("inginious.tests.lti13")
    mgr._global_config = {}
    return mgr


def grade_context(can_put=True):
    ags = SimpleNamespace(
        can_put_grade=lambda: can_put,
        put_grade=lambda grade: FakeAgs.next_effect())
    return {"ags": ags, "sub": "user-sub", "grade": 87.0}


class FakeAgs:
    next_effect = staticmethod(lambda: None)


def test_v13_send_no_capability_is_rejected(manager_1_3):
    with pytest.raises(RemoteRejectedError) as exc_info:
        manager_1_3.send(grade_context(can_put=False))
    assert "grade-write capability" in str(exc_info.value)


def fake_service_exception(status_code):
    response = SimpleNamespace(
        status_code=status_code,
        url="http://platform/ags?access_token=SUPERSECRET",
        text="internal platform secret body")
    return LtiServiceException(response)


def test_v13_send_http_classification_and_safety(manager_1_3):
    for code, expected in [(503, RemoteTemporaryError), (500, RemoteTemporaryError),
                           (429, RemoteTemporaryError), (408, RemoteTemporaryError),
                           (401, RemoteRejectedError), (403, RemoteRejectedError),
                           (404, RemoteRejectedError), (400, RemoteRejectedError)]:
        FakeAgs.next_effect = staticmethod(lambda c=code: (_ for _ in ()).throw(fake_service_exception(c)))
        with pytest.raises(expected) as exc_info:
            manager_1_3.send(grade_context())
        message = str(exc_info.value)
        assert "HTTP {}".format(code) in message
        assert "SUPERSECRET" not in message
        assert "secret body" not in message


def test_v13_network_error_is_temporary(manager_1_3):
    FakeAgs.next_effect = staticmethod(lambda: (_ for _ in ()).throw(
        requests.Timeout("http://platform/ags?token=SUPERSECRET")))
    with pytest.raises(RemoteTemporaryError) as exc_info:
        manager_1_3.send(grade_context())
    assert "SUPERSECRET" not in str(exc_info.value)


def test_v13_prepare_context_errors_are_local(manager_1_3):
    entry = SimpleNamespace(courseid="c", id=ObjectId(), message_launch_id="launch")
    with patch("inginious.frontend.lti.v1_3.Course") as course_cls:
        course_cls.get.side_effect = Exception("no such course")
        with pytest.raises(LocalContextError):
            manager_1_3.prepare(entry, 50.0)
