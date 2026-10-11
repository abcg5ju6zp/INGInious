# -*- coding: utf-8 -*-
#
# This file is part of INGInious. See the LICENSE and the COPYRIGHTS files for
# more information about the licensing of this file.

"""
Persistence layer for LTI grade deliveries.

The repository isolates the worker state machine from mongoengine so that the
state machine and its race conditions can be tested with an in-memory fake.
"""

from datetime import timedelta

from inginious.frontend.models.lti_delivery import (
    STATUS_PENDING, STATUS_PROCESSING, STATUS_SUCCEEDED, STATUS_FAILED, STATUS_SUPERSEDED)

# Attempts
MAX_ATTEMPTS = 5

# Lease / send timing. The hard send deadline MUST be shorter than the lease:
# lti 0.9.5 performs its HTTP call without a timeout, so a live-but-stuck owner
# must stop waiting before another worker is allowed to take over.
LEASE_TTL = 300
SEND_DEADLINE = 250
assert SEND_DEADLINE < LEASE_TTL

# Worker
POLL_INTERVAL = 5

# Bounded exponential backoff (seconds): 30, 60, 120, 240, ... capped at 1h
BACKOFF_BASE = 30
BACKOFF_MAX = 3600

# New deliveries wait a short grace period before becoming due; this keeps the
# platform write order correct if the process dies between inserting the new
# delivery and marking the old ones as superseded.
NEW_DELIVERY_GRACE = 2

# Error categories
CATEGORY_LOCAL_CONTEXT = "local_context"
CATEGORY_REMOTE_TEMPORARY = "remote_temporary"
CATEGORY_REMOTE_REJECTED = "remote_rejected"

# Fields the teacher UI is allowed to see. LTI tags (consumer key, sourcedid,
# service URL, message launch id) and secrets are never projected.
TEACHER_FIELDS = ('courseid', 'taskid', 'username', 'status', 'error_category',
                  'error_summary', 'attempts', 'next_attempt_at', 'sent_grade',
                  'created_at', 'updated_at', 'finished_at')


class DeliveryError(Exception):
    """ Base class for delivery failures. """


class LocalContextError(DeliveryError):
    """ Reading the local context (grade, course, LTI key/secret, launch) failed. Retryable. """


class RemoteTemporaryError(DeliveryError):
    """ The remote platform could not be reached or answered with a temporary failure. Retryable. """


class RemoteRejectedError(DeliveryError):
    """ The remote platform gave a definitive, non-retryable rejection. Terminal. """


def backoff_delay(attempts):
    """ Delay (seconds) after the ``attempts``-th failed send. """
    return min(BACKOFF_BASE * 2 ** (attempts - 1), BACKOFF_MAX)


class MongoDeliveryRepository:
    """ mongoengine-backed delivery repository for one LTI version's collection. """

    def __init__(self, model):
        self._model = model

    # -- setup ---------------------------------------------------------------

    def ensure_indexes(self):
        self._model.ensure_indexes()

    def migrate_legacy(self, now):
        """ Add the new state fields to documents written by the old publisher. Idempotent. """
        # Materialize first: the queue collections are tiny and we are writing
        # the very documents this query matches.
        for legacy in list(self._model.objects(status__exists=False)):
            legacy.update(
                set__status=STATUS_PENDING,
                set__attempts=legacy.nb_attempt or 0,
                set__next_attempt_at=now,
                set__created_at=now,
                set__updated_at=now
            )

    # -- worker leasing --------------------------------------------------------

    def reclaim_expired(self, now, owner):
        """ Atomically take over an expired processing lease. Returns the document or None. """
        return (self._model.objects(status=STATUS_PROCESSING, lease_until__lte=now)
                .order_by('lease_until')
                .modify(new=True,
                        set__lease_owner=owner,
                        set__lease_until=now + timedelta(seconds=LEASE_TTL),
                        set__updated_at=now,
                        inc__attempts=1))

    def iter_due_pending(self, now, limit=50):
        """ Due pending deliveries in FIFO order (next_attempt_at). """
        return list(self._model.objects(status=STATUS_PENDING, next_attempt_at__lte=now)
                    .order_by('next_attempt_at').limit(limit))

    def has_processing_same_key(self, username, courseid, taskid):
        return self._model.objects(username=username, courseid=courseid, taskid=taskid,
                                   status=STATUS_PROCESSING).count() > 0

    def claim_pending(self, doc_id, now, owner):
        """ Atomically claim a pending delivery. Returns the document or None if lost the race. """
        return (self._model.objects(id=doc_id, status=STATUS_PENDING)
                .modify(new=True,
                        set__status=STATUS_PROCESSING,
                        set__lease_owner=owner,
                        set__lease_until=now + timedelta(seconds=LEASE_TTL),
                        set__updated_at=now,
                        inc__attempts=1))

    # -- worker settlement -----------------------------------------------------

    def is_superseded(self, doc_id):
        """ Fresh read of the supersession marker (D1 preflight). """
        return self._model.objects(id=doc_id, superseded_by__ne=None).count() > 0

    def settle_success(self, doc_id, owner, now, grade):
        """ Confirm success, only if we still own the lease and it was not superseded. """
        return self._model.objects(id=doc_id, status=STATUS_PROCESSING, lease_owner=owner,
                                   superseded_by=None).modify(
            new=True,
            set__status=STATUS_SUCCEEDED,
            set__finished_at=now,
            set__updated_at=now,
            set__error_category=None,
            set__error_summary=None,
            set__sent_grade=grade,
            set__lease_owner=None,
            set__lease_until=None) is not None

    def settle_retry(self, doc_id, owner, now, category, summary, next_attempt_at):
        """ Release the delivery back to pending with bounded backoff. """
        return self._model.objects(id=doc_id, status=STATUS_PROCESSING, lease_owner=owner,
                                   superseded_by=None).modify(
            new=True,
            set__status=STATUS_PENDING,
            set__next_attempt_at=next_attempt_at,
            set__updated_at=now,
            set__error_category=category,
            set__error_summary=summary,
            set__lease_owner=None,
            set__lease_until=None) is not None

    def settle_failed(self, doc_id, owner, now, category, summary):
        """ Mark the delivery as terminally failed (attempts exhausted or definitive rejection). """
        return self._model.objects(id=doc_id, status=STATUS_PROCESSING, lease_owner=owner,
                                   superseded_by=None).modify(
            new=True,
            set__status=STATUS_FAILED,
            set__finished_at=now,
            set__updated_at=now,
            set__error_category=category,
            set__error_summary=summary,
            set__lease_owner=None,
            set__lease_until=None) is not None

    def close_superseded(self, doc_id, owner, now):
        """ Close a processing delivery whose supersession marker is set. No remote conclusion is written. """
        return self._model.objects(id=doc_id, status=STATUS_PROCESSING, lease_owner=owner,
                                   superseded_by__ne=None).modify(
            new=True,
            set__status=STATUS_SUPERSEDED,
            set__finished_at=now,
            set__updated_at=now,
            set__lease_owner=None,
            set__lease_until=None) is not None

    # -- enqueue ---------------------------------------------------------------

    def insert_delivery(self, fields, next_attempt_at):
        """ Create a new delivery with a fresh stable identity. Returns the saved document. """
        doc = self._model(status=STATUS_PENDING, attempts=0, next_attempt_at=next_attempt_at, **fields)
        doc.save()
        return doc

    def supersede_active(self, username, courseid, taskid, new_id, now):
        """
        Mark prior active deliveries of the same user/course/task as superseded.

        Pending ones become terminal directly; processing ones keep their
        ``processing`` status and only get the marker, so that (a) the new
        delivery stays behind the ordering barrier until the in-flight send is
        over and (b) the old owner aborts before/after the send.
        """
        key = {"username": username, "courseid": courseid, "taskid": taskid}
        self._model.objects(id__ne=new_id, status=STATUS_PENDING, **key).update(
            set__status=STATUS_SUPERSEDED,
            set__superseded_by=new_id,
            set__finished_at=now,
            set__updated_at=now)
        self._model.objects(id__ne=new_id, status=STATUS_PROCESSING, **key).update(
            set__superseded_by=new_id,
            set__updated_at=now)

    # -- teacher actions -------------------------------------------------------

    def requeue_failed(self, courseid, doc_id, now):
        """
        Reset a failed delivery for another round of attempts. Returns True only if
        the document belonged to ``courseid`` and was in the failed state.
        """
        doc = self._model.objects(id=doc_id, courseid=courseid, status=STATUS_FAILED).modify(
            new=True,
            set__status=STATUS_PENDING,
            set__attempts=0,
            set__next_attempt_at=now + timedelta(seconds=NEW_DELIVERY_GRACE),
            set__updated_at=now,
            set__error_category=None,
            set__error_summary=None,
            set__lease_owner=None,
            set__lease_until=None,
            set__superseded_by=None,
            set__finished_at=None)
        if doc is None:
            return False
        self.supersede_active(doc.username, courseid, doc.taskid, doc.id, now)
        return True

    def list_for_course(self, courseid, status=None, taskid=None, username=None, limit=200):
        query = {"courseid": courseid}
        if status:
            query["status"] = status
        if taskid:
            query["taskid"] = taskid
        if username:
            query["username"] = username
        return list(self._model.objects(**query).only(*TEACHER_FIELDS)
                    .order_by('-created_at').limit(limit))
