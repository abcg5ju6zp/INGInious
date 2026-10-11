# -*- coding: utf-8 -*-
#
# This file is part of INGInious. See the LICENSE and the COPYRIGHTS files for
# more information about the licensing of this file.

""" LTI grade delivery workers.

Each user x course x task passback is a persistent delivery with its own
status, attempt counter, next attempt time and error classification.
Deliveries are leased atomically from the database, so multiple workers never
send the same delivery at the same time, and deliveries that were "processing"
when the process died are recovered after the lease expires.
"""

import logging
import os
import queue
import socket
import threading
import uuid
from abc import ABCMeta, abstractmethod
from datetime import timedelta

from bson import ObjectId
from bson.errors import InvalidId

from inginious.frontend.models.lti_delivery import utcnow
from inginious.frontend.lti.delivery import (
    MongoDeliveryRepository,
    DeliveryError, LocalContextError, RemoteTemporaryError, RemoteRejectedError,
    CATEGORY_LOCAL_CONTEXT, CATEGORY_REMOTE_TEMPORARY, CATEGORY_REMOTE_REJECTED,
    MAX_ATTEMPTS, POLL_INTERVAL, SEND_DEADLINE, NEW_DELIVERY_GRACE, backoff_delay)


_INTERNAL_ERROR_SUMMARY = "internal error during delivery"


def _default_worker_id():
    return "{}:{}:{}".format(socket.gethostname(), os.getpid(), uuid.uuid4().hex[:8])


class LTIScorePublisher(threading.Thread, metaclass=ABCMeta):
    """ Base class for the LTI 1.1 / 1.3 score publishers. """
    _submission_tags = {}

    def __init__(self, mongo_document, user_manager, repository=None, clock=None,
                 worker_id=None, send_runner=None, autostart=True):
        super(LTIScorePublisher, self).__init__()
        self.daemon = True
        self._mongo_document = mongo_document
        self._user_manager = user_manager
        self._logger = getattr(self, "_logger", None) or logging.getLogger("inginious.webapp.lti")

        self._repo = repository if repository is not None else MongoDeliveryRepository(mongo_document)
        self._clock = clock or utcnow
        self._worker_id = worker_id or _default_worker_id()
        self._send_runner = send_runner or self._deadline_send_runner
        self._stop_event = threading.Event()

        if repository is None:
            # Self-heal legacy documents and make sure indexes exist (additive
            # change, no DB_VERSION bump). Idempotent and safe with several
            # processes starting concurrently.
            now = self._clock()
            self._repo.migrate_legacy(now)
            self._repo.ensure_indexes()

        if autostart:
            self.start()

    # -- lifecycle -------------------------------------------------------------

    def stop(self):
        self._stop_event.set()

    def run(self):
        while not self._stop_event.is_set():
            try:
                worked = self._tick()
            except Exception:
                # A tick-level failure (e.g. database briefly unreachable) must
                # never kill the worker thread: degenerate into an idle wait.
                self._logger.error("LTI score publisher tick failed.", exc_info=True)
                worked = False
            if not worked:
                self._stop_event.wait(POLL_INTERVAL)

    # -- worker core -----------------------------------------------------------

    def _tick(self):
        """ Process exactly one due delivery. Returns False when nothing was due. """
        now = self._clock()
        entry = self._claim(now)
        if entry is None:
            return False

        # A processing delivery reclaimed after a crash. If it was superseded
        # while its owner was gone, close it WITHOUT sending.
        if entry.superseded_by is not None:
            self._safe_close_superseded(entry, now)
            return True

        grade = None
        success = False
        category = None
        summary = None
        try:
            try:
                grade = self._user_manager.get_task_cache(
                    entry.username, entry.courseid, entry.taskid)["grade"]
            except Exception:
                self._logger.error("Could not read local grade for delivery %s.", entry.id, exc_info=True)
                raise LocalContextError("grade not available locally for this user/task")

            context = self.prepare(entry, grade)

            # D1: last check before hitting the network. A newer delivery may
            # have superseded this one while the local context was being read.
            if self._repo.is_superseded(entry.id):
                self._safe_close_superseded(entry, now)
                return True

            self._send_runner(self.send, context)
            success = True
        except LocalContextError as exc:
            category, summary = CATEGORY_LOCAL_CONTEXT, str(exc)
            self._logger.warning("Local context error for delivery %s: %s", entry.id, summary)
        except RemoteTemporaryError as exc:
            category, summary = CATEGORY_REMOTE_TEMPORARY, str(exc)
            self._logger.warning("Temporary remote failure for delivery %s: %s", entry.id, summary)
        except RemoteRejectedError as exc:
            category, summary = CATEGORY_REMOTE_REJECTED, str(exc)
            self._logger.error("Definitive remote rejection for delivery %s: %s", entry.id, summary)
        except DeliveryError as exc:
            category, summary = CATEGORY_REMOTE_TEMPORARY, str(exc)
            self._logger.error("Delivery error for delivery %s: %s", entry.id, summary, exc_info=True)
        except Exception:
            # Anything else: keep the delivery alive but bounded, and never
            # persist the raw exception text (it may contain sensitive data).
            category, summary = CATEGORY_REMOTE_TEMPORARY, _INTERNAL_ERROR_SUMMARY
            self._logger.error("Unexpected error while processing delivery %s.", entry.id, exc_info=True)

        self._settle(entry, now, success, grade, category, summary)
        return True

    def _claim(self, now):
        """ Lease one delivery: expired leases take priority over due pending ones. """
        entry = self._repo.reclaim_expired(now, self._worker_id)
        if entry is not None:
            return entry

        for candidate in self._repo.iter_due_pending(now):
            # Ordering barrier: a delivery for the same user/course/task that
            # is currently being (or was) sent must finish first, so a newer
            # grade can never be overwritten by a late older send.
            if self._repo.has_processing_same_key(candidate.username, candidate.courseid, candidate.taskid):
                continue
            entry = self._repo.claim_pending(candidate.id, now, self._worker_id)
            if entry is not None:
                return entry
        return None

    def _settle(self, entry, now, success, grade, category, summary):
        """ Persist the outcome of one leased delivery. Never raises out. """
        try:
            if success:
                if not self._repo.settle_success(entry.id, self._worker_id, now, grade):
                    # Superseded (or lease lost) between the send and the confirm:
                    # discard the remote conclusion locally.
                    self._repo.close_superseded(entry.id, self._worker_id, now)
                else:
                    self._logger.debug("Successfully sent grade to TC: %s", entry.id)
                return

            if self._repo.is_superseded(entry.id):
                self._repo.close_superseded(entry.id, self._worker_id, now)
            elif category == CATEGORY_REMOTE_REJECTED or entry.attempts >= MAX_ATTEMPTS:
                # A definitive rejection is terminal even before the attempt cap;
                # other failures become terminal once the bounded budget is spent.
                if not self._repo.settle_failed(entry.id, self._worker_id, now, category, summary):
                    self._repo.close_superseded(entry.id, self._worker_id, now)
                else:
                    self._logger.error(
                        "Delivery %s terminally failed after %d attempt(s) [%s]: %s",
                        entry.id, entry.attempts, category, summary)
            else:
                next_attempt_at = now + timedelta(seconds=backoff_delay(entry.attempts))
                if not self._repo.settle_retry(entry.id, self._worker_id, now, category, summary, next_attempt_at):
                    self._repo.close_superseded(entry.id, self._worker_id, now)
                else:
                    self._logger.debug(
                        "Delivery %s failed [%s], retry #%d scheduled at %s.",
                        entry.id, category, entry.attempts + 1, next_attempt_at)
        except Exception:
            # The delivery stays "processing" and is reclaimed after lease expiry.
            self._logger.error("Could not settle delivery %s; it will be reclaimed after the lease expires.",
                               entry.id, exc_info=True)

    def _safe_close_superseded(self, entry, now):
        try:
            self._repo.close_superseded(entry.id, self._worker_id, now)
        except Exception:
            self._logger.error("Could not close superseded delivery %s.", entry.id, exc_info=True)

    def _deadline_send_runner(self, send, context):
        """ Run send(context) in a daemon thread, giving up after SEND_DEADLINE.

        lti 0.9.5 issues its HTTP request without a timeout; after the deadline
        this worker no longer waits for (and will never write a conclusion
        based on) that call. The lease TTL is longer than the deadline, so no
        other owner can act before that.
        """
        result = queue.Queue(maxsize=1)

        def runner():
            try:
                send(context)
            except BaseException as exc:  # re-raised in the owning thread
                result.put(exc)
            else:
                result.put(None)

        thread = threading.Thread(target=runner, name="lti-grade-send", daemon=True)
        thread.start()
        try:
            exc = result.get(timeout=SEND_DEADLINE)
        except queue.Empty:
            raise RemoteTemporaryError("send deadline exceeded, giving up before lease expiry")
        if exc is not None:
            raise exc

    # -- enqueue / teacher actions ---------------------------------------------

    def add(self, submission):
        """ Enqueue one fresh delivery per username after a graded submission. """
        for tag in self._submission_tags.keys():
            if tag not in submission:
                return

        now = self._clock()
        next_attempt_at = now + timedelta(seconds=NEW_DELIVERY_GRACE)
        for username in submission["username"]:
            fields = {"username": username,
                      "courseid": submission["courseid"],
                      "taskid": submission["taskid"]}
            fields.update({key: submission[key] for key in self._submission_tags.keys()})

            # Insert first (fresh stable identity), then supersede previous
            # active deliveries. The grace delay + the processing barrier keep
            # the remote write order correct even if we die between the two.
            entry = self._repo.insert_delivery(fields, next_attempt_at)
            self._repo.supersede_active(username, submission["courseid"], submission["taskid"],
                                        entry.id, now)

    def requeue(self, courseid, delivery_id):
        """ Teacher-requested reset of a terminally failed delivery. Returns True on success. """
        try:
            doc_id = ObjectId(str(delivery_id))
        except (InvalidId, TypeError):
            return False
        return self._repo.requeue_failed(courseid, doc_id, self._clock())

    def list_deliveries(self, courseid, status=None, taskid=None, username=None, limit=200):
        """ Deliveries of a course visible to teachers (LTI tags are never projected). """
        return self._repo.list_for_course(courseid, status, taskid, username, limit)

    def tag_submission(self, submission, lti_info):
        """
        Tags the submission with the information needed for score publishing
        :param submission: the submission dictionary
        :param lti_info: the lti session information
        """
        for submission_key, lti_info_key in self._submission_tags.items():
            if lti_info[lti_info_key] is None:
                self._logger.error(lti_info_key + " is None, but grade needs to be sent back to LTI platform! Ignoring.")
                return
            submission[submission_key] = lti_info[lti_info_key]

    # -- subclass contract ------------------------------------------------------

    @abstractmethod
    def prepare(self, entry, grade):
        """
        Read the local context needed for the send (course, key/secret, launch)
        and return an opaque context passed to :meth:`send`.
        Raises LocalContextError on failure.
        """

    @abstractmethod
    def send(self, context):
        """
        Perform the (idempotent) remote grade write.
        Raises RemoteTemporaryError or RemoteRejectedError on failure.
        """
