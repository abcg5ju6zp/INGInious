# -*- coding: utf-8 -*-
#
# This file is part of INGInious. See the LICENSE and the COPYRIGHTS files for
# more information about the licensing of this file.

""" Lightweight write gate and operation locks for draining-based course deletion.

Kept in its own module (and free of any import of the manager, courses or
plugins) so that low-level callers such as the user manager, the LTI publisher
threads and the independent webdav process can consult the gate without
triggering import cycles.
"""

import threading
import time
from datetime import datetime, timezone, timedelta

from pymongo import ReturnDocument

from inginious.frontend.models.course_deletion import CourseDeletion, CourseOperationLock

# Process-local positive/negative cache of the gate: a stale positive answer is
# harmless (the tombstone never expires), a stale negative answer only lingers
# for a few seconds.
GATE_CACHE_TTL = 1.5

DELETE_LOCK_TTL = 30.0

_gate_cache = {}
_gate_cache_lock = threading.Lock()


def is_course_draining(courseid):
    """ True iff an active deletion tombstone exists for ``courseid``. """
    if courseid is None:
        return False
    now = time.monotonic()
    with _gate_cache_lock:
        cached = _gate_cache.get(courseid)
        if cached is not None and cached[1] > now:
            return cached[0]
    result = CourseDeletion.objects(id=courseid).only("id").first() is not None
    with _gate_cache_lock:
        _gate_cache[courseid] = (result, now + GATE_CACHE_TTL)
    return result


def assert_course_writable(courseid):
    """ Raise :class:`CourseDrainingException` when the course is being deleted. """
    if is_course_draining(courseid):
        from inginious.common.exceptions import CourseDrainingException
        raise CourseDrainingException(
            _("This course is being deleted. Modifications are temporarily blocked."))


def invalidate_gate_cache(courseid=None):
    with _gate_cache_lock:
        if courseid is None:
            _gate_cache.clear()
        else:
            _gate_cache.pop(courseid, None)


def _now():
    return datetime.now(tz=timezone.utc)


def acquire_operation_lock(courseid, op, holder_id, ttl_seconds, now=None, coll=None):
    """ Two-step atomic lock acquisition. Returns True on success.

    Step 1 upserts an idle lock document; step 2 conditionally takes over when
    the caller already holds the lock or the current holder's lease has expired.
    """
    coll = coll if coll is not None else CourseOperationLock._get_collection()
    now = now or _now()
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    coll.find_one_and_update(
        {"_id": courseid},
        {"$setOnInsert": {"op": None, "holder_id": None, "acquired_at": None, "lease_until": epoch}},
        upsert=True)
    doc = coll.find_one_and_update(
        {"_id": courseid,
         "$or": [{"holder_id": holder_id}, {"holder_id": None}, {"lease_until": {"$lt": now}}]},
        {"$set": {"op": op, "holder_id": holder_id, "acquired_at": now,
                  "lease_until": now + timedelta(seconds=ttl_seconds)}},
        return_document=ReturnDocument.AFTER)
    return doc is not None


def renew_operation_lock(courseid, holder_id, ttl_seconds, now=None, coll=None):
    coll = coll if coll is not None else CourseOperationLock._get_collection()
    now = now or _now()
    coll.update_one({"_id": courseid, "holder_id": holder_id},
                    {"$set": {"lease_until": now + timedelta(seconds=ttl_seconds)}})


def release_operation_lock(courseid, holder_id, now=None, coll=None):
    """ Expires the lock only if the caller still owns it (ABA-safe). """
    coll = coll if coll is not None else CourseOperationLock._get_collection()
    now = now or _now()
    coll.update_one({"_id": courseid, "holder_id": holder_id},
                    {"$set": {"lease_until": now - timedelta(seconds=1)}})
