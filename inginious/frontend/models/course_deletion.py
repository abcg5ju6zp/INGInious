# -*- coding: utf-8 -*-
#
# This file is part of INGInious. See the LICENSE and the COPYRIGHTS files for
# more information about the licensing of this file.

""" Persistent state for the safe (draining-based) course deletion feature.

Two documents live in two collections:

- :class:`CourseDeletion` (``course_deletions``) is the tombstone: the authoritative
  state of the deletion state machine. Its presence alone is the write/read gate
  seen by every process (webapp, webdav). The document is only removed by the
  state machine itself, once the final ``course_deleted`` event has been published.
- :class:`CourseOperationLock` (``course_operation_locks``) serializes the three
  mutating course-wide operations: delete, archive and export.
"""

from mongoengine import Document, StringField, DateTimeField, DictField, IntField, ListField, MapField, \
    EmbeddedDocument, EmbeddedDocumentField

# Phases only move forward. Blocking sub-conditions are carried by ``blocked``
# (see BLOCKED_*) so a blocked course stays in its phase and never reopens.
PHASE_REQUESTED = "requested"
PHASE_DRAINING = "draining"
PHASE_WIPING_DB = "wiping_db"
PHASE_DELETING_FS = "deleting_fs"
PHASE_FINALIZING = "finalizing"

PHASES = [PHASE_REQUESTED, PHASE_DRAINING, PHASE_WIPING_DB, PHASE_DELETING_FS, PHASE_FINALIZING]

# Running grading jobs did not drain in time; the admin may keep waiting.
BLOCKED_AWAITING_JOBS = "awaiting_jobs"
# A plugin refused (or failed in time) to release its references; it may be isolated.
BLOCKED_PLUGIN_REFUSED = "plugin_refused"
# The course directory could not be removed; the file finalization may be retried.
BLOCKED_FS_ERROR = "fs_error"

BLOCKED_STATES = [BLOCKED_AWAITING_JOBS, BLOCKED_PLUGIN_REFUSED, BLOCKED_FS_ERROR]

# Pin statuses
PIN_PENDING = "pending"
PIN_ACTIVE = "active"
PIN_DONE = "done"
PIN_SKIPPED = "skipped"
PIN_BLOCKED = "blocked"


class DrainPin(EmbeddedDocument):
    """ The drain conclusion of a single reference holder. """
    name = StringField(required=True)
    status = StringField(required=True, default=PIN_PENDING)
    recorded_at = DateTimeField()
    detail = DictField(default=dict)


class CourseDeletion(Document):
    """ Tombstone for a course being deleted. ``_id`` is the exact, pinned course id. """
    id = StringField(primary_key=True)
    deletion_id = StringField(required=True)  # stable identity of the deletion attempt (uuid4)
    requested_by = StringField(required=True)
    requested_at = DateTimeField(required=True)
    updated_at = DateTimeField(required=True)

    # Snapshot of course.yaml taken at request time; survives the directory removal
    # so the status page and the course_deleted hook still have something to work with.
    descriptor = DictField()
    course_name = StringField()

    phase = StringField(required=True, choices=PHASES, default=PHASE_REQUESTED)
    blocked = StringField(choices=BLOCKED_STATES)
    blocked_since = DateTimeField()
    blocked_detail = DictField()

    gate_published_at = DateTimeField()
    pins = MapField(EmbeddedDocumentField(DrainPin), default=dict)
    db_steps = ListField(StringField(), default=list)
    isolated_plugins = ListField(StringField(), default=list)
    facts = ListField(StringField(), default=list)

    fs_attempts = IntField(default=0)
    fs_last_error = StringField()

    owner_id = StringField()
    lease_until = DateTimeField()

    event_published_at = DateTimeField()

    meta = {
        "collection": "course_deletions",
        "indexes": [
            {"fields": ["deletion_id"], "unique": True},
            ("owner_id", "lease_until")
        ]
    }


class CourseOperationLock(Document):
    """ Per-course mutex between delete, archive and export operations.

    The document is never deleted: releasing a lock only expires ``lease_until``,
    which avoids ABA races when a holder dies.
    """
    id = StringField(primary_key=True)  # courseid
    op = StringField(choices=["delete", "archive", "export"])
    holder_id = StringField()
    acquired_at = DateTimeField()
    lease_until = DateTimeField()

    meta = {"collection": "course_operation_locks"}
