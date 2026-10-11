# -*- coding: utf-8 -*-
#
# This file is part of INGInious. See the LICENSE and the COPYRIGHTS files for
# more information about the licensing of this file.

from datetime import datetime, timezone

from mongoengine import Document, StringField, DateTimeField, IntField, FloatField, ObjectIdField


def utcnow():
    return datetime.now(timezone.utc)


# Delivery statuses
STATUS_PENDING = "pending"
STATUS_PROCESSING = "processing"
STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED = "failed"
STATUS_SUPERSEDED = "superseded"

TERMINAL_STATUSES = (STATUS_SUCCEEDED, STATUS_FAILED, STATUS_SUPERSEDED)
ACTIVE_STATUSES = (STATUS_PENDING, STATUS_PROCESSING)


class LTIDelivery(Document):
    """
    A single LTI grade delivery (one user x course x task passback attempt).

    Subclasses must set their own collection name and keep the LTI-specific
    fields needed to actually perform the remote call (those fields are never
    exposed to the teacher UI).
    """
    meta = {
        'abstract': True,
        'indexes': [
            ('status', 'next_attempt_at'),       # due deliveries for the worker
            ('status', 'lease_until'),           # expired leases reclaim
            ('username', 'courseid', 'taskid', 'status'),  # supersession barrier
            ('courseid', 'created_at')           # teacher page
        ]
    }

    courseid = StringField()
    taskid = StringField()
    username = StringField()

    # State machine
    status = StringField(default=STATUS_PENDING)
    error_category = StringField(default=None)   # local_context / remote_temporary / remote_rejected
    error_summary = StringField(default=None, max_length=500)
    attempts = IntField(default=0)               # number of send attempts (incremented on lease)
    next_attempt_at = DateTimeField(default=None)

    # Lease
    lease_owner = StringField(default=None)
    lease_until = DateTimeField(default=None)

    # Supersession chain (id of the newer delivery of the same user/course/task)
    superseded_by = ObjectIdField(default=None)

    # Evidence
    sent_grade = FloatField(default=None)
    created_at = DateTimeField(default=utcnow)
    updated_at = DateTimeField(default=utcnow)
    finished_at = DateTimeField(default=None)
