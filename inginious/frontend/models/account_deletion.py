# -*- coding: utf-8 -*-
#
# This file is part of INGInious. See the LICENSE and the COPYRIGHTS files for
# more information about the licensing of this file.

""" Model describing a staged, resumable account deletion.

A single AccountDeletion document is the stable identity of a deletion process:
it freezes the target user (its users-collection ObjectId, which is never reused),
the username at request time, the cleanup mode (erase or anonymize) and the state
of every cleanup stage. Processing is idempotent: after a lost response or a process
restart, the same document is used to continue the work, so a later re-registered
account with the same username is never mistaken for the old one.
"""

from datetime import datetime, timezone

from mongoengine import Document, StringField, ListField, BooleanField, DateTimeField, \
    IntField, ObjectIdField, EmbeddedDocument, EmbeddedDocumentListField, DynamicField


# Ordered cleanup stages. Order is significant: the first failing stage also
# determines the global failure status when several stages fail during one pass.
STAGE_ENROLLMENTS = "enrollments"
STAGE_SUBMISSIONS = "submissions"
STAGE_SESSIONS = "sessions"
STAGE_LTI = "lti"
STAGE_PLUGINS = "plugins"
DELETION_STAGES = [STAGE_ENROLLMENTS, STAGE_SUBMISSIONS, STAGE_SESSIONS, STAGE_LTI, STAGE_PLUGINS]

# Failure classes: course unregistration, data store deletion, plugin cleanup.
# Each one produces a distinct, independently retryable observable state.
FAILURE_ENROLLMENTS = "enrollments"
FAILURE_DATASTORE = "datastore"
FAILURE_PLUGINS = "plugins"

STAGE_FAILURE_TYPE = {
    STAGE_ENROLLMENTS: FAILURE_ENROLLMENTS,
    STAGE_SUBMISSIONS: FAILURE_DATASTORE,
    STAGE_SESSIONS: FAILURE_DATASTORE,
    STAGE_LTI: FAILURE_DATASTORE,
    STAGE_PLUGINS: FAILURE_PLUGINS,
}

# Global statuses
STATUS_SEALED = "sealed"            # credentials sealed, no destructive stage ran yet (cancellable)
STATUS_CLEANING = "cleaning"        # at least one stage ran; deletion can only continue
STATUS_FAILED_ENROLLMENTS = "failed_enrollments"
STATUS_FAILED_DATASTORE = "failed_datastore"
STATUS_FAILED_PLUGINS = "failed_plugins"
STATUS_COMPLETED = "completed"      # all strong references cleared/anonymized and identity removed
STATUS_CANCELLED = "cancelled"      # cancelled before the irreversible boundary, login restored

FAILURE_STATUS = {
    FAILURE_ENROLLMENTS: STATUS_FAILED_ENROLLMENTS,
    FAILURE_DATASTORE: STATUS_FAILED_DATASTORE,
    FAILURE_PLUGINS: STATUS_FAILED_PLUGINS,
}

TERMINAL_STATUSES = (STATUS_COMPLETED, STATUS_CANCELLED)

MODE_ERASE = "erase"
MODE_ANONYMIZE = "anonymize"


class DeletionStage(EmbeddedDocument):
    """ State of a single cleanup stage. """
    name = StringField(required=True, choices=DELETION_STAGES)
    state = StringField(required=True, default="pending", choices=["pending", "in_progress", "failed", "done"])
    attempts = IntField(required=True, default=0)
    failure_type = StringField()
    last_error = StringField()
    # References that still have to be cleaned (descriptors, content is stage-specific)
    remaining = ListField(DynamicField(), default=list)
    updated_at = DateTimeField()

    def mark_in_progress(self):
        self.state = "in_progress"
        self.attempts += 1
        self.updated_at = datetime.now(timezone.utc)

    def mark_done(self):
        self.state = "done"
        self.failure_type = None
        self.last_error = None
        self.remaining = []
        self.updated_at = datetime.now(timezone.utc)

    def mark_failed(self, failure_type, error, remaining=None):
        self.state = "failed"
        self.failure_type = failure_type
        self.last_error = str(error)
        self.remaining = remaining or []
        self.updated_at = datetime.now(timezone.utc)

    def to_dict(self):
        return {
            "name": self.name,
            "state": self.state,
            "attempts": self.attempts,
            "failure_type": self.failure_type,
            "last_error": self.last_error,
            "remaining": self.remaining,
            "updated_at": self.updated_at
        }


class AccountDeletion(Document):
    # Stable identity of the targeted user. ObjectIds are never reused, which protects
    # a later re-registered account with the same username.
    target_user_id = ObjectIdField(required=True)
    # Frozen snapshot of the username/email at request time.
    username = StringField(required=True)
    email = StringField()

    requested_by = StringField(required=True)  # "self" or the initiator's username
    mode = StringField(required=True, default=MODE_ERASE, choices=[MODE_ERASE, MODE_ANONYMIZE])
    # Stable pseudonym used when historical grades must be kept de-identified.
    pseudonym = StringField()

    status = StringField(required=True, default=STATUS_SEALED)
    stages = EmbeddedDocumentListField(DeletionStage, default=list)

    # Snapshot of the strong references discovered before stages run, kept stable
    # afterwards so plugin handlers can still locate data core stages removed.
    references = ListField(DynamicField(), default=list)

    # True once a destructive stage has successfully run: cancellation is no longer possible
    irreversible = BooleanField(required=True, default=False)

    # Simple per-document processing lock (with stale lock recovery)
    locked_at = DateTimeField()
    locked_by = StringField()

    error = StringField()
    created_at = DateTimeField(default=lambda: datetime.now(timezone.utc))
    updated_at = DateTimeField(default=lambda: datetime.now(timezone.utc))
    completed_at = DateTimeField()

    meta = {
        "collection": "account_deletions",
        "indexes": [
            "username",
            "status",
            "target_user_id",
        ]
    }

    def get_stage(self, name):
        for stage in self.stages:
            if stage.name == name:
                return stage
        return None

    def fresh_stages(self, names=None):
        names = names or DELETION_STAGES
        return [s for s in self.stages if s.name in names and s.state != "done"]

    def all_stages_done(self):
        return all(s.state == "done" for s in self.stages)

    def to_dict(self):
        return {
            "id": str(self.id),
            "target_user_id": str(self.target_user_id),
            "username": self.username,
            "email": self.email,
            "requested_by": self.requested_by,
            "mode": self.mode,
            "pseudonym": self.pseudonym,
            "status": self.status,
            "irreversible": self.irreversible,
            "error": self.error,
            "created_at": str(self.created_at),
            "updated_at": str(self.updated_at),
            "completed_at": str(self.completed_at) if self.completed_at else None,
            "stages": [s.to_dict() for s in self.stages]
        }
