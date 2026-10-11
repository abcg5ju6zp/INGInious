# -*- coding: utf-8 -*-
#
# This file is part of INGInious. See the LICENSE and the COPYRIGHTS files for
# more information about the licensing of this file.

""" Staged, idempotent and resumable account deletion.

See inginious.frontend.models.account_deletion for the persisted state machine.
"""

import logging
import threading

import bson
from datetime import datetime, timezone, timedelta

from mongoengine import Q

from inginious.frontend.models import (
    AccountDeletion, DeletionStage, User, Submission, UserTask,
    CourseClass, Group, Audience, Session, LISOutcome, LTIGrade,
)
from inginious.frontend.models.account_deletion import (
    DELETION_STAGES, STAGE_ENROLLMENTS, STAGE_SUBMISSIONS, STAGE_SESSIONS, STAGE_LTI, STAGE_PLUGINS,
    STAGE_FAILURE_TYPE, FAILURE_DATASTORE,
    FAILURE_STATUS, TERMINAL_STATUSES,
    STATUS_SEALED, STATUS_CLEANING, STATUS_COMPLETED, STATUS_CANCELLED,
    MODE_ERASE, MODE_ANONYMIZE,
)


class AccountDeletionError(Exception):
    """ Base error for the account deletion subsystem. """


class AccountSealedError(AccountDeletionError):
    """ Raised when an operation targets a user whose account is sealed for deletion. """


class DeletionInProgressError(AccountDeletionError):
    """ Raised when an account deletion already exists for a user / username. """


class StageError(AccountDeletionError):
    """ A cleanup stage failed; carries the references it could not clean. """
    def __init__(self, message, remaining=None):
        super().__init__(message)
        self.remaining = remaining or []


class AccountDeletionManager:
    """ Creates, seals, processes, retries and cancels account deletions.

    Every destructive operation is idempotent and driven by the AccountDeletion
    document, so a lost HTTP response or a process restart can always resume the
    work using the same stable deletion identity.
    """

    LOCK_TIMEOUT = timedelta(minutes=10)

    def __init__(self, user_manager=None, plugin_manager=None):
        self._user_manager = user_manager
        self._plugin_manager = plugin_manager
        self._logger = logging.getLogger("inginious.webapp.deletions")

    def set_user_manager(self, user_manager):
        self._user_manager = user_manager

    def set_plugin_manager(self, plugin_manager):
        self._plugin_manager = plugin_manager

    #####################################################
    #                   Queries / guards                #
    #####################################################

    @staticmethod
    def get_deletion(deletion_id):
        try:
            return AccountDeletion.objects(id=deletion_id).first()
        except bson.errors.InvalidId:
            return None

    @staticmethod
    def get_active_deletion(username=None, user_id=None):
        """ Returns the non-terminal deletion targeting the username/user, if any. """
        query = Q(status__nin=TERMINAL_STATUSES)
        if username is not None:
            query &= Q(username=username)
        if user_id is not None:
            query &= Q(target_user_id=user_id)
        if username is None and user_id is None:
            return None
        return AccountDeletion.objects(query).order_by("-created_at").first()

    @staticmethod
    def is_username_deleting(username):
        """ True while an active (resumable) deletion freezes this username. """
        return AccountDeletion.objects(username=username, status__nin=TERMINAL_STATUSES).first() is not None

    @staticmethod
    def is_user_sealed(user):
        return user is not None and getattr(user, "sealed", False)

    #####################################################
    #               Initiation / sealing                #
    #####################################################

    def initiate(self, username, requested_by, confirmation_email=None, mode=MODE_ERASE):
        """
        Create the stable deletion record, freeze the target identity and seal
        login, API credentials and new submission capability.

        :return: the saved AccountDeletion document (status == "sealed")
        :raises AccountDeletionError when preconditions are not met
        """
        if mode not in (MODE_ERASE, MODE_ANONYMIZE):
            raise AccountDeletionError("Unknown deletion mode: %s" % mode)

        user = User.objects(username=username).first()
        if user is None:
            raise AccountDeletionError("User %s does not exist" % username)
        if user.sealed:
            existing = self.get_active_deletion(username=username, user_id=user.id)
            if existing is not None:
                raise DeletionInProgressError("A deletion is already in progress for this user")
            raise AccountDeletionError("User is sealed but no deletion record exists")
        if confirmation_email is not None and user.email != confirmation_email:
            raise AccountDeletionError("Confirmation email does not match")

        if self.get_active_deletion(username=username) is not None or \
                self.get_active_deletion(user_id=user.id) is not None:
            raise DeletionInProgressError("A deletion is already in progress for this user")

        deletion = AccountDeletion(
            target_user_id=user.id,
            username=username,
            email=user.email,
            requested_by=requested_by,
            mode=mode,
            status=STATUS_SEALED,
            stages=[DeletionStage(name=name) for name in DELETION_STAGES],
        )
        deletion.save()

        # Atomic seal: move credentials aside, freeze login / API / submissions.
        sealed_user = User.objects(id=user.id, sealed__ne=True).modify(
            set__sealed=True,
            set__deletion_id=deletion.id,
            set__sealed_password=user.password or "",
            set__sealed_apikey=user.apikey or "",
            unset__password=True,
            unset__apikey=True,
            new=True,
        )
        if sealed_user is None:
            deletion.delete()
            raise AccountDeletionError("Could not seal user %s" % username)

        # Existing sessions must no longer grant access either.
        Session.objects(Q(username=username) | Q(lti__username=username)).update(loggedin=False)

        self._logger.info("Account deletion %s initiated for user %s by %s (mode=%s)",
                          deletion.id, username, requested_by, mode)
        return deletion.reload()

    def cancel(self, deletion_id):
        """
        Cancel a deletion while it is still reversible (sealed, no stage has run).
        Login credentials are restored.
        :raises AccountDeletionError once the irreversible boundary is crossed.
        """
        deletion = self.get_deletion(deletion_id)
        if deletion is None:
            raise AccountDeletionError("Deletion record not found")
        if deletion.status == STATUS_CANCELLED:
            return deletion
        if deletion.status == STATUS_COMPLETED:
            raise AccountDeletionError("Deletion is already completed")
        if deletion.irreversible or any(s.state == "done" for s in deletion.stages):
            raise AccountDeletionError("The irreversible boundary has been crossed; "
                                       "the deletion can only continue")

        user = User.objects(id=deletion.target_user_id).first()
        if user is None:
            raise AccountDeletionError("Target user record no longer exists")

        update_kwargs = {
            "set__sealed": False,
            "unset__sealed_password": True,
            "unset__sealed_apikey": True,
            "unset__deletion_id": True,
        }
        if user.sealed_password:
            update_kwargs["set__password"] = user.sealed_password
        else:
            update_kwargs["unset__password"] = True
        if user.sealed_apikey:
            update_kwargs["set__apikey"] = user.sealed_apikey
        else:
            update_kwargs["unset__apikey"] = True
        User.objects(id=user.id, sealed=True).modify(**update_kwargs)
        deletion.status = STATUS_CANCELLED
        deletion.updated_at = datetime.now(timezone.utc)
        deletion.save()
        self._logger.info("Account deletion %s cancelled; login restored for %s",
                          deletion.id, deletion.username)
        return deletion

    #####################################################
    #                  Processing engine                #
    #####################################################

    def resume_pending(self, limit=100):
        """ Continue every non-terminal deletion. Called at startup. """
        resumed = 0
        for deletion in AccountDeletion.objects(status__nin=TERMINAL_STATUSES).limit(limit):
            try:
                self.process(deletion.id)
                resumed += 1
            except Exception:
                self._logger.exception("Failed to resume deletion %s", deletion.id)
        if resumed:
            self._logger.info("Resumed %d pending account deletion(s)", resumed)
        return resumed

    def start_resume_thread(self):
        """ Resume pending deletions in a background daemon thread (app startup). """
        thread = threading.Thread(target=self.resume_pending, daemon=True,
                                  name="account-deletion-resume")
        thread.start()
        return thread

    def retry(self, deletion_id, stage=None):
        """ Retry a deletion, optionally limiting the pass to a single stage. """
        return self.process(deletion_id, stages=[stage] if stage else None)

    def process(self, deletion_id, stages=None):
        """
        Run every non-done stage (idempotently), then finalize if all strong
        references are gone. Safe to call repeatedly and concurrently: a stale
        lock is the only one ever overridden.
        """
        deletion = AccountDeletion.objects(id=deletion_id).first() if not isinstance(
            deletion_id, AccountDeletion) else deletion_id
        if deletion is None:
            raise AccountDeletionError("Deletion record not found")
        if deletion.status in TERMINAL_STATUSES:
            return deletion

        now = datetime.now(timezone.utc)
        locked = AccountDeletion.objects(
            id=deletion.id,
            __raw__={"$or": [{"locked_at": None},
                             {"locked_at": {"$lt": now - self.LOCK_TIMEOUT}}]}
        ).modify(set__locked_at=now, set__locked_by="process", new=True)
        if locked is None:
            # Another worker is processing it right now.
            return deletion.reload()
        deletion = locked

        try:
            deletion = self._process_locked(deletion, stages)
        finally:
            AccountDeletion.objects(id=deletion.id).update(unset__locked_at=True, unset__locked_by=True)
        return deletion.reload()

    def _process_locked(self, deletion, only_stages):
        # The user document exists (sealed) until finalize; its identity must
        # still match the frozen snapshot.
        user = User.objects(id=deletion.target_user_id).first()
        if user is None:
            deletion.error = "Target user document disappeared before completion"
            deletion.status = FAILURE_STATUS[FAILURE_DATASTORE]
            deletion.save()
            return deletion
        if not user.sealed or user.deletion_id != deletion.id or user.username != deletion.username:
            deletion.error = "Target user does not match the frozen deletion identity"
            deletion.status = FAILURE_STATUS[FAILURE_DATASTORE]
            deletion.save()
            return deletion

        if not deletion.pseudonym:
            deletion.pseudonym = "deleted-user-%s" % deletion.id
            deletion.save()

        # Snapshot references before any destructive stage, and keep that snapshot
        # stable so a later retry of the plugins stage still sees the original
        # references (e.g. git repo folder names).
        submissions_stage = deletion.get_stage(STAGE_SUBMISSIONS)
        if submissions_stage is None or submissions_stage.state != "done":
            deletion.references = self._collect_references(deletion.username)
        deletion.status = STATUS_CLEANING
        deletion.error = None
        deletion.save()

        failures = []  # (stage_name, failure_type, error, remaining)
        for stage in deletion.stages:
            if stage.state == "done":
                continue
            if only_stages and stage.name not in only_stages:
                continue

            stage.mark_in_progress()
            deletion.save()
            try:
                remaining = self._run_stage(deletion, stage.name, user)
                remaining = remaining or []
                if remaining:
                    raise StageError("References to the user remain: %s" % remaining, remaining)
                stage.mark_done()
                deletion.irreversible = True
            except Exception as ex:
                failure_type = STAGE_FAILURE_TYPE[stage.name]
                remaining = ex.remaining if isinstance(ex, StageError) else stage.remaining
                stage.mark_failed(failure_type, ex, remaining)
                failures.append((stage.name, failure_type, str(ex), remaining))
                self._logger.warning("Deletion %s stage %s failed: %s",
                                     deletion.id, stage.name, ex)
            deletion.save()

        deletion = deletion.reload()
        if deletion.all_stages_done():
            return self._finalize(deletion, user)

        if failures:
            # Deterministic observable status: first failing stage in stage order.
            ordered = {name: i for i, name in enumerate(DELETION_STAGES)}
            _, failure_type, error, _ = sorted(failures, key=lambda f: ordered[f[0]])[0]
            deletion.status = FAILURE_STATUS[failure_type]
            deletion.error = error
        else:
            deletion.status = STATUS_CLEANING
        deletion.updated_at = datetime.now(timezone.utc)
        deletion.save()
        return deletion

    def _finalize(self, deletion, user):
        # Last verification: no strong reference to the login identity may remain.
        remaining = self._scan_remaining(deletion.username)
        if remaining:
            ref_stage = {
                "course": STAGE_ENROLLMENTS, "group": STAGE_ENROLLMENTS,
                "audience": STAGE_ENROLLMENTS,
                "submission": STAGE_SUBMISSIONS, "user_task": STAGE_SUBMISSIONS,
                "session": STAGE_SESSIONS,
                "lti1.1_outcome": STAGE_LTI, "lti1.3_grade": STAGE_LTI,
            }
            stage_name = ref_stage.get(remaining[0].get("type"), STAGE_SUBMISSIONS)
            failure_type = STAGE_FAILURE_TYPE[stage_name]
            deletion.get_stage(stage_name).mark_failed(
                failure_type, "Unresolved references remain before identity removal", remaining)
            deletion.status = FAILURE_STATUS[failure_type]
            deletion.error = "Unresolved references remain: %s" % remaining
            deletion.save()
            return deletion

        removed = User.objects(id=user.id, sealed=True, deletion_id=deletion.id,
                               username=deletion.username).modify(remove=True)
        if removed is None:
            deletion.status = FAILURE_STATUS[FAILURE_DATASTORE]
            deletion.error = "Could not remove the final identity record"
            deletion.save()
            return deletion

        deletion.status = STATUS_COMPLETED
        deletion.irreversible = True
        deletion.error = None
        deletion.completed_at = datetime.now(timezone.utc)
        deletion.updated_at = deletion.completed_at
        deletion.save()
        self._logger.info("Account deletion %s completed for user %s", deletion.id, deletion.username)
        return deletion

    #####################################################
    #                      Stages                        #
    #####################################################

    def _run_stage(self, deletion, stage_name, user):
        handler = {
            STAGE_ENROLLMENTS: self._stage_enrollments,
            STAGE_SUBMISSIONS: self._stage_submissions,
            STAGE_SESSIONS: self._stage_sessions,
            STAGE_LTI: self._stage_lti,
            STAGE_PLUGINS: self._stage_plugins,
        }[stage_name]
        return handler(deletion, user)

    def _stage_enrollments(self, deletion, user):
        """ Remove the user from course classes, groups and audiences (students & tutors). """
        username = deletion.username

        # Unregister from every course, including archived ones. Idempotent $pulls.
        course_ids = [c.id for c in CourseClass.objects(students=username).only("id")]
        for course_id in course_ids:
            try:
                Audience.objects(courseid=course_id, students=username).update(pull__students=username)
                Audience.objects(courseid=course_id, tutors=username).update(pull__tutors=username)
                Group.objects(courseid=course_id, students=username).update(pull__students=username)
                CourseClass.objects(id=course_id).update(pull__students=username)
            except Exception as ex:
                raise StageError("Failed to unregister from course %s: %s" % (course_id, ex),
                                 [{"type": "course", "id": course_id}])

        # Clean residual references in courses the user is not registered to.
        try:
            Audience.objects(students=username).update(pull__students=username)
            Audience.objects(tutors=username).update(pull__tutors=username)
            Group.objects(students=username).update(pull__students=username)
        except Exception as ex:
            raise StageError("Failed to remove group/audience memberships: %s" % ex,
                             [{"type": "group_or_audience", "username": username}])

        return self._enrollment_remaining(username)

    @staticmethod
    def _enrollment_remaining(username):
        remaining = []
        for course in CourseClass.objects(students=username).only("id"):
            remaining.append({"type": "course", "id": course.id})
        for group in Group.objects(students=username).only("id", "courseid"):
            remaining.append({"type": "group", "id": str(group.id), "courseid": group.courseid})
        for audience in Audience.objects(Q(students=username) | Q(tutors=username)).only(
                "id", "courseid"):
            remaining.append({"type": "audience", "id": str(audience.id), "courseid": audience.courseid})
        return remaining

    def _stage_submissions(self, deletion, user):
        """ Submissions, GridFS blobs and per-task caches.

        - erase mode: personal submissions (with their input/archive blobs) are
          deleted; for group submissions the user is pulled out, teammates keep
          their record and PII about the leaving user is stripped from the input.
        - anonymize mode: the username is replaced by a non-reversible pseudonym;
          grades are retained but can no longer be joined to a login identity.
        """
        username = deletion.username
        try:
            submission_ids = [s.id for s in Submission.objects(username=username).only("id")]
            for submission_id in submission_ids:
                self._purge_submission(
                    Submission.objects(id=submission_id).first(), deletion)

            if deletion.mode == MODE_ERASE:
                UserTask.objects(username=username).delete()
            else:
                for user_task in UserTask.objects(username=username):
                    UserTask.objects(id=user_task.id, username=username).update(
                        set__username=deletion.pseudonym)
        except StageError:
            raise
        except Exception as ex:
            raise StageError("Data store failure while deleting submissions: %s" % ex,
                             [{"type": "submission", "username": username}])

        remaining = []
        if Submission.objects(username=username).count() > 0:
            remaining.append({"type": "submission", "username": username})
        if UserTask.objects(username=username).count() > 0:
            remaining.append({"type": "user_task", "username": username})
        return remaining

    def _purge_submission(self, submission, deletion):
        if submission is None:
            return
        username = deletion.username
        if username not in submission.username:
            return

        if deletion.mode == MODE_ERASE and submission.username == [username]:
            # Personal submission: remove GridFS blobs first, then the document.
            try:
                submission.input.delete()
                submission.archive.delete()
                submission.delete()
            except Exception as ex:
                raise StageError("Failed to delete submission %s data: %s" % (submission.id, ex),
                                 [{"type": "submission", "id": str(submission.id)}])
            return

        # Shared group submission: detach the user (or pseudonymize) while keeping
        # the teammates' record. The GridFS input blob is rewritten so it no
        # longer carries the leaving user's identity.
        new_usernames = [u for u in submission.username if u != username]
        try:
            data = submission.get_input()
            data = self._rewrite_identity_fields(data, username, deletion, new_usernames)
            submission.input.delete()
            submission.set_input(data)

            if deletion.mode == MODE_ANONYMIZE:
                new_usernames = new_usernames + [deletion.pseudonym]
            submission.username = new_usernames
            submission.user_ip = None
            submission.save()
        except StageError:
            raise
        except Exception as ex:
            raise StageError("Failed to rewrite shared submission %s: %s" % (submission.id, ex),
                             [{"type": "submission", "id": str(submission.id)}])

    @staticmethod
    def _rewrite_identity_fields(data, username, deletion, remaining_usernames):
        """ Remove/pseudonymize @username and @email injected into a submission input. """
        mode = deletion.mode
        original_usernames = data.get("@username")

        # Emails are positionally aligned with usernames when group data is injected;
        # process them BEFORE @username is rewritten.
        if "@email" in data and isinstance(data["@email"], str) and \
                isinstance(original_usernames, str) and username in original_usernames.split(","):
            emails = data["@email"].split(",")
            names = original_usernames.split(",")
            if len(emails) == len(names):
                rebuilt = []
                for name, email in zip(names, emails):
                    if name == username:
                        if mode == MODE_ANONYMIZE:
                            rebuilt.append(deletion.pseudonym + "@deleted.local")
                        # erase mode: the email of the leaving user is dropped too
                    else:
                        rebuilt.append(email)
                data["@email"] = ",".join(rebuilt)
            elif mode == MODE_ANONYMIZE:
                data["@email"] = deletion.pseudonym + "@deleted.local"
            else:
                data["@email"] = ""

        if "@username" in data and isinstance(data["@username"], str):
            names = data["@username"].split(",")
            if username in names:
                if mode == MODE_ERASE:
                    data["@username"] = ",".join(u for u in names if u != username)
                else:
                    data["@username"] = ",".join(
                        deletion.pseudonym if u == username else u for u in names)
        return data

    def _stage_sessions(self, deletion, user):
        username = deletion.username
        try:
            Session.objects(Q(username=username) | Q(lti__username=username)).delete()
        except Exception as ex:
            raise StageError("Failed to delete active sessions: %s" % ex,
                             [{"type": "session", "username": username}])
        remaining = []
        if Session.objects(Q(username=username) | Q(lti__username=username)).count() > 0:
            remaining.append({"type": "session", "username": username})
        return remaining

    def _stage_lti(self, deletion, user):
        """ Remove pending LTI grade-publishing records that reference the user. """
        username = deletion.username
        try:
            LISOutcome.objects(username=username).delete()
            LTIGrade.objects(username=username).delete()
        except Exception as ex:
            raise StageError("Failed to delete LTI grade queues: %s" % ex,
                             [{"type": "lti_queue", "username": username}])
        remaining = []
        if LISOutcome.objects(username=username).count() > 0:
            remaining.append({"type": "lti1.1_outcome", "username": username})
        if LTIGrade.objects(username=username).count() > 0:
            remaining.append({"type": "lti1.3_grade", "username": username})
        return remaining

    def _stage_plugins(self, deletion, user):
        """ Let every plugin that declared user data clean it up.

        Plugin failures never abort the other plugins and are reported separately
        with the references each plugin could not clean.
        """
        if self._plugin_manager is None:
            return []

        results = self._plugin_manager.run_user_data_deletion(
            username=deletion.username,
            user_id=str(deletion.target_user_id),
            deletion_id=str(deletion.id),
            mode=deletion.mode,
            pseudonym=deletion.pseudonym,
            references=deletion.references,
        )
        remaining = []
        errors = []
        for result in results:
            if result.get("error"):
                errors.append("%s: %s" % (result["plugin"], result["error"]))
            for ref in result.get("remaining") or []:
                remaining.append({"type": "plugin", "plugin": result["plugin"], "ref": ref})
        if errors:
            raise StageError("Plugin cleanup failed: " + "; ".join(errors), remaining)
        return remaining

    #####################################################
    #               Reference inventory                 #
    #####################################################

    @staticmethod
    def _collect_references(username):
        """ Snapshot every known strong reference before stages modify anything. """
        refs = []
        for course in CourseClass.objects(students=username).only("id"):
            refs.append({"type": "course", "id": course.id})
        for group in Group.objects(students=username).only("id", "courseid"):
            refs.append({"type": "group", "id": str(group.id), "courseid": group.courseid})
        for audience in Audience.objects(Q(students=username) | Q(tutors=username)).only(
                "id", "courseid"):
            refs.append({"type": "audience", "id": str(audience.id), "courseid": audience.courseid})
        for submission in Submission.objects(username=username).only(
                "id", "courseid", "taskid", "username"):
            refs.append({"type": "submission", "id": str(submission.id),
                         "courseid": submission.courseid, "taskid": submission.taskid,
                         "usernames": list(submission.username)})
        return refs

    @staticmethod
    def _scan_remaining(username):
        """ Verify that no collection still strongly references the login identity. """
        remaining = AccountDeletionManager._enrollment_remaining(username)
        if Submission.objects(username=username).count() > 0:
            remaining.append({"type": "submission", "username": username})
        if UserTask.objects(username=username).count() > 0:
            remaining.append({"type": "user_task", "username": username})
        if Session.objects(Q(username=username) | Q(lti__username=username)).count() > 0:
            remaining.append({"type": "session", "username": username})
        if LISOutcome.objects(username=username).count() > 0:
            remaining.append({"type": "lti1.1_outcome", "username": username})
        if LTIGrade.objects(username=username).count() > 0:
            remaining.append({"type": "lti1.3_grade", "username": username})
        return remaining
