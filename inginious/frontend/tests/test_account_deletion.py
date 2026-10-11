# -*- coding: utf-8 -*-
#
# This file is part of INGInious. See the LICENSE and the COPYRIGHTS files for
# more information about the licensing of this file.

""" Tests for the staged, resumable account deletion engine.

Requires a MongoDB server on localhost:27017 (as provided by the CI environment);
skipped otherwise.
"""

import datetime

import pytest
import pymongo
import flask
from gridfs import GridFS
from mongoengine import connect, connection
from bson import ObjectId

from inginious.frontend.user_manager import UserManager, AccountSealedError
from inginious.frontend.account_deletion import (
    AccountDeletionManager, AccountDeletionError, DeletionInProgressError, StageError,
)
from inginious.frontend.models import (
    User, Submission, UserTask, CourseClass, Group, Audience, Session,
    LISOutcome, LTIGrade, AccountDeletion, LTIData,
)
from inginious.frontend.models.account_deletion import (
    STATUS_SEALED, STATUS_COMPLETED, STATUS_CANCELLED,
    STATUS_FAILED_ENROLLMENTS, STATUS_FAILED_DATASTORE, STATUS_FAILED_PLUGINS,
    STAGE_ENROLLMENTS, STAGE_SUBMISSIONS, STAGE_PLUGINS,
    MODE_ERASE, MODE_ANONYMIZE, TERMINAL_STATUSES,
)

MONGO_HOST = "localhost"
MONGO_DB = "test_inginious_deletions"

COLLECTIONS = [
    "users", "courses", "groups", "audiences", "submissions", "user_tasks",
    "sessions", "lis_outcome_queue", "lti_grade_queue", "account_deletions",
    "fs.files", "fs.chunks",
]


@pytest.fixture
def db():
    try:
        client = pymongo.MongoClient(MONGO_HOST, serverSelectionTimeoutMS=2000)
        client.admin.command("ping")
    except Exception:
        pytest.skip("MongoDB not available on %s" % MONGO_HOST)

    conn = connect(db=MONGO_DB, host=MONGO_HOST, uuidRepresentation="standard")
    database = conn[MONGO_DB]
    for name in COLLECTIONS:
        database.drop_collection(name)
    yield database
    try:
        connection.disconnect()
    except Exception:
        pass


@pytest.fixture
def manager(db):
    user_manager = UserManager([])
    mgr = AccountDeletionManager(user_manager, None)
    return mgr


def make_user(username="student1", email="student1@example.com", password="password-pw",
              apikey=None):
    user = User(username=username, realname="Real " + username, email=email,
                password=UserManager.hash_password(password))
    if apikey:
        user.apikey = apikey
    user.save()
    return user


def make_submission(courseid, taskid, usernames, inputdata=None, grade=50.0,
                    username_field=None):
    sub = Submission(courseid=courseid, taskid=taskid, username=list(usernames),
                     status="done", submitted_on=datetime.datetime.now(datetime.timezone.utc),
                     grade=grade, result="success")
    sub.set_input(inputdata or {"@username": ",".join(usernames),
                                "@email": ",".join(u + "@example.com" for u in usernames)})
    sub.archive.put(b"archive-bytes")
    sub.save()
    return sub


def build_referenced_world(db):
    """ target 'student1' is referenced everywhere; teammate 'student2' must survive. """
    user1 = make_user("student1", apikey="key1")
    user2 = make_user("student2", "student2@example.com")

    CourseClass(id="courseA", students=["student1", "student2"]).save()
    CourseClass(id="courseB", students=["student1"]).save()
    Group(description="g1", courseid="courseA", size=4, students=["student1", "student2"],
          audiences=[]).save()
    Audience(description="a1", courseid="courseA", students=["student1"],
             tutors=["student1"]).save()

    make_submission("courseA", "task1", ["student1"])
    make_submission("courseA", "task2", ["student1", "student2"])
    UserTask(courseid="courseA", taskid="task1", username="student1", grade=100.0,
             tried=3, succeeded=True).save()
    UserTask(courseid="courseA", taskid="task2", username="student1", grade=50.0).save()
    UserTask(courseid="courseA", taskid="task2", username="student2", grade=75.0).save()

    Session(permanent=True, loggedin=True, username="student1", email="student1@example.com").save()
    Session(permanent=True, is_lti=True, loggedin=False,
            lti=LTIData(username="student1", email="student1@example.com")).save()

    LISOutcome(courseid="courseA", taskid="task1", username="student1", nb_attempt=0).save()
    LTIGrade(courseid="courseA", taskid="task1", username="student1", nb_attempt=0).save()
    return user1, user2


class TestSealing:
    def test_initiate_seals_credentials_and_returns_stable_record(self, db, manager):
        make_user(apikey="apikey123")
        Session(permanent=True, loggedin=True, username="student1").save()

        deletion = manager.initiate("student1", requested_by="admin")

        assert deletion.status == STATUS_SEALED
        assert deletion.username == "student1"
        assert deletion.target_user_id == User.objects(username="student1").first().id
        assert len(deletion.stages) == 5

        user = User.objects(username="student1").first()
        assert user.sealed is True
        assert user.deletion_id == deletion.id
        assert not user.password
        assert not user.apikey
        assert user.sealed_password.startswith("argon2id-")
        assert user.sealed_apikey == "apikey123"

        # existing sessions must be logged out immediately
        assert Session.objects(username="student1", loggedin=True).count() == 0

    def test_initiate_twice_refused(self, db, manager):
        make_user()
        manager.initiate("student1", requested_by="admin")
        with pytest.raises(DeletionInProgressError):
            manager.initiate("student1", requested_by="admin")

    def test_initiate_missing_user(self, db, manager):
        with pytest.raises(AccountDeletionError):
            manager.initiate("ghost", requested_by="admin")

    def test_initiate_wrong_confirmation_email(self, db, manager):
        make_user()
        with pytest.raises(AccountDeletionError):
            manager.initiate("student1", requested_by="self",
                             confirmation_email="wrong@example.com")

    def test_login_and_api_blocked_while_sealed(self, db, manager):
        make_user(password="secretpw", apikey="key1")
        manager.initiate("student1", requested_by="self")
        um = manager._user_manager

        assert um.auth_user("student1", "secretpw", do_connect=False) is None
        assert um.get_user_api_key("student1") is None
        assert um.course_register_user(None, "student1", force=True) is False
        assert um.is_user_under_deletion("student1") is True
        assert um.filter_available_usernames(["student1", "student2"]) == ["student2"]

        app = flask.Flask(__name__)
        app.secret_key = "x"
        with app.test_request_context("/"):
            with pytest.raises(AccountSealedError):
                um.connect_user(User.objects(username="student1").first())

    def test_sealed_user_stats_callback_does_not_recreate_cache(self, db, manager):
        make_user()
        manager.initiate("student1", requested_by="admin")
        # A grading completion arriving during deletion must not upsert user_tasks
        manager._user_manager.update_user_stats(
            "student1", None,
            {"courseid": "courseA", "taskid": "task1", "id": ObjectId()},
            "success", 100.0, "", True, None)
        assert UserTask.objects(username="student1").count() == 0


class TestFullErasure:
    def test_all_references_cleared_and_identity_removed(self, db, manager):
        user1, user2 = build_referenced_world(db)

        deletion = manager.process(manager.initiate("student1", "admin").id)

        assert deletion.status == STATUS_COMPLETED
        assert deletion.irreversible is True
        assert deletion.completed_at is not None
        assert User.objects(username="student1").first() is None
        assert User.objects(username="student2").first() is not None

        # enrollments / groups / audiences (students AND tutors)
        assert CourseClass.objects(id="courseA", students="student1").count() == 0
        assert CourseClass.objects(id="courseB", students="student1").count() == 0
        assert CourseClass.objects(id="courseA").first().students == ["student2"]
        assert Group.objects(students="student1").count() == 0
        assert Audience.objects(students="student1").count() == 0
        assert Audience.objects(tutors="student1").count() == 0
        assert Group.objects().first().students == ["student2"]

        # personal submission removed (document + GridFS blobs)
        assert Submission.objects(username="student1").count() == 0
        assert Submission.objects(courseid="courseA", taskid="task1").count() == 0
        # group submission survives for the teammate, identity stripped
        group_sub = Submission.objects(courseid="courseA", taskid="task2").first()
        assert group_sub is not None
        assert group_sub.username == ["student2"]
        inp = group_sub.get_input()
        assert inp["@username"] == "student2"
        assert "student1" not in inp["@email"]
        assert inp["@email"] == "student2@example.com"

        # task caches
        assert UserTask.objects(username="student1").count() == 0
        assert UserTask.objects(username="student2").count() == 1

        # sessions and LTI queues
        assert Session.objects(username="student1").count() == 0
        assert LISOutcome.objects(username="student1").count() == 0
        assert LTIGrade.objects(username="student1").count() == 0

        # no GridFS orphan from the personal submission: 2 blobs (input+archive)
        # of the teammate's shared submission remain
        assert len(list(GridFS(db).find())) == 2

    def test_unreferenced_new_user_completes_immediately(self, db, manager):
        make_user()
        deletion = manager.process(manager.initiate("student1", "self").id)
        assert deletion.status == STATUS_COMPLETED
        assert User.objects(username="student1").first() is None


class TestResumeAndIdempotency:
    def test_process_is_idempotent(self, db, manager):
        build_referenced_world(db)
        deletion = manager.process(manager.initiate("student1", "admin").id)
        again = manager.process(deletion.id)
        assert again.status == STATUS_COMPLETED
        assert again.id == deletion.id

    def test_new_process_resumes_with_same_deletion_identity(self, db):
        build_referenced_world(db)
        # first process only seals (e.g. the initiating response was lost)
        um1 = UserManager([])
        mgr1 = AccountDeletionManager(um1)
        deletion = mgr1.initiate("student1", "admin")

        # a brand new manager (as after a restart) resumes it
        um2 = UserManager([])
        mgr2 = AccountDeletionManager(um2)
        resumed = mgr2.resume_pending()
        assert resumed == 1
        finished = AccountDeletion.objects(id=deletion.id).first()
        assert finished.status == STATUS_COMPLETED
        assert User.objects(username="student1").first() is None

    def test_single_stage_progress_then_full_resume(self, db, manager):
        build_referenced_world(db)
        deletion = manager.initiate("student1", "admin")
        deletion = manager.retry(deletion.id, STAGE_ENROLLMENTS)
        assert deletion.status != STATUS_COMPLETED
        assert deletion.get_stage(STAGE_ENROLLMENTS).state == "done"
        assert deletion.irreversible is True
        assert Submission.objects(username="student1").count() == 2

        deletion = manager.retry(deletion.id)
        assert deletion.status == STATUS_COMPLETED

    def test_re_registered_same_name_user_is_never_swept(self, db, manager):
        build_referenced_world(db)
        deletion = manager.process(manager.initiate("student1", "admin").id)
        assert deletion.status == STATUS_COMPLETED

        # a different person registers the same username later
        new_user = User(username="student1", realname="New Person",
                        email="new@example.com", password=UserManager.hash_password("pw")).save()
        CourseClass(id="courseC", students=["student1"]).save()
        # resuming terminal records does nothing, and the new identity is untouched
        manager.resume_pending()
        assert User.objects(id=new_user.id).first() is not None
        assert CourseClass.objects(id="courseC").first().students == ["student1"]


class TestFailuresAreDistinctAndRetryable:
    def _fail(self, manager, stage_name, refs):
        def broken(deletion, user):
            raise StageError("simulated failure of %s" % stage_name, refs)
        setattr(manager, "_stage_" + stage_name, broken)

    def test_enrollment_failure_is_observable_and_retryable(self, db, manager):
        build_referenced_world(db)
        self._fail(manager, "enrollments", [{"type": "course", "id": "courseA"}])
        deletion = manager.process(manager.initiate("student1", "admin").id)

        assert deletion.status == STATUS_FAILED_ENROLLMENTS
        stage = deletion.get_stage(STAGE_ENROLLMENTS)
        assert stage.state == "failed"
        assert stage.failure_type == "enrollments"
        assert stage.attempts == 1
        assert stage.remaining == [{"type": "course", "id": "courseA"}]
        assert User.objects(username="student1").first() is not None  # identity kept

        del manager._stage_enrollments
        deletion = manager.retry(deletion.id)
        assert deletion.status == STATUS_COMPLETED

    def test_datastore_failure_is_distinct(self, db, manager):
        build_referenced_world(db)
        self._fail(manager, "submissions", [{"type": "submission", "username": "student1"}])
        deletion = manager.process(manager.initiate("student1", "admin").id)
        assert deletion.status == STATUS_FAILED_DATASTORE
        assert deletion.get_stage(STAGE_SUBMISSIONS).failure_type == "datastore"

        del manager._stage_submissions
        deletion = manager.retry(deletion.id, STAGE_SUBMISSIONS)
        assert deletion.status == STATUS_COMPLETED

    def test_plugin_failure_is_distinct_and_keeps_reference_list(self, db):
        um = UserManager([])

        class FailingPlugins:
            calls = 0

            def run_user_data_deletion(self, **kwargs):
                FailingPlugins.calls += 1
                if FailingPlugins.calls == 1:
                    return [{"plugin": "myplugin",
                             "remaining": ["store://student1"],
                             "error": "store unavailable"}]
                return [{"plugin": "myplugin", "remaining": [], "error": None}]

        manager = AccountDeletionManager(um, FailingPlugins())
        build_referenced_world(db)
        deletion = manager.process(manager.initiate("student1", "admin").id)
        assert deletion.status == STATUS_FAILED_PLUGINS
        stage = deletion.get_stage(STAGE_PLUGINS)
        assert stage.state == "failed"
        assert stage.remaining == [{"type": "plugin", "plugin": "myplugin",
                                    "ref": "store://student1"}]
        assert "store unavailable" in stage.last_error
        # identity record is kept while plugin data remains
        assert User.objects(username="student1").first() is not None

        deletion = manager.retry(deletion.id, STAGE_PLUGINS)
        assert deletion.status == STATUS_COMPLETED
        assert User.objects(username="student1").first() is None


    def test_reference_recreated_after_stage_blocks_identity_removal(self, db, manager):
        """ If a strong reference exists at finalization (e.g. a racing re-add),
        the identity must be kept and the proper failure state reported. """
        build_referenced_world(db)
        original_enrollments = manager._stage_enrollments

        def racy_enrollments(deletion, user):
            result = original_enrollments(deletion, user)
            # simulate a concurrent write re-attaching the user after the pull
            CourseClass.objects(id="courseB").update(push__students="student1")
            return result

        manager._stage_enrollments = racy_enrollments
        deletion = manager.process(manager.initiate("student1", "admin").id)
        assert deletion.status == STATUS_FAILED_ENROLLMENTS
        assert User.objects(username="student1").first() is not None

        del manager._stage_enrollments
        deletion = manager.retry(deletion.id)
        assert deletion.status == STATUS_COMPLETED
        assert User.objects(username="student1").first() is None


class TestCancel:
    def test_cancel_before_boundary_restores_login(self, db, manager):
        make_user(password="secretpw", apikey="key1")
        deletion = manager.initiate("student1", requested_by="self")
        restored = manager.cancel(deletion.id)
        assert restored.status == STATUS_CANCELLED

        user = User.objects(username="student1").first()
        assert user.sealed is False
        assert user.deletion_id is None
        assert user.password.startswith("argon2id-")
        assert user.apikey == "key1"
        assert manager._user_manager.auth_user("student1", "secretpw", do_connect=False) is not None

    def test_cancel_after_boundary_refused(self, db, manager):
        build_referenced_world(db)
        deletion = manager.initiate("student1", "admin")
        manager.retry(deletion.id, STAGE_ENROLLMENTS)
        with pytest.raises(AccountDeletionError):
            manager.cancel(deletion.id)
        # only possible outcome is to continue cleanup
        assert manager.retry(deletion.id).status == STATUS_COMPLETED


class TestAnonymization:
    def test_grades_kept_de_identified(self, db, manager):
        user1, user2 = build_referenced_world(db)
        deletion = manager.initiate("student1", "admin", mode=MODE_ANONYMIZE)
        deletion = manager.process(deletion.id)

        assert deletion.status == STATUS_COMPLETED
        pseudonym = deletion.pseudonym
        assert pseudonym.startswith("deleted-user-")

        # identity gone, login cannot be found
        assert User.objects(username="student1").first() is None

        # no grade record references the login identity
        assert Submission.objects(username="student1").count() == 0
        assert UserTask.objects(username="student1").count() == 0

        # grades survive under the pseudonym and cannot be reversed to a login
        anon_task = UserTask.objects(username=pseudonym).first()
        assert anon_task is not None
        assert anon_task.grade in (100.0, 50.0)
        assert User.objects(username=pseudonym).first() is None

        anon_sub = Submission.objects(username=pseudonym, courseid="courseA",
                                      taskid="task1").first()
        assert anon_sub is not None
        inp = anon_sub.get_input()
        assert "student1" not in inp["@username"]
        assert pseudonym in inp["@username"]

        # shared submission keeps teammate + pseudonym
        shared = Submission.objects(courseid="courseA", taskid="task2").first()
        assert set(shared.username) == {"student2", pseudonym}


class TestCreationCompatibility:
    def test_active_deletion_freezes_username_and_email(self, db, manager):
        make_user()
        manager.initiate("student1", "admin")
        um = manager._user_manager
        assert um.create_user({"username": "student1", "realname": "x",
                               "email": "other@example.com", "password": "pw1234"}) is not None
        assert um.create_user({"username": "other", "realname": "x",
                               "email": "student1@example.com", "password": "pw1234"}) is not None
        assert User.objects(username="student1").count() == 1

    def test_creation_works_normally_and_after_completion(self, db, manager):
        um = manager._user_manager
        assert um.create_user({"username": "newuser", "realname": "New",
                               "email": "new@example.com", "password": "pw1234"}) is None
        assert User.objects(username="newuser").first() is not None

        make_user()
        manager.process(manager.initiate("student1", "admin").id)
        assert um.create_user({"username": "student1", "realname": "Reused",
                               "email": "student1@example.com", "password": "pw1234"}) is None
        assert User.objects(username="student1").count() == 1

    def test_normal_unregistration_still_works(self, db, manager):
        make_user()
        CourseClass(id="courseA", students=["student1"]).save()
        Group(description="g", courseid="courseA", size=2, students=["student1"]).save()
        Audience(description="a", courseid="courseA", students=["student1"], tutors=[]).save()

        manager._user_manager.course_unregister_user("courseA", "student1")
        assert CourseClass.objects(id="courseA").first().students == []
        assert Group.objects().first().students == []
        assert Audience.objects().first().students == []

    def test_user_manager_delete_user_runs_full_flow_outside_request(self, db):
        make_user(apikey="k")
        result = UserManager([]).delete_user("student1")
        assert result is not None
        assert result.status == STATUS_COMPLETED
        assert User.objects(username="student1").first() is None
