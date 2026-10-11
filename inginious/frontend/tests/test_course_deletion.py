# -*- coding: utf-8 -*-
#
# This file is part of INGInious. See the LICENSE and the COPYRIGHTS files for
# more information about the licensing of this file.

""" Unit tests for the draining-based course deletion state machine.

These tests never touch MongoDB: the mongoengine document classes used by
``inginious.frontend.course_deletion`` are replaced by a tiny in-memory fake,
while the real local filesystem provider drives the file-side behavior.
"""
# pylint: disable=redefined-outer-name, protected-access

import os
import shutil
import stat
import tempfile
from datetime import datetime, timezone, timedelta

import pytest

from inginious.common.filesystems import init_fs_provider
from inginious.common.filesystems.local import LocalFSProvider
from inginious.frontend import course_deletion as cd
from inginious.frontend.course_deletion import (CourseDeletionManager, job_belongs_to_course, filter_course_jobs,
                                               compute_running_deadline, lti_launch_matches, BLOCKED_FS_ERROR)
from inginious.frontend.course_deletion_gate import acquire_operation_lock, release_operation_lock
from pymongo.errors import DuplicateKeyError


# ---------------------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------------------

class TestPureHelpers:
    def test_job_belongs_to_course(self):
        assert job_belongs_to_course("test/task1", "test")
        assert job_belongs_to_course("test/a/b", "test")  # task ids may carry slashes elsewhere
        # prefix collisions and taskless jobs (info is an environment name)
        assert not job_belongs_to_course("test2/x", "test")
        assert not job_belongs_to_course("docker", "docker")
        assert not job_belongs_to_course(None, "test")
        assert not job_belongs_to_course("", "test")

    def test_filter_course_jobs(self):
        running = [("r1", True, "agent", "test/t1", "l", 100, 30),
                   ("r2", True, "agent", "test2/t1", "l", 100, 30),
                   ("r3", True, "agent", "mcq", "l", 100, 30)]
        waiting = [("w1", True, "test/t2", "l", 30), ("w2", False, "other/t2", "l", 30)]
        r, w = filter_course_jobs(running, waiting, "test")
        assert [e[0] for e in r] == ["r1"]
        assert [e[0] for e in w] == ["w1"]
        assert filter_course_jobs(None, None, "test") == ([], [])

    def test_running_deadline(self):
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        first_seen = now
        started = now - timedelta(seconds=100)
        # natural end (started + 200 timeout) + 60s grace lands at now+160, under the 1800 hard cap
        deadline = compute_running_deadline(first_seen, started, 200, now, grace=60, hard_cap=1800)
        assert deadline == started + timedelta(seconds=260)
        # hard cap wins when the task declares no timeout
        assert compute_running_deadline(first_seen, started, -1, now, hard_cap=1800) == \
            first_seen + timedelta(seconds=1800)
        # hard cap wins over a very long task timeout
        assert compute_running_deadline(first_seen, started, 999999, now, hard_cap=1800) == \
            first_seen + timedelta(seconds=1800)

    def test_lti_launch_matches(self):
        value = {"target_link_uri": "https://x/lti1.3/launch/cs101/task1?x=1", "other": 1}
        assert lti_launch_matches(value, "cs101")
        assert not lti_launch_matches(value, "cs10")
        assert not lti_launch_matches(None, "cs101")
        assert not lti_launch_matches({"v": object()}, "cs101")  # non-serializable -> no false match


# ---------------------------------------------------------------------------------------
# Two-step operation lock against an in-memory pymongo-like collection
# ---------------------------------------------------------------------------------------

class TestOperationLock:
    def test_acquire_renew_takeover(self):
        coll = FakeCollection({}, "course_operation_locks", [])
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        assert acquire_operation_lock("c", "delete", "A", 10, now=now, coll=coll)
        # another live holder is rejected
        assert not acquire_operation_lock("c", "archive", "B", 10, now=now + timedelta(seconds=5), coll=coll)
        # the same holder can re-enter
        assert acquire_operation_lock("c", "delete", "A", 10, now=now + timedelta(seconds=5), coll=coll)
        # once the lease expires, takeover is allowed
        assert acquire_operation_lock("c", "archive", "B", 10, now=now + timedelta(seconds=20), coll=coll)
        assert coll.find_one({"_id": "c"})["holder_id"] == "B"
        # a stale release by the previous holder must not affect the new one
        release_operation_lock("c", "A", now=now + timedelta(seconds=21), coll=coll)
        assert coll.find_one({"_id": "c"})["holder_id"] == "B"


# ---------------------------------------------------------------------------------------
# End-to-end state machine on a fake database and a real local FS
# ---------------------------------------------------------------------------------------

class FakeBlob:
    def __init__(self, coll, sub_id, name):
        self._coll, self._sub_id, self._name = coll, sub_id, name

    def delete(self):
        self._coll.log.append("submissions.blob.{}:{}".format(self._name, self._sub_id))


class FakeDoc(dict):
    def __init__(self, coll, doc):
        super().__init__(doc)
        object.__setattr__(self, "_coll", coll)

    def save(self, force_insert=False):
        existing = self._coll.store.get(self["_id"])
        if force_insert and existing is not None:
            raise DuplicateKeyError("dup")
        self._coll.store[self["_id"]] = self
        return self

    def delete(self):
        self._coll.log.append("{}:delete".format(self._coll.name))
        self._coll.store.pop(self["_id"], None)

    @property
    def input(self):
        return FakeBlob(self._coll, self["_id"], "input")

    @property
    def archive(self):
        return FakeBlob(self._coll, self["_id"], "archive")

    @property
    def value(self):
        return self.get("value")


class FakeCollection:
    def __init__(self, store, name, log):
        self.store = store
        self.name = name
        self.log = log
        self.deleted = []

    # -- helpers ---------------------------------------------------------------
    def _matches(self, doc, filt):
        if filt is None:
            return True
        for key, expected in filt.items():
            if key == "$or":
                if not any(self._matches(doc, alt) for alt in expected):
                    return False
            elif key == "$and":
                if not all(self._matches(doc, alt) for alt in expected):
                    return False
            elif key.startswith("$"):
                raise NotImplementedError(key)
            elif "." in key:
                nxt = doc
                parts = key.split(".")
                ok = True
                for part in parts[:-1]:
                    nxt = nxt.get(part) if isinstance(nxt, dict) else None
                leaf = parts[-1]
                if isinstance(expected, dict) and "$exists" in expected:
                    present = isinstance(nxt, dict) and leaf in nxt
                    ok = present == bool(expected["$exists"])
                else:
                    present = isinstance(nxt, dict) and leaf in nxt
                    value = nxt.get(leaf) if present else None
                    if isinstance(value, list):
                        ok = expected in value
                    else:
                        ok = value == expected
                if not ok:
                    return False
            else:
                if expected == {"$exists": True}:
                    if key not in doc:
                        return False
                elif isinstance(expected, dict) and "$lt" in expected:
                    if doc.get(key) is None or not doc.get(key) < expected["$lt"]:
                        return False
                else:
                    value = doc.get(key)
                    if isinstance(value, list):
                        if expected not in value:
                            return False
                    elif value != expected:
                        return False
        return True

    def _apply(self, doc, upd):
        for k, v in upd.get("$set", {}).items():
            self._assign(doc, k, v)
        for k in upd.get("$unset", {}):
            self._unassign(doc, k)
        for k, v in upd.get("$pull", {}).items():
            cur = self._walk(doc, k)
            if isinstance(cur, list) and v in cur:
                cur.remove(v)
        for k, v in upd.get("$addToSet", {}).items():
            cur = self._walk(doc, k)
            value = v["$each"] if isinstance(v, dict) and "$each" in v else v
            if cur is None:
                cur = []
                self._assign(doc, k, cur)
            if value not in cur:
                cur.append(value)

    @staticmethod
    def _walk(doc, dotted):
        cur = doc
        for part in dotted.split("."):
            if not isinstance(cur, dict) or part not in cur:
                return None
            cur = cur[part]
        return cur

    @classmethod
    def _assign(cls, doc, dotted, value):
        parts = dotted.split(".")
        cur = doc
        for part in parts[:-1]:
            cur = cur.setdefault(part, {})
        cur[parts[-1]] = value

    @classmethod
    def _unassign(cls, doc, dotted):
        parts = dotted.split(".")
        cur = doc
        for part in parts[:-1]:
            if not isinstance(cur, dict) or part not in cur:
                return
            cur = cur[part]
        cur.pop(parts[-1], None)

    # -- pymongo-ish API --------------------------------------------------------
    def find_one(self, filt=None):
        return next((d for d in list(self.store.values()) if self._matches(d, filt)), None)

    def find(self, filt=None, projection=None):
        return [d for d in list(self.store.values()) if self._matches(d, filt)]

    def find_one_and_update(self, filt, update, upsert=False, return_document=None):
        doc = self.find_one(filt)
        created = False
        if doc is None and upsert:
            doc = {k: v for k, v in filt.items() if not k.startswith("$")}
            self._apply(doc, {"$set": update.get("$setOnInsert", {})})
            self.store[doc["_id"]] = doc
            created = True
        if doc is None:
            return None
        self._apply(doc, update)
        self.log.append("{}:find_and_modify".format(self.name))
        return doc if (return_document or created) else doc

    def update_one(self, filt, update):
        doc = self.find_one(filt)
        if doc is not None:
            self._apply(doc, update)
            self.log.append("{}:update_one".format(self.name))
        return type("R", (), {"modified_count": 1 if doc else 0})()

    def update_many(self, filt, update):
        matches = [d for d in list(self.store.values()) if self._matches(d, filt)]
        for doc in matches:
            self._apply(doc, update)
        self.log.append("{}:update_many({})".format(self.name, len(matches)))
        return type("R", (), {"modified_count": len(matches)})()

    def count_documents(self, filt=None):
        return len([d for d in self.store.values() if self._matches(d, filt)])

    def delete_one(self, filt):
        doc = self.find_one(filt)
        if doc is not None:
            self.deleted.append(dict(doc))
            self.store.pop(doc["_id"], None)
            self.log.append("{}:delete_one".format(self.name))
        return type("R", (), {"deleted_count": 1 if doc else 0})()

    def delete_many(self, filt=None):
        doomed = [d for d in list(self.store.values()) if self._matches(d, filt)]
        for doc in doomed:
            self.deleted.append(dict(doc))
            self.store.pop(doc["_id"], None)
        self.log.append("{}:delete_many({})".format(self.name, len(doomed)))
        return type("R", (), {"deleted_count": len(doomed)})()


class FakeQS:
    def __init__(self, model, filters):
        self._model = model
        self._filters = {"_id" if k == "id" else k: v for k, v in filters.items()}

    def _docs(self):
        return [d for d in list(self._model.coll.store.values()) if self._model.coll._matches(d, self._filters)]

    def only(self, *_):
        return self

    def first(self):
        docs = self._docs()
        return docs[0] if docs else None

    def count(self):
        return len(self._docs())

    def __iter__(self):
        return iter(self._docs())

    def delete(self):
        for doc in self._docs():
            doc.delete()

    def update(self, **kwargs):
        update = {"$set": {}, "$unset": {}}
        for key, value in kwargs.items():
            if key.startswith("unset__"):
                update["$unset"][key[len("unset__"):]] = ""
            else:
                update["$set"][key] = value
        self._model.coll.update_many(self._filters, update)


class FakeModel:
    def __init__(self, store, log, name):
        self.coll = FakeCollection(store.setdefault(name, {}), name, log)

    def objects(self, **filters):
        return FakeQS(self, filters)

    def _get_collection(self):
        return self.coll

    def __call__(self, **kwargs):
        if "id" in kwargs:
            kwargs["_id"] = kwargs.pop("id")
        return FakeDoc(self.coll, kwargs)


class FakePluginManager:
    def __init__(self):
        self.drain_answer = []
        self.deleted_events = []

    def call_hook(self, name, **kwargs):
        if name == "course_deletion_drain":
            return list(self.drain_answer)
        if name == "course_deleted":
            self.deleted_events.append(kwargs)
        return []


class FakeClient:
    def __init__(self, running=None, waiting=None):
        self._snapshot = (running or [], waiting or [])
        self.killed = []

    def get_job_queue_snapshot(self):
        return self._snapshot

    def kill_job(self, job_id):
        self.killed.append(job_id)


class FakeCourse:
    def __init__(self, courseid="cs101"):
        self._id = courseid

    def get_id(self):
        return self._id

    def get_descriptor(self):
        return {"name": "Test course"}


@pytest.fixture()
def ctx(monkeypatch):
    tmp = tempfile.mkdtemp()
    init_fs_provider(LocalFSProvider(tmp))
    store = {}
    log = []
    models = {}
    for name in ["course_deletions", "course_operation_locks", "submissions", "courses", "audiences", "groups",
                 "user_tasks", "lis_outcome_queue", "lti_grade_queue", "lti_launch", "sessions", "users"]:
        models[name] = FakeModel(store, log, name)
    by_attr = {"CourseDeletion": "course_deletions",
               "Submission": "submissions", "CourseClass": "courses", "Audience": "audiences", "Group": "groups",
               "UserTask": "user_tasks", "LISOutcome": "lis_outcome_queue", "LTIGrade": "lti_grade_queue",
               "LaunchData": "lti_launch", "Session": "sessions", "User": "users"}
    for attr, cname in by_attr.items():
        monkeypatch.setattr(cd, attr, models[cname])
    # the gate module holds its own CourseDeletion reference
    import inginious.frontend.course_deletion_gate as cg
    monkeypatch.setattr(cg, "CourseDeletion", models["course_deletions"])
    monkeypatch.setattr(cg, "CourseOperationLock", models["course_operation_locks"])
    plugins = FakePluginManager()
    monkeypatch.setattr(cd, "plugin_manager", plugins)
    cd.invalidate_gate_cache()

    yield {"tmp": tmp, "store": store, "log": log, "models": models, "plugins": plugins}

    cd.invalidate_gate_cache()
    shutil.rmtree(tmp, ignore_errors=True)


def new_manager(client=None, **config):
    cfg = {"tick_interval": 0.01, "lease_seconds": 5.0, "settle_seconds": 0.0, "running_hard_cap": 10.0,
           "plugin_total_budget": 2.0}
    cfg.update(config)
    return CourseDeletionManager(client=client or FakeClient(), config=cfg)


def seed_refs(ctx, courseid="cs101", with_dir=True, with_data=True):
    if with_dir:
        os.mkdir(os.path.join(ctx["tmp"], courseid))
        with open(os.path.join(ctx["tmp"], courseid, "course.yaml"), "w") as f:
            f.write("name: Test\n")
    if not with_data:
        return
    s = ctx["models"]["submissions"]
    for sid in ["s1", "s2"]:
        doc = s(_id=sid, courseid=courseid, taskid="t1", username=["u"], status="done")
        doc.save()
    ctx["models"]["courses"](_id=courseid, students=["u"]).save()
    ctx["models"]["audiences"](_id="a1", courseid=courseid).save()
    ctx["models"]["groups"](_id="g1", courseid=courseid).save()
    ctx["models"]["user_tasks"](_id="u1", courseid=courseid).save()
    ctx["models"]["lis_outcome_queue"](_id="o1", courseid=courseid).save()
    ctx["models"]["lti_grade_queue"](_id="l1", courseid=courseid).save()
    ctx["models"]["sessions"](_id="se1", lti={"task": [courseid, "t1"]}).save()
    ctx["models"]["users"](_id="user1", pinned_courses=[courseid, "other"],
                          ltibindings={courseid + "/cons": "v", "other/k": "v2"}).save()


class TestStateMachine:
    def test_fast_path_deletes_everything(self, ctx):
        seed_refs(ctx)
        manager = new_manager()
        tomb, created = manager.request_deletion(FakeCourse(), "admin")
        assert created
        assert manager.run_now("cs101", max_seconds=5.0)

        # tombstone and all course-scoped DB records are gone
        assert ctx["store"]["course_deletions"] == {}
        assert ctx["store"]["submissions"] == {}
        assert ctx["store"]["audiences"] == {}
        assert ctx["store"]["groups"] == {}
        assert ctx["store"]["user_tasks"] == {}
        assert ctx["store"]["courses"] == {}  # the CourseClass document itself, not just students
        assert ctx["store"]["lis_outcome_queue"] == {}
        assert ctx["store"]["lti_grade_queue"] == {}
        assert ctx["store"]["sessions"] == {}

        # GridFS blobs removed before the submission documents
        blob_idx = [i for i, e in enumerate(ctx["log"]) if "submissions.blob" in e]
        sub_del_idx = next(i for i, e in enumerate(ctx["log"]) if e == "submissions:delete")
        assert len(blob_idx) == 4 and max(blob_idx) < sub_del_idx
        # user_tasks are only wiped after jobs/LTI went quiet: LTI cleanup precedes it
        lti_idx = ctx["log"].index("lti_grade_queue:delete")
        ut_idx = ctx["log"].index("user_tasks:delete")
        assert lti_idx < ut_idx

        # user references cleaned surgically
        user = ctx["store"]["users"]["user1"]
        assert "cs101" not in user["pinned_courses"] and "other" in user["pinned_courses"]
        assert "cs101/cons" not in user["ltibindings"] and "other/k" in user["ltibindings"]

        # directory removed and the delete lock released
        assert not os.path.exists(os.path.join(ctx["tmp"], "cs101"))
        lock = ctx["store"]["course_operation_locks"]["cs101"]
        assert lock["lease_until"] < datetime.now(tz=timezone.utc)
        # final event published exactly once
        assert len(ctx["plugins"].deleted_events) == 1
        assert ctx["plugins"].deleted_events[0]["courseid"] == "cs101"

    def test_externally_removed_directory_is_recorded(self, ctx):
        seed_refs(ctx, with_dir=False)  # directory vanished before deletion even started
        manager = new_manager()
        manager.request_deletion(FakeCourse(), "admin")
        assert manager.run_now("cs101", max_seconds=5.0)
        # the fact was carried on the removed tombstone
        captured = ctx["models"]["course_deletions"].coll.deleted[-1]
        assert "fs_externally_removed" in captured["facts"]

    def test_fs_error_blocks_and_can_retry(self, ctx):
        seed_refs(ctx)
        course_dir = os.path.join(ctx["tmp"], "cs101")
        os.chmod(course_dir, stat.S_IRUSR | stat.S_IXUSR)  # read-only: rmtree cannot unlink children
        try:
            manager = new_manager()
            manager.request_deletion(FakeCourse(), "admin")
            assert not manager.run_now("cs101", max_seconds=0.5)
            tomb = manager.get_deletion("cs101")
            assert tomb["blocked"] == BLOCKED_FS_ERROR
            assert tomb["fs_attempts"] >= 1

            # admin retries after fixing permissions
            os.chmod(course_dir, stat.S_IRWXU)
            assert manager.retry_fs("cs101")
            assert manager.run_now("cs101", max_seconds=5.0)
            assert manager.get_deletion("cs101") is None
            assert not os.path.exists(course_dir)
        finally:
            os.path.exists(course_dir) and os.chmod(course_dir, stat.S_IRWXU)

    def test_request_deletion_is_idempotent(self, ctx):
        manager = new_manager()
        t1, created1 = manager.request_deletion(FakeCourse(), "admin")
        t2, created2 = manager.request_deletion(FakeCourse(), "admin")
        assert created1 and not created2
        assert t1["deletion_id"] == t2["deletion_id"]
        assert ctx["models"]["course_deletions"].coll.count_documents() == 1

    def test_running_jobs_are_killed_and_block(self, ctx):
        seed_refs(ctx)
        now = int(datetime.now(tz=timezone.utc).timestamp())
        running = [("j1", True, "agent", "cs101/t1", "l", now, 3600)]
        client = FakeClient(running=running)
        manager = new_manager(client=client)
        manager.request_deletion(FakeCourse(), "admin")
        # soft deadline (hard cap = 10s) not reached yet: job observed, not killed
        assert not manager.run_now("cs101", max_seconds=0.2)
        assert manager.get_deletion("cs101")["phase"] == "draining"
        assert client.killed == []
        # jump first_seen into the past by rewriting the pin, then tick: killed, still blocked
        pin = manager.get_deletion("cs101")["pins"]["backend_jobs"]
        pin["detail"]["running"]["j1"]["first_seen"] -= timedelta(seconds=100)
        # hard timeout has passed but the kill grace (60s) has not: killed but still draining
        assert not manager.run_now("cs101", max_seconds=0.2)
        assert "j1" in client.killed
        tomb = manager.get_deletion("cs101")
        # DB cleanup never started while a job survives
        assert tomb["phase"] == "draining"
        assert tomb.get("db_steps") in (None, [])

    def test_plugin_refusal_can_be_isolated(self, ctx):
        seed_refs(ctx)
        ctx["plugins"].drain_answer = [{"plugin": "bad", "state": "refused", "message": "nope"}]
        manager = new_manager(plugin_total_budget=0.0)
        manager.request_deletion(FakeCourse(), "admin")
        assert not manager.run_now("cs101", max_seconds=0.5)
        tomb = manager.get_deletion("cs101")
        assert tomb["blocked"] == "plugin_refused"
        assert manager.isolate_plugin("cs101", "bad")
        assert manager.run_now("cs101", max_seconds=5.0)
        assert manager.get_deletion("cs101") is None
        captured = ctx["models"]["course_deletions"].coll.deleted[-1]
        assert "bad" in captured["isolated_plugins"]
