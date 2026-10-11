# -*- coding: utf-8 -*-
#
# This file is part of INGInious. See the LICENSE and the COPYRIGHTS files for
# more information about the licensing of this file.

from datetime import datetime, timezone
import logging
import random

from flask import  request, redirect, render_template, session, url_for

from inginious.frontend.models import Submission, UserTask, Group, Audience, CourseClass
from inginious.frontend.courses import Course
from inginious.frontend.pages.course_admin.utils import INGIniousAdminPage
from inginious.frontend.user_manager import UserManager
from inginious.common.exceptions import CourseNotArchivable, CourseOperationInProgressException


class CourseDangerZonePage(INGIniousAdminPage):
    """ Course administration page: danger zone (archive and safe deletion) """
    _logger = logging.getLogger("inginious.webapp.danger_zone")

    def dump_course(self, course):
        """
            Creates a new course (Archive course), gives it a course id resulting of the concatenation of the original id
            and the archiving date. This archive course is marked as archived and given an archive date in its YAML descriptor.
            The original course keeps their course id and all related submissions, user_tasks, audiences, courses and
            groups are updated to point to the archive course.
        """

        courseid = course.get_id()
        course_fs = course.get_fs()
        if course.is_archive():
            raise CourseNotArchivable()

        # Copy archive course
        archive_course_id = courseid + "_archive_" + datetime.now(tz=timezone.utc).strftime("%Y_%m_%d_%H_%M_%S")
        archive_course_fs = Course(archive_course_id, {"name": archive_course_id}).get_fs()
        archive_course_fs.copy_to(course_fs.prefix)

        # Update archive YAML file
        archive_course_content = course.get_descriptor()
        archive_course_content["archived"] = True
        archive_course_content["archive_date"] = datetime.now(tz=timezone.utc).isoformat()

        # Save archived course
        Course(archive_course_id, archive_course_content).save()

        # Update course id in DB
        Submission.objects(courseid=courseid).update(set__courseid=archive_course_id)
        UserTask.objects(courseid=courseid).update(set__courseid=archive_course_id)
        Group.objects(courseid=courseid).update(set__courseid=archive_course_id)
        Audience.objects(courseid=courseid).update(set__courseid=archive_course_id)
        old_course_class = CourseClass.objects(id=courseid).modify(remove=True)

        if old_course_class:
            CourseClass(id=archive_course_id, students=old_course_class.students).save()

        self._logger.info("Course %s archived as %s.", courseid, archive_course_id)
        return courseid, archive_course_id

    def GET_AUTH(self, courseid):  # pylint: disable=arguments-differ
        """ GET request """
        course, __ = self.get_course_and_check_rights(courseid, allow_all_staff=False)
        return self.page(course)

    def POST_AUTH(self, courseid):  # pylint: disable=arguments-differ
        """ POST request """
        course, __ = self.get_course_and_check_rights(courseid, allow_all_staff=False)

        msg = ""
        error = False
        data = request.form

        if not data.get("token", "") == session.token:
            msg = _("Operation aborted due to invalid token.")
            error = True
            return self.page(course, msg, error)

        deletion = self.course_deletion_manager.get_deletion(courseid)

        # A deletion is already in progress: the same request can only query or
        # drive it forward, never start a second cleanup (nor archive anything).
        if deletion is not None:
            action = data.get("deletion_action", "")
            if action == "continue_waiting":
                self.course_deletion_manager.continue_waiting(courseid)
            elif action == "isolate_plugin":
                self.course_deletion_manager.isolate_plugin(courseid, data.get("plugin", ""))
            elif action == "retry_fs":
                self.course_deletion_manager.retry_fs(courseid)
            self.course_deletion_manager.run_now(courseid)
            if self.course_deletion_manager.get_deletion(courseid) is None:
                return redirect(url_for("indexpage"))
            return self.page(course)

        if "wipeall" in data:
            if not data.get("courseid", "") == courseid:
                msg = _("Wrong course id.")
                error = True
            else:
                try:
                    self.require_course_writable(courseid)
                    with self.course_deletion_manager.operation_lock(courseid, "archive",
                                                                     lease_seconds=1800, heartbeat=30):
                        courseid, archive_course_id = self.dump_course(course)
                    msg = _("Course archived as : ") + archive_course_id
                except CourseOperationInProgressException as ex:
                    msg = str(ex)
                    error = True
                except Exception as ex:
                    msg = _("An error occurred while dumping course from database: {}").format(repr(ex))
                    error = True
        elif "deleteall" in data:
            if not data.get("courseid", "") == courseid:
                msg = _("Wrong course id.")
                error = True
            else:
                try:
                    tombstone, __ = self.course_deletion_manager.request_deletion(course, session.username)
                    # Fast path: a course without active references is fully deleted
                    # within this request, preserving the previous UX. Otherwise the
                    # background coordinator takes over and the status page tracks it.
                    finalized = self.course_deletion_manager.run_now(courseid)
                    if finalized:
                        return redirect(url_for("indexpage"))
                    msg = _("Course deletion started. It will finish in the background; see status below.")
                except CourseOperationInProgressException as ex:
                    msg = str(ex)
                    error = True
                except Exception as ex:
                    msg = _("An error occurred while deleting the course data: {}").format(repr(ex))
                    error = True

        return self.page(course, msg, error)

    def page(self, course, msg="", error=False):
        """ Get all data and display the page """
        thehash = UserManager.hash_password_sha512(str(random.getrandbits(256)))
        session.token = thehash

        deletion = self.course_deletion_manager.get_deletion(course.get_id())
        deletion_view = None
        if deletion is not None:
            deletion_view = self._build_deletion_view(deletion)

        return render_template("course_admin/danger_zone.html", course=course, thehash=thehash,
                               msg=msg, error=error, deletion=deletion_view)

    def _build_deletion_view(self, deletion):
        """ Serializes a tombstone into a compact dict for the status panel. """
        pins = {}
        for name, pin in (deletion.pins or {}).items():
            pins[name] = {"status": pin.status, "detail": pin.detail}
        return {
            "deletion_id": deletion.deletion_id,
            "phase": deletion.phase,
            "blocked": deletion.blocked,
            "blocked_detail": deletion.blocked_detail or {},
            "requested_by": deletion.requested_by,
            "requested_at": deletion.requested_at,
            "updated_at": deletion.updated_at,
            "pins": pins,
            "db_steps": deletion.db_steps or [],
            "isolated_plugins": deletion.isolated_plugins or [],
            "facts": deletion.facts or [],
            "fs_attempts": deletion.fs_attempts or 0,
            "fs_last_error": deletion.fs_last_error,
            "refused_plugins": sorted(
                (deletion.blocked_detail or {}).get("plugins", [])) if deletion.blocked == "plugin_refused" else []
        }
