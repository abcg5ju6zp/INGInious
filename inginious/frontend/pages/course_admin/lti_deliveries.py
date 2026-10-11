# -*- coding: utf-8 -*-
#
# This file is part of INGInious. See the LICENSE and the COPYRIGHTS files for
# more information about the licensing of this file.

""" Course administration page: LTI grade deliveries. """
import logging
import random

from flask import current_app, redirect, render_template, request, session, url_for
from werkzeug.exceptions import Forbidden, NotFound

from inginious.frontend.pages.course_admin.utils import INGIniousAdminPage
from inginious.frontend.user_manager import UserManager
from inginious.frontend.models.lti_delivery import (
    STATUS_PENDING, STATUS_PROCESSING, STATUS_SUCCEEDED, STATUS_FAILED, STATUS_SUPERSEDED)
from inginious.frontend.lti.delivery import (
    CATEGORY_LOCAL_CONTEXT, CATEGORY_REMOTE_TEMPORARY, CATEGORY_REMOTE_REJECTED)


LTI_VERSIONS = ("1.1", "1.3")

STATUS_LABELS = {
    STATUS_PENDING: ("Waiting", "secondary"),
    STATUS_PROCESSING: ("In progress", "info"),
    STATUS_SUCCEEDED: ("Succeeded", "success"),
    STATUS_FAILED: ("Failed", "danger"),
    STATUS_SUPERSEDED: ("Superseded", "secondary"),
}

CATEGORY_LABELS = {
    CATEGORY_LOCAL_CONTEXT: "Local context unavailable",
    CATEGORY_REMOTE_TEMPORARY: "Temporary platform failure",
    CATEGORY_REMOTE_REJECTED: "Definitive platform rejection",
}


class CourseLTIDeliveriesPage(INGIniousAdminPage):
    """ Lists LTI grade passback deliveries and allows requeuing failed ones. """
    _logger = logging.getLogger("inginious.webapp.course_admin.lti_deliveries")

    def GET_AUTH(self, courseid):  # pylint: disable=arguments-differ
        course, __ = self.get_course_and_check_rights(courseid, allow_all_staff=False)

        status = request.args.get("status") or ""
        taskid = (request.args.get("taskid") or "").strip()
        username = (request.args.get("username") or "").strip()

        deliveries = []
        for version in LTI_VERSIONS:
            publisher = current_app.lti_score_publishers.get(version)
            if publisher is None:
                continue
            for delivery in publisher.list_deliveries(
                    courseid,
                    status=status or None,
                    taskid=taskid or None,
                    username=username or None):
                deliveries.append((version, delivery))

        deliveries.sort(key=lambda item: item[1].created_at, reverse=True)
        deliveries = deliveries[:100]

        thehash = UserManager.hash_password_sha512(str(random.getrandbits(256)))
        session.token = thehash

        return render_template("course_admin/lti_deliveries.html", course=course,
                               deliveries=deliveries, status=status, taskid=taskid,
                               username=username, thehash=thehash,
                               status_labels=STATUS_LABELS, category_labels=CATEGORY_LABELS,
                               max_attempts=5)

    def POST_AUTH(self, courseid):  # pylint: disable=arguments-differ
        course, __ = self.get_course_and_check_rights(courseid, allow_all_staff=False)

        if request.form.get("token", "") != session.token:
            raise Forbidden(description=_("Operation aborted due to invalid token."))

        version = request.form.get("version", "")
        delivery_id = request.form.get("delivery_id", "")
        if version not in LTI_VERSIONS:
            raise NotFound(description=_("Invalid LTI version."))

        publisher = current_app.lti_score_publishers.get(version)
        if publisher is not None and publisher.requeue(courseid, delivery_id):
            self._logger.info("Delivery %s (%s) requeued for course %s.", delivery_id, version, courseid)

        return redirect(url_for("courseltideliveriespage", courseid=courseid))
