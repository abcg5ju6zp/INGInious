# -*- coding: utf-8 -*-
#
# This file is part of INGInious. See the LICENSE and the COPYRIGHTS files for
# more information about the licensing of this file.

from datetime import datetime
import logging
import hashlib

import requests
from pylti1p3.contrib.flask import FlaskMessageLaunch
from pylti1p3.grade import Grade
from pylti1p3.launch_data_storage.base import LaunchDataStorage
from pylti1p3.tool_config import ToolConfDict
from pylti1p3.exception import LtiServiceException

from inginious.frontend.lti import LTIScorePublisher
from inginious.frontend.courses import Course
from inginious.frontend.lti.delivery import LocalContextError, RemoteTemporaryError, RemoteRejectedError
from inginious.frontend.models import LTIGrade, LaunchData


def lti_keyset_hash(issuer: str, client_id: str) -> str:
    return hashlib.md5((issuer + client_id).encode('utf-8')).digest().hex()

def lti_tool(course_config, global_config) -> ToolConfDict:
    """ LTI Tool object. """

    # Merge course with global config
    lti_config = course_config.copy()
    for iss, cfgs in global_config.items():
        lti_config.setdefault(iss, [])
        lti_config[iss] += cfgs

    lti_tool = ToolConfDict(lti_config)
    for iss in lti_config:
        for client_config in lti_config[iss]:
            lti_tool.set_private_key(iss, client_config['private_key'], client_id=client_config['client_id'])
            lti_tool.set_public_key(iss, client_config['public_key'], client_id=client_config['client_id'])
    return lti_tool


class MongoLTILaunchDataStorage(LaunchDataStorage):
    """
    Stores LTI Launch messages in database during the handshake process and
    to submit grades later using the LTIGradeManager.
    """
    _session_cookie_name = None

    def can_set_keys_expiration(self) -> bool:
        return False  # TODO(mp): I think it's reasonable to clean LTI Launch messages further than a week away tho

    def get_value(self, key: str):
        entry = LaunchData.objects(key=key).first()
        return entry.value if entry else None

    def set_value(self, key: str, value, exp) -> None:
        LaunchData.objects(key=key).update(key=key, value=value, upsert=True)

    def check_value(self, key: str) -> bool:
        return bool(LaunchData.objects(key=key).first())


class LTIGradeManager(LTIScorePublisher):
    _submission_tags = {"message_launch_id": "message_launch_id"}

    def __init__(self, user_manager, global_config):
        self._logger = logging.getLogger("inginious.webapp.lti1_3.grade_manager")
        self._global_config = global_config
        super(LTIGradeManager, self).__init__(LTIGrade, user_manager)

    def prepare(self, entry, grade):
        try:
            course = Course.get(entry.courseid)
        except Exception:
            self._logger.error("Could not load course %s for LTI 1.3 delivery.", entry.courseid, exc_info=True)
            raise LocalContextError("course could not be loaded")

        try:
            tool_config = lti_tool(course.lti_config(), self._global_config)
            message_launch = FlaskMessageLaunch.from_cache(
                entry.message_launch_id, request=None, tool_config=tool_config,
                launch_data_storage=MongoLTILaunchDataStorage())
            launch_data = message_launch.get_launch_data()
            ags = message_launch.get_ags()
        except Exception:
            self._logger.error("Could not restore LTI 1.3 launch context for delivery %s.",
                               entry.id, exc_info=True)
            raise LocalContextError("LTI 1.3 launch context could not be restored")

        return {"ags": ags, "sub": launch_data['sub'], "grade": grade}

    def send(self, context):
        ags = context["ags"]
        if not ags.can_put_grade():
            # This launch never granted the grade-write capability; retrying
            # the same launch cannot change that.
            raise RemoteRejectedError("platform did not grant grade-write capability for this launch")

        grade = Grade()
        grade.set_score_given(context["grade"]) \
            .set_score_maximum(100.0) \
            .set_timestamp(datetime.now().astimezone().isoformat()) \
            .set_activity_progress('Completed') \
            .set_grading_progress('FullyGraded') \
            .set_user_id(context["sub"])

        try:
            ags.put_grade(grade)
        except LtiServiceException as lti_ex:
            status_code = getattr(getattr(lti_ex, "response", None), "status_code", None)
            # Never persist str(LtiServiceException): it contains the URL and
            # the full response body.
            if status_code == 408 or status_code == 429 or (status_code is not None and status_code >= 500):
                raise RemoteTemporaryError("platform reported a temporary failure (HTTP {})".format(status_code))
            raise RemoteRejectedError("platform rejected the grade (HTTP {})".format(status_code))
        except requests.RequestException:
            raise RemoteTemporaryError("network error while contacting the LTI platform")
