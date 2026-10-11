# -*- coding: utf-8 -*-
#
# This file is part of INGInious. See the LICENSE and the COPYRIGHTS files for
# more information about the licensing of this file.

""" Manages the calls to the Tool Consumer (LTI 1.1 outcome service). """
import logging

import requests
from lti import OutcomeRequest, InvalidLTIConfigError

from inginious.frontend.lti import LTIScorePublisher
from inginious.frontend.courses import Course
from inginious.frontend.lti.delivery import LocalContextError, RemoteTemporaryError, RemoteRejectedError

from inginious.frontend.models import LISOutcome


class LTIOutcomeManager(LTIScorePublisher):
    _submission_tags = {"outcome_service_url": "outcome_service_url", "outcome_result_id": "outcome_result_id",
                        "outcome_consumer_key": "consumer_key"}

    def __init__(self, user_manager):
        self._logger = logging.getLogger("inginious.webapp.lti1_1.outcome_manager")
        super(LTIOutcomeManager, self).__init__(LISOutcome, user_manager)

    def prepare(self, entry, grade):
        try:
            course = Course.get(entry.courseid)
        except Exception:
            self._logger.error("Could not load course %s for LTI 1.1 delivery.", entry.courseid, exc_info=True)
            raise LocalContextError("course could not be loaded")

        try:
            consumer_secret = course.lti_keys()[entry.outcome_consumer_key]
        except KeyError:
            raise LocalContextError("consumer key is not configured for this course")
        except Exception:
            self._logger.error("Could not read LTI keys of course %s.", entry.courseid, exc_info=True)
            raise LocalContextError("LTI keys could not be read for this course")

        clip = lambda n, minn, maxn: min(max(n, minn), maxn)
        return {
            "consumer_key": entry.outcome_consumer_key,
            "consumer_secret": consumer_secret,
            "lis_outcome_service_url": entry.outcome_service_url,
            "lis_result_sourcedid": entry.outcome_result_id,
            "grade": clip(grade / 100.0, 0.0, 1.0)
        }

    def send(self, context):
        try:
            outcome_response = OutcomeRequest({
                "consumer_key": context["consumer_key"],
                "consumer_secret": context["consumer_secret"],
                "lis_outcome_service_url": context["lis_outcome_service_url"],
                "lis_result_sourcedid": context["lis_result_sourcedid"]
            }).post_replace_result(context["grade"])
        except InvalidLTIConfigError:
            raise LocalContextError("invalid LTI 1.1 outcome configuration")
        except requests.RequestException:
            # Connection, timeout, TLS ... never persist URLs or platform bodies.
            raise RemoteTemporaryError("network error while contacting the LTI platform")

        code_major = outcome_response.code_major
        response_code = outcome_response.response_code

        if code_major == "success":
            return
        if code_major in ("failure", "unsupported"):
            # A parsed definitive answer from the platform.
            raise RemoteRejectedError("platform rejected the grade (code_major={})".format(code_major))
        if code_major == "processing":
            raise RemoteTemporaryError("platform reported the result as still processing")

        # No parseable IMS envelope: rely on the HTTP status.
        if response_code is None or response_code == 408 or response_code == 429 or response_code >= 500:
            raise RemoteTemporaryError(
                "platform did not provide a definitive answer (HTTP {})".format(response_code))
        if 400 <= response_code < 500:
            raise RemoteRejectedError("platform rejected the grade (HTTP {})".format(response_code))
        raise RemoteTemporaryError("unparseable platform response (HTTP {})".format(response_code))
