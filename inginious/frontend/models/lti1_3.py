# -*- coding: utf-8 -*-
#
# This file is part of INGInious. See the LICENSE and the COPYRIGHTS files for
# more information about the licensing of this file.

from mongoengine import Document, StringField, ListField, DynamicField, IntField

from inginious.frontend.models.lti_delivery import LTIDelivery


class LTIGrade(LTIDelivery):
    # Deprecated: superseded by ``attempts``; kept to self-migrate legacy documents.
    nb_attempt = IntField()

    message_launch_id = StringField()

    meta = {'collection': 'lti_grade_queue'}

class LaunchData(Document):
    key = StringField(required=True)
    context = ListField(required=True)
    value = DynamicField(required=True)

    meta = {'collection': 'lti_launch'}
