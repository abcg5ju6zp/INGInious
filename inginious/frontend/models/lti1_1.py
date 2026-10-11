# -*- coding: utf-8 -*-
#
# This file is part of INGInious. See the LICENSE and the COPYRIGHTS files for
# more information about the licensing of this file.

from mongoengine import Document, StringField, DateTimeField, IntField

from inginious.frontend.models.lti_delivery import LTIDelivery


class Nonce(Document):
    timestamp = DateTimeField()
    nonce = StringField()
    expiration = DateTimeField()

    meta = {
        'collection': 'nonce',
        'indexes': [
            ('timestamp', 'nonce'),
            {
                'fields': ['expiration'],
                'expireAfterSeconds': 0 # use field value
            }
        ]
    }

class LISOutcome(LTIDelivery):
    # Deprecated: superseded by ``attempts``; kept to self-migrate legacy documents.
    nb_attempt = IntField()

    outcome_consumer_key = StringField()
    outcome_result_id = StringField()
    outcome_service_url = StringField()

    meta = {'collection': 'lis_outcome_queue'}
