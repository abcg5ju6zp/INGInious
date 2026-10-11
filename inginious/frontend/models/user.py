# -*- coding: utf-8 -*-
#
# This file is part of INGInious. See the LICENSE and the COPYRIGHTS files for
# more information about the licensing of this file.

import tzlocal

from mongoengine import Document,  StringField, ListField, MapField, BooleanField, DynamicField, ObjectIdField


class User(Document):
    username = StringField(required=True)
    realname = StringField(required=True)
    email = StringField(required=True)
    password = StringField()
    language = StringField(required=True, default="en")
    code_indentation = StringField(choices=["2", "3", "4", "tabs"], default="4")
    bindings = MapField(ListField()) # TODO: use custom validation or refactor
    ltibindings = MapField(StringField())
    tos_accepted = BooleanField(default=False)
    apikey = StringField(default=None)
    timezone = StringField(default=lambda: tzlocal.get_localzone_name())
    pinned_courses = ListField(StringField(), default=list)
    activate = StringField()
    reset = StringField()

    # Account deletion: when sealed, login, API credentials and new submissions are
    # frozen while the staged cleanup runs. The credentials are moved aside so that
    # a cancellation (before the irreversible boundary) can restore the account.
    sealed = BooleanField(required=True, default=False)
    sealed_password = StringField()
    sealed_apikey = StringField()
    deletion_id = ObjectIdField()

    meta = {"collection": "users", "indexes": ["username", "email"]}
