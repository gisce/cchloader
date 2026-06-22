# -*- coding: utf-8 -*-
from __future__ import absolute_import

from marshmallow import Schema, fields

class BaseSchema(Schema):
    """
    Base Schema to add unique_fields for TimeScale upsert purpose
    """
    unique_fields = ['name', 'utc_timestamp']
