# -*- coding: utf-8 -*-
from __future__ import absolute_import

from cchloader.adapters import CchAdapter
from cchloader.models.cch_autocons import CchAutoconsSchema
from marshmallow import Schema, fields, pre_load


class A5dBaseAdapter(Schema):
    """ A5D Adapter
    """

    @pre_load
    def fix_numbers(self, data):
        for attr, field in self.fields.items():
            if isinstance(field, fields.Integer):
                if not data.get(attr):
                    data[attr] = None
        return data

    @pre_load
    def fix_season(self, data):
        valid_values = [0, 1]
        season = data.get('season')
        if season and season.isdigit() and season in map(str, valid_values):
            data['season'] = int(season)
        else:
            data['season'] = None

    @pre_load
    def fix_source(self, data):
        valid_values = [1, 2, 3, 4, 5, 6]
        source = data.get('source')
        if source and source.isdigit() and int(source) in valid_values:
            data['source'] = int(source)
        else:
            data['source'] = None


class A5dAdapter(A5dBaseAdapter, CchAdapter, CchAutoconsSchema):
    pass
