# -*- encoding: utf-8 -*-
from cchloader.adapters import CchAdapter
from cchloader.models.epfpf import EPFPFSchema
from marshmallow import Schema, pre_load

class EPFPFBaseAdapter(Schema):
    """EPFPF Adapter
    """

    @pre_load
    def fix_cierre(self, data):
        cierre = data.get('cierre')
        if not cierre:
            data['cierre'] = ''

class EPFPFAdapter(EPFPFBaseAdapter, CchAdapter, EPFPFSchema):
    pass