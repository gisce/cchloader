# -*- encoding: utf-8 -*-
from marshmallow import Schema, fields
from marshmallow.validate import OneOf

tipomedida_valid = [x for x in range(1,100)]  # TODO revisar valors possibles a REE

class EPFPFSchema(Schema):
    name = fields.String(position=0, required=True)
    year = fields.String(position=1)
    month = fields.String(position=2)
    day = fields.String(position=3)
    periodo = fields.Integer(position=4)
    magnitud = fields.String(position=5, validate=OneOf(['AE','AS','F1','F2','R1','R2','R3','R4']))
    valor = fields.Integer(position=6) # kWh
    firmeza = fields.String(position=7, validate=OneOf(['F','P']))
    cierre = fields.String(position=8, validate=OneOf(['P','D','']))
    tipo_medida = fields.Integer(position=9, validate=OneOf(tipomedida_valid))  # TODO revisar format i valors possibles a REE


EPFPFSchema()
