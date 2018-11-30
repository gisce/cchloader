from marshmallow import fields


class CustomNumberField(fields.Number):
    def _validated(self, value):
        if isinstance(value, int):
            self.num_type = int
        elif isinstance(value, float):
            self.num_type = float
        return super(CustomNumberField, self)._validated(value)
