

class AlterFieldConvertPrimitiveMutation:
    def __init__(self, updater, schema, spec_name, field_name, new_type):
        self.updater = updater
        self.schema = schema

        self.spec_name = spec_name
        self.field_name = field_name
        self.new_type = new_type

    def __repr__(self):
        return "<AlterFieldTypeConvertPrimitiveMutation>"

    def execute(self):
        update_id = str(self.schema.create_update())

        spec = self.schema.specs[self.spec_name]
        self.schema.alter_field_convert_type(self.spec_name, self.field_name, self.new_type)

        # find and update dependent calcs
        start_agg = []

        self.updater.update_for_field(self.spec_name, self.field_name, update_id, start_agg)

        self.schema.cleanup_update(update_id)
