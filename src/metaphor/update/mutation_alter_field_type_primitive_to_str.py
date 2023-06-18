

class AlterFieldTypePrimitiveToStrMutation:
    def __init__(self, updater, schema, spec_name, field_name, field_value):
        self.updater = updater
        self.schema = schema

        self.spec_name = spec_name
        self.field_name = field_name
        self.field_value = field_value

    def __repr__(self):
        return "<AlterFieldTypePrimitiveToStrMutation>"

    def execute(self):
        update_id = str(self.schema.create_update())

        spec = self.schema.specs[self.spec_name]
        self.schema.alter_field_type_to_str(self.spec_name, self.field_name)

        # find and update dependent calcs
        start_agg = []

        self.updater.update_for_field(self.spec_name, self.field_name, update_id, start_agg)

        self.schema.cleanup_update(update_id)
