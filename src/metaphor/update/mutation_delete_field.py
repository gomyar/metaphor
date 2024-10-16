

class DeleteFieldMutation:
    def __init__(self, updater, from_schema, to_schema, spec_name, field_name):
        self.updater = updater
        self.from_schema = from_schema

        self.spec_name = spec_name
        self.field_name = field_name

    def __repr__(self):
        return "<DeleteFieldMutation>"

    def execute(self):
        update_id = str(self.from_schema.create_update())

        spec = self.from_schema.specs[self.spec_name]
        self.from_schema.delete_field_value(self.spec_name, self.field_name)
        self.from_schema._do_delete_field(self.spec_name, self.field_name)

        # find and update dependent calcs
        start_agg = []

        self.updater.update_for_field(self.spec_name, self.field_name, update_id, start_agg)

        self.from_schema.cleanup_update(update_id)
