

class DefaultFieldMutation:
    def __init__(self, updater, from_schema, to_schema, spec_name, field_name, field_value=None):
        self.updater = updater
        self.to_schema = to_schema

        self.spec_name = spec_name
        self.field_name = field_name
        self.field_value = field_value

    def __repr__(self):
        return "<DefaultFieldMutation>"

    def execute(self):

        spec = self.to_schema.specs[self.spec_name]
        field = spec.fields[self.field_name]

        # only action required on a field create is default
        if field.default is not None:
            update_id = str(self.to_schema.create_update())
            self.to_schema.default_field_value(self.spec_name, self.field_name, self.field_value)

            # find and update dependent calcs
            start_agg = []

            self.updater.update_for_field(self.spec_name, self.field_name, update_id, start_agg)

            self.to_schema.cleanup_update(update_id)
