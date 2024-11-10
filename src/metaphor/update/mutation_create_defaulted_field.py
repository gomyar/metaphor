

class DefaultFieldMutation:
    def __init__(self, updater, from_schema, to_schema, spec_name, field_name, field_type, field_value=None, field_target=None):
        self.updater = updater
        self.to_schema = to_schema

        self.spec_name = spec_name
        self.field_name = field_name
        self.field_value = field_value
        self.field_type = field_type
        self.field_target = field_target

    def __repr__(self):
        return "<DefaultFieldMutation>"

    def actions(self):
        return None

    def execute(self, action=None):
        spec = self.to_schema.get_spec(self.spec_name)
        field = spec.fields[self.field_name]

        # only action required on a field create is default
        if field.default is not None:
            update_id = str(self.to_schema.create_update())
            self.to_schema.default_field_value(self.spec_name, self.field_name, self.field_value)

            # find and update dependent calcs
            start_agg = []

            self.updater.update_for_field(self.spec_name, self.field_name, update_id, start_agg)

            self.to_schema.cleanup_update(update_id)
