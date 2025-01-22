

class CreateFieldMutation:
    def __init__(self, mutation, schema, spec_name, field_name, field_type, default=None, field_target=None, calc_str=None, is_reverse=None, indexed=None, unique=None, unique_global=None):
        self.mutation = mutation
        self.schema = schema

        self.spec_name = spec_name
        self.field_name = field_name
        self.default = default
        self.field_type = field_type
        self.field_target = field_target
        self.calc_str = calc_str
        self.is_reverse = is_reverse
        self.indexed = indexed

    def __repr__(self):
        return "<CreateFieldMutation>"

    def actions(self):
        return None

    def execute(self, action=None):
        if self.is_reverse:
            return

        update_id = str(self.schema.create_update())

        self.schema.create_field(self.spec_name, self.field_name, self.field_type, calc_str=self.calc_str, default=self.default, field_target=self.field_target)

        if self.default is not None:
            update_id = str(self.schema.create_update())
            self.schema.default_field_value(self.spec_name, self.field_name, self.default)

        if self.default or self.calc_str:
            self.mutation.updater.update_for_field(self.spec_name, self.field_name, update_id, [])

        if self.indexed:
            self.schema.create_index_for_field(self.spec_name, self.field_name)

        self.schema.cleanup_update(update_id)
