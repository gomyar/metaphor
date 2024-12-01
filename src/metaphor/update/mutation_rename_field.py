

class RenameFieldMutation:
    def __init__(self, mutation, schema, spec_name, from_field_name, to_field_name, field_name, field_type, default=None, field_target=None, calc_str=None):
        self.mutation = mutation
        self.schema = schema

        self.spec_name = spec_name
        self.from_field_name = from_field_name
        self.to_field_name = to_field_name

        self.field_name = field_name
        self.field_type = field_type
        self.default = default
        self.field_target = field_target
        self.calc_str = calc_str

    def __repr__(self):
        return "<RenameFieldMutation>"

    def actions(self):
#        return ["create_field", "move", "delete_field"]
        return None

    def execute(self, action=None):
        spec = self.schema.get_spec(self.spec_name)
        field = spec.fields[self.from_field_name]

        update_id = str(self.schema.create_update())

        self.schema.rename_field(self.spec_name, self.from_field_name, self.to_field_name)

        if self.schema.get_spec(self.spec_name).fields[self.from_field_name].field_type != self.field_type:
            self.schema.alter_field_convert_type(self.spec_name, self.field_name, self.field_type)

        self.schema.cleanup_update(update_id)
