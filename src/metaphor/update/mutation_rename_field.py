

class RenameFieldMutation:
    def __init__(self, updater, from_schema, to_schema, spec_name, from_field_name, to_field_name):
        self.updater = updater
        self.to_schema = to_schema
        self.from_schema = from_schema

        self.spec_name = spec_name
        self.from_field_name = from_field_name
        self.to_field_name = to_field_name

    def __repr__(self):
        return "<RenameFieldMutation>"

    def actions(self):
        return ["create_field", "move", "delete_field"]

    def execute(self, action=None):
        spec = self.from_schema.get_spec(self.spec_name)
        field = spec.fields[self.from_field_name]

        update_id = str(self.to_schema.create_update())

        if action == 'delete_field':
            self.from_schema.rename_field(self.spec_name, self.from_field_name, self.to_field_name)

        self.to_schema.cleanup_update(update_id)
