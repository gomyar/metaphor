

class RenameSpecMutation:
    def __init__(self, updater, from_schema, to_schema, spec_name, to_spec_name):
        self.updater = updater
        self.to_schema = to_schema
        self.from_schema = from_schema

        self.spec_name = spec_name
        self.to_spec_name = to_spec_name

    def __repr__(self):
        return "<RenameSpecMutation>"

    def actions(self):
        return ["create_spec", "move", "delete_spec"]

    def execute(self, action=None):
        spec = self.from_schema.specs[self.spec_name]

        update_id = str(self.to_schema.create_update())

        if action == "create_spec":
            self.from_schema.rename_spec(self.spec_name, self.to_spec_name)

        self.to_schema.cleanup_update(update_id)
