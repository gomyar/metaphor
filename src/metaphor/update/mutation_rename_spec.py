

class RenameSpecMutation:
    def __init__(self, updater, schema, spec_name, to_spec_name):
        self.updater = updater
        self.schema = schema

        self.spec_name = spec_name
        self.to_spec_name = to_spec_name

    def __repr__(self):
        return "<RenameSpecMutation>"

    def actions(self):
        return ["create_spec", "move", "delete_spec"]

    def execute(self, action=None):
        spec = self.schema.specs[self.spec_name]

        update_id = str(self.schema.create_update())

        if action == "create_spec":
            self.schema.rename_spec(self.spec_name, self.to_spec_name)

        self.schema.cleanup_update(update_id)
