

class CreateSpecMutation:
    def __init__(self, updater, schema, spec_name):
        self.updater = updater
        self.schema = schema

        self.spec_name = spec_name

    def __repr__(self):
        return "<CreateSpecMutation>"

    def actions(self):
        return None

    def execute(self, action=None):
        update_id = str(self.schema.create_update())
        self.schema.create_spec(self.spec_name)
        self.schema.cleanup_update(update_id)
