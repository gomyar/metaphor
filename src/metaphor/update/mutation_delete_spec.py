

class DeleteSpecMutation:
    def __init__(self, updater, from_schema, to_schema, spec_name):
        self.updater = updater
        self.to_schema = to_schema
        self.from_schema = from_schema

        self.spec_name = spec_name

    def __repr__(self):
        return "<DeleteSpecMutation>"

    def execute(self):
        update_id = str(self.from_schema.create_update())

        self.from_schema.delete_spec(self.spec_name)

        self.from_schema.delete_resources_of_type(self.spec_name)

        self.from_schema.cleanup_update(update_id)
