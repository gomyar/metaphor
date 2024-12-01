

class DeleteFieldMutation:
    def __init__(self, mutation, schema, spec_name, field_name):
        self.mutation = mutation
        self.schema = schema

        self.spec_name = spec_name
        self.field_name = field_name

    def __repr__(self):
        return "<DeleteFieldMutation>"

    def actions(self):
        return None

    def execute(self, action=None):
        update_id = str(self.schema.create_update())

        if self.spec_name != 'root':
            self.schema.delete_field_value(self.spec_name, self.field_name)
            self.schema._do_delete_field(self.spec_name, self.field_name)

        # find and update dependent calcs
        start_agg = []

        self.mutation.updater.update_for_field(self.spec_name, self.field_name, update_id, start_agg)

        self.schema.cleanup_update(update_id)
