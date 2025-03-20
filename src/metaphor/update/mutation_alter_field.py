

class AlterFieldMutation:
    def __init__(self, mutation, schema, spec_name, field_name, new_type):
        self.mutation = mutation
        self.schema = schema

        self.spec_name = spec_name
        self.field_name = field_name
        self.new_type = new_type

    def __repr__(self):
        return "<AlterFieldMutation>"

    def actions(self):
        return None

    def execute(self, action=None):
        update_id = str(self.schema.create_update())

        spec = self.schema.specs[self.spec_name]
        from_field = self.schema.specs[self.spec_name].fields[self.field_name]
        to_field = self.mutation.to_schema.specs[self.spec_name].fields[self.field_name]

        if from_field.field_type != to_field.field_type:
            self.schema.alter_field_convert_type(self.spec_name, self.field_name, self.new_type)

        if from_field.indexed != to_field.indexed or from_field.unique != to_field.unique or from_field.unique_global != to_field.unique_global:
            self.schema.create_index_for_field(self.spec_name, self.field_name, to_field.unique, to_field.unique_global)

            if from_field.unique_global and not to_field.unique_global:
                self.schema.drop_index_for_field("global", self.spec_name, self.field_name)
            elif from_field.unique and not to_field.unique:
                self.schema.drop_index_for_field("unique", self.spec_name, self.field_name)
            elif from_field.indexed and not to_field.indexed:
                self.schema.drop_index_for_field("index", self.spec_name, self.field_name)

        self.mutation.updater.update_for_field(self.spec_name, self.field_name, update_id, [])

        self.schema.cleanup_update(update_id)
