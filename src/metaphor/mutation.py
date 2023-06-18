
from .updater import Updater
from .update.mutation_create_defaulted_field import DefaultFieldMutation
from .update.mutation_delete_field import DeleteFieldMutation
from .update.mutation_alter_field_type_primitive_to_str import AlterFieldTypePrimitiveToStrMutation


class Mutation(object):
    def __init__(self, from_schema, to_schema):
        self.from_schema = from_schema
        self.to_schema = to_schema
        self.updater = Updater(to_schema)
        self.steps = []

    def init(self):
        self.init_created_defaulted_fields()
        self.init_altered_fields()
        self.init_deleted_fields()

    def init_created_defaulted_fields(self):
        for spec_name, to_spec in self.to_schema.specs.items():
            if spec_name in self.from_schema.specs:
                from_spec = self.from_schema.specs[spec_name]
                for field_name, field in to_spec.fields.items():
                    if field_name not in from_spec.fields:
                        if field.default is not None:
                            field = self.to_schema.specs[spec_name].fields[field_name]
                            self.steps.append(DefaultFieldMutation(self.updater, self.to_schema, spec_name, field_name, field.default))

    def init_altered_fields(self):
        alter_map = {
            ('int', 'str'): AlterFieldTypePrimitiveToStrMutation,
            ('float', 'str'): AlterFieldTypePrimitiveToStrMutation,
            ('bool', 'str'): AlterFieldTypePrimitiveToStrMutation,
            ('datetime', 'str'): AlterFieldTypePrimitiveToStrMutation,
        }
        for spec_name, to_spec in self.to_schema.specs.items():
            if spec_name in self.from_schema.specs:
                from_spec = self.from_schema.specs[spec_name]
                for field_name, field in to_spec.fields.items():
                    if field_name in from_spec.fields and field.field_type != from_spec.fields[field_name].field_type:
                        field = self.to_schema.specs[spec_name].fields[field_name]
                        mutation = alter_map[(from_spec.fields[field_name].field_type, to_spec.fields[field_name].field_type)]
                        self.steps.append(mutation(self.updater, self.to_schema, spec_name, field_name, field.default))


    def init_deleted_fields(self):
        for spec_name, to_spec in self.to_schema.specs.items():
            if spec_name in self.from_schema.specs:
                from_spec = self.from_schema.specs[spec_name]
                for field_name, field in from_spec.fields.items():
                    if field_name not in to_spec.fields:
                        field = self.from_schema.specs[spec_name].fields[field_name]
                        self.steps.append(DeleteFieldMutation(self.updater, self.from_schema, spec_name, field_name, field.default))


    def mutate(self):
        for step in self.steps:
            step.execute()
