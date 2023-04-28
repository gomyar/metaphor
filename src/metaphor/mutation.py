
from .updater import Updater
from .update.mutation_create_defaulted_field import DefaultFieldMutation


class Mutation(object):
    def __init__(self, from_schema, to_schema):
        self.from_schema = from_schema
        self.to_schema = to_schema
        self.updater = Updater(to_schema)
        self.steps = []

    def init(self):
        for spec_name, spec in self.to_schema.specs.items():
            if spec_name in self.from_schema.specs:
                from_spec = self.from_schema.specs[spec_name]
                for field_name, field in spec.fields.items():
                    if field_name not in from_spec.fields:
                        if field.default is not None:
                            field = self.to_schema.specs[spec_name].fields[field_name]
                            self.steps.append(DefaultFieldMutation(self.updater, self.to_schema, spec_name, field_name, field.default))

    def mutate(self):
        for step in self.steps:
            step.execute()
