
from .updater import Updater
from .update.mutation_create_defaulted_field import DefaultFieldMutation
from .update.mutation_delete_field import DeleteFieldMutation
from .update.mutation_alter_field_convert_primitive import AlterFieldConvertPrimitiveMutation

from metaphor.lrparse.lrparse import parse_url


class Mutation(object):
    def __init__(self, from_schema, to_schema):
        self.from_schema = from_schema
        self.to_schema = to_schema
        self.updater = Updater(to_schema)
        self.create_steps = []
        self.alter_steps = []
        self.delete_steps = []
        self.pre_data_steps = []
        self.post_data_steps = []

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
                            self.create_steps.append(DefaultFieldMutation(self.updater, self.to_schema, spec_name, field_name, field.default))

    def init_altered_fields(self):
        type_map = {
            'str': 'string',
            'int': 'int',
            'float': 'double',
            'bool': 'bool',
            'datetime': 'date',
        }
        for spec_name, to_spec in self.to_schema.specs.items():
            if spec_name in self.from_schema.specs:
                from_spec = self.from_schema.specs[spec_name]
                for field_name, field in to_spec.fields.items():
                    if field_name in from_spec.fields and field.field_type != from_spec.fields[field_name].field_type:
                        field = self.to_schema.specs[spec_name].fields[field_name]
                        new_type = type_map[to_spec.fields[field_name].field_type]
                        self.alter_steps.append(
                            AlterFieldConvertPrimitiveMutation(
                                self.updater, self.to_schema, spec_name, field_name, new_type)
                        )

    def init_deleted_fields(self):
        for spec_name, to_spec in self.to_schema.specs.items():
            if spec_name in self.from_schema.specs:
                from_spec = self.from_schema.specs[spec_name]
                for field_name, field in from_spec.fields.items():
                    if field_name not in to_spec.fields:
                        field = self.from_schema.specs[spec_name].fields[field_name]
                        self.delete_steps.append(DeleteFieldMutation(self.updater, self.from_schema, spec_name, field_name, field.default))

    def mutate(self):
        for step in self.create_steps:
            step.execute()
        for step in self.pre_data_steps:
            self.execute_data_step(step)
        for step in self.alter_steps:
            step.execute()
        for step in self.delete_steps:
            step.execute()

    def add_pre_data_step(self, action, target_calc, target_field_name, move_calc):
        self.pre_data_steps.append({
            "action": action,
            "target_calc": target_calc,
            "target_field_name": target_field_name,
            "move_calc": move_calc,
        })

    def execute_data_step(self, step):
        strategy = {
            "move": self.execute_move_data,
        }
        strategy[step['action']](**step)

    def execute_move_data(self, target_calc, target_field_name, move_calc, **kwargs):
        # if root collection
        if target_calc == 'root' and target_field_name in self.from_schema.root.fields:
            #   move filtered resources to root collection
            from_tree = parse_url(move_calc, self.schema.root)
            #   for all fitered resources
            aggregate_query, from_spec, is_aggregate = from_tree.aggregation(None)
            #   alter parent info

            #   adjust for orderedcollections in parent

        # if filtered non-root resources
        #   for each resource in filter
        #   nested agg using filter
        #   alter parent info
        #   adjust for orderedcollections in parent

