
from .update.mutation_create_defaulted_field import DefaultFieldMutation
from .update.mutation_delete_field import DeleteFieldMutation
from .update.mutation_alter_field_convert_primitive import AlterFieldConvertPrimitiveMutation
from .updater import Updater

from metaphor.lrparse.lrparse import parse_url


ACTIONS = {
    "create_field": DefaultFieldMutation,
    "convert_field": AlterFieldConvertPrimitiveMutation,
    "delete_field": DeleteFieldMutation,
}


class MutationFactory(object):
    def __init__(self, from_schema, to_schema):
        self.from_schema = from_schema
        self.to_schema = to_schema
        self.mutation = Mutation(from_schema, to_schema)

    def create(self):
        self.init_created_defaulted_fields()
        self.init_altered_fields()
        self.init_deleted_fields()
        return self.mutation

    def init_created_defaulted_fields(self):
        for spec_name, to_spec in self.to_schema.specs.items():
            if spec_name in self.from_schema.specs:
                from_spec = self.from_schema.specs[spec_name]
                for field_name, field in to_spec.fields.items():
                    if field_name not in from_spec.fields:
                        if field.default is not None:
                            field = self.to_schema.specs[spec_name].fields[field_name]
                            self.mutation.steps.append({
                                "action": "create_field",
                                "params": {
                                    "spec_name": spec_name,
                                    "field_name": field_name,
                                    "field_value": field.default,
                                }
                            })

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
                        self.mutation.steps.append({
                            "action": "convert_field",
                            "params": {
                                "spec_name": spec_name,
                                "field_name": field_name,
                                "new_type": new_type,
                            }
                        })

    def init_deleted_fields(self):
        for spec_name, to_spec in self.to_schema.specs.items():
            if spec_name in self.from_schema.specs:
                from_spec = self.from_schema.specs[spec_name]
                for field_name, field in from_spec.fields.items():
                    if field_name not in to_spec.fields:
                        field = self.from_schema.specs[spec_name].fields[field_name]
                        self.mutation.steps.append({
                            "action": "delete_field",
                            "params": {
                                "spec_name": spec_name,
                                "field_name": field_name,
                            }
                        })


class Mutation:
    def __init__(self, from_schema, to_schema):
        self.from_schema = from_schema
        self.to_schema = to_schema
        self.updater = Updater(to_schema)
        self.steps = []
        self.move_steps = []

    def mutate(self):
        for step in self.steps:
            ACTIONS[step['action']](self.updater, self.from_schema, self.to_schema, **step['params']).execute()
        for step in self.move_steps:
            self.execute_data_step(step)

    def add_move_step(self, from_path, to_path):
        self.move_steps.append({
            "from_path": from_path,
            "to_path": to_path,
        })

    def execute_data_step(self, step):
        self.execute_move_data(**step)

    def execute_move_data(self, from_path, to_path):
        if '/' in to_path:
            parent_path, field_name = to_path.rsplit('/', 1)
            tree = parse_canonical_url(parent_path, self.from_schema.root)  # "from_schema" or "to_schema" depending?

            aggregate_query = tree.create_aggregation(None)
            spec = tree.infer_type()

            field_spec = spec.fields[field_name]

            # if we're using a simplified parser we can probably just pull the id off the path
            cursor = tree.root_collection().aggregate(aggregate_query)
            parent_resource = next(cursor)

            return self.updater.move_resource(from_path, to_path, parent_resource['_id'], parent_resource['_canonical_url'], field_name, spec.name)
        else:
            field_name = to_path
            root_field_spec = self.from_schema.root.fields[to_path]
            return self.updater.move_resource(from_path, to_path, None, None, field_name, root_field_spec.name)

        # if filtered non-root resources
        #   for each resource in filter
        #   nested agg using filter
        #   alter parent info

