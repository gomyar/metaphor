
from .update.mutation_create_defaulted_field import DefaultFieldMutation
from .update.mutation_delete_field import DeleteFieldMutation
from .update.mutation_alter_field_convert_primitive import AlterFieldConvertPrimitiveMutation
from .update.mutation_create_spec import CreateSpecMutation
from .update.mutation_delete_spec import DeleteSpecMutation
from .update.mutation_rename_field import RenameFieldMutation

from .updater import Updater

from metaphor.lrparse.lrparse import parse_canonical_url
from metaphor.lrparse.lrparse import parse_url


ACTIONS = {
    "create_field": DefaultFieldMutation,
    "convert_field": AlterFieldConvertPrimitiveMutation,
    "delete_field": DeleteFieldMutation,
    "create_spec": CreateSpecMutation,
    "delete_spec": DeleteSpecMutation,
    "rename_field": RenameFieldMutation,
}


class MutationFactory(object):
    def __init__(self, from_schema, to_schema):
        self.from_schema = from_schema
        self.to_schema = to_schema
        self.mutation = Mutation(from_schema, to_schema)

    def create(self):
        self.init_created_specs()
        self.init_deleted_specs()
        self.init_created_defaulted_fields()
        self.init_altered_fields()
        self.init_deleted_fields()
        return self.mutation

    def init_created_specs(self):
        for spec_name, to_spec in self.to_schema.specs.items():
            if spec_name not in self.from_schema.specs:
                spec = self.to_schema.specs[spec_name]
                self.mutation.steps.append({
                    "action": "create_spec",
                    "params": {
                        "spec_name": spec_name,
                    }
                })
                for field_name, field in spec.fields.items():
                    self.mutation.steps.append({
                        "action": "create_field",
                        "params": {
                            "spec_name": spec_name,
                            "field_name": field_name,
                            "field_value": field.default,
                        }
                    })


    def init_deleted_specs(self):
        for spec_name, to_spec in self.from_schema.specs.items():
            if spec_name not in self.to_schema.specs:
                spec = self.from_schema.specs[spec_name]
                self.mutation.steps.append({
                    "action": "delete_spec",
                    "params": {
                        "spec_name": spec_name,
                    }
                })

    def init_created_defaulted_fields(self):
        for spec_name, to_spec in self.to_schema.specs.items():
            if spec_name in self.from_schema.specs:
                from_spec = self.from_schema.specs[spec_name]
                for field_name, field in to_spec.fields.items():
                    if field_name not in from_spec.fields:
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

    def _find_delete_field_step(self, spec_name, field_name):
        for step in self.steps:
            if step['action'] == 'delete_field' and step['params']['spec_name'] == spec_name and step['params']['field_name'] == field_name:
                return step

    def _find_create_field_step(self, spec_name, field_name):
        for step in self.steps:
            if step['action'] == 'create_field' and step['params']['spec_name'] == spec_name and step['params']['field_name'] == field_name:
                return step

    def convert_delete_field_to_rename(self, spec_name, from_field_name, to_field_name):
        delete_step = self._find_delete_field_step(spec_name, from_field_name)
        create_step = self._find_create_field_step(spec_name, to_field_name)

        self.steps.pop(self.steps.index(delete_step))
        self.steps.pop(self.steps.index(create_step))

        self.steps.append({
            "action": "rename_field",
            "params": {
                "spec_name": spec_name,
                "from_field_name": from_field_name,
                "to_field_name": to_field_name,
            }})

    def _find_step(self, action, spec_name, field_name):
        for step in self.steps:
            if step['action'] == action and step['params']['spec_name'] == spec_name and step['params']['from_field_name'] == field_name:
                return step

    def cancel_rename_field(self, spec_name, field_name):
        rename_step = self._find_step('rename_field', spec_name, field_name)

        self.steps.pop(self.steps.index(rename_step))

        self.steps.append({
            "action": "create_field",
            "params": {
                "spec_name": spec_name,
                "field_name": rename_step['params']['to_field_name'],
            }
        })
        self.steps.append({
            "action": "delete_field",
            "params": {
                "spec_name": spec_name,
                "field_name": field_name,
            }
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

