
from datetime import datetime
import traceback

from .update.mutation_create_defaulted_field import DefaultFieldMutation
from .update.mutation_delete_field import DeleteFieldMutation
from .update.mutation_alter_field_convert_primitive import AlterFieldConvertPrimitiveMutation
from .update.mutation_create_spec import CreateSpecMutation
from .update.mutation_delete_spec import DeleteSpecMutation
from .update.mutation_rename_field import RenameFieldMutation
from .update.mutation_rename_spec import RenameSpecMutation

from .updater import Updater

from metaphor.lrparse.lrparse import parse_canonical_url
from metaphor.lrparse.lrparse import parse_url

import logging
log = logging.getLogger(__name__)


ACTIONS = {
    "create_field": DefaultFieldMutation,
    "alter_field": AlterFieldConvertPrimitiveMutation,
    "delete_field": DeleteFieldMutation,
    "create_spec": CreateSpecMutation,
    "delete_spec": DeleteSpecMutation,
    "rename_field": RenameFieldMutation,
    "rename_spec": RenameSpecMutation,
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
        self.init_root_fields()
        self.mutation._sort_steps()
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
                self.mutation._create_fields_for(spec)

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
                                "field_type": field.field_type,
                                "field_target": field.target_spec_name,
                            }
                        })

    def init_root_fields(self):
        for field_name, field in self.to_schema.root.fields.items():
            if field_name not in self.from_schema.root.fields:
                field = self.to_schema.root.fields[field_name]
                self.mutation.steps.append({
                    "action": "create_field",
                    "params": {
                        "spec_name": "root",
                        "field_name": field_name,
                        "field_value": field.default,
                        "field_type": field.field_type,
                        "field_target": field.target_spec_name,
                    }
                })

    def init_altered_fields(self):
        for spec_name, to_spec in self.to_schema.specs.items():
            if spec_name in self.from_schema.specs:
                from_spec = self.from_schema.specs[spec_name]
                for field_name, field in to_spec.fields.items():
                    if field_name in from_spec.fields and field.field_type != from_spec.fields[field_name].field_type:
                        new_type = to_spec.fields[field_name].field_type
                        self.mutation._create_alter_field_step(spec_name, field_name, new_type)


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
        self._id = None
        self.from_schema = from_schema
        self.to_schema = to_schema
        self.updater = Updater(to_schema)
        self.steps = []
        self.move_steps = []
        self.data_steps = []
        self.state = None
        self.error = None

    def mutate(self):
        try:
            self.set_mutation_state(self, state="running", updated=datetime.now(), run_datetime=datetime.now())

            for step in self.steps:
                ACTIONS[step['action']](self.updater, self.from_schema, self.to_schema, **step['params']).execute()
            for step in self.move_steps:
                self.execute_data_step(step)

            self.set_mutation_state(self, state="complete", updated=datetime.now(), complete_datetime=datetime.now())
        except Exception as e:
            log.exception("Exception mutating schema")
            self.set_mutation_state(self, state="error", updated=datetime.now(), error=traceback.format_exc())

    def set_mutation_state(self, mutation, **state):
        self.from_schema.db.metaphor_mutation.find_one_and_update({"_id": mutation._id}, {"$set": state})

    def add_move_step(self, from_path, to_path):
        self.move_steps.append({
            "from_path": from_path,
            "to_path": to_path,
        })

    def convert_delete_field_to_rename(self, spec_name, from_field_name, to_field_name):
        delete_step = self._find_step('delete_field', spec_name=spec_name, field_name=from_field_name)
        create_step = self._find_step('create_field', spec_name=spec_name, field_name=to_field_name)

        self.steps.pop(self.steps.index(delete_step))
        self.steps.pop(self.steps.index(create_step))

        self.steps.append({
            "action": "rename_field",
            "params": {
                "spec_name": spec_name,
                "from_field_name": from_field_name,
                "to_field_name": to_field_name,
            }})

        from_field = self.from_schema.specs[spec_name].fields[from_field_name]
        to_field = self._corresponding_field(spec_name, from_field_name, to_field_name)

        if from_field.field_type != to_field.field_type:
            self._create_alter_field_step(spec_name, to_field_name, to_field.field_type)

        self._sort_steps()

    def _corresponding_field(self, spec_name, field_name, to_field_name):
        try:
            return self.to_schema.specs[spec_name].fields[field_name]
        except KeyError as ke:
            rename_spec_step = self._find_step('rename_spec', spec_name=spec_name)
            if rename_spec_step:
                spec_name = rename_spec_step['params']['to_spec_name']
                return self.to_schema.specs[spec_name].fields[to_field_name]

            rename_field_step = self._find_step('rename_field', spec_name=spec_name, from_field_name=field_name)
            if rename_field_step:
                field_name = rename_field_step['params']['to_field_name']

            rename_field_step = self._find_step('rename_field', spec_name=spec_name, from_field_name=to_field_name)
            if rename_field_step:
                field_name = rename_field_step['params']['to_field_name']

            return self.to_schema.specs[spec_name].fields[to_field_name]

    def _sort_steps(self):
        values = {
            "create_spec": 0,
            "create_field": 20,
            "delete_field": 50,
            "rename_spec": 60,
            "rename_field": 70,
            "alter_field": 80,
            "delete_spec": 100,
        }
        def cmp(lhs):
            return values[lhs['action']]
        self.steps = sorted(self.steps, key=cmp)

    def _find_step(self, action, **params):
        try:
            return next(self._find_all_steps(action, **params))
        except StopIteration as si:
            return None

    def _find_all_steps(self, action, **params):
        for step in self.steps:
            if step['action'] == action and all(step['params'].get(k) == params[k] for k in params):
                yield step

    def convert_delete_spec_to_rename(self, from_spec_name, to_spec_name):
        delete_step = self._find_step("delete_spec", spec_name=from_spec_name)
        create_step = self._find_step("create_spec", spec_name=to_spec_name)

        self.steps.pop(self.steps.index(delete_step))
        self.steps.pop(self.steps.index(create_step))

        for create_field_step in self._find_all_steps("create_field", spec_name=to_spec_name):
            self.steps.pop(self.steps.index(create_field_step))

        # find altered fields
        from_spec = self.from_schema.specs[from_spec_name]
        to_spec = self.to_schema.specs[to_spec_name]

        # add create field steps
        for field_name, field in to_spec.fields.items():
            if field_name not in from_spec.fields:
                self.steps.append({
                    "action": "create_field",
                    "params": {
                        "spec_name": from_spec_name,
                        "field_name": field_name,
                        "field_value": field.default,
                        "field_type": field.field_type,
                        "field_target": field.target_spec_name,
                    }
                })

        # add delete field steps
        for field_name, field in from_spec.fields.items():
            if field_name not in to_spec.fields:
                self.steps.append({
                    "action": "delete_field",
                    "params": {
                        "spec_name": from_spec_name,
                        "field_name": field_name,
                        "field_value": field.default,
                    }
                })

        # add rename_spec step
        self.steps.append({
            "action": "rename_spec",
            "params": {
                "spec_name": from_spec_name,
                "to_spec_name": to_spec_name,
            }})

        self._sort_steps()


    def cancel_rename_field(self, spec_name, from_field_name):
        rename_step = self._find_step('rename_field', spec_name=spec_name, from_field_name=from_field_name)

        self.steps.pop(self.steps.index(rename_step))

        rename_spec_step = self._find_step('rename_spec', spec_name=spec_name)
        if rename_spec_step:
            field = self.from_schema.specs[spec_name].fields[rename_step['params']['from_field_name']]
            self.steps.append({
                "action": "create_field",
                "params": {
                    "spec_name": spec_name,
                    "field_name": rename_step['params']['to_field_name'],
                    "field_type": field.field_type,
                    "field_target": field.target_spec_name,
                }
            })
        else:
            field = self.to_schema.specs[spec_name].fields[rename_step['params']['to_field_name']]

            self.steps.append({
                "action": "create_field",
                "params": {
                    "spec_name": spec_name,
                    "field_name": field.name,
                    "field_type": field.field_type,
                    "field_target": field.target_spec_name,
                }
            })

        self.steps.append({
            "action": "delete_field",
            "params": {
                "spec_name": spec_name,
                "field_name": from_field_name,
            }
        })

        self._sort_steps()

    def _remove_steps_for_spec(self, spec_name):
        all_steps = list(filter(lambda f: f['params'].get('spec_name') == spec_name, self.steps))
        for step in all_steps:
            self.steps.pop(self.steps.index(step))

    def cancel_rename_spec(self, spec_name):
        rename_step = self._find_step('rename_spec', spec_name=spec_name)

        self.steps.pop(self.steps.index(rename_step))

        from_spec_name = rename_step['params']['spec_name']
        to_spec_name = rename_step['params']['to_spec_name']

        self._remove_steps_for_spec(from_spec_name)
        self._remove_steps_for_spec(to_spec_name)

        self.steps.append({
            "action": "create_spec",
            "params": {
                "spec_name": to_spec_name,
            }
        })
        self._create_fields_for(self.to_schema.specs[to_spec_name])
        self.steps.append({
            "action": "delete_spec",
            "params": {
                "spec_name": from_spec_name,
            }
        })
        self._sort_steps()

    def _create_fields_for(self, spec):
        for field_name, field in spec.fields.items():
            self.steps.append({
                "action": "create_field",
                "params": {
                    "spec_name": spec.name,
                    "field_name": field_name,
                    "field_value": field.default,
                    "field_type": field.field_type,
                    "field_target": field.target_spec_name,
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

    def _create_alter_field_step(self, spec_name, field_name, new_type):
        type_map = {
            'str': 'string',
            'int': 'int',
            'float': 'double',
            'bool': 'bool',
            'datetime': 'date',
        }

        self.steps.append({
            "action": "alter_field",
            "params": {
                "spec_name": spec_name,
                "field_name": field_name,
                "new_type": type_map[new_type],
            }
        })


