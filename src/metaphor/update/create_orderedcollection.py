
from toposort import toposort


class CreateOrderedCollectionUpdate:
    def __init__(self, updater, schema, spec_name, parent_spec_name, parent_field, parent_id, data, grants):
        self.updater = updater
        self.schema = schema

        self.spec_name = spec_name
        self.parent_spec_name = parent_spec_name
        self.parent_field = parent_field
        self.parent_id = parent_id
        self.data = data
        self.grants = grants

    def execute(self):
        resource_id = self.schema.create_orderedcollection_entry(self.spec_name, self.parent_spec_name, self.parent_field, self.parent_id, self.data, self.grants)
        parent_spec = self.schema.specs[self.parent_spec_name]
        spec = parent_spec.build_child_spec(self.parent_field)

        for (calc_spec_name, calc_field_name), calc_tree in self.schema.calc_trees.items():
            # update for resources
            if "%s.%s" % (self.parent_spec_name, self.parent_field) in calc_tree.get_resource_dependencies():
                self.updater._perform_updates_for_affected_calcs(spec, resource_id, calc_spec_name, calc_field_name)

        # recalc local calcs
        calc_field_deps = {}
        for field_name, field_spec in spec.fields.items():
            if field_spec.field_type == 'calc':
                calc_field_deps[spec.name + '.' + field_name] = self.schema.calc_trees[(spec.name, field_name)].get_resource_dependencies()

        sorted_field_deps = toposort(calc_field_deps)
        for layer in sorted_field_deps:
            for field_str in layer:
                spec_name, field_name = field_str.split('.')
                if spec_name == spec.name:
                    field_spec = spec.fields[field_name]
                    if field_spec.field_type == 'calc':
                        self.updater.update_calc(spec.name, field_name, resource_id)
                        self.updater._recalc_for_field_update(spec, spec.name, field_name, resource_id)

        return resource_id
