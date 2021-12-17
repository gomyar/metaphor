

class CreateResourceUpdate:
    def __init__(self, updater, schema, spec_name, fields, parent_field_name, parent_spec_name,
                 parent_id, grants):
        self.updater = updater
        self.schema = schema

        self.spec_name = spec_name
        self.spec = self.schema.specs[spec_name]
        self.fields = fields

        self.parent_field_name = parent_field_name
        self.parent_spec_name = parent_spec_name
        self.parent_id = parent_id
        self.grants = grants

    def execute(self):
        # must add _create_updated field to resource instead of creating updater document
        resource_id = self.schema.insert_resource(
            self.spec_name, self.fields, self.parent_field_name, self.parent_spec_name,
            self.parent_id, self.grants)

        for (calc_spec_name, calc_field_name), calc_tree in self.schema.calc_trees.items():
            # update for resources
            # unsure if this is necessary
            if "%s.%s" % (self.parent_spec_name, self.parent_field_name) in calc_tree.get_resource_dependencies():
                self.updater._perform_updates_for_affected_calcs(self.spec, resource_id, calc_spec_name, calc_field_name)

            # update for fields
            for field_name in self.fields:
                field_dep = "%s.%s" % (self.spec_name, field_name)
                if field_dep in calc_tree.get_resource_dependencies():
                    self.updater._perform_updates_for_affected_calcs(self.spec, resource_id, calc_spec_name, calc_field_name)

        # recalc local calcs
        for field_name, field_spec in self.spec.fields.items():
            if field_spec.field_type == 'calc':
                self.updater.update_calc(self.spec.name, field_name, resource_id)
                self.updater._recalc_for_field_update(self.spec, self.spec.name, field_name, resource_id)

        # check if new resource is read grant
        if self.spec_name == 'grant' and self.fields['type'] == 'read':
            self.updater._update_grants(resource_id, self.fields['url'])

        return resource_id
