
class CreateLinkCollectionUpdate:
    def __init__(self, updater, schema, parent_spec_name, parent_id, parent_field, link_id):
        self.updater = updater
        self.schema = schema

        self.parent_spec_name = parent_spec_name
        self.parent_id = parent_id
        self.parent_field = parent_field
        self.link_id = link_id

    def execute(self):
        self.schema.create_linkcollection_entry(self.parent_spec_name, self.parent_id, self.parent_field, self.link_id)
        parent_spec = self.schema.specs[self.parent_spec_name]
        spec = parent_spec.build_child_spec(self.parent_field)

        for (calc_spec_name, calc_field_name), calc_tree in self.schema.calc_trees.items():
            # update for resources
            if "%s.%s" % (self.parent_spec_name, self.parent_field) in calc_tree.get_resource_dependencies():
                self.updater._perform_updates_for_affected_calcs(spec, self.link_id, calc_spec_name, calc_field_name)

        # recalc local calcs
        for field_name, field_spec in parent_spec.fields.items():
            if field_spec.field_type == 'calc':
                self.updater.update_calc(self.parent_spec_name, field_name, self.parent_id)
                self.updater._recalc_for_field_update(spec, self.parent_spec_name, field_name, self.parent_id)


