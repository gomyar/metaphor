
class DeleteOrderedCollectionUpdate:
    def __init__(self, updater, schema, parent_spec_name, parent_id, parent_field, link_id):
        self.updater = updater
        self.schema = schema
        self.parent_spec_name = parent_spec_name
        self.parent_id = parent_id
        self.parent_field = parent_field
        self.link_id = link_id

    def execute(self):
        parent_spec = self.schema.specs[self.parent_spec_name]
        spec = parent_spec.build_child_spec(self.parent_field)

        cursors = []

        # find affected resources before deleting
        for (calc_spec_name, calc_field_name), calc_tree in self.schema.calc_trees.items():
            # update for resources
            if "%s.%s" % (self.parent_spec_name, self.parent_field) in calc_tree.get_resource_dependencies():

                affected_ids = self.updater.get_affected_ids_for_resource(calc_spec_name, calc_field_name, spec, self.link_id)

                if affected_ids:
                    cursors.append((affected_ids, calc_spec_name, calc_field_name))

        # perform delete
        self.schema.delete_linkcollection_entry(
            self.parent_spec_name, self.parent_id, self.parent_field, self.link_id)
        self.schema.delete_resource(spec.name, self.link_id)

        # update affected resources
        for affected_ids, calc_spec_name, calc_field_name in cursors:
            for affected_id in affected_ids:
                affected_id = self.schema.encodeid(affected_id)
                self.updater.update_calc(calc_spec_name, calc_field_name, affected_id)
                self.updater._recalc_for_field_update(spec, calc_spec_name, calc_field_name, affected_id)

        # recalc local calcs
        for field_name, field_spec in parent_spec.fields.items():
            if field_spec.field_type == 'calc':
                self.updater.update_calc(self.parent_spec_name, field_name, self.link_id)
                self.updater._recalc_for_field_update(spec, self.parent_spec_name, field_name, self.link_id)

        # delete any links to resource
        for linked_spec_name, linked_spec in self.schema.specs.items():
            for field_name, field in linked_spec.fields.items():
                if field.field_type == 'link' and field.target_spec_name == spec.name:
                    # find all resources with link to target id
                    for resource_data in self.schema.db['resource_%s' % linked_spec_name].find({field_name: self.schema.decodeid(self.link_id)}):
                        # call update_resource on resource
                        self.updater.update_fields(linked_spec_name, self.schema.encodeid(resource_data['_id']), {field_name: None})

                if field.field_type == 'linkcollection' and field.target_spec_name == spec.name:
                    # find all resources with link to target id
                    for resource_data in self.schema.db['resource_%s' % linked_spec_name].find({'%s._id' % field_name: self.schema.decodeid(self.link_id)}):
                        # call update_resource on resource
                        self.updater.delete_linkcollection_entry(linked_spec_name, resource_data['_id'], field_name, resource_id)

        # run update
        return self.link_id


