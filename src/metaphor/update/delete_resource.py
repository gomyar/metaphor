
class DeleteResourceUpdate:
    def __init__(self, updater, schema, spec_name, resource_id, parent_spec_name, parent_field_name):
        self.updater = updater
        self.schema = schema
        self.spec_name = spec_name
        self.resource_id = resource_id
        self.parent_spec_name = parent_spec_name
        self.parent_field_name = parent_field_name

    def execute(self):
        spec = self.schema.specs[self.spec_name]

        cursors = []

        for (calc_spec_name, calc_field_name), calc_tree in self.schema.calc_trees.items():
            # update for resources
            # unsure if this is necessary
            if "%s.%s" % (self.parent_spec_name, self.parent_field_name) in calc_tree.get_resource_dependencies():
                affected_ids = self.updater.get_affected_ids_for_resource(calc_spec_name, calc_field_name, spec, self.resource_id)
                if affected_ids:
                    cursors.append((affected_ids, calc_spec_name, calc_field_name))

            # find root collection dependencies
            if self.parent_spec_name is None:
                for field_name, field in self.schema.root.fields.items():
                    if field.field_type in ['collection', 'linkcollection']:
                        if field.target_spec_name == self.spec_name:
                            affected_ids = self.updater.get_affected_ids_for_resource(calc_spec_name, calc_field_name, spec, self.resource_id)
                            if affected_ids:
                                cursors.append((affected_ids, calc_spec_name, calc_field_name))

        # must add _create_updated field to resource instead of creating updater document
        original_resource = self.schema.delete_resource(self.spec_name, self.resource_id)

        for affected_ids, calc_spec_name, calc_field_name in cursors:
            for affected_id in affected_ids:
                affected_id = self.schema.encodeid(affected_id)
                self.updater.update_calc(calc_spec_name, calc_field_name, affected_id)
                self.updater._recalc_for_field_update(spec, calc_spec_name, calc_field_name, affected_id)

        # delete child resources
        for field_name, field in spec.fields.items():
            if field.field_type == 'collection':
                for child_resource in self.schema.db['resource_%s' % field.target_spec_name].find({'_parent_id': self.schema.decodeid(self.resource_id)}, {'_id': 1}):
                    self.updater.delete_resource(field.target_spec_name, self.schema.encodeid(child_resource['_id']), self.spec_name, field_name)

        # delete any links to resource
        for linked_spec_name, spec in self.schema.specs.items():
            for field_name, field in spec.fields.items():
                if field.field_type == 'link' and field.target_spec_name == self.spec_name:
                    # find all resources with link to target id
                    for resource_data in self.schema.db['resource_%s' % linked_spec_name].find({field_name: self.schema.decodeid(self.resource_id)}):
                        # call update_resource on resource
                        self.updater.update_fields(linked_spec_name, self.schema.encodeid(resource_data['_id']), {field_name: None})

                if field.field_type == 'linkcollection' and field.target_spec_name == self.spec_name:
                    # find all resources with link to target id
                    for resource_data in self.schema.db['resource_%s' % linked_spec_name].find({'%s._id' % field_name: self.schema.decodeid(self.resource_id)}):
                        # call update_resource on resource
                        self.updater.delete_linkcollection_entry(linked_spec_name, resource_data['_id'], field_name, self.resource_id)

        # check if resource is read grant
        if self.spec_name == 'grant':
            self.updater._remove_grants(self.resource_id, original_resource['url'])

        return self.resource_id


