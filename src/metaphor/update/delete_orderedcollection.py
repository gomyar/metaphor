class DeleteOrderedCollectionUpdate:
    def __init__(self, update_id, updater, schema, parent_spec_name, parent_id, parent_field, link_id):
        self.update_id = update_id
        self.updater = updater
        self.schema = schema
        self.parent_spec_name = parent_spec_name
        self.parent_id = parent_id
        self.parent_field = parent_field
        self.link_id = link_id

    def execute(self):
        spec_name = self.schema.specs[self.parent_spec_name].build_child_spec(self.parent_field).name

        # mark link deleted
        self.schema.mark_link_collection_item_deleted(self.parent_spec_name, self.parent_id, self.parent_field, self.link_id)
        # mark resource as _deleted
        original_resource = self.schema.mark_resource_deleted(spec_name, self.link_id)

        # update
        start_agg = [
            {"$match": {"_id": self.parent_id}}
        ]

        dependent_fields = self.schema._fields_with_dependant_calcs(self.parent_spec_name)
        self.updater.update_for(self.parent_spec_name, dependent_fields, self.update_id, start_agg)

        # delete link
        self.schema.delete_linkcollection_entry(
            self.parent_spec_name, self.parent_id, self.parent_field, self.link_id)
        # delete resource
        self.schema.delete_resource(spec_name, self.link_id)

        # delete any links to resource
        for linked_spec_name, linked_spec in self.schema.specs.items():
            for field_name, field in linked_spec.fields.items():
                if field.field_type == 'link' and field.target_spec_name == spec_name:
                    # find all resources with link to target id
                    for resource_data in self.schema.db['resource_%s' % linked_spec_name].find({field_name: self.schema.decodeid(self.link_id)}):
                        # call update_resource on resource
                        self.updater.update_fields(linked_spec_name, self.schema.encodeid(resource_data['_id']), {field_name: None})

                if field.field_type == 'linkcollection' and field.target_spec_name == spec_name:
                    # find all resources with link to target id
                    for resource_data in self.schema.db['resource_%s' % linked_spec_name].find({'%s._id' % field_name: self.schema.decodeid(self.link_id)}):
                        # call update_resource on resource
                        self.updater.delete_linkcollection_entry(linked_spec_name, resource_data['_id'], field_name, resource_id)

