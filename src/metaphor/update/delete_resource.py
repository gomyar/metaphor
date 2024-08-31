
import time
import gevent


class DeleteResourceUpdate:
    def __init__(self, update_id, updater, schema, spec_name, resource_id, parent_spec_name, parent_field_name):
        self.update_id = update_id
        self.updater = updater
        self.schema = schema
        self.spec_name = spec_name
        self.resource_id = resource_id
        self.parent_spec_name = parent_spec_name
        self.parent_field_name = parent_field_name

    def execute(self):
        # mark resource as _deleted
        original_resource = self.schema.mark_resource_deleted(self.spec_name, self.resource_id)

        # perform updates
        # find and update dependent calcs
        start_agg = [
            {"$match": {"_id": self.schema.decodeid(self.resource_id)}}
        ]

        dependent_fields = self.schema._fields_with_dependant_calcs(self.spec_name)
        self.updater.update_for(self.spec_name, dependent_fields, self.update_id, start_agg)

        # delete resource
        self.schema.delete_resource(self.spec_name, self.resource_id)

        # delete any links to resource
        for linked_spec_name, spec in self.schema.specs.items():
            for field_name, field in spec.fields.items():
                if field.field_type == 'link' and field.target_spec_name == self.spec_name:
                    # find all resources with link to target id
                    for resource_data in self.schema.db['metaphor_resource'].find({"_type": linked_spec_name, field_name: self.schema.decodeid(self.resource_id)}):
                        # call update_resource on resource
                        self.updater.update_fields(linked_spec_name, self.schema.encodeid(resource_data['_id']), {field_name: None})

                if field.field_type == 'linkcollection' and field.target_spec_name == self.spec_name:
                    # find all resources with link to target id
                    for resource_data in self.schema.db['metaphor_resource'].find({"_type": linked_spec_name, '%s._id' % field_name: self.schema.decodeid(self.resource_id)}):
                        # call update_resource on resource
                        self.updater.delete_linkcollection_entry(linked_spec_name, resource_data['_id'], field_name, self.resource_id)

        # check if resource is read grant
        if self.spec_name == 'grant':
            self.updater._remove_grants(self.resource_id, original_resource['url'])

        # delete child resources
        for field_name, field in spec.fields.items():
            if field.field_type == 'collection':
                for child_resource in self.schema.db['metaphor_resource'].find({"_type": field.target_spec_name, '_parent_id': self.schema.decodeid(self.resource_id)}, {'_id': 1}):
                    self.updater.delete_resource(field.target_spec_name, self.schema.encodeid(child_resource['_id']), self.spec_name, field_name)
