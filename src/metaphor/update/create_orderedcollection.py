
from toposort import toposort


class CreateOrderedCollectionUpdate:
    def __init__(self, update_id, updater, schema, spec_name, parent_spec_name, parent_field, parent_id, data):
        self.update_id = update_id
        self.updater = updater
        self.schema = schema

        self.spec_name = spec_name
        self.spec = self.schema.specs[spec_name]
        self.parent_spec_name = parent_spec_name
        self.parent_field = parent_field
        self.parent_id = parent_id
        self.data = data

    def execute(self):
        dependent_fields = self.schema._fields_with_dependant_calcs(self.spec_name)
        extra_fields = None
        if dependent_fields:
            extra_fields = {'_dirty': {
                self.update_id: dependent_fields
            }}

        resource_id = self.schema.create_orderedcollection_entry(self.spec_name, self.parent_spec_name, self.parent_field, self.parent_id, self.data, extra_fields)

        start_agg = [
            {"$match": {"_id": self.schema.decodeid(resource_id)}}
        ]

        # TODO: collate all affected calcs together
        # update local resource calcs
        for field_name, field in self.spec.fields.items():
            if field.field_type == 'calc':
                self.updater.perform_single_update_aggregation(self.spec_name, self.spec_name, field_name, self.schema.calc_trees[self.spec_name, field_name], start_agg, [], self.update_id)

        for field_name in dependent_fields:
            self.updater.update_for_field(self.spec_name, field_name, self.update_id, start_agg)

        self.updater.update_for_resource(self.spec_name, self.update_id, start_agg)

        return resource_id
