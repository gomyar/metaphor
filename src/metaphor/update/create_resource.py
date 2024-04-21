
from toposort import toposort


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
        update_id = str(self.schema.create_update())

        dependent_fields = self.schema._fields_with_dependant_calcs(self.spec_name)
        extra_fields = None
        if dependent_fields:
            extra_fields = {'_dirty': {
                update_id: dependent_fields
            }}

        # create resource
        resource_id = self.schema.insert_resource(
            self.spec_name, self.fields, self.parent_field_name, self.parent_spec_name,
            self.parent_id, self.grants, extra_fields)

        # find and update dependent calcs
        start_agg = [
            {"$match": {"_id": self.schema.decodeid(resource_id)}}
        ]

        # create grant links
        for grant in self.grants:
            self.schema.db['metaphor_link'].insert_one({"_type": self.spec_name, "_from_id": self.schema.decodeid(resource_id), "_from_field_name": "_grants", "_to_id": grant})

        # TODO: collate all affected calcs together
        # update local resource calcs
        for field_name, field in self.spec.fields.items():
            if field.field_type == 'calc':
                self.updater.perform_single_update_aggregation(self.spec_name, self.spec_name, field_name, self.schema.calc_trees[self.spec_name, field_name], start_agg, [], update_id)

        self.updater.update_for(self.spec_name, dependent_fields, update_id, start_agg)

        # check if new resource is read grant
        if self.spec_name == 'grant' and self.fields['type'] == 'read':
            self.updater._update_grants(resource_id, self.fields['url'])

        # cleanup update
        self.schema.cleanup_update(update_id)

        return resource_id
