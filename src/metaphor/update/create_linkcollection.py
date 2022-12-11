
class CreateLinkCollectionUpdate:
    def __init__(self, updater, schema, parent_spec_name, parent_id, parent_field, link_id):
        self.updater = updater
        self.schema = schema

        self.parent_spec_name = parent_spec_name
        self.parent_id = parent_id
        self.parent_field = parent_field
        self.link_id = link_id

    def execute(self):
        update_id = str(self.schema.create_update())

        self.schema.create_linkcollection_entry(self.parent_spec_name, self.parent_id, self.parent_field, self.link_id)

        start_agg = [
            {"$match": {"_id": self.schema.decodeid(self.parent_id)}}
        ]

        # update local resource calcs
#        parent_spec = self.schema.specs[self.parent_spec_name]
#        for field_name, field in parent_spec.fields.items():
#            if field.field_type == 'calc':
#                self.updater.perform_single_update_aggregation(self.parent_spec_name, self.parent_spec_name, field_name, self.schema.calc_trees[self.parent_spec_name, field_name], start_agg, [], update_id)

        self.updater.update_for_field(self.parent_spec_name, self.parent_field, update_id, start_agg)
        self.schema.cleanup_update(update_id)
