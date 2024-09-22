
class DeleteLinkCollectionUpdate:
    def __init__(self, update_id, updater, schema, parent_spec_name, parent_id, parent_field, link_id):
        self.update_id = update_id
        self.updater = updater
        self.schema = schema
        self.parent_spec_name = parent_spec_name
        self.parent_id = parent_id
        self.parent_field = parent_field
        self.link_id = link_id

    def execute(self):
        # mark link deleted
        self.schema.mark_link_collection_item_deleted(self.parent_spec_name, self.parent_id, self.parent_field, self.link_id)

        # update
        start_agg = [
            {"$match": {"_id": self.parent_id}}
        ]

        dependent_fields = self.schema._fields_with_dependant_calcs(self.parent_spec_name)
        self.updater.update_for(self.parent_spec_name, dependent_fields, self.update_id, start_agg)

        # delete link
        self.schema.delete_linkcollection_entry(
            self.parent_spec_name, self.parent_id, self.parent_field, self.link_id)
