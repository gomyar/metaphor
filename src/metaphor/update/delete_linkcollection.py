
class DeleteLinkCollectionUpdate:
    def __init__(self, updater, schema, parent_spec_name, parent_id, parent_field, link_id):
        self.updater = updater
        self.schema = schema
        self.parent_spec_name = parent_spec_name
        self.parent_id = parent_id
        self.parent_field = parent_field
        self.link_id = link_id

    def execute(self):
        update_id = str(self.schema.create_update())

        # mark link deleted
        self.schema.mark_link_collection_item_deleted(self.parent_spec_name, self.parent_id, self.parent_field, self.link_id)

        # update
        start_agg = [
            {"$match": {"_id": self.parent_id}}
        ]

        dependent_fields = self.schema._fields_with_dependant_calcs(self.parent_spec_name)
        self.updater.update_for(self.parent_spec_name, dependent_fields, update_id, start_agg)

        # delete link
        self.schema.delete_linkcollection_entry(
            self.parent_spec_name, self.parent_id, self.parent_field, self.link_id)

        # cleanup update
        self.schema.cleanup_update(update_id)

    def _execute(self):
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

        # update affected resources
        for affected_ids, calc_spec_name, calc_field_name in cursors:
            for affected_id in affected_ids:
                affected_id = self.schema.encodeid(affected_id)
                self.updater.update_calc(calc_spec_name, calc_field_name, affected_id)

        # recalc local calcs
        for field_name, field_spec in parent_spec.fields.items():
            if field_spec.field_type == 'calc':
                self.updater.update_calc(self.parent_spec_name, field_name, self.link_id)

        # run update
        return self.link_id
