
import re

from metaphor.lrparse.lrparse import parse_url

from metaphor.lrparse.reverse_aggregator import ReverseAggregator


class MoveResourceUpdate:
    def __init__(self, update_id, updater, schema, from_path, to_path, target_id, target_field_name, target_spec_name):
        self.update_id = update_id
        self.updater = updater
        self.schema = schema

        self.from_path = from_path
        self.to_path = to_path
        self.target_id = target_id
        self.target_field_name = target_field_name
        self.target_spec_name = target_spec_name

    def execute(self):
        self.mark_update_delete()
        self.run_update_for_marked()

        self.move_and_mark_undeleted()

        self.mark_undeleted()

        self.run_update_for_marked()

        self.remove_moving_flag()

    def mark_update_delete(self):
        from_tree = parse_url(self.from_path, self.schema.root)

        aggregate_query = from_tree.create_aggregation()
        from_spec = from_tree.infer_type()
        is_aggregate = from_tree.is_collection()

        # mark all first-level resources
        mark_query = aggregate_query + [
            {"$match": {"_moving": {"$exists": False}}},
            {"$project": {
                "_deleted": {"$toBool": True},
                "_moving": self.update_id,
            }},
            {"$merge": {
                "into": "resource_%s" % to_spec.name,
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }},
        ]

        self.schema.db['resource_%s' % to_spec.name].aggregate(mark_query)

        # mark all children
        child_query = [
            {"$match": {"_moving": self.update_id}},
            {"$graphLookup": {
                "from": "resource_%s" % from_spec.name,
                "as": "_all_children",
                "startWith": "$_parent_id",
                "connectFromField": "_parent_id",
                "connectToField": "_id",
                "depthField": "_depth",
            }},

            {"$unwind": "$_all_children"},
            {"$replaceRoot": {"newRoot": "$_all_children"}},
            {"$project": {
                "_deleted": {"$toBool": True},
                "_moving": self.update_id,
                "_moving_child": {"$toBool": True},
            }},
            {"$merge": {
                "into": "resource_%s" % to_spec.name,
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }},
        ]

        self.schema.db['resource_%s' % from_spec.name].aggregate(child_query)

    def run_update_for_marked(self):
        from_tree = parse_url(self.from_path, self.schema.root)
        from_spec = from_tree.infer_type()
        all_child_resources = self._collect_child_resources(from_spec.name)
        all_resources = set(all_child_resources + [from_spec.name])
        for spec_name in all_resources:
            self.perform_update_for_moved_resources(spec_name)

    def _collect_child_resources(self, spec_name):
        child_specs = []
        for field_name, field in self.schema.specs[spec_name].fields.items():
            if field.field_type in ('collection', 'orderedcollection'):
                child_specs.append(field.target_spec_name)
                child_specs.extend(self._collect_child_resources(field.target_spec_name))
        return list(set(child_specs))

    def move_and_mark_undeleted(self):
        aggregation = [
            {"$match": {
                "_moving": self.update_id,
                "_moving_child": {"$exists": False},
            }},
            {"$project": {
                "_parent_id": self.target_id or None,
                "_parent_type": self.target_spec_name or 'root',
                "_parent_field_name": self.target_field_name,
            }},
            {"$merge": {
                "into": "resource_%s" % to_spec.name,
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }},
        ]
        self.schema.db['resource_%s' % from_spec.name].aggregate(aggregation)

    def mark_undeleted(self):
        # TODO: replace with graphlookup
        self.schema.db.resource_resource.update_many(
            {"_moving": self.update_id},
            {"$unset": {"_deleted": ""}})

    def remove_moving_flag(self):
        self.schema.db.resource_resource.update_many(
            {"_moving": self.update_id},
            {"$unset": {"_moving": "", "_moving_child": ""}})

    def perform_update_for_moved_resources(self, spec_name):
        dependent_fields = self.schema._fields_with_dependant_calcs(spec_name)
        start_agg = [
            {"$match": {
                "_type": spec_name,
                "_moving": self.update_id,
            }},
        ]
        self.updater.update_for(spec_name, dependent_fields, self.update_id, start_agg)


