
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

        self.perform_move()
        self.unset_field_for_all_children("_deleted")

        self.run_update_for_marked()
        self.unset_field_for_all_children("_moving")

    def unset_field_for_all_children(self, field_name):
        from_tree = parse_url(self.from_path, self.schema.root)

        aggregate_query = from_tree.create_aggregation()
        from_spec = from_tree.infer_type()

        all_child_specs = self._collect_child_spec_names(from_spec.name)
        for child_spec_name in all_child_specs + [from_spec.name]:
            self.schema.db['resource_%s' % child_spec_name].update_many(
                {"_moving": self.update_id},
                {"$unset": {field_name: ""}},
            )

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
                "into": "resource_%s" % from_spec.name,
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }},
        ]

        from_tree.root_collection().aggregate(mark_query)

        def mark_children_moving_deleted(spec_name):
            children = self._child_specs(spec_name)
            for child_spec_name in children:
                self.schema.db['resource_%s' % child_spec_name].aggregate([
                    {"$match": {
                        "_moving": self.update_id,
                    }},
                    {"$project": {
                        "_deleted": {"$toBool": True},
                        "_moving": self.update_id,
                    }},
                    {"$merge": {
                        "into": "resource_%s" % child_spec_name,
                        "on": "_id",
                        "whenMatched": "merge",
                        "whenNotMatched": "discard",
                    }},
                ])
                mark_children_moving_deleted(child_spec_name)

        mark_children_moving_deleted(from_spec.name)

    def perform_move(self):
        from_tree = parse_url(self.from_path, self.schema.root)

        aggregate_query = from_tree.create_aggregation()
        from_spec = from_tree.infer_type()
        is_aggregate = from_tree.is_collection()

        if '/' in self.to_path:
            parent_path, field_name = self.to_path.rsplit('/', 1)
            parent_tree = parse_url(parent_path, self.schema.root)

            cursor = parent_tree.root_collection().aggregate(
                parent_tree.create_aggregation())
            parent_resource = next(cursor)
            parent_id = parent_resource['_id']
        else:
            parent_id = None
            field_name = self.to_path

        move_agg = [
            {"$match": {"_moving": self.update_id}},
            {"$project": {
                "_parent_id": parent_id,
                "_parent_field_name": field_name,
            }},
            {"$merge": {
                "into": "resource_%s" % from_spec.name,
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }},
        ]
        self.schema.db['resource_%s' % from_spec.name].aggregate(move_agg)

    def run_update_for_marked(self):
        from_tree = parse_url(self.from_path, self.schema.root)
        from_spec = from_tree.infer_type()
        all_child_specs = self._collect_child_spec_names(from_spec.name)
        all_specs = set(all_child_specs + [from_spec.name])
        for spec_name in all_specs:
            self.perform_update_for_moved_resources(spec_name)

    def _collect_child_spec_names(self, spec_name):
        child_specs = []
        for field_name, field in self.schema.specs[spec_name].fields.items():
            if field.field_type in ('collection', 'orderedcollection'):
                child_specs.append(field.target_spec_name)
                child_specs.extend(self._collect_child_spec_names(field.target_spec_name))
        return list(set(child_specs))

    def _child_specs(self, spec_name):
        child_specs = []
        for field_name, field in self.schema.specs[spec_name].fields.items():
            if field.field_type in ('collection', 'orderedcollection'):
                child_specs.append(field.target_spec_name)
        return list(set(child_specs))

    def _perform_move(self):
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
                "into": "resource_%s" % self.target_spec_name,
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }},
        ]
        self.schema.db['resource_%s' % self.target_spec_name].aggregate(aggregation)

    def perform_update_for_moved_resources(self, spec_name):
        dependent_fields = self.schema._fields_with_dependant_calcs(spec_name)
        start_agg = [
            {"$match": {
                "_moving": self.update_id,
            }},
        ]
        self.updater.update_for(spec_name, dependent_fields, self.update_id, start_agg)
