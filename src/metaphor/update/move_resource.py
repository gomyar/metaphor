
import re

from metaphor.lrparse.lrparse import parse_url
from metaphor.lrparse.lrparse import parse_canonical_url

from metaphor.lrparse.reverse_aggregator import ReverseAggregator


class MoveResourceUpdate:
    def __init__(self, update_id, updater, schema, from_path, to_path, target_id, target_canonical_url, target_field_name, target_spec_name):
        self.update_id = update_id
        self.updater = updater
        self.schema = schema

        self.from_path = from_path
        self.to_path = to_path
        self.target_id = target_id
        self.target_canonical_url = target_canonical_url
        self.target_field_name = target_field_name
        self.target_spec_name = target_spec_name

    def execute(self):
        self.mark_update_delete()
        self.run_update_for_marked()

        self.move_and_mark_undeleted()
        self.rebuild_child_canonical_urls()
        self.rebuild_grants()

        self.mark_undeleted()

        self.run_update_for_marked()

        self.remove_moving_flag()

    def mark_update_delete(self):
        from_tree = parse_url(self.from_path, self.schema.root)

        aggregate_query = from_tree.create_aggregation(None)
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
                "into": "metaphor_resource",
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }},
        ]

        self.schema.db.metaphor_resource.aggregate(mark_query)

        # mark all children
        child_query = [
            {"$match": {"_moving": self.update_id}},
            {"$lookup": {
                "from": "metaphor_resource",
                "as": "_all_children",
                "let": {"id": {"$concat": ["^", "$_canonical_url"]}},
                "pipeline": [
                    {"$match": { "$expr": {
                        "$regexMatch": {
                            "input": "$_parent_canonical_url",
                            "regex": "$$id",
                        }
                    }}}
                ]
            }},
            {"$unwind": "$_all_children"},
            {"$replaceRoot": {"newRoot": "$_all_children"}},
            {"$project": {
                "_deleted": {"$toBool": True},
                "_moving": self.update_id,
                "_moving_child": {"$toBool": True},
            }},
            {"$merge": {
                "into": "metaphor_resource",
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }},
        ]

        self.schema.db.metaphor_resource.aggregate(child_query)

    def rebuild_child_canonical_urls(self):
        aggregation = [
            {"$match": {
                "_moving": self.update_id,
                "_moving_child": True,
            }},
            {"$graphLookup": {
                "from": "metaphor_resource",
                "as": "_parent_canonical_url",
                "startWith": "$_parent_id",
                "connectFromField": "_parent_id",
                "connectToField": "_id",
                "depthField": "_depth",
            }},
            {"$addFields": {
                "_parent_canonical_url": {
                    "$sortArray": {
                        "input": "$_parent_canonical_url",
                        "sortBy": {"_depth": 1},
                    }
                }
            }},
            {"$addFields": {
                '_parent_canonical_url': {
                    '$reduce': {
                        'input': '$_parent_canonical_url',
                        'initialValue': '',
                        'in': {
                            '$concat': [
                                '/',
                                '$$this._parent_field_name',
                                '/ID',
                                {"$toString": '$$this._id'},
                                '$$value',
                                ]
                        }
                    }
                }
            }},
            {"$addFields": {
                '_canonical_url': {
                    "$concat": ["$_parent_canonical_url", "/", "$_parent_field_name", "/ID", {"$toString": "$_id"}]
                }
            }},
            {"$project": {
                "_canonical_url": 1,
                "_parent_canonical_url": 1,
                "_deleted": {"$toBool": False},
                "_grants": [],
            }},
            {"$merge": {
                "into": "metaphor_resource",
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }},
        ]
        self.schema.db.metaphor_resource.aggregate(aggregation)

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
                "_parent_canonical_url": self.target_canonical_url or '/',
                "_parent_field_name": self.target_field_name,
            }},
            {"$merge": {
                "into": "metaphor_resource",
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }},
        ]
        self.schema.db.metaphor_resource.aggregate(aggregation)

    def mark_undeleted(self):
        self.schema.db.metaphor_resource.update_many(
            {"_moving": self.update_id},
            {"$unset": {"_deleted": ""}})

    def remove_moving_flag(self):
        self.schema.db.metaphor_resource.update_many(
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

    def rebuild_grants(self):
        pass


