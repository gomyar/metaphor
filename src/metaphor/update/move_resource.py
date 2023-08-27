
import re

from metaphor.lrparse.lrparse import parse_url
from metaphor.lrparse.lrparse import parse_canonical_url

from metaphor.lrparse.reverse_aggregator import ReverseAggregator


class MoveResourceUpdate:
    def __init__(self, updater, schema, from_path, to_path, target_resource, target_field_name, target_spec_name):
        self.updater = updater
        self.schema = schema

        self.from_path = from_path
        self.to_path = to_path
        self.target_resource = target_resource
        self.target_field_name = target_field_name
        self.target_spec_name = target_spec_name

    def execute(self):
        self.update_id = str(self.schema.create_update())

        self.perform_move()
        self.rebuild_canonical_urls()
        self.rebuild_grants()
        self.perform_update_for_moved_resources()
        self.perform_update_for_source_collections()
        self.perform_update_for_target_collection()
        self.schema.cleanup_update(self.update_id)

    def execute_root(self):
        self.update_id = str(self.schema.create_update())

        self.perform_move_to_root()
        self.rebuild_canonical_urls()
        self.rebuild_grants()
        self.perform_update_for_moved_resources()
        self.perform_update_for_source_collections()
        self.perform_update_for_target_collection()
        self.schema.cleanup_update(self.update_id)

    def affected_aggs(self):
        from_tree = parse_url(self.from_path, self.schema.root)
        from_agg, from_spec, _ = from_tree.aggregation(None)

        aggs = []
        for (calc_spec_name, calc_field_name), calc_tree in self.schema.calc_trees.items():
            if from_tree.get_resource_dependencies().intersection(calc_tree.get_resource_dependencies()):
                aggregations = ReverseAggregator(self.schema).get_for_resource(calc_tree, from_spec.name, None, calc_spec_name, calc_field_name)

                for aggregation in aggregations:
                    if aggregation:
                        aggs.append((calc_spec_name, calc_field_name, aggregation))
        return aggs

    def affected_aggs_to_path(self):
        to_tree = parse_url(self.to_path, self.schema.root)
        to_agg, to_spec, _ = to_tree.aggregation(None)

        aggs = []
        for (calc_spec_name, calc_field_name), calc_tree in self.schema.calc_trees.items():
            if to_tree.get_resource_dependencies().intersection(calc_tree.get_resource_dependencies()):
                aggregations = ReverseAggregator(self.schema).get_for_resource(calc_tree, to_spec.name, None, calc_spec_name, calc_field_name)

                for aggregation in aggregations:
                    if aggregation:
                        aggs.append((calc_spec_name, calc_field_name, aggregation))
        return aggs

    def all_dependent_fields_in_tree(self, spec_name):
        dependent_fields = self.schema._fields_with_dependant_calcs(spec_name)
        for field_name, field in self.schema.specs[spec_name].fields.items():
            if field.field_type in ('collection', 'orderedcollection'):
                dependent_fields.extend(self.all_dependent_fields_in_tree(field.target_spec_name))
        return list(set(dependent_fields))

    def perform_update_for_moved_resources(self, spec_name=None):
        if spec_name is None:
            from_tree = parse_url(self.from_path, self.schema.root)
            spec_name = from_tree.spec.name

        dependent_fields = self.schema._fields_with_dependant_calcs(spec_name)
        start_agg = [
            {"$match": {
                "_dirty.%s" % self.update_id: {"$exists": True},
            }}
        ]
        self.updater.update_for(spec_name, dependent_fields, self.update_id, start_agg)
        for field_name, field in self.schema.specs[spec_name].fields.items():
            if field.field_type in ('collection', 'orderedcollection'):
                self.perform_update_for_moved_resources(field.target_spec_name)

    def perform_move(self):
        from_tree = parse_url(self.from_path, self.schema.root)
        parent_canonical_url = self.target_resource['_canonical_url']

        aggregate_query, from_spec, is_aggregate = from_tree.aggregation(None)

        all_dependent_fields = self.all_dependent_fields_in_tree(from_tree.spec.name)

        # move all resources to target parent resource
        aggregate_query.extend([
            {"$project": {
                "_parent_type": self.target_spec_name,
                "_parent_id": self.target_resource['_id'],
                "_parent_field_name": self.target_field_name,
                "_parent_canonical_url": parent_canonical_url or "/",
                "_canonical_url": {
                    "$concat": [parent_canonical_url, "/", self.target_field_name, "/ID", {"$toString": "$_id"}],
                },
                "_dirty.%s" % self.update_id: all_dependent_fields,
            }},
            {"$merge": {
                "into": "resource_%s" % from_tree.spec.name,
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }},
        ])

        from_tree.root_collection().aggregate(aggregate_query)

    def perform_move_to_root(self):
        from_tree = parse_url(self.from_path, self.schema.root)
        aggregate_query, from_spec, is_aggregate = from_tree.aggregation(None)

        all_dependent_fields = self.all_dependent_fields_in_tree(from_tree.spec.name)

        # move all resources to target parent resource
        aggregate_query.extend([
            {"$project": {
                "_parent_type": "root",
                "_parent_id": None,
                "_parent_field_name": self.to_path,
                "_parent_canonical_url": "/",
                "_canonical_url": {
                    "$concat": ["/", self.to_path, "/ID", {"$toString": "$_id"}],
                },
                "_dirty.%s" % self.update_id: all_dependent_fields,
            }},
            {"$merge": {
                "into": "resource_%s" % from_tree.spec.name,
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }},
        ])

        from_tree.root_collection().aggregate(aggregate_query)

    def rebuild_canonical_urls(self):
        pass

    def rebuild_grants(self):
        pass

    def perform_update_for_source_collections(self):
        pass

    def perform_update_for_target_collection(self):
        pass
