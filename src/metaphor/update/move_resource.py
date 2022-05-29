
import re

from metaphor.lrparse.lrparse import parse_url
from metaphor.lrparse.lrparse import parse_canonical_url

from metaphor.lrparse.reverse_aggregator import ReverseAggregator


class MoveResourceUpdate:
    def __init__(self, updater, schema, parent_id, parent_spec_name, parent_field_name, to_path, from_path=None):
        self.updater = updater
        self.schema = schema

        self.parent_id = parent_id
        self.parent_spec_name = parent_spec_name
        self.parent_field_name = parent_field_name
        self.to_path = to_path
        self.from_path = from_path

    def from_path_agg(self):
        from_tree = parse_url(self.from_path, self.schema.root)
        aggregate_query, from_spec, is_aggregate = from_tree.aggregation(None)
        return aggregate_query, from_spec, is_aggregate

    def to_path_agg(self):
        to_tree = parse_url(self.to_path, self.schema.root)
        aggregate_query, to_spec, is_aggregate = to_tree.aggregation(None)
        return aggregate_query, to_spec, is_aggregate

    def affected_aggs(self):
        from_tree = parse_url(self.from_path, self.schema.root)
        from_agg, from_spec, _ = self.from_path_agg()

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

    def affected_ids_to_path(self):
        to_tree = parse_url(self.to_path, self.schema.root)
        to_agg, to_spec, _ = self.to_path_agg()

        affected_ids = set()
        for calc_spec_name, calc_field_name, aggregation in self.affected_aggs_to_path():
            affected_aggregation = to_agg + aggregation
            affected_aggregation.append({"$project": {"_id": True}})
            cursor = to_tree.root_collection().aggregate(
                affected_aggregation)
            found = set((calc_spec_name, calc_field_name, r['_id']) for r in cursor)
            affected_ids.update(found)
        return affected_ids

    def affected_ids(self):
        from_tree = parse_url(self.from_path, self.schema.root)
        from_agg, from_spec, _ = self.from_path_agg()

        affected_ids = set()
        for calc_spec_name, calc_field_name, aggregation in self.affected_aggs():
            affected_aggregation = from_agg + aggregation
            affected_aggregation.append({"$project": {"_id": True}})
            cursor = from_tree.root_collection().aggregate(
                affected_aggregation)
            found = set((calc_spec_name, calc_field_name, r['_id']) for r in cursor)
            affected_ids.update(found)
        return affected_ids

    def execute(self):
        # collect ids
        affected_ids = self.affected_ids()

        # perform update
        self.perform_move()

        affected_ids_to_path = self.affected_ids_to_path()
        affected_ids.update(affected_ids_to_path)

        # perform update
        from_tree = parse_url(self.from_path, self.schema.root)
        spec = from_tree.spec
        for calc_spec_name, calc_field_name, affected_id in affected_ids:
            affected_id = self.schema.encodeid(affected_id)
            self.updater.update_calc(calc_spec_name, calc_field_name, affected_id)
            self.updater._recalc_for_field_update(spec, calc_spec_name, calc_field_name, affected_id)

        return None

    def _read_parent_canonical_url(self):
        if self.parent_id:
            parent = self.schema.db['resource_%s' % self.parent_spec_name].find_one(
                {'_id': self.parent_id},
                {'_canonical_url': 1})
            return parent['_canonical_url']
        else:
            return ''

    def perform_move(self):
        parent_canonical_url = self._read_parent_canonical_url()
        from_tree = parse_url(self.from_path, self.schema.root)

        aggregate_query, from_spec, is_aggregate = from_tree.aggregation(None)

        aggregate_query.extend([
            {"$project": {
                "_parent_type": self.parent_spec_name,
                "_parent_id": self.parent_id,
                "_parent_field_name": self.parent_field_name,
                "_parent_canonical_url": parent_canonical_url or "/",
                "_canonical_url": {
                    "$concat": [parent_canonical_url, "/", self.parent_field_name, "/ID", {"$toString": "$_id"}],
                }
            }},
            {"$merge": {
                "into": "resource_%s" % from_tree.spec.name,
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard",
            }},
        ])

        from_tree.root_collection().aggregate(aggregate_query)
