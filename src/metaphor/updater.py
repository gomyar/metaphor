
from werkzeug.security import generate_password_hash

from metaphor.lrparse.reverse_aggregator import ReverseAggregator
from metaphor.lrparse.lrparse import parse_canonical_url

from metaphor.update.create_resource import CreateResourceUpdate
from metaphor.update.fields_update import FieldsUpdate
from metaphor.update.delete_resource import DeleteResourceUpdate
from metaphor.update.delete_linkcollection import DeleteLinkCollectionUpdate
from metaphor.update.delete_orderedcollection import DeleteOrderedCollectionUpdate
from metaphor.update.create_linkcollection import CreateLinkCollectionUpdate
from metaphor.update.create_orderedcollection import CreateOrderedCollectionUpdate
from metaphor.update.move_resource import MoveResourceUpdate
from metaphor.update_aggregation import create_update_aggregation

import logging

log = logging.getLogger('metaphor')


class Updater(object):
    def __init__(self, schema):
        self.schema = schema

    def update_for(self, spec_name, field_names, update_id, start_agg):
        dependent_calcs = self.schema.all_dependent_calcs_for(spec_name, field_names)
        self._perform_aggregation_for_dependent_calcs(dependent_calcs, start_agg, spec_name, update_id)

    def update_for_field(self, spec_name, field_name, update_id, start_agg):
        # find calcs for field
        dependent_calcs = self.schema._dependent_calcs_for_field(spec_name, field_name)
        self._perform_aggregation_for_dependent_calcs(dependent_calcs, start_agg, spec_name, update_id)

    def update_for_resource(self, spec_name, update_id, start_agg):
        # find calcs for field
        dependent_calcs = self.schema._dependent_calcs_for_resource(spec_name)
        self._perform_aggregation_for_dependent_calcs(dependent_calcs, start_agg, spec_name, update_id)

    def _perform_aggregation_for_dependent_calcs(self, dependent_calcs, start_agg, spec_name, update_id):
        for (calc_spec_name, calc_field_name), calc in dependent_calcs.items():
            # create update_aggs
            dependent_aggs = self.build_reverse_aggregations_to_calc(calc_spec_name, calc_field_name, self.schema.specs[spec_name], None)
            if not dependent_aggs and calc_spec_name == spec_name:
                # this finds calcs in the same resource as the original resource
                dependent_aggs = [[]]
            unique_aggs = []
            for reverse_agg in dependent_aggs:
                if reverse_agg not in unique_aggs:
                    unique_aggs.append(reverse_agg)
            for reverse_agg in unique_aggs:
                self.perform_single_update_aggregation(spec_name, calc_spec_name, calc_field_name, calc, start_agg, reverse_agg, update_id)

    def perform_single_update_aggregation(self, spec_name, calc_spec_name, calc_field_name, calc, start_agg, reverse_agg, update_id):
        # run reverse_agg + update_agg + calc_field_dirty_agg
            # run update for altered calc
        update_agg = calc.create_aggregation()
        if calc.is_primitive():
            calc_field_dirty_agg = [
                {"$project": {
                    calc_field_name: "$_update._val",
                    "_id": 1,
                    "_dirty.%s" % update_id: {"$setUnion": [
                        {"$ifNull": ["$_dirty.%s" % update_id, []]},
                        [calc_field_name]]},
                }},
            ]
        else:
            calc_field_dirty_agg = [
                {"$project": {
                    "_id": 1,
                    calc_field_name: "$_update",
                }},
                {"$project": {
                    "_id": 1,
                    "%s._id" % calc_field_name: 1,
                    "_dirty.%s" % update_id: {"$setUnion": [
                        {"$ifNull": ["$_dirty.%s" % update_id, []]},
                        [calc_field_name]]},
                }},
            ]
        merge_agg = [
            {"$merge": {
                "into": "metaphor_resource",
                "on": "_id",
                "whenNotMatched": "discard",
            }}
        ]
        # nest this into dependent agg
        nested_agg = [
            {"$lookup": {
                "from": "metaphor_resource",
                "as": "_update",
                "let": {"id": "$_id"},
                "pipeline": [
                    {"$match": {"$expr": {
                        "$and": [
                            {"$eq": ["$_id", "$$id"]},
                            {"$eq": ["$_type", calc_spec_name]},
                        ]
                    }}},
                ] + update_agg,
            }},
        ]
        if not calc.is_collection():
            nested_agg.extend([
                {"$unwind": {"path": "$_update", "preserveNullAndEmptyArrays": True}},
                {"$addFields": {"_update": {"$ifNull": ["$_update", {"_val": None}]}}},
            ])
        type_agg = [{"$match": {"_type": spec_name}}]
        self.schema.db["metaphor_resource"].aggregate(type_agg + start_agg + reverse_agg + nested_agg + calc_field_dirty_agg + merge_agg)

        # update for subsequent calcs, if any
        new_start_agg = [
            {"$match": {"$expr": {
                "$and": [
                    {"$in": [calc_field_name, {"$ifNull": ["$_dirty.%s" % update_id, []]}]},
                    {"$eq": ["$_type", calc_spec_name]},
                ]
            }}}
        ]
        self.update_for_field(calc_spec_name, calc_field_name, update_id, new_start_agg)

    def build_reverse_aggregations_to_calc(self, calc_spec_name, calc_field_name, resource_spec, resource_id):
        calc_tree = self.schema.calc_trees[calc_spec_name, calc_field_name]
        aggregations = ReverseAggregator(self.schema).get_for_resource(
            calc_tree,
            resource_spec.name,
            None,
            calc_spec_name,
            calc_field_name)
        return aggregations

    def update_calc_for_single_resource_change(self, calc_spec_name, calc_field_name, updated_resource_name, updated_resource_id):
        calc_tree = self.schema.calc_trees[calc_spec_name, calc_field_name]
        updated_resource_spec = self.schema.specs[updated_resource_name]
        for aggregation in self.build_reverse_aggregations_to_calc(calc_spec_name, calc_field_name, updated_resource_spec, updated_resource_id):
            if aggregation:
                self._run_update_merge(calc_spec_name, calc_field_name, calc_tree, aggregation, updated_resource_spec)

    def _run_update_merge(self, calc_spec_name, calc_field_name, calc_tree, match_agg, updated_resource_spec):
        agg = create_update_aggregation(calc_spec_name, calc_field_name, calc_tree, match_agg)
        self.schema.db['metaphor_resource'].aggregate([{"$match": {"_type": updated_resource_spec.name}}] + agg)

    def create_resource(self, spec_name, parent_spec_name, parent_field_name,
                        parent_id, fields, grants=None):
        update_id = str(self.schema.create_update())
        return_val = CreateResourceUpdate(update_id, self, self.schema, spec_name, fields, parent_field_name, parent_spec_name,
                 parent_id, grants).execute()
        self.schema.cleanup_update(update_id)
        return return_val

    def create_linkcollection_entry(self, parent_spec_name, parent_id, parent_field, link_id):
        update_id = str(self.schema.create_update())
        CreateLinkCollectionUpdate(update_id, self, self.schema, parent_spec_name, parent_id, parent_field, link_id).execute()
        self.schema.cleanup_update(update_id)

    def create_orderedcollection_entry(self, spec_name, parent_spec_name, parent_field, parent_id, data, grants=None):
        update_id = str(self.schema.create_update())
        return_val = CreateOrderedCollectionUpdate(update_id, self, self.schema, spec_name, parent_spec_name, parent_field, parent_id, data, grants).execute()
        self.schema.cleanup_update(update_id)
        return return_val

    def delete_resource(self, spec_name, resource_id, parent_spec_name, parent_field_name):
        update_id = str(self.schema.create_update())
        return_val = DeleteResourceUpdate(update_id, self, self.schema, spec_name, resource_id, parent_spec_name, parent_field_name).execute()
        self.schema.cleanup_update(update_id)
        return return_val

    def delete_linkcollection_entry(self, parent_spec_name, parent_id, parent_field, link_id):
        update_id = str(self.schema.create_update())
        return_val = DeleteLinkCollectionUpdate(update_id, self, self.schema, parent_spec_name, parent_id, parent_field, link_id).execute()
        self.schema.cleanup_update(update_id)
        return return_val

    def delete_orderedcollection_entry(self, parent_spec_name, parent_id, parent_field, link_id):
        update_id = str(self.schema.create_update())
        return_val = DeleteOrderedCollectionUpdate(update_id, self, self.schema, parent_spec_name, parent_id, parent_field, link_id).execute()
        self.schema.cleanup_update(update_id)
        return return_val

    def update_fields(self, spec_name, resource_id, fields):
        update_id = str(self.schema.create_update())
        return_val = FieldsUpdate(update_id, self, self.schema, spec_name, resource_id, fields).execute()
        self.schema.cleanup_update(update_id)
        return return_val

    def move_resource(self, from_path, to_path, target_id, target_canonical_url, target_field_name, target_spec_name):
        update_id = str(self.schema.create_update())
        return_val = MoveResourceUpdate(update_id, self, self.schema, from_path, to_path, target_id, target_canonical_url, target_field_name, target_spec_name).execute()
        self.schema.cleanup_update(update_id)
        return return_val

    def move_resource_to_root(self, from_path, to_path):
        update_id = str(self.schema.create_update())
        return_val = MoveResourceUpdate(update_id, self, self.schema, from_path, to_path, None, None, to_path).execute_root()
        self.schema.cleanup_update(update_id)
        return return_val

    def create_user(self, username, password):
        pw_hash = generate_password_hash(password)
        return self.create_resource(
            'user',
            'root',
            'users',
            None,
            {'username': username, 'password': pw_hash, 'admin': True},
            self.schema.read_root_grants('users'))

    def delete_user(self, username):
        user = self.schema.db['metaphor_resource'].find_one({'_type': 'user', 'username': username})
        self.delete_resource('user', self.schema.encodeid(user['_id']), user['_parent_type'], user['_parent_field_name'])
