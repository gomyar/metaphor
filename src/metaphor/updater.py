
from werkzeug.security import generate_password_hash

from metaphor.lrparse.reverse_aggregator import ReverseAggregator
from metaphor.lrparse.lrparse import parse_canonical_url

from metaphor.update.create_resource import CreateResourceUpdate
from metaphor.update.fields_update import FieldsUpdate
from metaphor.update.delete_resource import DeleteResourceUpdate
from metaphor.update.delete_linkcollection import DeleteLinkCollectionUpdate
from metaphor.update.create_linkcollection import CreateLinkCollectionUpdate
from metaphor.update.move_resource import MoveResourceUpdate
from metaphor.update.move_link import MoveLinkUpdate
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

    def update_calc_for_local_fields(self, calc_spec_name, calc_field_name, updated_resource_name, updated_resource_id):
        calc_tree = self.schema.calc_trees[calc_spec_name, calc_field_name]
        updated_resource_spec = self.schema.specs[calc_spec_name]
        aggregation = [{"$match": {"_id": self.schema.decodeid(updated_resource_id)}}]
        self._run_update_merge(calc_spec_name, calc_field_name, calc_tree, aggregation, updated_resource_spec)


    def _calculate_aggregated_resource(self, resource_name, field_name, calc_tree, resource_id):
        match_agg = [{"$match": {"_id": self.schema.decodeid(resource_id)}}]
        self._run_update_merge(resource_name, field_name, calc_tree, match_agg)

    def _run_update_merge(self, calc_spec_name, calc_field_name, calc_tree, match_agg, updated_resource_spec):
        agg = create_update_aggregation(calc_spec_name, calc_field_name, calc_tree, match_agg)
        self.schema.db['metaphor_resource'].aggregate([{"$match": {"_type": updated_resource_spec.name}}] + agg)

    def _perform_updates_for_affected_calcs(self, spec, resource_id, calc_spec_name, calc_field_name):
        affected_ids = self.get_affected_ids_for_resource(calc_spec_name, calc_field_name, spec, resource_id)
        for affected_id in affected_ids:
            affected_id = self.schema.encodeid(affected_id)
            self.update_calc(calc_spec_name, calc_field_name, affected_id)
            self._recalc_for_field_update(self.schema.specs[calc_spec_name], calc_spec_name, calc_field_name, affected_id)

    def _recalc_for_field_update(self, spec, field_spec_name, field_name, resource_id):
        field_dep = "%s.%s" % (field_spec_name, field_name)
        # find foreign dependencies
        for (calc_spec_name, calc_field_name), calc_tree in self.schema.calc_trees.items():
            if field_dep in calc_tree.get_resource_dependencies():
                self._perform_updates_for_affected_calcs(spec, resource_id, calc_spec_name, calc_field_name)
            elif spec.fields.get(field_name) and spec.fields[field_name].field_type == 'link':
                if spec.name in calc_tree.get_resource_dependencies():
                    self._perform_updates_for_affected_calcs(spec, resource_id, calc_spec_name, calc_field_name)
            elif spec.fields.get(field_name) and spec.fields[field_name].field_type == 'calc':
                if not spec.fields[field_name].infer_type().is_primitive():
                    if spec.name in calc_tree.get_resource_dependencies():
                        self._perform_updates_for_affected_calcs(spec, resource_id, calc_spec_name, calc_field_name)

        # find local dependencies (other calcs in same resource)
        for field_name, field in spec.fields.items():
            if field.field_type == 'calc' and field_dep in field.get_resource_dependencies():
                self.update_calc(spec.name, field_name, resource_id)
                self._recalc_for_field_update(spec, spec.name, field_name, resource_id)

        return resource_id

    def _update_grants(self, grant_id, url):
        # aggregate target resources
#        from_tree = parse_canonical_url(url, self.schema.root)
#        aggregate_query, from_spec, is_aggregate = from_tree.aggregation(None)

        # update _parent_id, _parent_field to target parent resource for all resources

        # update grants
#        for field_name, field in from_spec.fields.items():
#            if field.field_type == 'collection':
#                aggregate_query.extend([
#                    {"$lookup": {
#                        "from": "resource_%s" % field.target_spec_name,
#                        "as": "_grant_%s" % field.target_spec_name,
#                        "let": {"id": "$_id"},
#                        "pipeline": [
#                            {"$match": {
#                                "$expr": {"$and": [
#                                    "$eq": ["$_parent_id": "$$id"]},
#                                    "$eq": ["$_parent_field_name": field_name]},
#                                ])
#                            }},
#                        ]
#                    }}
#                ])

        # descend into children


        self.schema.db['metaphor_resource'].update_many({'_canonical_url': {"$regex": "^%s" % url}}, {"$addToSet": {'_grants': self.schema.decodeid(grant_id)}})
        for resource in self.schema.db['metaphor_resource'].find({'_canonical_url': {"$regex": "^%s" % url}}, {"_id": 1}):
            self.schema.db['metaphor_link'].insert_one({"_type": "grant", "_from_id": resource['_id'], "_from_field_name": "_grants", "_to_id": self.schema.decodeid(grant_id)})

    def _remove_grants(self, grant_id, url):
        for spec_name, spec in self.schema.specs.items():
            self.schema.db['metaphor_resource'].update_many({'_canonical_url': {"$regex": "^%s" % url}}, {"$pull": {'_grants': self.schema.decodeid(grant_id)}})

    def create_resource(self, spec_name, parent_spec_name, parent_field_name,
                        parent_id, fields, grants=None):
        return CreateResourceUpdate(self, self.schema, spec_name, fields, parent_field_name, parent_spec_name,
                 parent_id, grants).execute()

    def create_linkcollection_entry(self, parent_spec_name, parent_id, parent_field, link_id):
        CreateLinkCollectionUpdate(self, self.schema, parent_spec_name, parent_id, parent_field, link_id).execute()

    def delete_resource(self, spec_name, resource_id, parent_spec_name, parent_field_name):
        return DeleteResourceUpdate(self, self.schema, spec_name, resource_id, parent_spec_name, parent_field_name).execute()

    def delete_linkcollection_entry(self, parent_spec_name, parent_id, parent_field, link_id):
        return DeleteLinkCollectionUpdate(self, self.schema, parent_spec_name, parent_id, parent_field, link_id).execute()

    def update_fields(self, spec_name, resource_id, fields):
        return FieldsUpdate(self, self.schema, spec_name, resource_id, fields).execute()

    def move_resource(self, from_path, to_path, target_id, target_canonical_url, target_field_name, target_spec_name):
        return MoveResourceUpdate(self, self.schema, from_path, to_path, target_id, target_canonical_url, target_field_name, target_spec_name).execute()

    def move_resource_to_root(self, from_path, to_path):
        return MoveResourceUpdate(self, self.schema, from_path, to_path, None, None, to_path).execute_root()

    def remove_spec_field(self, spec_name, field_name):
        self.schema.remove_spec_field(spec_name, field_name)

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
