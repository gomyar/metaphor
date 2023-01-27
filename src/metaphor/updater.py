
from werkzeug.security import generate_password_hash

from metaphor.lrparse.reverse_aggregator import ReverseAggregator

from metaphor.update.create_resource import CreateResourceUpdate
from metaphor.update.fields_update import FieldsUpdate
from metaphor.update.delete_resource import DeleteResourceUpdate
from metaphor.update.delete_linkcollection import DeleteLinkCollectionUpdate
from metaphor.update.delete_orderedcollection import DeleteOrderedCollectionUpdate
from metaphor.update.create_linkcollection import CreateLinkCollectionUpdate
from metaphor.update.create_orderedcollection import CreateOrderedCollectionUpdate
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
        for (calc_spec_name, calc_field_name), calc in dependent_calcs.items():
            # create update_aggs
            dependent_aggs = self.build_reverse_aggregations_to_calc(calc_spec_name, calc_field_name, self.schema.specs[spec_name], None)
            if not dependent_aggs:
                # this finds calcs in the same resource as the original resource
                dependent_aggs = [[]]
            for reverse_agg in dependent_aggs:
                self.perform_single_update_aggregation(spec_name, calc_spec_name, calc_field_name, calc, start_agg, reverse_agg, update_id)

    def update_for_field(self, spec_name, field_name, update_id, start_agg):
        # find calcs for field
        dependent_calcs = self.schema._dependent_calcs_for_field(spec_name, field_name)
        for (calc_spec_name, calc_field_name), calc in dependent_calcs.items():
            # create update_aggs
            dependent_aggs = self.build_reverse_aggregations_to_calc(calc_spec_name, calc_field_name, self.schema.specs[spec_name], None)
            if not dependent_aggs:
                # this finds calcs in the same resource as the original resource
                dependent_aggs = [[]]
            for reverse_agg in dependent_aggs:
                self.perform_single_update_aggregation(spec_name, calc_spec_name, calc_field_name, calc, start_agg, reverse_agg, update_id)

    def update_for_resource(self, spec_name, update_id, start_agg):
        # find calcs for field
        dependent_calcs = self.schema._dependent_calcs_for_resource(spec_name)
        for (calc_spec_name, calc_field_name), calc in dependent_calcs.items():
            # create update_aggs
            dependent_aggs = self.build_reverse_aggregations_to_calc(calc_spec_name, calc_field_name, self.schema.specs[spec_name], None)
            if not dependent_aggs:
                # this finds calcs in the same resource as the original resource
                dependent_aggs = [[]]
            for reverse_agg in dependent_aggs:
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
                "into": "resource_%s" % calc_spec_name,
                "on": "_id",
                "whenNotMatched": "discard",
            }}
        ]
        # nest this into dependent agg
        nested_agg = [
            {"$lookup": {
                "from": "resource_%s" % calc_spec_name,
                "as": "_update",
                "let": {"id": "$_id"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$_id", "$$id"]}}},
                ] + update_agg,
            }},
        ]
        if not calc.is_collection():
            nested_agg.extend([
                {"$unwind": {"path": "$_update", "preserveNullAndEmptyArrays": True}},
                {"$addFields": {"_update": {"$ifNull": ["$_update", {"_val": None}]}}},
            ])
        log.debug("Aggregate %s on resource_%s: %s", calc_field_name, spec_name, start_agg + reverse_agg + nested_agg + calc_field_dirty_agg + merge_agg)
        self.schema.db["resource_%s" % spec_name].aggregate(start_agg + reverse_agg + nested_agg + calc_field_dirty_agg + merge_agg)

        # update for subsequent calcs, if any
        start_agg = [
            {"$match": {"$expr": {"$in": [calc_field_name, {"$ifNull": ["$_dirty.%s" % update_id, []]}]}}}
        ]
        self.update_for_field(calc_spec_name, calc_field_name, update_id, start_agg)

    def get_affected_ids_for_resource(self, calc_spec_name, calc_field_name, resource_spec, resource_id):
#        log.debug("get_affected_ids_for_resource(%s, %s, %s, %s)", calc_spec_name, calc_field_name, resource_spec, resource_id)
        affected_ids = []
        for aggregation in self.build_reverse_aggregations_to_calc(calc_spec_name, calc_field_name, resource_spec, resource_id):
            if aggregation:
                aggregation.append({"$project": {"_id": True}})
                cursor = self.schema.db['resource_%s' % resource_spec.name].aggregate(aggregation)
                found = [r['_id'] for r in cursor]
                affected_ids.extend(found)
        return affected_ids

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
        self.schema.db['resource_%s' % updated_resource_spec.name].aggregate(agg)

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
        for spec_name, spec in self.schema.specs.items():
            self.schema.db['resource_%s' % spec_name].update_many({'_canonical_url': {"$regex": "^%s" % url}}, {"$addToSet": {'_grants': self.schema.decodeid(grant_id)}})

    def _remove_grants(self, grant_id, url):
        for spec_name, spec in self.schema.specs.items():
            self.schema.db['resource_%s' % spec_name].update_many({'_canonical_url': {"$regex": "^%s" % url}}, {"$pull": {'_grants': self.schema.decodeid(grant_id)}})

    def create_resource(self, spec_name, parent_spec_name, parent_field_name,
                        parent_id, fields, grants=None):
        return CreateResourceUpdate(self, self.schema, spec_name, fields, parent_field_name, parent_spec_name,
                 parent_id, grants).execute()

    def create_linkcollection_entry(self, parent_spec_name, parent_id, parent_field, link_id):
        CreateLinkCollectionUpdate(self, self.schema, parent_spec_name, parent_id, parent_field, link_id).execute()

    def create_orderedcollection_entry(self, spec_name, parent_spec_name, parent_field, parent_id, data, grants=None):
        return CreateOrderedCollectionUpdate(self, self.schema, spec_name, parent_spec_name, parent_field, parent_id, data, grants).execute()

    def delete_resource(self, spec_name, resource_id, parent_spec_name, parent_field_name):
        return DeleteResourceUpdate(self, self.schema, spec_name, resource_id, parent_spec_name, parent_field_name).execute()

    def delete_linkcollection_entry(self, parent_spec_name, parent_id, parent_field, link_id):
        return DeleteLinkCollectionUpdate(self, self.schema, parent_spec_name, parent_id, parent_field, link_id).execute()

    def delete_orderedcollection_entry(self, parent_spec_name, parent_id, parent_field, link_id):
        return DeleteOrderedCollectionUpdate(self, self.schema, parent_spec_name, parent_id, parent_field, link_id).execute()

    def update_fields(self, spec_name, resource_id, fields):
        return FieldsUpdate(self, self.schema, spec_name, resource_id, fields).execute()

    def move_resource(self, parent_path, parent_spec_name, field_name, to_path, from_path=None):
        return MoveResourceUpdate(self, self.schema, parent_path, parent_spec_name, field_name, to_path, from_path).execute()

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
        user = self.schema.db['resource_user'].find_one({'username': username})
        self.delete_resource('user', self.schema.encodeid(user['_id']), user['_parent_type'], user['_parent_field_name'])
