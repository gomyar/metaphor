
import gevent
import time
from datetime import datetime

from gridfs import GridFS

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


def schedule(func, *args):
    gthread = gevent.spawn(func, *args)
    gthread.join()


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
#                print("    perforam agg from %s for %s.%s:  %s" % (spec_name, calc_spec_name, calc_field_name, reverse_agg))
                self.perform_single_update_aggregation(spec_name, calc_spec_name, calc_field_name, calc, start_agg, reverse_agg, update_id)

    def perform_single_update_aggregation(self, spec_name, calc_spec_name, calc_field_name, calc, start_agg, reverse_agg, update_id):
        gthread = gevent.spawn(self._caught_single_update_aggregation, spec_name, calc_spec_name, calc_field_name, calc, start_agg, reverse_agg, update_id)
        if not self.schema.specs[calc_spec_name].fields[calc_field_name].background:
            gthread.join()

    def _caught_single_update_aggregation(self, spec_name, calc_spec_name, calc_field_name, calc, start_agg, reverse_agg, update_id):
        try:
            start = time.time()
            self._perform_single_update_aggregation(spec_name, calc_spec_name, calc_field_name, calc, start_agg, reverse_agg, update_id)
            end = time.time()
#            log.debug("Update agg %s.%s took %s secs", calc_spec_name, calc_field_name, end - start)
        except Exception as e:
            self.schema.update_error(update_id, str(e))
            raise

    def _perform_single_update_aggregation(self, spec_name, calc_spec_name, calc_field_name, calc, start_agg, reverse_agg, update_id):
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
        #log.debug("Aggregate %s on resource_%s: %s", calc_field_name, spec_name, start_agg + reverse_agg + nested_agg + calc_field_dirty_agg + merge_agg)
        self.schema.db["resource_%s" % spec_name].aggregate(start_agg + reverse_agg + nested_agg + calc_field_dirty_agg + merge_agg)

        # update for subsequent calcs, if any
        new_start_agg = [
            {"$match": {"$expr": {"$in": [calc_field_name, {"$ifNull": ["$_dirty.%s" % update_id, []]}]}}}
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
        self.schema.db['resource_%s' % updated_resource_spec.name].aggregate(agg)

    def create_resource(self, spec_name, parent_spec_name, parent_field_name,
                        parent_id, fields):
        update_id = str(self.schema.create_update())
        return_val = CreateResourceUpdate(update_id, self, self.schema, spec_name, fields, parent_field_name, parent_spec_name,
                parent_id).execute()
        self.schema.cleanup_update(update_id)
        return return_val

    def create_file(self, spec_name, parent_id, field_name, stream, content_type, user=None, old_file_id=None):
        fs = GridFS(self.schema.db)

        file_id = fs.put(
            stream,
            parent_id=parent_id,
            content_type=content_type,
            upload_timestamp=datetime.utcnow(),
            uploaded_by=user.user_id if user else None,
        )
        self.update_fields(spec_name, parent_id, {field_name: file_id})
        if old_file_id:
            fs.delete(old_file_id)
        return self.schema.encodeid(file_id)

    def delete_file(self, spec_name, parent_id, field_name, file_id):
        fs = GridFS(self.schema.db)
        fs.delete(file_id)
        self.update_fields(spec_name, self.schema.encodeid(parent_id), {field_name: None})

    def create_linkcollection_entry(self, parent_spec_name, parent_id, parent_field, link_id):
        update_id = str(self.schema.create_update())
        CreateLinkCollectionUpdate(update_id, self, self.schema, parent_spec_name, parent_id, parent_field, link_id).execute()
        self.schema.cleanup_update(update_id)

    def create_orderedcollection_entry(self, spec_name, parent_spec_name, parent_field, parent_id, data):
        update_id = str(self.schema.create_update())
        return_val = CreateOrderedCollectionUpdate(update_id, self, self.schema, spec_name, parent_spec_name, parent_field, parent_id, data).execute()
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

    def move_resource(self, from_path, to_path, target_id, target_field_name, target_spec_name):
        log.debug("Move resource from %s to %s", from_path, to_path)
        update_id = str(self.schema.create_update())
        return_val = MoveResourceUpdate(update_id, self, self.schema, from_path, to_path, target_id, target_field_name, target_spec_name).execute()
        self.schema.cleanup_update(update_id)
        return return_val

    def move_resource_to_root(self, from_path, to_path, target_spec_name):
        update_id = str(self.schema.create_update())
        return_val = MoveResourceUpdate(update_id, self, self.schema, from_path, to_path, None, to_path, target_spec_name).execute()
        self.schema.cleanup_update(update_id)
        return return_val

    def delete_user(self, username):
        user = self.schema.db['resource_user'].find_one({'username': username})
        self.delete_resource('user', self.schema.encodeid(user['_id']), user['_parent_type'], user['_parent_field_name'])

    def delete_links_to_resource(self, spec_name, resource_id):
        spec = self.schema.specs[spec_name]
        for linked_spec_name, spec in self.schema.specs.items():
            for field_name, field in spec.fields.items():
                if field.field_type == 'link' and field.target_spec_name == spec_name:
                    # find all resources with link to target id
                    for resource_data in self.schema.db['resource_%s'%linked_spec_name].find({field_name: self.schema.decodeid(resource_id)}):
                        # call update_resource on resource
                        self.update_fields(linked_spec_name, self.schema.encodeid(resource_data['_id']), {field_name: None})

                if field.field_type == 'linkcollection' and field.target_spec_name == spec_name:
                    # find all resources with link to target id
                    for resource_data in self.schema.db['resource_%s'%linked_spec_name].find({'%s._id' % field_name: self.schema.decodeid(resource_id)}):
                        # call update_resource on resource
                        self.delete_linkcollection_entry(linked_spec_name, resource_data['_id'], field_name, resource_id)

    def create_basic_user(self, email, password, groups=None, admin=False):
        user_id = self.create_user_resource(email, groups, admin)
        self.schema.create_basic_identity(self.schema.decodeid(user_id), email, password)
        return user_id

    def create_user_resource(self, email, groups=None, admin=False):
        groups = groups or []
        user_id = self.create_resource(
            'user',
            'root',
            'users',
            None,
            {'email': email, 'admin': admin})
        for group in groups:
            self.schema.add_user_to_group(group, self.schema.decodeid(user_id))
        return user_id
