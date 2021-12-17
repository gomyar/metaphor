
from metaphor.lrparse.reverse_aggregator import ReverseAggregator

from metaphor.update.create_resource import CreateResourceUpdate
from metaphor.update.fields_update import FieldsUpdate
from metaphor.update.delete_resource import DeleteResourceUpdate
from metaphor.update.delete_linkcollection import DeleteLinkCollectionUpdate
from metaphor.update.delete_orderedcollection import DeleteOrderedCollectionUpdate
from metaphor.update.create_linkcollection import CreateLinkCollectionUpdate
from metaphor.update.create_orderedcollection import CreateOrderedCollectionUpdate

import logging

log = logging.getLogger('metaphor')


class Updater(object):
    def __init__(self, schema):
        self.schema = schema

    def get_affected_ids_for_resource(self, calc_spec_name, calc_field_name, resource_spec, resource_id):
        log.debug("get_affected_ids_for_resource(%s, %s, %s, %s)", calc_spec_name, calc_field_name, resource_spec, resource_id)
        affected_ids = []
        for aggregation in self.build_reverse_aggregations_to_calc(calc_spec_name, calc_field_name, resource_spec, resource_id):
            aggregation.append({"$project": {"_id": True}})
            cursor = self.schema.db['resource_%s' % resource_spec.name].aggregate(aggregation)
            for resource in cursor:
#                yield resource['_id']
                affected_ids.append(resource['_id'])
        log.debug("returns: %s", affected_ids)
        return affected_ids

    def build_reverse_aggregations_to_calc(self, calc_spec_name, calc_field_name, resource_spec, resource_id):
        calc_tree = self.schema.calc_trees[calc_spec_name, calc_field_name]
        aggregations = ReverseAggregator(self.schema).get_for_resource(calc_tree, resource_spec.name, self.schema.decodeid(resource_id))
        return aggregations

    def update_calc(self, resource_name, calc_field_name, resource_id):
        log.debug("Updating calc: %s %s %s", resource_name, calc_field_name, resource_id)
        calc_tree = self.schema.calc_trees[resource_name, calc_field_name]

        update_result = {}
        if calc_tree.infer_type().is_primitive():
            result = calc_tree.calculate(resource_id)
            update_result[calc_field_name] = result
        elif calc_tree.is_collection():
            result = self._calculate_resource(calc_tree, resource_id)
            update_result[calc_field_name] = result
        else:
            # link result
            result = self._calculate_resource(calc_tree, resource_id)
            update_result[calc_field_name] = result
            if result:
                update_result['_canonical_url_%s' % calc_field_name] = self.schema.load_canonical_parent_url(calc_tree.infer_type().name, self.schema.encodeid(result))
            else:
                update_result['_canonical_url_%s' % calc_field_name] = None
        log.debug("Writing : %s", result)
        self.schema.db['resource_%s' % resource_name].update({'_id': self.schema.decodeid(resource_id)}, {"$set": update_result})

    def _calculate_resource(self, calc_tree, resource_id):
        aggregate_query, _, is_aggregate = calc_tree.aggregation(resource_id)
        aggregate_query.append(
            {"$project": {
                '_id': True,
            }})
        # can probably aggregate and write directly to the field from here
        cursor = calc_tree.root_collection().aggregate(aggregate_query)
        results = [resource['_id'] for resource in cursor]
        if is_aggregate:
            result = results
        else:
            result = results[0] if results else None
        return result

    def _perform_updates_for_affected_calcs(self, spec, resource_id, calc_spec_name, calc_field_name):
        affected_ids = self.get_affected_ids_for_resource(calc_spec_name, calc_field_name, spec, resource_id)
        for affected_id in affected_ids:
            affected_id = self.schema.encodeid(affected_id)
            self.update_calc(calc_spec_name, calc_field_name, affected_id)
            self._recalc_for_field_update(spec, calc_spec_name, calc_field_name, affected_id)

    def _recalc_for_field_update(self, spec, field_spec_name, field_name, resource_id):
        # find foreign dependencies
        for (calc_spec_name, calc_field_name), calc_tree in self.schema.calc_trees.items():
            # update for fields
            field_dep = "%s.%s" % (field_spec_name, field_name)
            if field_dep in calc_tree.get_resource_dependencies():
                self._perform_updates_for_affected_calcs(spec, resource_id, calc_spec_name, calc_field_name)
            elif spec.fields.get(field_name) and spec.fields[field_name].field_type == 'link':
                if spec.name in calc_tree.get_resource_dependencies():
                    self._perform_updates_for_affected_calcs(spec, resource_id, calc_spec_name, calc_field_name)
            elif spec.fields.get(field_name) and spec.fields[field_name].field_type == 'calc':
                if not spec.fields[field_name].infer_type().is_primitive():
                    if spec.name in calc_tree.get_resource_dependencies():
                        self._perform_updates_for_affected_calcs(spec, resource_id, calc_spec_name, calc_field_name)

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

    def remove_spec_field(self, spec_name, field_name):
        self.schema.remove_spec_field(spec_name, field_name)
