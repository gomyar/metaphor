
import logging

log = logging.getLogger('metaphor')


class Updater(object):
    def __init__(self, schema):
        self.schema = schema

    def get_affected_ids_for_resource(self, calc_spec_name, calc_field_name, resource_spec, resource_id):
        log.debug("get_affected_ids_for_resource(%s, %s, %s, %s)", calc_spec_name, calc_field_name, resource_spec, resource_id)
        affected_ids = []
        for aggregation in self.build_reverse_aggregations_to_calc(calc_spec_name, calc_field_name, resource_spec, resource_id):
            cursor = self.schema.db['resource_%s' % resource_spec.name].aggregate(aggregation)
            for resource in cursor:
                affected_ids.append(resource['_id'])
        log.debug("returns: %s", affected_ids)
        return affected_ids

    def build_reverse_aggregations_to_calc(self, calc_spec_name, calc_field_name, resource_spec, resource_id):
        calc_tree = self.schema.calc_trees[calc_spec_name, calc_field_name]
        aggregations = calc_tree.build_reverse_aggregations(resource_spec, resource_id)
        return aggregations

    def update_calc(self, resource_name, calc_field_name, resource_id):
        log.debug("Updating calc: %s %s %s", resource_name, calc_field_name, resource_id)
        calc_tree = self.schema.calc_trees[resource_name, calc_field_name]
        if calc_tree.infer_type().is_primitive():
            result = calc_tree.calculate(resource_id)
        else:
            result = self._calculate_resource(calc_tree, resource_id)
        log.debug("Writing : %s", result)
        self.schema.db['resource_%s' % resource_name].update({'_id': self.schema.decodeid(resource_id)}, {"$set": {calc_field_name: result}})

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
            result = results[0]
        return result

    def update_fields(self, spec_name, resource_id, **fields):
        spec = self.schema.specs[spec_name]
        self.schema.update_resource_fields(spec_name, resource_id, fields)
        self._perform_calc_updates(spec_name, resource_id, fields)

    def _perform_calc_updates(self, spec_name, resource_id, fields):
        log.debug("_perform_calc_updates: %s, %s, %s", spec_name, resource_id, fields.keys())

        # update resources' own calcs first
        spec = self.schema.specs[spec_name]
        for field_name, field in spec.fields.items():
            log.debug("Check field: %s", field_name)
            if field.field_type == 'calc':
                self.update_calc(spec_name, field_name, resource_id)

        # update everybody else's
        for (calc_spec_name, calc_field_name), calc_tree in sorted(self.schema.calc_trees.items()):
            affected_ids = self.get_affected_ids_for_resource(calc_spec_name, calc_field_name, spec, resource_id)
            affected_ids = set(affected_ids)
            for affected_id in affected_ids:
                affected_id = self.schema.encodeid(affected_id)
                if affected_id != resource_id:
                    self.update_calc(calc_spec_name, calc_field_name, affected_id)
#                    self._perform_calc_updates(calc_spec_name, affected_id, {calc_field_name: None})

    def create_resource(self, spec_name, parent_spec_name, parent_field_name,
                        parent_id, fields):
        spec = self.schema.specs[spec_name]
        resource_id = self.schema.insert_resource(
            spec_name, fields, parent_field_name, parent_spec_name,
            parent_id)
        self._perform_calc_updates(spec_name, resource_id, fields)
        return resource_id

    def create_linkcollection_entry(self, parent_spec_name, parent_id, parent_field, link_id):
        self.schema.create_linkcollection_entry(parent_spec_name, parent_id, parent_field, link_id)
        parent_spec = self.schema.specs[parent_spec_name]
        spec = parent_spec.build_child_spec(parent_field)
        self._perform_calc_updates(spec.name, link_id, {parent_field: None})
