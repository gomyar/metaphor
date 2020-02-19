
class Updater(object):
    def __init__(self, schema):
        self.schema = schema

    def get_calcs_affected_by_field(self, spec_name, field_name):
        resource = self.schema.specs[spec_name]
        field = resource.fields[field_name]
        found = set()
        for ((calc_spec_name, calc_field_name), calc_tree) in self.schema.calc_trees.items():
            calc_spec = self.schema.specs[calc_spec_name]
            for resource_ref in calc_tree.all_resource_refs():
                child_spec = calc_spec.resolve_child(resource_ref)
                if child_spec == field:
                    found.add((calc_spec_name, calc_field_name))
        return found

    def get_affected_ids_for_resource(self, calc_spec_name, calc_field_name, resource_spec, resource_id):
        affected_ids = []
        for aggregation in self.build_reverse_aggregations_to_calc(calc_spec_name, calc_field_name, resource_spec, resource_id):
            cursor = self.schema.db['resource_%s' % resource_spec.name].aggregate(aggregation)
            for resource in cursor:
                affected_ids.append(resource['_id'])
        return affected_ids

    def build_reverse_aggregations_to_calc(self, calc_spec_name, calc_field_name, resource_spec, resource_id):
        calc_tree = self.schema.calc_trees[calc_spec_name, calc_field_name]
        aggregations = calc_tree.build_reverse_aggregations(resource_spec, resource_id)
        return aggregations

    def update_calc(self, resource_name, calc_field_name, resource_id):
        calc_tree = self.schema.calc_trees[resource_name, calc_field_name]
        if calc_tree.infer_type().is_primitive():
            result = calc_tree.calculate(resource_id)
        else:
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
        self.schema.db['resource_%s' % resource_name].update({'_id': self.schema.decodeid(resource_id)}, {"$set": {calc_field_name: result}})

    def update_fields(self, spec_name, resource_id, **fields):
        spec = self.schema.specs[spec_name]
        self.schema.update_resource_fields(spec_name, resource_id, fields)
        self._perform_calc_updates(spec_name, resource_id, fields)

    def _perform_calc_updates(self, spec_name, resource_id, fields):
        spec = self.schema.specs[spec_name]
        for (calc_spec_name, calc_field_name), calc_tree in self.schema.calc_trees.items():
            for field_name, field_value in fields.items():
                affected_ids = self.get_affected_ids_for_resource(calc_spec_name, calc_field_name, spec, resource_id)
                for affected_id in affected_ids:
                    self.update_calc(calc_spec_name, calc_field_name, self.schema.encodeid(affected_id))

    def create_resource(self, spec_name, parent_spec_name, parent_field_name,
                        parent_id, fields):
        spec = self.schema.specs[spec_name]
        resource_id = self.schema.insert_resource(
            spec_name, fields, parent_field_name, parent_spec_name,
            parent_id)
        self._perform_calc_updates(spec_name, resource_id, fields)
        return resource_id