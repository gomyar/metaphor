
import logging

log = logging.getLogger('metaphor')


class Updater(object):
    def __init__(self, schema):
        self.schema = schema

    def get_affected_ids_for_resource(self, calc_spec_name, calc_field_name, resource_spec, resource_id):
        log.debug("get_affected_ids_for_resource(%s, %s, %s, %s)", calc_spec_name, calc_field_name, resource_spec, resource_id)
        affected_ids = []
        for aggregation in self.build_reverse_aggregations_to_calc(calc_spec_name, calc_field_name, resource_spec, resource_id):
#            aggregation.append({"$match": {"_id": {"$ne": self.schema.decodeid(resource_id)}}})
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

    def _recalc_for_field_update(self, spec, field_spec_name, field_name, resource_id):
        # find foreign dependencies
        for (calc_spec_name, calc_field_name), calc_tree in self.schema.calc_trees.items():
            # update for fields
            field_dep = "%s.%s" % (field_spec_name, field_name)
            if field_dep in calc_tree.get_resource_dependencies():
                affected_ids = self.get_affected_ids_for_resource(calc_spec_name, calc_field_name, spec, resource_id)
                affected_ids = set(affected_ids)
                for affected_id in affected_ids:
                    affected_id = self.schema.encodeid(affected_id)
                    self.update_calc(calc_spec_name, calc_field_name, affected_id)
                    self._recalc_for_field_update(spec, calc_spec_name, calc_field_name, affected_id)
            elif spec.fields.get(field_name) and spec.fields[field_name].field_type == 'link':
                if spec.name in calc_tree.get_resource_dependencies():
                    affected_ids = self.get_affected_ids_for_resource(calc_spec_name, calc_field_name, spec, resource_id)
                    affected_ids = set(affected_ids)
                    for affected_id in affected_ids:
                        affected_id = self.schema.encodeid(affected_id)
                        self.update_calc(calc_spec_name, calc_field_name, affected_id)
                        self._recalc_for_field_update(spec, calc_spec_name, calc_field_name, affected_id)
            elif spec.fields.get(field_name) and spec.fields[field_name].field_type == 'calc':
                if not spec.fields[field_name].infer_type().is_primitive():
                    if spec.name in calc_tree.get_resource_dependencies():
                        affected_ids = self.get_affected_ids_for_resource(calc_spec_name, calc_field_name, spec, resource_id)
                        affected_ids = set(affected_ids)
                        for affected_id in affected_ids:
                            affected_id = self.schema.encodeid(affected_id)
                            self.update_calc(calc_spec_name, calc_field_name, affected_id)
                            self._recalc_for_field_update(spec, calc_spec_name, calc_field_name, affected_id)

        return resource_id

    def _recalc_for_resource_create():
        pass

    def _recalc_for_resource_delete():
        pass

    def _recalc_for_resource_linked():
        pass

    def _recalc_for_resource_unlinked():
        pass

    def create_resource(self, spec_name, parent_spec_name, parent_field_name,
                        parent_id, fields):
        spec = self.schema.specs[spec_name]
        # must add _create_updated field to resource instead of creating updater document
        resource_id = self.schema.insert_resource(
            spec_name, fields, parent_field_name, parent_spec_name,
            parent_id)

        for (calc_spec_name, calc_field_name), calc_tree in self.schema.calc_trees.items():
            # update for resources
            # unsure if this is necessary
            if "%s.%s" % (parent_spec_name, parent_field_name) in calc_tree.get_resource_dependencies():
                affected_ids = self.get_affected_ids_for_resource(calc_spec_name, calc_field_name, spec, resource_id)
                affected_ids = set(affected_ids)
                for affected_id in affected_ids:
                    affected_id = self.schema.encodeid(affected_id)
                    self.update_calc(calc_spec_name, calc_field_name, affected_id)
                    self._recalc_for_field_update(spec, calc_spec_name, calc_field_name, affected_id)


            # update for fields
            for field_name in fields:
                field_dep = "%s.%s" % (spec_name, field_name)
                if field_dep in calc_tree.get_resource_dependencies():
                    affected_ids = self.get_affected_ids_for_resource(calc_spec_name, calc_field_name, spec, resource_id)
                    affected_ids = set(affected_ids)
                    for affected_id in affected_ids:
                        affected_id = self.schema.encodeid(affected_id)
                        self.update_calc(calc_spec_name, calc_field_name, affected_id)
                        self._recalc_for_field_update(spec, calc_spec_name, calc_field_name, affected_id)

        # recalc local calcs
        for field_name, field_spec in spec.fields.items():
            if field_spec.field_type == 'calc':
                self.update_calc(spec.name, field_name, resource_id)
                self._recalc_for_field_update(spec, spec.name, field_name, resource_id)

        return resource_id

    def create_linkcollection_entry(self, parent_spec_name, parent_id, parent_field, link_id):
        self.schema.create_linkcollection_entry(parent_spec_name, parent_id, parent_field, link_id)
        parent_spec = self.schema.specs[parent_spec_name]
        spec = parent_spec.build_child_spec(parent_field)

        for (calc_spec_name, calc_field_name), calc_tree in self.schema.calc_trees.items():
            # update for resources
            if "%s.%s" % (parent_spec_name, parent_field) in calc_tree.get_resource_dependencies():
                affected_ids = self.get_affected_ids_for_resource(calc_spec_name, calc_field_name, spec, link_id)
                affected_ids = set(affected_ids)
                for affected_id in affected_ids:
                    affected_id = self.schema.encodeid(affected_id)
                    self.update_calc(calc_spec_name, calc_field_name, affected_id)
                    self._recalc_for_field_update(spec, calc_spec_name, calc_field_name, affected_id)

        # recalc local calcs
        for field_name, field_spec in parent_spec.fields.items():
            if field_spec.field_type == 'calc':
                self.update_calc(parent_spec_name, field_name, link_id)
                self._recalc_for_field_update(spec, parent_spec_name, field_name, link_id)



    def update_fields(self, spec_name, resource_id, fields):
        spec = self.schema.specs[spec_name]
        self.schema.update_resource_fields(spec_name, resource_id, fields)

        # update local resource calcs
        #   update dependent field calcs (involving fields)
        # update foreign resource calcs (involving fields)

        for (calc_spec_name, calc_field_name), calc_tree in self.schema.calc_trees.items():
            # update for fields
            for field_name in fields:
                field_dep = "%s.%s" % (spec_name, field_name)
                if field_dep in calc_tree.get_resource_dependencies():
                    affected_ids = self.get_affected_ids_for_resource(calc_spec_name, calc_field_name, spec, resource_id)
                    affected_ids = set(affected_ids)
                    for affected_id in affected_ids:
                        affected_id = self.schema.encodeid(affected_id)
                        self.update_calc(calc_spec_name, calc_field_name, affected_id)
                        self._recalc_for_field_update(spec, calc_spec_name, calc_field_name, affected_id)
