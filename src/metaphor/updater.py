
from metaphor.lrparse.reverse_aggregator import ReverseAggregator

import logging

log = logging.getLogger('metaphor')


class CreateResourceUpdate:
    def __init__(self, updater, schema, spec_name, fields, parent_field_name, parent_spec_name,
                 parent_id, grants):
        self.updater = updater
        self.schema = schema
        self.spec_name = spec_name
        self.spec = self.schema.specs[spec_name]
        self.fields = fields

        self.parent_field_name = parent_field_name
        self.parent_spec_name = parent_spec_name
        self.parent_id = parent_id
        self.grants = grants

    def execute(self):
        # must add _create_updated field to resource instead of creating updater document
        resource_id = self.schema.insert_resource(
            self.spec_name, self.fields, self.parent_field_name, self.parent_spec_name,
            self.parent_id, self.grants)

        for (calc_spec_name, calc_field_name), calc_tree in self.schema.calc_trees.items():
            # update for resources
            # unsure if this is necessary
            if "%s.%s" % (self.parent_spec_name, self.parent_field_name) in calc_tree.get_resource_dependencies():
                self.updater._perform_updates_for_affected_calcs(self.spec, resource_id, calc_spec_name, calc_field_name)

            # update for fields
            for field_name in self.fields:
                field_dep = "%s.%s" % (self.spec_name, field_name)
                if field_dep in calc_tree.get_resource_dependencies():
                    self.updater._perform_updates_for_affected_calcs(self.spec, resource_id, calc_spec_name, calc_field_name)

        # recalc local calcs
        for field_name, field_spec in self.spec.fields.items():
            if field_spec.field_type == 'calc':
                self.updater.update_calc(self.spec.name, field_name, resource_id)
                self.updater._recalc_for_field_update(self.spec, self.spec.name, field_name, resource_id)

        # check if new resource is read grant
        if self.spec_name == 'grant' and self.fields['type'] == 'read':
            self.updater._update_grants(resource_id, self.fields['type'], self.fields['url'])

        return resource_id


class FieldsUpdate:
    def __init__(self, updater, schema, spec_name, resource_id, fields):
        self.updater = updater
        self.schema = schema

        self.spec_name = spec_name
        self.resource_id = resource_id
        self.fields = fields

    def execute(self):
        spec = self.schema.specs[self.spec_name]
        self.schema.update_resource_fields(self.spec_name, self.resource_id, self.fields)

        # update local resource calcs
        #   update dependent field calcs (involving fields)
        # update foreign resource calcs (involving fields)

        # recalc local calcs
        # moved this ahead of the code to recalc foreign calcs
        # there may be an opportunity to check whether the change only affects the current resource
        # (if there are only "self." references in the calc)
        # this will stop the reverse aggregation, which finds itself only and becomes a no-op
        # careful with multi layered calcs within the same resource
        for field_name, field_spec in spec.fields.items():
            if field_spec.field_type == 'calc':
                self.updater.update_calc(spec.name, field_name, self.resource_id)
                self.updater._recalc_for_field_update(spec, spec.name, field_name, self.resource_id)

        for (calc_spec_name, calc_field_name), calc_tree in self.schema.calc_trees.items():
            # update for fields
            for field_name in self.fields:
                field_dep = "%s.%s" % (self.spec_name, field_name)
                if field_dep in calc_tree.get_resource_dependencies():
                    self.updater._perform_updates_for_affected_calcs(spec, self.resource_id, calc_spec_name, calc_field_name)



class DeleteResourceUpdate:
    def __init__(self, updater, schema, spec_name, resource_id, parent_spec_name, parent_field_name):
        self.updater = updater
        self.schema = schema
        self.spec_name = spec_name
        self.resource_id = resource_id
        self.parent_spec_name = parent_spec_name
        self.parent_field_name = parent_field_name

    def execute(self):
        spec = self.schema.specs[self.spec_name]

        cursors = []

        for (calc_spec_name, calc_field_name), calc_tree in self.schema.calc_trees.items():
            # update for resources
            # unsure if this is necessary
            if "%s.%s" % (self.parent_spec_name, self.parent_field_name) in calc_tree.get_resource_dependencies():
                affected_ids = self.updater.get_affected_ids_for_resource(calc_spec_name, calc_field_name, spec, self.resource_id)
                if affected_ids:
                    cursors.append((affected_ids, calc_spec_name, calc_field_name))

            # find root collection dependencies
            if self.parent_spec_name is None:
                for field_name, field in self.schema.root.fields.items():
                    if field.field_type in ['collection', 'linkcollection']:
                        if field.target_spec_name == self.spec_name:
                            affected_ids = self.updater.get_affected_ids_for_resource(calc_spec_name, calc_field_name, spec, self.resource_id)
                            if affected_ids:
                                cursors.append((affected_ids, calc_spec_name, calc_field_name))

        # must add _create_updated field to resource instead of creating updater document
        self.schema.delete_resource(self.spec_name, self.resource_id)

        for affected_ids, calc_spec_name, calc_field_name in cursors:
            for affected_id in affected_ids:
                affected_id = self.schema.encodeid(affected_id)
                self.updater.update_calc(calc_spec_name, calc_field_name, affected_id)
                self.updater._recalc_for_field_update(spec, calc_spec_name, calc_field_name, affected_id)

        # delete child resources
        for field_name, field in spec.fields.items():
            if field.field_type == 'collection':
                for child_resource in self.schema.db['resource_%s' % field.target_spec_name].find({'_parent_id': self.schema.decodeid(self.resource_id)}, {'_id': 1}):
                    self.updater.delete_resource(field.target_spec_name, self.schema.encodeid(child_resource['_id']), self.spec_name, field_name)

        # delete any links to resource
        for linked_spec_name, spec in self.schema.specs.items():
            for field_name, field in spec.fields.items():
                if field.field_type == 'link' and field.target_spec_name == self.spec_name:
                    # find all resources with link to target id
                    for resource_data in self.schema.db['resource_%s' % linked_spec_name].find({field_name: self.schema.decodeid(self.resource_id)}):
                        # call update_resource on resource
                        self.updater.update_fields(linked_spec_name, self.schema.encodeid(resource_data['_id']), {field_name: None})

                if field.field_type == 'linkcollection' and field.target_spec_name == self.spec_name:
                    # find all resources with link to target id
                    for resource_data in self.schema.db['resource_%s' % linked_spec_name].find({'%s._id' % field_name: self.schema.decodeid(self.resource_id)}):
                        # call update_resource on resource
                        self.updater.delete_linkcollection_entry(linked_spec_name, resource_data['_id'], field_name, self.resource_id)

        return self.resource_id


class DeleteLinkCollectionUpdate:
    def __init__(self, updater, schema, parent_spec_name, parent_id, parent_field, link_id):
        self.updater = updater
        self.schema = schema
        self.parent_spec_name = parent_spec_name
        self.parent_id = parent_id
        self.parent_field = parent_field
        self.link_id = link_id

    def execute(self):
        parent_spec = self.schema.specs[self.parent_spec_name]
        spec = parent_spec.build_child_spec(self.parent_field)

        cursors = []

        # find affected resources before deleting
        for (calc_spec_name, calc_field_name), calc_tree in self.schema.calc_trees.items():
            # update for resources
            if "%s.%s" % (self.parent_spec_name, self.parent_field) in calc_tree.get_resource_dependencies():

                affected_ids = self.updater.get_affected_ids_for_resource(calc_spec_name, calc_field_name, spec, self.link_id)

                if affected_ids:
                    cursors.append((affected_ids, calc_spec_name, calc_field_name))

        # perform delete
        self.schema.delete_linkcollection_entry(
            self.parent_spec_name, self.parent_id, self.parent_field, self.link_id)

        # update affected resources
        for affected_ids, calc_spec_name, calc_field_name in cursors:
            for affected_id in affected_ids:
                affected_id = self.schema.encodeid(affected_id)
                self.updater.update_calc(calc_spec_name, calc_field_name, affected_id)
                self.updater._recalc_for_field_update(spec, calc_spec_name, calc_field_name, affected_id)

        # recalc local calcs
        for field_name, field_spec in parent_spec.fields.items():
            if field_spec.field_type == 'calc':
                self.updater.update_calc(self.parent_spec_name, field_name, self.link_id)
                self.updater._recalc_for_field_update(spec, self.parent_spec_name, field_name, self.link_id)

        # run update
        return self.link_id


class DeleteOrderedCollectionUpdate:
    def __init__(self, updater, schema, parent_spec_name, parent_id, parent_field, link_id):
        self.updater = updater
        self.schema = schema
        self.parent_spec_name = parent_spec_name
        self.parent_id = parent_id
        self.parent_field = parent_field
        self.link_id = link_id

    def execute(self):
        parent_spec = self.schema.specs[self.parent_spec_name]
        spec = parent_spec.build_child_spec(self.parent_field)

        cursors = []

        # find affected resources before deleting
        for (calc_spec_name, calc_field_name), calc_tree in self.schema.calc_trees.items():
            # update for resources
            if "%s.%s" % (self.parent_spec_name, self.parent_field) in calc_tree.get_resource_dependencies():

                affected_ids = self.updater.get_affected_ids_for_resource(calc_spec_name, calc_field_name, spec, self.link_id)

                if affected_ids:
                    cursors.append((affected_ids, calc_spec_name, calc_field_name))

        # perform delete
        self.schema.delete_linkcollection_entry(
            self.parent_spec_name, self.parent_id, self.parent_field, self.link_id)
        self.schema.delete_resource(spec.name, self.link_id)

        # update affected resources
        for affected_ids, calc_spec_name, calc_field_name in cursors:
            for affected_id in affected_ids:
                affected_id = self.schema.encodeid(affected_id)
                self.updater.update_calc(calc_spec_name, calc_field_name, affected_id)
                self.updater._recalc_for_field_update(spec, calc_spec_name, calc_field_name, affected_id)

        # recalc local calcs
        for field_name, field_spec in parent_spec.fields.items():
            if field_spec.field_type == 'calc':
                self.updater.update_calc(self.parent_spec_name, field_name, self.link_id)
                self.updater._recalc_for_field_update(spec, self.parent_spec_name, field_name, self.link_id)

        # delete any links to resource
        for linked_spec_name, linked_spec in self.schema.specs.items():
            for field_name, field in linked_spec.fields.items():
                if field.field_type == 'link' and field.target_spec_name == spec.name:
                    # find all resources with link to target id
                    for resource_data in self.schema.db['resource_%s' % linked_spec_name].find({field_name: self.schema.decodeid(self.link_id)}):
                        # call update_resource on resource
                        self.updater.update_fields(linked_spec_name, self.schema.encodeid(resource_data['_id']), {field_name: None})

                if field.field_type == 'linkcollection' and field.target_spec_name == spec.name:
                    # find all resources with link to target id
                    for resource_data in self.schema.db['resource_%s' % linked_spec_name].find({'%s._id' % field_name: self.schema.decodeid(self.link_id)}):
                        # call update_resource on resource
                        self.updater.delete_linkcollection_entry(linked_spec_name, resource_data['_id'], field_name, resource_id)

        # run update
        return self.link_id


class CreateLinkCollectionUpdate:
    def __init__(self, updater, schema, parent_spec_name, parent_id, parent_field, link_id):
        self.updater = updater
        self.schema = schema

        self.parent_spec_name = parent_spec_name
        self.parent_id = parent_id
        self.parent_field = parent_field
        self.link_id = link_id

    def execute(self):
        self.schema.create_linkcollection_entry(self.parent_spec_name, self.parent_id, self.parent_field, self.link_id)
        parent_spec = self.schema.specs[self.parent_spec_name]
        spec = parent_spec.build_child_spec(self.parent_field)

        for (calc_spec_name, calc_field_name), calc_tree in self.schema.calc_trees.items():
            # update for resources
            if "%s.%s" % (self.parent_spec_name, self.parent_field) in calc_tree.get_resource_dependencies():
                self.updater._perform_updates_for_affected_calcs(spec, self.link_id, calc_spec_name, calc_field_name)

        # recalc local calcs
        for field_name, field_spec in parent_spec.fields.items():
            if field_spec.field_type == 'calc':
                self.updater.update_calc(self.parent_spec_name, field_name, self.parent_id)
                self.updater._recalc_for_field_update(spec, self.parent_spec_name, field_name, self.parent_id)


class CreateOrderedLinkCollectionUpdate:
    def __init__(self, updater, schema, spec_name, parent_spec_name, parent_field, parent_id, data, grants):
        self.updater = updater
        self.schema = schema

        self.spec_name = spec_name
        self.parent_spec_name = parent_spec_name
        self.parent_field = parent_field
        self.parent_id = parent_id
        self.data = data
        self.grants = grants

    def execute(self):
        resource_id = self.schema.create_orderedcollection_entry(self.spec_name, self.parent_spec_name, self.parent_field, self.parent_id, self.data, self.grants)
        parent_spec = self.schema.specs[self.parent_spec_name]
        spec = parent_spec.build_child_spec(self.parent_field)

        for (calc_spec_name, calc_field_name), calc_tree in self.schema.calc_trees.items():
            # update for resources
            if "%s.%s" % (self.parent_spec_name, self.parent_field) in calc_tree.get_resource_dependencies():
                self.updater._perform_updates_for_affected_calcs(spec, resource_id, calc_spec_name, calc_field_name)

        # recalc local calcs
        for field_name, field_spec in parent_spec.fields.items():
            if field_spec.field_type == 'calc':
                self.updater.update_calc(self.parent_spec_name, field_name, self.parent_id)
                self._recalc_for_field_update(spec, self.parent_spec_name, field_name, self.parent_id)

        return resource_id


class CreateLinkUpdate:
    pass


class RecalcUpdate:
    def __init__(self, spec_name, calc_field_name):
        self.spec_name = spec_name
        self.calc_field_name = calc_field_name


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

    def create_resource(self, spec_name, parent_spec_name, parent_field_name,
                        parent_id, fields, grants=None):
        return CreateResourceUpdate(self, self.schema, spec_name, fields, parent_field_name, parent_spec_name,
                 parent_id, grants).execute()

    def _update_grants(self, grant_id, grant_type, url):
        for spec_name, spec in self.schema.specs.items():
            self.schema.db['resource_%s' % spec_name].update_many({'_canonical_url': {"$regex": "^%s" % url}}, {"$addToSet": {'_grants': self.schema.decodeid(grant_id)}})

    def create_linkcollection_entry(self, parent_spec_name, parent_id, parent_field, link_id):
        CreateLinkCollectionUpdate(self, self.schema, parent_spec_name, parent_id, parent_field, link_id).execute()

    def create_orderedcollection_entry(self, spec_name, parent_spec_name, parent_field, parent_id, data, grants=None):
        return CreateOrderedLinkCollectionUpdate(self, self.schema, spec_name, parent_spec_name, parent_field, parent_id, data, grants).execute()

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
