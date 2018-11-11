
from collections import defaultdict
from pymongo import ReturnDocument

from datetime import datetime

from metaphor.updater import Updater
from metaphor.resource import Resource


class Update(object):
    '''
    if update running, wait on notify or _processing_at < 3 seconds

    _initiate_update_processing
    update db fields, push _updated=False,
      _processing_at=now()
    _updated_fields.addIf(fields)

    _set_dependents_updated
    for each dependent calc, if the value has changed (from above)
    update dependent resources, push _updated=True,
    _updated_fields.addIf(updated_calc_fields)

    _finalize_update
    finalize current resource update:
    if _updated=False
      push _updated_fields={}, _processing_at=None
    if _updated=True
      push _processing_at=None
      notify waiting updaters

    _kickoff_updaters
    kickoff updater for each dependent resource
    '''

    def __init__(self, schema, update_pool):
        self.schema = schema
        self.old_updater = Updater(schema)
        self.update_pool = update_pool
        self.resource = None
        self.updated_field_names = []
        self.dependents = []
        self.gthreads = []

    def fields_updated(self, resource, fields):
        # _initiate_update_processing
        self._initiate_update_processing(resource, resource.data.keys())
        # _update_dependents
        self._update_dependents(resource, fields)
        # _finalize_update
        self._finalize_update(resource)

    def resource_created(self, resource):
        # _initiate_update_processing
        ### should we assume the resource is initiated on create? (yes)
#        self._initiate_update_processing(resource, fields)
        # _update_dependents
        self._update_resource_dependents(resource, resource.data.keys())
        # _finalize_update
        self._finalize_update(resource)

    def _initiate_update_processing(self, resource, fields):
        resource_name = 'resource_%s' % resource.spec.name

        update_dict = fields.copy()
        update_dict.update({'_updated': None, '_processing': datetime.now()})

        resource_data = self.schema.db[resource_name].find_and_modify(
            query={'_id': resource._id, '_processing': None},
            update={
                '$addToSet': {
                    '_updated_fields': {
                        '$each': fields.keys(),
                    }
                },
                '$set': update_dict},
            new=True,
        )
        return resource_data

    def _update_dependents(self, resource, fields):
        altered_ids = set()
        for field_name in fields:
            # find dependent calc fields for each field
            found = self.old_updater.find_affected_calcs_for_field(
                resource.build_child(field_name))

            # find resource ids for each resource type
            altered = self.old_updater.find_altered_resource_ids(
                found, resource)
            for spec_name, field_name, ids in altered:

                # mark resources as updated
                resource_name = 'resource_%s' % spec_name
                self.schema.db[resource_name].update_many(
                    {'_id': {'$in': ids}},
                    {'$set': {'_updated': True},
                    '$addToSet': {'_updated_fields': field_name}}
                    )
                altered_ids = altered_ids.union(ids)
        return altered_ids

    def _update_resource_dependents(self, resource):
        altered_ids = set()
        found = self.old_updater.find_affected_calcs_for_resource(
            resource)

        # find resource ids for each resource type
        altered = self.old_updater.find_altered_resource_ids(
            found, resource)
        for spec_name, field_name, ids in altered:

            # mark resources as updated
            resource_name = 'resource_%s' % spec_name
            self.schema.db[resource_name].update_many(
                {'_id': {'$in': ids}},
                {'$set': {'_updated': True},
                '$addToSet': {'_updated_fields': field_name}}
                )
            altered_ids = altered_ids.union(ids)
        return altered_ids

    def _finalize_update(self, resource):
        resource_name = 'resource_%s' % resource.spec.name

        resource_data = self.schema.db[resource_name].find_and_modify(
            query={'_id': resource._id, '_updated': None},
            update={
                '$set': {
                    '_updated_fields': None,
                    '_processing': None,
                }},
            new=True,
        )
        return resource_data

    def _perform_dependency_update(self, spec_name, resource_id):
        resource_name = 'resource_%s' % spec_name

        resource_data = self.schema.db[resource_name].find_and_modify(
            query={'_id': resource_id, '_processing': None},
            update={
                '_updated': None, '_processing': datetime.now()},
            new=True,
        )
        resource = Resource(None, "self", self.schema.specs[spec_name], resource_data)

        for field_name in resource_data['_updated_fields']:
            calc_field = resource.build_child(update[field_name])
            resource.data[update[field_name]] = calc_field.calculate()
            # find dependants of field_name, recalculate...
            # new updater?

        self._update_dependents(resource, resource.data.keys())
        self._finalize_update(resource)



    def init(self):
        self.update_id = self.schema.db['metaphor_update'].insert({})


    def fields_updated(self, resource, fields):
        self.init()
        self.init_update_resource(resource, fields)
        self.init_dependency_update()
        self.spawn_dependent_updates()
        self.wait_for_dependent_updates()
        self.finalize_update()


    def _update_local_dependencies(self, resource, field_names, data):
        local_deps = resource.local_field_dependencies(field_names)
        local_fields = [dep[0].field_name for dep in local_deps]
        if local_fields:
            recalced_fields = resource._recalc_fields(local_fields)
            data.update(recalced_fields)
            resource.data.update(recalced_fields)
            local_fields += self._update_local_dependencies(resource, local_fields, data)
        return local_fields

    def init_update_resource(self, spec_name, resource_id, fields):
        # write update state to mongo - spawn 'processing' writer
        self.schema.db['metaphor_update'].find_one_and_update({'_id': self.update_id}, {
            '$set': {
                'spec_name': spec_name,
                'resource_id': resource_id,
                'fields': fields.keys(),
                'dependents': [],
                'processing': datetime.now(),
            }
        }, return_document=ReturnDocument.AFTER)

        # update fields in resource
        resource_data = self.schema.db['resource_%s' % (spec_name,)].find_one_and_update(
            {'_id': resource_id},
            {
                '$set': fields,
            }, return_document=ReturnDocument.AFTER)

        self.resource = Resource(None, "self", self.schema.specs[spec_name], resource_data)
        updated_calc_fields = self._update_local_dependencies(self.resource, fields.keys(), fields)
        updated_data = dict((name, self.resource.data[name]) for name in updated_calc_fields)

        resource_data = self.schema.db['resource_%s' % (spec_name,)].update(
            {'_id': resource_id},
            {
                '$set': updated_data,
            })

        # update local calcs
        self.updated_field_names = set(fields.keys() + updated_data.keys())

    def _zip_altered(self, altered):
        dependents_dict = defaultdict(lambda: set())
        for spec_name, field_name, ids in altered:
            for resource_id in ids:
                dependents_dict[spec_name, resource_id].add(field_name)
        dependents = list()
        for (spec_name, resid), ids in dependents_dict.items():
            dependents.append((spec_name, resid, list(ids)))
        return dependents

    def init_dependency_update(self):
        # find dependent resources
        found = self.resource.foreign_field_dependencies(self.updated_field_names)

        # write to update obj id list
        altered = self.old_updater.find_altered_resource_ids(found, self.resource)

        self.dependents = self._zip_altered(altered)

        self.schema.db['metaphor_update'].update_one({'_id': self.update_id}, {
            '$set': {
                'dependents': self.dependents,
            }
        })

    def spawn_dependent_updates(self):
        # call pool
        self.gthreads = []
        for spec_name, resource_id, fields in self.dependents:
            self.gthreads.append(gevent.spawn(self.update_fields, spec_name, resource_id, fields))

    def wait_for_dependent_updates(self):
        # wait for updates to finish
        gevent.joinall(self.gthreads)

    def finalize_update(self):
        # wipe data

        # kill 'processing' writer
        pass
