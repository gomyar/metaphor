
from collections import defaultdict
from pymongo import ReturnDocument

import gevent

from datetime import datetime

import toposort

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

    def __init__(self, schema):
        self.schema = schema
        self.old_updater = Updater(schema)
        self.resource = None
        self.updated_field_names = []
        self.dependents = []
        self.gthreads = []

    def init(self):
        self.update_id = self.schema.db['metaphor_update'].insert({})

    def fields_updated(self, spec_name, resource_id, fields):
        self.init()
        self.init_update_resource(spec_name, resource_id, fields)
        self.init_dependency_update()
        self.spawn_dependent_updates()
        self.wait_for_dependent_updates()
        self.finalize_update()

    def fields_updated_inner(self, spec_name, resource_id, field_names):
        self.init()
        self.init_update_resource_inner(spec_name, resource_id, field_names)
        self.init_dependency_update()
        self.spawn_dependent_updates()
        self.wait_for_dependent_updates()
        self.finalize_update()

    def resource_created(self, spec_name, resource_id, fields):
        self.init()
        self.init_update_resource(spec_name, resource_id, fields)
        self.init_dependency_create()
        self.spawn_dependent_updates()
        self.wait_for_dependent_updates()
        self.finalize_update()

    def _recurse_local_deps(self, resource, field_names):
        deps = []
        local_deps = resource.local_field_dependencies(field_names)
        local_fields = [dep[0].field_name for dep in local_deps]
        if local_fields:
            deps.extend((dep[0].field_name, dep[1][5:]) for dep in local_deps)
            deps.extend(self._recurse_local_deps(resource, local_fields))
        return deps

    def _update_local_dependencies(self, resource, field_names):
        # topsort this guy
        all_deps = self._recurse_local_deps(resource, field_names)
        dep_tree = defaultdict(lambda: set())
        for field_name, dep_name in all_deps:
            dep_tree[field_name].add(dep_name)

        layers = list(toposort.toposort(dep_tree))
        # we can safely remove the first layer
        layers = layers[1:]

        updated_fields = {}
        for field_names in layers:
            data = resource._recalc_fields(field_names)
            resource.data.update(data)
            updated_fields.update(data)
        return updated_fields

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
        updated_calc_fields = self._update_local_dependencies(self.resource, fields.keys())
        updated_data = dict((name, self.resource.data[name]) for name in updated_calc_fields)

        if updated_data:
            resource_data = self.schema.db['resource_%s' % (spec_name,)].update(
                {'_id': resource_id},
                {
                    '$set': updated_data,
                })

        # update local calcs
        self.updated_field_names = set(fields.keys() + updated_data.keys())

    def init_update_resource_inner(self, spec_name, resource_id, field_names):
        # write update state to mongo - spawn 'processing' writer
        self.schema.db['metaphor_update'].find_one_and_update({'_id': self.update_id}, {
            '$set': {
                'spec_name': spec_name,
                'resource_id': resource_id,
                'fields': field_names,
                'dependents': [],
                'processing': datetime.now(),
            }
        }, return_document=ReturnDocument.AFTER)

        # update fields in resource
        resource_data = self.schema.db['resource_%s' % (spec_name,)].find_one(
            {'_id': resource_id})

        self.resource = Resource(None, "self", self.schema.specs[spec_name], resource_data)

        altered_data = self.resource._recalc_fields(field_names)

        updated_calc_fields = self._update_local_dependencies(self.resource, field_names)
        # TODO: add update foreign here
        updated_data = dict((name, self.resource.data[name]) for name in updated_calc_fields)
        updated_data.update(altered_data)

        if updated_data:
            resource_data = self.schema.db['resource_%s' % (spec_name,)].update(
                {'_id': resource_id},
                {
                    '$set': updated_data,
                })

        # update local calcs
        self.updated_field_names = set(field_names + updated_data.keys())

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

    def init_dependency_create(self):
        found = self.old_updater.find_affected_calcs_for_resource(self.resource.spec)
        altered = self.old_updater.find_altered_resource_ids(found, self.resource)

        self.dependents = self._zip_altered(altered)
        # filtering out own resource as local fields have already been accounted for
        #self.dependents = [dep for dep in self.dependents if dep[1] != self.resource._id]

        self.schema.db['metaphor_update'].update_one({'_id': self.update_id}, {
            '$set': {
                'dependents': self.dependents,
            }
        })

    def spawn_dependent_updates(self):
        # call pool
        self.gthreads = []
        for spec_name, resource_id, fields in self.dependents:
            new_update = Update(self.schema)
#            new_update.fields_updated_inner(spec_name, resource_id, fields)
            self.gthreads.append(gevent.spawn(new_update.fields_updated_inner, spec_name, resource_id, fields))

    def wait_for_dependent_updates(self):
        # wait for updates to finish
        gevent.joinall(self.gthreads)

    def finalize_update(self):
        # wipe data
        self.schema.db['metaphor_update'].remove({'_id': self.update_id})

        # kill 'processing' writer
        pass
