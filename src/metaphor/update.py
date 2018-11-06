
from datetime import datetime

from metaphor.updater import Updater


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

    def fields_updated(self, resource, fields):
        # _initiate_update_processing
        self._initiate_update_processing(resource, resource.data.keys())
        # _update_dependents
        self._update_dependents(resource, resource.data.keys())
        # _finalize_update
        self._finalize_update(resource)

    def resource_created(self, resource, fields):
        # _initiate_update_processing
        ### should we assume the resource is initiated on create? (yes)
#        self._initiate_update_processing(resource, fields)
        # _update_dependents
        self._update_resource_dependents(resource, fields)
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
        pass

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


'''
update:
    {
        _updated: True,
        _updated_at: now(),
        _updated_fields: {},
        _waiting_updaters: ['aef15f1f1f', 'aefagf2138d', etc]
    }

'''

# make sure mass update works in mongo
