
import gevent
from datetime import datetime
from datetime import timedelta

from metaphor.resource import Resource
from metaphor.resource import ResourceLinkSpec
from metaphor.resource import CollectionSpec
from metaphor.resource import CollectionResource


class Updater(object):
    def __init__(self, schema):
        self.schema = schema
        self._updater_gthread = None
        self._running = None

    def start_updater(self):
        self._running = True
        self._updater_gthread = gevent.spawn(self._run_updater)

    def wait_for_updates(self):
        self._running = False
        self._updater_gthread.join()

    def _run_updater(self):
        while self._running:
            self._do_update()

    def _do_update(self):
        # find resources updated > 3 seconds ago
        for spec_name in self.schema.specs:
            resource_data = self.schema.db['resource_%s' % spec_name].find_and_modify(
                query={'_updated.at': {'$gt': datetime.now() - timedelta(seconds=3)}},
                update={'$set': {'_updated.at': datetime.now()}}
            )
            if resource_data:
                resource = Resource(None, "self", self.schema.specs[spec_name], resource_data)
                updater_ids = self.create_updaters(resource)
                self.run_updaters(updater_ids)
        # find updaters updated > 3 seconds ago

        gevent.sleep(0.1)

    def create_updaters(self, resource):
        found = self.find_affected_calcs_for_resource(resource)
        altered = self.find_altered_resource_ids(found, resource)
        updater_ids = []
        for spec, field_name, ids in altered:
            updater_ids.append(self._save_updates(spec, field_name, ids))
        return updater_ids

    def _save_updates(self, spec, field_name, ids, parent_id=None):
        update = {'spec': spec, 'field_name': field_name, 'resource_ids': list(ids)}
        if parent_id:
            update['parent_id'] = parent_id
        return self.schema.db['metaphor_updates'].insert(update)

    def find_affected_calcs_for_field(self, field):
        found = set()
        for calc_spec in self.schema._all_calcs:
            for resource_ref in calc_spec.all_resource_refs():
                resolved_field_spec = calc_spec.resolve_spec(resource_ref)
                if field.spec == resolved_field_spec:
                    found.add((calc_spec, resource_ref, resource_ref.rsplit('.', 1)[0]))
        return found

    def find_affected_calcs_for_resource(self, resource):
        found = set()
        for calc_spec in self.schema._all_calcs:
            for resource_ref in calc_spec.all_resource_refs():
                spec_hier = calc_spec.resolve_spec_hier(resource_ref)
                relative_ref = resource_ref.split('.')
                while spec_hier:
                    if type(spec_hier[-1]) in (CollectionSpec, ResourceLinkSpec):
                        spec = spec_hier[-1].target_spec
                    else:
                        spec = spec_hier[-1]
                    if resource.spec == spec:
                        found.add((calc_spec, resource_ref, ".".join(relative_ref)))
                    spec_hier.pop()
                    # may be root
                    if relative_ref:
                        relative_ref.pop()
        return found

    def find_altered_resource_ids(self, found, resource):
        altered = set()
        for calc_spec, resource_ref, relative_ref in found:
            # collection representing changed resources
            collection_spec = CollectionSpec(calc_spec.parent.name)
            collection_spec.schema = self.schema
            # need to distinguish between 'self' starting calcs and root collection calcs
            root = CollectionResource(None, 'self', collection_spec, None)
            child = root.build_child_dot(relative_ref)
            chain = child.build_aggregate_chain()
            if resource_ref.startswith('self.'):
                chain.insert(0, {'$match': {'_id': resource._id}})
            cursor = child.spec._collection().aggregate(chain)
            ids = set()
            for data in cursor:
                if child._parent:
                    ids.add(data[child._parent.build_aggregate_path()]['_id'])
                else:
                    ids.add(data['_id'])
            if ids:
                altered.add((calc_spec.parent.name, calc_spec.field_name, tuple(ids)))
        return altered

    def run_updaters(self, updater_ids):
        for update_id in updater_ids:
            self.perform_update(update_id)

    def perform_update(self, update_id):
        update = self.schema.db['metaphor_updates'].find_one({'_id': update_id})
        threads = [gevent.spawn(self._perform_update, update, update_id, resource_id) for resource_id in update['resource_ids']]
        gevent.joinall(threads)

    def _perform_update(self, update, update_id, resource_id):
        resource_data = self.schema.db['resource_%s' % (update['spec'],)].find_one({'_id': resource_id})
        resource = Resource(None, "self", self.schema.specs[update['spec']], resource_data)
        calc_field = resource.build_child(update['field_name'])
        resource.data[update['field_name']] = calc_field.calculate()
        resource.spec._collection().update(
            {'_id': resource._id},
            {'$set': {
                update['field_name']: calc_field.calculate(),
                '_updated': resource._update_dict([update['field_name']]),
            }})

        found = self.find_affected_calcs_for_field(calc_field)
        altered = self.find_altered_resource_ids(found, resource)
        for spec, field_name, ids in altered:
            inner_update_id = self._save_updates(spec, field_name, ids, update_id)
            self.perform_update(inner_update_id)
