
import gevent
from datetime import datetime
from datetime import timedelta

from metaphor.resource import Resource
from metaphor.resource import Field
from metaphor.resource import ResourceLinkSpec
from metaphor.resource import ReverseLinkSpec
from metaphor.resource import CollectionSpec
from metaphor.resource import CollectionResource
from metaphor.resource import LinkCollectionSpec
from metaphor.resource import CalcSpec
from metaphor.resource import ResourceLinkSpec


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
        for spec_name in self.schema.specs.keys():
            resource_data = self.schema.db['resource_%s' % spec_name].find_and_modify(
                query={'_updated.at': {'$lt': datetime.now() - timedelta(seconds=3)}},
                update={'$set': {'_updated.at': datetime.now()}}
            )
            if resource_data:
                resource = Resource(None, "self", self.schema.specs[spec_name], resource_data)
                updater_ids = self.create_updaters(resource)
                self.run_updaters(updater_ids)
        # find updaters updated > 3 seconds ago

        gevent.sleep(0.1)

    def create_updaters(self, resource):
        found = self.find_affected_calcs_for_resource(resource.spec)
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

    def find_affected_calcs_for_field(self, field_spec):
        found = set()
        for calc_spec in self.schema._all_calcs:
            for resource_ref in calc_spec.all_resource_refs():
                resolved_field_spec = calc_spec.resolve_spec(resource_ref)
                if field_spec == resolved_field_spec: # or field.spec == resolved_field_spec.parent:
                    found.add((calc_spec, resource_ref, resource_ref.rsplit('.', 1)[0]))
        # a CalcSpec which returns a resource link is really a ResourceLinkSpec of a sort
        if type(field_spec) == ResourceLinkSpec or (type(field_spec) == CalcSpec and not field_spec.is_primitive()):
            found = found.union(self.find_affected_calcs_for_resource(field_spec))
        return found

    def find_affected_calcs_for_resource(self, resource_spec):
        found = set()
        for calc_spec in self.schema._all_calcs:
            for resource_ref in calc_spec.all_resource_refs():
                spec_hier = calc_spec.resolve_spec_hier(resource_ref)
                relative_ref = resource_ref.split('.')
                while spec_hier:
                    if type(spec_hier[-1]) in (CollectionSpec, ResourceLinkSpec, ReverseLinkSpec, LinkCollectionSpec):
                        spec = spec_hier[-1].target_spec
                    else:
                        spec = spec_hier[-1]
                    if resource_spec == spec:
                        found.add((calc_spec, resource_ref, ".".join(relative_ref)))
                    elif type(resource_spec) == ResourceLinkSpec and resource_spec.target_spec == spec:
                        # lousy hack
                        found.add((calc_spec, resource_ref, ".".join(relative_ref)))
                    spec_hier.pop()
                    # may be root
                    if relative_ref:
                        relative_ref.pop()
        return found

    def find_altered_resource_ids(self, found, resource):
        altered = set()
        for calc_spec, resource_ref, relative_ref in found:
            ids = set()
            # collection representing changed resources
            collection_spec = CollectionSpec(calc_spec.parent.name)
            collection_spec.schema = self.schema

            # if ref begins with 'self' then aggregate relevant resources to be changed
            if resource_ref.startswith('self.'):
                # need to distinguish between 'self' starting calcs and root collection calcs
                root = CollectionResource(None, 'self', collection_spec, None)
                child = root.build_child_dot(relative_ref)
                chain = child.build_aggregate_chain()
                if type(child.spec) == CalcSpec and not child.spec.is_primitive():
                    chain.append({'$match': {'%s._id' % (child._parent.build_aggregate_path(),): resource._id}})
                else:
                    chain.insert(0, {'$match': {'_id': resource._id}})
                cursor = child.spec._collection().aggregate(chain)
                for data in cursor:
                    if child._parent:
                        ids.add(data[child._parent.build_aggregate_path()]['_id'])
                    else:
                        ids.add(data['_id'])
            # if ref is global (starts with root-level collection) then change applies to all members in calc_spec resource
            else:
                cursor = collection_spec._collection().find()
                for data in cursor:
                    ids.add(data['_id'])

            if ids:
                altered.add((calc_spec.parent.name, calc_spec.field_name, tuple(ids)))
        return altered

    def run_updaters(self, updater_ids):
        for update_id in updater_ids:
            self.perform_update(update_id)

    def perform_update(self, update_id):
        self._perform_update_single(update_id)

    def _perform_update_single(self, update_id):
        update = self.schema.db['metaphor_updates'].find_one({'_id': update_id})
        for resource_id in update['resource_ids']:
            self._perform_update(update, update_id, resource_id)

    def _perform_update_async(self, update_id):
        update = self.schema.db['metaphor_updates'].find_one({'_id': update_id})
        threads = [gevent.spawn(self._perform_update, update, update_id, resource_id) for resource_id in update['resource_ids']]
        gevent.joinall(threads, raise_error=True)

    def _perform_update(self, update, update_id, resource_id):
        resource_data = self.schema.db['resource_%s' % (update['spec'],)].find_one({'_id': resource_id})
        resource = Resource(None, "self", self.schema.specs[update['spec']], resource_data)
        calc_field = resource.build_child(update['field_name'])
        calc_result = calc_field.calculate()
        if isinstance(calc_result, Resource):
            result = calc_result._id
        elif isinstance(calc_result, Field):
            result = calc_result.data
        else:
            result = calc_result
        resource.data[update['field_name']] = result
        resource.spec._collection().update(
            {'_id': resource._id},
            {'$set': {
                update['field_name']: result,
#                '_updated': resource._update_dict([update['field_name']]),
            }})

        found = self.find_affected_calcs_for_field(calc_field.spec)
        altered = self.find_altered_resource_ids(found, resource)
        for spec, field_name, ids in altered:
            inner_update_id = self._save_updates(spec, field_name, ids, update_id)
            self.perform_update(inner_update_id)
