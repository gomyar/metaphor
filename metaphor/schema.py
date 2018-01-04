

from metaphor.resource import ResourceSpec
from metaphor.resource import Resource
from metaphor.resource import ResourceLinkSpec
from metaphor.resource import CollectionSpec
from metaphor.resource import CollectionResource
from resource import RootResource

class Schema(object):
    def __init__(self, db, version):
        self.db = db
        self.version = version
        self.specs = {}
        self.root_spec = ResourceSpec('root')
        self.add_resource_spec(self.root_spec)
        self._all_calcs = []
        self.root = RootResource(self)

    def __repr__(self):
        return "<Schema %s>" % (self.version)

    def add_calc(self, calc_spec):
        self._all_calcs.append(calc_spec)

    def serialize(self):
        return dict(
            (name, spec.serialize()) for (name, spec) in self.specs.items())

    def add_resource_spec(self, resource_spec):
        self.specs[resource_spec.name] = resource_spec
        resource_spec.schema = self

    def add_root(self, name, spec):
        self.specs['root'].add_field(name, spec)

    def all_types(self):
        return sorted(self.specs.keys())

    def find_affected_calcs_for_field(self, field):
        found = set()
        for calc_spec in self._all_calcs:
            for resource_ref in calc_spec.all_resource_refs():
                resolved_field_spec = calc_spec.resolve_spec(resource_ref)
                if field.spec == resolved_field_spec:
                    found.add((calc_spec, resource_ref, resource_ref.rsplit('.', 1)[0]))
        return found

    def find_affected_calcs_for_resource(self, resource):
        found = set()
        for calc_spec in self._all_calcs:
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
            collection_spec.schema = self
            root = CollectionResource(None, 'self', collection_spec, None)
            child = root.build_child_dot(relative_ref)
            chain = child.build_aggregate_chain()
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

    def _perform_update(self, update_id):
        updates = self.db['metaphor_updates'].find({'_id': update_id})
        for update in updates:
            for resource_id in update['resource_ids']:
                resource_data = self.db['resource_%s' % (update['spec'],)].find_one({'_id': resource_id})
                resource = Resource(None, "self", self.specs[update['spec']], resource_data)
                calc_field = resource.build_child(update['field_name'])
                resource.data[update['field_name']] = calc_field.calculate()
                resource.spec._collection().update({'_id': resource._id}, {'$set': {update['field_name']: calc_field.calculate()}})

                # find updated resource ids
                found = self.find_affected_calcs_for_field(calc_field)
                altered = self.find_altered_resource_ids(found, resource)
                # save update
                for spec, field_name, ids in altered:
                    inner_update_id = self._save_updates(spec, field_name, ids, update_id)
                    self._perform_update(inner_update_id)

        # perform any new updates

    def _save_updates(self, spec, field_name, ids, parent_id=None):
        update = {'spec': spec, 'field_name': field_name, 'resource_ids': list(ids)}
        if parent_id:
            update['parent_id'] = parent_id
        return self.db['metaphor_updates'].insert(update)

    def find_dependent_resources_for_resource(self, resource):
        found = self.find_affected_calcs_for_resource(resource)
        return self.find_altered_resource_ids(found, resource)

    def perform_update_for(self, altered):
        for spec, field_name, ids in altered:
            update_id = self._save_updates(spec, field_name, ids)
            self._perform_update(update_id)

    def kickoff_create_update(self, parent_resource, new_resource):
        altered = self.find_dependent_resources_for_resource(new_resource)
        self.perform_update_for(altered)

    def kickoff_update(self, resource, field_name):
        found = self.find_affected_calcs_for_field(resource.build_child(field_name))
        altered = self.find_altered_resource_ids(found, resource)
        # save update
        for spec, field_name, ids in altered:
            inner_update_id = self._save_updates(spec, field_name, ids)
            self._perform_update(inner_update_id)
