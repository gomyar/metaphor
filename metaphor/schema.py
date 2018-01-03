

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
                while len(spec_hier) > 1:
                    if type(spec_hier[-1]) == CollectionSpec:
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

    def kickoff_create(self, parent_resource, new_resource):
        found = self.find_affected_calcs_for_resource(new_resource)
        altered_ids = self.find_altered_resource_ids(found, new_resource)
        for spec, field_name, ids in altered_ids:
            update_id = self._save_updates(spec, field_name, ids)
            self._perform_update(update_id)

        # update each one, kickoff update for each calc
        for field_name in new_resource.spec.fields.keys():
            self.kickoff_update(new_resource, field_name)

        # find collections affected by insert
        for calc_spec in self._all_calcs:
            for resource_ref in calc_spec.all_resource_refs():
                spec_hier = calc_spec.resolve_spec_hier(resource_ref)
                relative_ref = resource_ref.split('.')
                while spec_hier:
                    if parent_resource.spec == spec_hier[-1]:
                        self._update_found(
                            [(calc_spec, '.'.join(relative_ref))],
                            parent_resource._parent,
                            parent_resource.spec)
                    spec_hier.pop()
                    # may be root
                    if relative_ref:
                        relative_ref.pop()

    def kickoff_update(self, resource, field_name):
        # find all calc specs which refer to this field_name
        field_spec = resource.spec.fields[field_name]
        found = set()
        for calc_spec in self._all_calcs:
            for resource_ref in calc_spec.all_resource_refs():
                resolved_field_spec = calc_spec.resolve_spec(resource_ref)
                if field_spec == resolved_field_spec:
                    found.add((calc_spec, resource_ref))

        if found:
            self._update_found(found, resource, field_spec)

    def build_aggregate_chain(self, calc_spec, resource_ref):
        path = resource_ref.split('.')[:-1]
        current_field_name = resource_ref.split('.')[-1]
        reverse_path = ""
        aggregate_chain = [{"$unwind": "$_owners"}]
        while len(path) > 1:
            parent_field = path[-1]
            if parent_field == 'self':
                parent_spec = calc_spec.parent
            else:
                parent_spec = calc_spec.resolve_spec('.'.join(path))

            if type(parent_spec) == ResourceSpec:
                parent_spec_name = parent_spec.name
            elif type(parent_spec) == CollectionSpec:
                parent_spec_name = parent_spec.parent.name
            elif type(parent_spec) == ResourceLinkSpec:
                parent_spec_name = parent_spec.parent.name
            else:
                raise Exception("Cannot reverse up resource %s" % (parent_spec,))

            current_field_name = path.pop()

            subowner = "%s._owners" % (reverse_path,) if reverse_path else "_owners"

            aggregate_chain.append(
                {"$match": {"%s.owner_spec" % (subowner,): parent_spec_name,
                            "%s.owner_field" % (subowner,): current_field_name}})
            aggregate_chain.append(
                {"$lookup": {
                    "from": "resource_%s" % (parent_spec_name,),
                    "localField": "%s.owner_id" % (subowner,),
                    "foreignField": "_id",
                    "as": "%s" % (reverse_path + "__" + parent_field,),
                }})
            reverse_path += "__" + parent_field
            aggregate_chain.append({"$unwind": "$%s" % (reverse_path,)})
        return aggregate_chain, reverse_path

    def _update_found(self, found, resource, starting_spec):
        # find all resources containing said calc
        for calc_spec, resource_ref in found:
            # walk backwards along calc spec
            path = resource_ref.split('.')[:-1]
            if path == ['self']:
                calc_field = resource.build_child(calc_spec.field_name)
                resource.update({calc_spec.field_name: calc_field.calculate()})
            else:
                aggregate_chain, reverse_path = self.build_aggregate_chain(calc_spec, resource_ref)

                cursor = starting_spec.parent._collection().aggregate(aggregate_chain)

                for resource_data in cursor:
                    res = calc_spec.parent.build_resource(None, 'self', resource_data['%s' % (reverse_path,)])
                    res_calc = res.build_child(calc_spec.field_name)
                    res.update({calc_spec.field_name: res_calc.calculate()})
