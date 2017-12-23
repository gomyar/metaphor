

from metaphor.resource import ResourceSpec
from metaphor.resource import ResourceLinkSpec
from metaphor.resource import CollectionSpec

class Schema(object):
    def __init__(self, db, version):
        self.db = db
        self.version = version
        self.specs = {}
        self.root_spec = ResourceSpec('root')
        self.add_resource_spec(self.root_spec)
        self._all_calcs = []
        self.root = self.root_spec.build_resource('root', {})

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

    def kickoff_create(self, parent_resource, new_resource):
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

    def _update_found(self, found, resource, starting_spec):

        # find all resources containing said calc
        for calc_spec, resource_ref in found:
            # walk backwards along calc spec
            path = resource_ref.split('.')[:-1]
            current_field_name = resource_ref.split('.')[-1]
            reverse_path = ''
            aggregate_chain = []
            if path == ['self']:
                calc_field = resource.build_child(calc_spec.field_name)
                resource.update({calc_spec.field_name: calc_field.calculate()})
            else:
                reverse_path = ""
                aggregate_chain.append({"$unwind": "$_owners"})
                while len(path) > 1: # and path != ['self']:
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

                cursor = starting_spec.parent._collection().aggregate(aggregate_chain)

                for resource_data in cursor:
                    res = calc_spec.parent.build_resource('self', resource_data['%s' % (reverse_path,)])
                    res_calc = res.build_child(calc_spec.field_name)
                    res.update({calc_spec.field_name: res_calc.calculate()})
