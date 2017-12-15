

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

    def build_dependency_tree(self):
        deps = {}
#        for field_name, field_spec in self.specs.items():
#            deps[field_name] = field_spec.dependencies()
        return deps

    def kickoff_create(self, collection, resource):
        # aggregation resources
        # find all calc specs which refer to this collection
        # find all resources containing said calc which rely on this collection
        # update each one, kickoff update for each calc
        pass
        for field_name in resource.spec.fields.keys():
            self.kickoff_update(resource, field_name)

    def _old_kickoff():
        for field_name, field_spec in collection.spec.fields.items():
            field = collection.build_child(field_name)
            deps = field.dependencies()
            for calc_field in deps:
                dep_res = calc_field._parent
                dep_res.update(field_name, calc_field.calculate())

    def kickoff_update(self, resource, field_name):
        # find all calc specs which refer to this field_name
        field_spec = resource.spec.fields[field_name]
        found = set()
        for calc_spec in self._all_calcs:
            for resource_ref in calc_spec.all_resource_refs():
                resolved_field_spec = calc_spec.resolve_spec(resource_ref)
                if field_spec == resolved_field_spec:
                    found.add((calc_spec, resource_ref))

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
                while path and path != ['self']:
                    parent_field = path[-1]
                    reverse_path = ""
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

                    aggregate_chain.append({"$unwind": "$_owners%s" % (reverse_path,)})
                    aggregate_chain.append(
                        {"$match": {"_owners%s.owner_spec" % (reverse_path,): parent_spec_name,
                                    "_owners%s.owner_field" % (reverse_path,): current_field_name}})
                    aggregate_chain.append(
                        {"$lookup": {
                            "from": "resource_%s" % (parent_spec_name,),
                            "localField": "_owners%s.owner_id" % (reverse_path,),
                            "foreignField": "_id",
                            "as": "_owners%s" % (reverse_path,),
                        }})
                    aggregate_chain.append({"$unwind": "$_owners%s" % (reverse_path,)})

                    if path and path!=['self']:
                        reverse_path += "_" + parent_field

                #aggregate_chain.append(
                #    {"$project": {"_owners%s._id" % (reverse_path,): 1}}
                #)
                cursor = field_spec.parent._collection().aggregate(aggregate_chain)

                for resource_data in cursor:
                    res = calc_spec.parent.build_resource('self', resource_data['_owners%s' % (reverse_path,)])
                    res_calc = res.build_child(calc_spec.field_name)
                    res.update({calc_spec.field_name: res_calc.calculate()})

    def _old_update():
        for field_name, field_spec in resource.spec.fields.items():
            field = resource.build_child(field_name)
            deps = field.dependencies()
            for calc_field in deps:
                dep_res = calc_field._parent
                dep_res.update(field_name, calc_field.calculate())
