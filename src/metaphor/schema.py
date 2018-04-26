
from metaphor.resource import ResourceSpec
from metaphor.resource import RootResource
from metaphor.resource import CalcSpec
from metaphor.updater import Updater


class Schema(object):
    def __init__(self, db, version):
        self.db = db
        self.version = version
        self.specs = None
        self.root_spec = None
        self._all_calcs = []
        self.root = None
        self.updater = None
        self._functions = dict()

        self.reset()

    def reset(self):
        if self.updater:
            self.updater.wait_for_updates()
        self.specs = {}
        self.root_spec = ResourceSpec('root')
        self.add_resource_spec(self.root_spec)
        self._all_calcs = []
        self.root = RootResource(self)
        self.updater = Updater(self)

    def dependency_tree(self):
        deps = {}
        for spec_name, spec in self.specs.items():
            for field_name, spec in spec.fields.items():
                if type(spec) == CalcSpec:
                    spec_deps = set()
                    for resource_ref in spec.all_resource_refs():
                        ref_spec = spec.resolve_spec(resource_ref)
                        if ref_spec.parent:
                            spec_deps.add("%s.%s" % (ref_spec.parent.name, ref_spec.field_name))
                        else:
                            spec_deps.add(ref_spec.name)
                    deps["%s.%s" % (spec_name, field_name)] = spec_deps
        return deps

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

    def create_updaters(self, resource):
        return self.updater.create_updaters(resource)

    def run_updaters(self, updater_ids):
        self.updater.run_updaters(updater_ids)

    def kickoff_create_update(self, new_resource):
        updater_ids = self.create_updaters(new_resource)
        self.run_updaters(updater_ids)

    def kickoff_update(self, resource, field_name):
        found = self.updater.find_affected_calcs_for_field(resource.build_child(field_name))
        altered = self.updater.find_altered_resource_ids(found, resource)
        updater_ids = []
        for spec, field_name, ids in altered:
            updater_ids.append(self.updater._save_updates(spec, field_name, ids))

        self.run_updaters(updater_ids)

    def kickoff_update_for_spec(self, spec, field_name):
        resources = spec._collection().find({}, {'_id': 1})
        resource_ids = [res['_id'] for res in resources]
        updater_id = self.updater._save_updates(spec.name, field_name, resource_ids)
        self.run_updaters([updater_id])

    def save(self):
        self.db['metaphor_schema'].insert(schema_data)

    def register_function(self, func_name, func):
        self._functions[func_name] = func

    def execute_function(self, func_name, resource):
        return self._functions[func_name](resource)
