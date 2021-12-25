
import json

from metaphor.updater import Updater
from urllib.error import HTTPError

from metaphor.lrparse.lrparse import parse
from toposort import toposort, CircularDependencyError

from .schema import Schema, Spec, Field, CalcField


class SchemaSerializer(object):
    def __init__(self, include_admindata=False):
        self.serializers = dict(
            Schema=self._serialize_schema,
            Spec=self._serialize_spec,
            Field=self._serialize_field,
            CalcField=self._serialize_calcfield,
        )
        self.include_admindata = include_admindata

    def serialize(self, pyobject):
        encoded_str = json.dumps(pyobject, default=self._encode, indent=4)
        return json.loads(encoded_str)

    def _encode(self, obj):
        obj_name =  type(obj).__name__
        if obj_name in self.serializers:
            data = self.serializers[obj_name](obj)
            return data
        raise TypeError("Cannot serialize object %s" % obj_name)

    def _serialize_schema(self, schema):
        return {
            'specs': schema.specs,
            'version': 'tbd',
            'root': schema.root,
        }

    def _serialize_spec(self, spec):
        return {
            'name': spec.name,
            'fields': spec.fields,
        }

    def _serialize_field(self, field):
        return {
            'name': field.name,
            'type': field.field_type,
            'target_spec_name': field.target_spec_name,
            'is_collection': field.is_collection(),
        }

    def _serialize_calcfield(self, field):
        calc_data = {
            'name': field.field_name,
            'type': field.field_type,
            'is_collection': field.is_collection(),
            'target_spec_name': None,
        }
        calc_type = field.infer_type()
        if not calc_type.is_primitive():
            calc_data['target_spec_name'] = calc_type.name
        if self.include_admindata:
            calc_data['calc_str'] = field.calc_str
        return calc_data


class AdminApi(object):
    def __init__(self, schema):
        self.schema = schema
        self.updater = Updater(schema)

    def format_schema(self, include_admindata=False):
        return SchemaSerializer(include_admindata).serialize(self.schema)

    def create_spec(self, spec_name):
        self.schema.db['metaphor_schema'].update(
            {'_id': self.schema._id},
            {"$set": {'specs.%s' % spec_name: {'fields': {}}}},
            upsert=True)
        self.schema.load_schema()

    def _check_field_name(self, field_name):
        if not field_name:
            raise HTTPError(None, 400, 'Field name cannot be blank', None, None)
        for start in ('link_', 'parent_', '_'):
            if field_name.startswith(start):
                raise HTTPError(None, 400, 'Field name cannot begin with "%s"' % (start,), None, None)
        if field_name in ('self', 'id'):
            raise HTTPError(None, 400, 'Field name cannot be reserverd word "%s"' % (field_name,), None, None)
        if not field_name[0].isalpha():
            raise HTTPError(None, 400, 'First character must be letter', None, None)

    def _check_calc_syntax(self, spec_name, calc_str):
        try:
            spec = self.schema.specs[spec_name]
            tree = parse(calc_str, spec)
        except SyntaxError as se:
            raise HTTPError(None, 400, 'SyntaxError in calc: %s' % str(se), None, None)

    def create_field(self, spec_name, field_name, field_type, field_target=None, calc_str=None):
        if spec_name != 'root' and spec_name not in self.schema.specs:
            raise HTTPError(None, 404, 'Not Found', None, None)
        if spec_name != 'root' and field_name in self.schema.specs[spec_name].fields:
            raise HTTPError(None, 400, 'Field already exists: %s' % field_name, None, None)
        self._check_field_name(field_name)
        self._update_field(spec_name, field_name, field_type, field_target, calc_str)

    def _update_field(self, spec_name, field_name, field_type, field_target=None, calc_str=None):
        if calc_str:
            self._check_calc_syntax(spec_name, calc_str)
            self._check_circular_dependencies(spec_name, field_name, calc_str)

        if field_type == 'calc':
            field_data = {'type': 'calc', 'calc_str': calc_str}
        elif field_type in ('int', 'str', 'float', 'bool'):
            field_data = {'type': field_type}
        else:
            field_data = {'type': field_type, 'target_spec_name': field_target}

        if spec_name == 'root':
            self.schema.db['metaphor_schema'].update(
                {'_id': self.schema._id},
                {"$set": {'root.%s' % (field_name,): field_data}},
                upsert=True)
        else:
            self.schema.db['metaphor_schema'].update(
                {'_id': self.schema._id},
                {"$set": {'specs.%s.fields.%s' % (spec_name, field_name): field_data}},
                upsert=True)
        self.schema.load_schema()
        if field_type == 'calc':
            for resource in self.schema.db['resource_%s' % spec_name].find({}, {'_id': 1}):
                self.updater.update_calc(spec_name, field_name, self.schema.encodeid(resource['_id']))

    def update_field(self, spec_name, field_name, field_type, field_target=None, calc_str=None):
        if spec_name != 'root' and field_name not in self.schema.specs[spec_name].fields:
            raise HTTPError(None, 400, 'Field does not exist: %s' % field_name, None, None)
        self._update_field(spec_name, field_name, field_type, field_target, calc_str)

    def _check_field_dependencies(self, spec_name, field_name):
        all_deps = self._get_field_dependencies(spec_name, field_name)
        if all_deps:
            raise HTTPError(None, 400, '%s.%s referenced by %s' % (spec_name, field_name, all_deps), None, None)

    def _get_field_dependencies(self, spec_name, field_name):
        all_deps = []
        for name, spec in self.schema.specs.items():
            for fname, field in spec.fields.items():
                if field.field_type == 'calc':
                    calc = self.schema.calc_trees[(name, fname)]
                    if "%s.%s" % (spec_name, field_name) in calc.get_resource_dependencies():
                        all_deps.append('%s.%s' % (name, fname))
        return all_deps

    def _check_circular_dependencies(self, spec_name, field_name, calc_str):
        spec = self.schema.specs[spec_name]
        tree = parse(calc_str, spec)
        deps = tree.get_resource_dependencies()
        dep_tree = {}
        for name, spec in self.schema.specs.items():
            for fname, field in spec.fields.items():
                if field.field_type == 'calc':
                    calc = self.schema.calc_trees[(name, fname)]
                    dep_tree["%s.%s" % (name, fname)] = calc.get_resource_dependencies()
        dep_tree["%s.%s" % (spec_name, field_name)] = deps
        try:
            list(toposort(dep_tree))
        except CircularDependencyError as cde:
            raise HTTPError(None, 400, '%s.%s has circular dependencies: %s' % (spec_name, field_name, cde.data), None, None)

    def delete_field(self, spec_name, field_name):
        self._check_field_dependencies(spec_name, field_name)

        spec = self.schema.specs[spec_name]
        field = spec.fields[field_name]
        if field.field_type in ('link', 'linkcollection'):
            self.schema.specs[field.target_spec_name].fields.pop('link_%s_%s' % (spec_name, field_name))
        spec.fields.pop(field_name)

        self.schema.db['metaphor_schema'].update(
            {'_id': self.schema._id},
            {"$unset": {'specs.%s.fields.%s' % (spec_name, field_name): ''}})
        self.updater.remove_spec_field(spec_name, field_name)

    def list_integrations(self):
        return self.schema.list_integrations()

    def create_integration(self, integration_data):
        self.schema.create_integration(integration_data)

    def update_integration(self, integration_data):
        self.schema.update_integration(integration_data)

    def delete_integration(self, integration_id):
        self.schema.delete_integration(integration_id)
