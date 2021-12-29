
import json

from metaphor.updater import Updater
from urllib.error import HTTPError

from .schema import Schema, Spec, Field, CalcField, DependencyException, MalformedFieldException


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
            'name': field.name,
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
        self.schema.create_spec(spec_name)

    def create_field(self, spec_name, field_name, field_type, field_target=None, calc_str=None):
        if spec_name != 'root' and spec_name not in self.schema.specs:
            raise HTTPError(None, 404, 'Not Found', None, None)

        try:
            self.schema.create_field(spec_name, field_name, field_type, field_target, calc_str)
        except MalformedFieldException as me:
            raise HTTPError(None, 400, str(me), None, None)

        self._update_for_calc_field(spec_name, field_name, field_type)

    def _update_for_calc_field(self, spec_name, field_name, field_type):
        if field_type == 'calc':
            for resource in self.schema.db['resource_%s' % spec_name].find({}, {'_id': 1}):
                self.updater.update_calc(spec_name, field_name, self.schema.encodeid(resource['_id']))

    def update_field(self, spec_name, field_name, field_type, field_target=None, calc_str=None):
        try:
            self.schema.update_field(spec_name, field_name, field_type, field_target, calc_str)
        except MalformedFieldException as me:
            raise HTTPError(None, 400, str(me), None, None)

        self._update_for_calc_field(spec_name, field_name, field_type)

    def delete_field(self, spec_name, field_name):
        if spec_name != 'root' and spec_name not in self.schema.specs:
            raise HTTPError(None, 404, 'Not Found', None, None)

        try:
            self.schema.delete_field(spec_name, field_name)
        except DependencyException as de:
            raise HTTPError(None, 400, str(de), None, None)

        self.updater.remove_spec_field(spec_name, field_name)

    def list_integrations(self):
        return self.schema.list_integrations()

    def create_integration(self, integration_data):
        self.schema.create_integration(integration_data)

    def update_integration(self, integration_data):
        self.schema.update_integration(integration_data)

    def delete_integration(self, integration_id):
        self.schema.delete_integration(integration_id)
