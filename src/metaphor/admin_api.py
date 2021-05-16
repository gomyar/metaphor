
from metaphor.updater import Updater
from urllib.error import HTTPError


class AdminApi(object):
    def __init__(self, schema):
        self.schema = schema
        self.updater = Updater(schema)

    def format_schema(self):
        schema_db = self.schema.db['metaphor_schema'].find_one()
        schema_json = {
            'version': 'tbd',
            'specs': schema_db['specs'] if schema_db else {},
            'root': schema_db['root'] if schema_db else {},
        }
        return schema_json

    def create_spec(self, spec_name):
        self.schema.db['metaphor_schema'].update(
            {'_id': self.schema._id},
            {"$set": {'specs.%s' % spec_name: {'fields': {}}}})
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
            from metaphor.lrparse.lrparse import parse
            spec = self.schema.specs[spec_name]
            tree = parse(calc_str, spec)
        except SyntaxError as se:
            raise HTTPError(None, 400, 'SyntaxError in calc: %s' % str(se), None, None)

    def create_field(self, spec_name, field_name, field_type, field_target=None, calc_str=None):
        self._check_field_name(field_name)
        if calc_str:
            self._check_calc_syntax(spec_name, calc_str)

        if field_type == 'calc':
            field_data = {'type': 'calc', 'calc_str': calc_str}
        elif field_type in ('int', 'str', 'float', 'bool'):
            field_data = {'type': field_type}
        else:
            field_data = {'type': field_type, 'target_spec_name': field_target}

        if spec_name == 'root':
            self.schema.db['metaphor_schema'].update(
                {'_id': self.schema._id},
                {"$set": {'root.%s' % (field_name,): field_data}})
        else:
            self.schema.db['metaphor_schema'].update(
                {'_id': self.schema._id},
                {"$set": {'specs.%s.fields.%s' % (spec_name, field_name): field_data}})
        self.schema.load_schema()
        if field_type == 'calc':
            for resource in self.schema.db['resource_%s' % spec_name].find({}, {'_id': 1}):
                self.updater.update_calc(spec_name, field_name, self.schema.encodeid(resource['_id']))

    def _check_field_dependencies(self, spec_name, field_name):
        all_deps = []
        for name, spec in self.schema.specs.items():
            for fname, field in spec.fields.items():
                if field.field_type == 'calc':
                    calc = self.schema.calc_trees[(name, fname)]
                    if "%s.%s" % (spec_name, field_name) in calc.get_resource_dependencies():
                        all_deps.append('%s.%s' % (name, fname))
        if all_deps:
            raise HTTPError(None, 400, '%s.%s referenced by %s' % (spec_name, field_name, all_deps), None, None)

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
