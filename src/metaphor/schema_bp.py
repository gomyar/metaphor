
import os
import logging
log = logging.getLogger('metaphor')

from flask import Blueprint
from flask import current_app
from flask import request
from flask import jsonify
from flask import render_template

from metaphor.schema_factory import SchemaFactory
from metaphor.resource import CollectionSpec


schema_bp = Blueprint(
    'schema',
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), 'templates'),
    static_folder=os.path.join(os.path.dirname(__file__), 'static/metaphor'),
    static_url_path='/static/metaphor', url_prefix='/schema')


@schema_bp.route("/root", methods=['GET', 'POST'])
def roots_list():
    schema = current_app.config['schema']
    if request.method == 'GET':
        return jsonify([spec.serialize() for spec in schema.root_spec.fields.values()])
    if request.method == 'POST':
        schema.add_root(request.json['name'], CollectionSpec(request.json['target']))
        SchemaFactory().save_schema(schema)
        return jsonify({})


@schema_bp.route("/specs/<spec_name>", methods=['GET', 'PATCH'])
def schema_update(spec_name):
    schema = current_app.config['schema']
    if request.method == 'GET':
        return jsonify(schema.specs[spec_name].serialize())
    elif request.method == 'PATCH':
        data = request.json
        try:
            spec = schema.specs[spec_name]
            for field_name, field_data in data.items():
                SchemaFactory().validate_field_spec(schema, spec, field_name, field_data)
        except Exception as e:
            log.exception("Exception calling /specs/%s", spec_name)
            response = jsonify({'error': str(e)})
            response.status_code = 400
            return response
        for field_name, field_data in data.items():
            SchemaFactory().add_field_to_spec(schema, spec_name, field_name, field_data)
        for field_name, field_data in data.items():
            if field_data['type'] == 'calc':
                schema.kickoff_update_for_spec(schema.specs[spec_name], field_name)
        SchemaFactory().save_schema(schema)
        return jsonify({})


@schema_bp.route("/specs/<spec_name>/<field_name>", methods=['DELETE'])
def schema_delete_field(spec_name, field_name):
    schema = current_app.config['schema']

    dep_name = "%s.%s" % (spec_name, field_name)
    deps = schema.dependency_tree()

    for calc_res, calc_deps in deps.items():
        if dep_name in calc_deps:
            response = jsonify({'error': "%s depended upon by %s" % (
                dep_name, calc_res)})
            response.status_code = 400
            return response

    schema.specs[spec_name].fields.pop(field_name)
    SchemaFactory().save_schema(schema)
    return jsonify({})


@schema_bp.route("/specs", methods=['GET', 'POST'])
def schema_list():
    try:
        schema = current_app.config['schema']
        if request.method == 'POST':
            data = request.json
            spec_name = data['name']
            fields = data.get('fields', {})
            SchemaFactory().add_resource_to_schema(schema, spec_name, fields)
            SchemaFactory().save_schema(schema)
            return jsonify({})
        if request.method == 'GET':
            info = '''
                POST {"name": "resource_name", "fields": {"field_name": {"type": "field_type", ...}}}
                to /schema/specs to add a resource.
                "fields" may contain the following for "type": %s

                POST {"name": "root_name", "target": "resource_name"}
                to /schema/root to add a root collection.
            ''' % (SchemaFactory().field_builders.keys())

            return jsonify({
                'version': schema.version,
                'info': info,
                'specs': dict([(spec_name, spec.serialize()) for (spec_name, spec) in schema.specs.items()])})
    except Exception as e:
        log.exception("Exception calling /specs")
        response = jsonify({'error': str(e)})
        response.status_code = 400
        return response


@schema_bp.route("/specfor", methods=['GET'], defaults={'path': None})
@schema_bp.route("/specfor/<path:path>", methods=['GET'])
def spec_for(path):
    api = current_app.config['api']
    if path:
        resource = api.build_resource(path)
    else:
        resource = api.schema.root
    return jsonify(resource.spec.serialize())


@schema_bp.route("/", methods=['GET'])
def root_get():
    schema = current_app.config['schema']
    if request.method == 'GET':
        return jsonify({
            'specs': request.base_url + 'specs',
            'root': request.base_url + 'root',
        })
