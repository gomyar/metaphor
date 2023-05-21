

from flask import Blueprint
from flask import render_template
from flask import current_app
from flask import request
from flask import jsonify
from metaphor.login import login_required
from metaphor.login import admin_required
from metaphor.admin_api import SchemaSerializer
from metaphor.admin_api import AdminApi
from metaphor.schema import Schema

from urllib.error import HTTPError

import flask_login

from bson.objectid import ObjectId

api_bp = Blueprint('api', __name__, template_folder='templates',
                   static_folder='static', url_prefix='/api')

admin_bp = Blueprint('admin', __name__, template_folder='templates',
                     static_folder='static', url_prefix='/admin')

search_bp = Blueprint('search', __name__, template_folder='templates',
                      static_folder='static', url_prefix='/search')


def serialize_field(field):
    if field.field_type == 'calc':
        return {'type': 'calc', 'calc_str': field.calc_str, 'is_primitive': field.is_primitive()}
    elif field.field_type in ('int', 'str', 'float', 'bool'):
        return {'type': field.field_type}
    else:
        return {'type': field.field_type, 'target_spec_name': field.target_spec_name}

def serialize_spec(spec):
    return {
        'name': spec.name,
        'fields': {
            name: serialize_field(f) for name, f in spec.fields.items()
        },
    }

def serialize_schema(schema):
    return {
        'specs': {
            name: serialize_spec(spec) for name, spec in schema.specs.items()
        }
    }


@search_bp.route("/<spec_name>", methods=['GET'])
@login_required
def search(spec_name):
    query = request.args.get('query')
    api = current_app.config['api']
    return jsonify(api.search_resource(spec_name, query))


@api_bp.route("/", methods=['GET'])
@login_required
def api_root():
    api = current_app.config['api']
    if request.method == 'GET':
        root_data = dict((key, '/'+ key) for key in api.schema.root.fields.keys())
        root_data['ego'] = '/ego'
        root_data['_meta'] = {'spec': {'name': 'root'}}
        return jsonify(root_data)


@api_bp.route("/<path:path>", methods=['GET', 'POST', 'DELETE', 'PUT', 'PATCH'])
@login_required
def api(path):
    try:
        api = current_app.config['api']
        user = flask_login.current_user
        if request.method == 'POST':
            user.grants = [g['_id'] for g in user.create_grants]

            return jsonify(api.post(path, request.json, user)), 201
        if request.method == 'PATCH':
            user.grants = [g['_id'] for g in user.update_grants]

            return jsonify(api.patch(path, request.json, user))
        if request.method == 'GET':
            user.grants = [g['_id'] for g in user.read_grants]
            result = api.get(path, request.args, user)
            if result is not None:
                return jsonify(result)
            else:
                return "Not Found", 404
        if request.method == 'DELETE':
            user.grants = [g['_id'] for g in user.delete_grants]

            return jsonify(api.delete(path, user))
        if request.method == 'PUT':
            user.grants = [g['_id'] for g in user.put_grants]

            return jsonify(api.put(path, request.json, user)), 201
    except HTTPError as he:
        return jsonify({"error": he.reason}), he.getcode()


@api_bp.route("/schema", methods=['GET'])
@login_required
def schema():
    api = current_app.config['api']

    serializer = SchemaSerializer(flask_login.current_user.is_admin())
    return jsonify(serializer.serialize(api.schema))


@admin_bp.route("/schemas/<schema_id>")
@admin_required
def schema_editor(schema_id):
    return render_template('metaphor/schema_editor.html', schema_id=schema_id)


@admin_bp.route("/")
@admin_required
def admin_index():
    return render_template('metaphor/admin.html')


@admin_bp.route("/api/schemas", methods=['GET', 'POST'])
@login_required
def admin_api_schemas():
    factory = current_app.config['schema_factory']
    if request.method == 'GET':
        schema_list = factory.list_schemas()
        for schema in schema_list:
            schema.load_schema()
        serializer = SchemaSerializer(flask_login.current_user.is_admin())
        return jsonify({
            "schemas": [serializer.serialize(s) for s in schema_list]
        })
    else:
        if request.json:
            factory.create_schema_from_import(request.json)
            return jsonify({'ok': 1})
        else:
            factory.create_schema()
            return jsonify({'ok': 1})


@admin_bp.route("/api/schemas/<schema_id>", methods=['GET'])
@login_required
def schema_editor_api(schema_id):
    factory = current_app.config['schema_factory']
    serializer = SchemaSerializer(flask_login.current_user.is_admin())
    schema = factory.load_schema(schema_id)
    return jsonify(serializer.serialize(schema))


@admin_bp.route("/api/schemas/<schema_id>/specs", methods=['POST'])
@admin_required
def schema_editor_create_spec(schema_id):
    factory = current_app.config['schema_factory']
    schema = factory.load_schema(schema_id)
    schema.create_spec(request.json['spec_name'])
    return jsonify({'success': 1})


@admin_bp.route("/api/schemas/<schema_id>/specs/<spec_name>/fields", methods=['POST'])
@admin_required
def schema_editor_create_field(schema_id, spec_name):
    factory = current_app.config['schema_factory']
    schema = factory.load_schema(schema_id)

    field_name = request.json['field_name']
    field_type = request.json['field_type']
    field_target = request.json['field_target']
    calc_str = request.json['calc_str']

    if spec_name != 'root' and spec_name not in self.schema.specs:
        return jsonify({"error": "Not Found"}), 404

    try:
        schema.create_field(spec_name, field_name, field_type, field_target, calc_str)
    except MalformedFieldException as me:
        return jsonify({"error": str(me)}), 400

    return jsonify({'success': 1})


@admin_bp.route("/api/schemas/<schema_id>/specs/<spec_name>/fields/<field_name>", methods=['DELETE', 'PATCH'])
@admin_required
def schema_editor_delete_field(schema_id, spec_name, field_name):
    factory = current_app.config['schema_factory']
    schema = factory.load_schema(schema_id)
    if schema.current:
        return jsonify({"error": "cannot alter current schema"}), 400

    if request.method == 'DELETE':
        if spec_name != 'root' and spec_name not in self.schema.specs:
            return jsonify({"error": "Not Found"}), 404
        try:
            schema.delete_field(spec_name, field_name)
        except DependencyException as de:
            log.exception("DependencyException on DELETE for %s %s %s", schema_id, spec_name, field_name)
            return jsonify({"error": str(me)}), 400
    else:
        field_type = request.json['field_type']
        field_target = request.json['field_target']
        calc_str = request.json['calc_str']

        try:
            schema.update_field(spec_name, field_name, field_type, field_target, calc_str)
        except Exception as e:
            log.exception("Exception on PATCH for %s %s %s", schema_id, spec_name, field_name)
            return jsonify({"error": str(e)}), 400

    return jsonify({'success': 1})

