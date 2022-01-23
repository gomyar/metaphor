

from flask import Blueprint
from flask import render_template
from flask import current_app
from flask import request
from flask import jsonify
from flask_login import login_required

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


@admin_bp.route("/schema_editor")
@login_required
def schema_editor():
    if not flask_login.current_user.is_admin():
        return "Unauthorized", 403
    return render_template('metaphor/schema_editor.html')


@admin_bp.route("/schema_editor/api", methods=['GET'])
@login_required
def schema_editor_api():
    admin_api = current_app.config['admin_api']
    return jsonify(admin_api.format_schema(flask_login.current_user.is_admin()))


@admin_bp.route("/schema_editor/api/specs", methods=['POST'])
@login_required
def schema_editor_create_spec():
    if not flask_login.current_user.is_admin():
        return "Unauthorized", 403
    admin_api = current_app.config['admin_api']
    admin_api.create_spec(request.json['spec_name'])
    return jsonify({'success': 1})


@admin_bp.route("/schema_editor/api/specs/<spec_name>/fields", methods=['POST'])
@login_required
def schema_editor_create_field(spec_name):
    if not flask_login.current_user.is_admin():
        return "Unauthorized", 403
    admin_api = current_app.config['admin_api']

    field_name = request.json['field_name']
    field_type = request.json['field_type']
    field_target = request.json['field_target']
    calc_str = request.json['calc_str']

    admin_api.create_field(spec_name, field_name, field_type, field_target, calc_str)
    return jsonify({'success': 1})


@admin_bp.route("/schema_editor/api/specs/<spec_name>/fields/<field_name>", methods=['DELETE', 'PATCH'])
@login_required
def schema_editor_delete_field(spec_name, field_name):
    if not flask_login.current_user.is_admin():
        return "Unauthorized", 403
    admin_api = current_app.config['admin_api']
    if request.method == 'DELETE':
        admin_api.delete_field(spec_name, field_name)
    else:
        field_type = request.json['field_type']
        field_target = request.json['field_target']
        calc_str = request.json['calc_str']

        admin_api.update_field(spec_name, field_name, field_type, field_target, calc_str)
    return jsonify({'success': 1})


@admin_bp.route("/schema_editor/api/export", methods=['GET'])
@login_required
def schema_export():
    if not flask_login.current_user.is_admin():
        return "Unauthorized", 403
    admin_api = current_app.config['admin_api']
    return jsonify(admin_api.export_schema())


@admin_bp.route("/schema_editor/api/import", methods=['POST'])
@login_required
def schema_import():
    if not flask_login.current_user.is_admin():
        return "Unauthorized", 403
    admin_api = current_app.config['admin_api']
    return jsonify(admin_api.import_schema(request.json))


@admin_bp.route("/integrations")
@login_required
def integrations_view():
    if not flask_login.current_user.is_admin():
        return "Unauthorized", 403
    return render_template('metaphor/integrations.html')


@admin_bp.route("/integrations/api", methods=['GET', 'POST'])
@login_required
def all_integrations():
    if not flask_login.current_user.is_admin():
        return "Unauthorized", 403
    admin_api = current_app.config['admin_api']

    if request.method == 'GET':
        def serialize_integration(integration):
            serialize = integration.copy()
            serialize['id'] = str(serialize.pop('_id'))
            return serialize
        integrations = admin_api.list_integrations()
        serialized = [serialize_integration(s) for s in integrations]

        return jsonify(serialized)
    else:
        data = request.json
        admin_api.create_integration(data)
        return jsonify({'success': 1})


@admin_bp.route("/integrations/api/<integration_id>", methods=['PATCH', 'DELETE'])
@login_required
def single_integration(integration_id):
    if not flask_login.current_user.is_admin():
        return "Unauthorized", 403
    admin_api = current_app.config['admin_api']

    if request.method == 'PATCH':
        data = request.json
        data['_id'] = ObjectId(data.pop('id'))
        admin_api.update_integration(data)
    else:
        admin_api.delete_integration(ObjectId(request.json['id']))
    return jsonify({'success': 1})
