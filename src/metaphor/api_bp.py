

from flask import Blueprint
from flask import render_template
from flask import current_app
from flask import request
from flask import jsonify
from flask_login import login_required

import flask_login

api_bp = Blueprint('api', __name__, template_folder='templates',
                   static_folder='static', url_prefix='/api')

browser_bp = Blueprint('browser', __name__, template_folder='templates',
                       static_folder='static', url_prefix='/browser')

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
        return jsonify(root_data)


@api_bp.route("/<path:path>", methods=['GET', 'POST', 'DELETE', 'PUT', 'PATCH'])
@login_required
def api(path):
    api = current_app.config['api']
    user = flask_login.current_user
    if request.method == 'POST':
        # check permissions
        if not any(('/'+path).startswith(grant_url['url']) for grant_url in flask_login.current_user.create_grants):
            return "Not Allowed", 403

        return jsonify(api.post(path, request.json)), 201
    if request.method == 'PATCH':
        if not any(('/'+path).startswith(grant_url['url']) for grant_url in flask_login.current_user.update_grants):
            return "Not Allowed", 403

        return jsonify(api.patch(path, request.json, user))
    if request.method == 'GET':
        user.grants = [g['_id'] for g in user.read_grants]
        result = api.get(path, None, user)
        if result is not None:
            return jsonify(result)
        else:
            return "Not Found", 404
    if request.method == 'DELETE':
        if not any(('/'+path).startswith(grant_url['url']) for grant_url in flask_login.current_user.delete_grants):
            return "Not Allowed", 403

        return jsonify(api.delete(path, user))


@browser_bp.route("/", methods=['GET'])
@login_required
def browser_root():
    api = current_app.config['api']
    resource = dict((key, '/' + key) for key in api.schema.root.fields.keys())
    resource['ego'] = '/ego'
    spec = api.schema.root
    return render_template('metaphor/api_browser.html',
        path='/', resource=resource, spec=serialize_spec(spec), is_collection=False, can_post=False, is_linkcollection=False,
        schema=serialize_schema(api.schema))


@browser_bp.route("/<path:path>", methods=['GET'])
@login_required
def browser(path):
    api = current_app.config['api']
    user = flask_login.current_user
    user.grants = [g['_id'] for g in user.read_grants]
    resource = api.get(path, None, user)
    spec, is_collection, can_post, is_linkcollection = api.get_spec_for(path, user)
    return render_template('metaphor/api_browser.html',
        path=path, resource=resource, spec=serialize_spec(spec), is_collection=is_collection, can_post=can_post, is_linkcollection=is_linkcollection,
        schema=serialize_schema(api.schema))


@admin_bp.route("/schema_editor")
@login_required
def schema_editor():
    if not flask_login.current_user.is_admin():
        return "Unauthorized", 403
    return render_template('metaphor/schema_editor.html')


@admin_bp.route("/schema_editor/api", methods=['GET'])
@login_required
def schema_editor_api():
    if not flask_login.current_user.is_admin():
        return "Unauthorized", 403
    admin_api = current_app.config['admin_api']
    return jsonify(admin_api.format_schema())


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


@admin_bp.route("/schema_editor/api/specs/<spec_name>/fields/<field_name>", methods=['DELETE'])
@login_required
def schema_editor_delete_field(spec_name, field_name):
    if not flask_login.current_user.is_admin():
        return "Unauthorized", 403
    admin_api = current_app.config['admin_api']
    admin_api.delete_field(spec_name, field_name)
    return jsonify({'success': 1})
