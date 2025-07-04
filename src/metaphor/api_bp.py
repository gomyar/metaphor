

from flask import Blueprint
from flask import render_template
from flask import current_app
from flask import request
from flask import jsonify
from flask import abort
from metaphor.login import login_required
from metaphor.login import admin_required
from metaphor.schema import Schema, CalcField
from metaphor.mutation import Mutation, MutationFactory
from metaphor.schema import DependencyException
from metaphor.schema_serializer import serialize_schema

from urllib.error import HTTPError
from gridfs import GridOut

import flask_login

from bson.objectid import ObjectId

import logging
log = logging.getLogger(__name__)


api_bp = Blueprint('api', __name__, template_folder='templates',
                   static_folder='static', url_prefix='/api')

admin_bp = Blueprint('admin', __name__, template_folder='templates',
                     static_folder='static', url_prefix='/admin')

search_bp = Blueprint('search', __name__, template_folder='templates',
                      static_folder='static', url_prefix='/search')


def serialize_mutation(mutation):
    api = current_app.config['api']
    user = api.schema.load_user_by_id(flask_login.current_user.user_id)
    from_schema = serialize_schema(mutation.from_schema, user.admin) if mutation.from_schema else None
    to_schema = serialize_schema(mutation.to_schema, user.admin) if mutation.to_schema else None
    return {
        'id': str(mutation._id),
        'from_schema': from_schema,
        'to_schema': to_schema,
        'steps': mutation.steps,
        'state': mutation.state,
        'error': mutation.error,
    }


def load_checked_schema(schema_id):
    factory = current_app.config['schema_factory']
    schema = factory.load_schema(schema_id)
    if not schema:
        abort(404)
    if schema.current:
        abort(403)
    return schema

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
        root_data = {}
        root_data['_meta'] = {'spec': {'name': 'root'}, 'resource_type': 'resource', 'is_collection': False}
        return jsonify(root_data)


@api_bp.route("/<path:path>", methods=['GET', 'POST', 'DELETE', 'PUT', 'PATCH'])
@login_required
def api(path):
    try:
        api = current_app.config['api']
        identity = flask_login.current_user
        user = api.schema.load_user_by_id(identity.user_id)
        if not user:
            return jsonify({"error": "Your identity does not have access to this service"}), 403
        content_type = request.content_type.split(';')[0] if request.content_type else None
        if content_type == 'multipart/form-data':
            return jsonify({"error": "Unsupported content type"}), 400
        if request.method == 'POST':
            if content_type == 'application/json':
                return jsonify(api.post(path, request.json, user, request)), 201
            else:
                return jsonify(api.post(path, None, user, request)), 201
        if request.method == 'PATCH':
            return jsonify(api.patch(path, request.json, user))
        if request.method == 'GET':
            result = api.get(path, request.args, user)
            if result is not None:
                if isinstance(result, GridOut):
                    response = current_app.response_class(result, mimetype=result.content_type)
                    if request.args.get('download'):
                        response.headers['Content-Disposition'] = f'attachment; filename="{request.args['download']}"'
                    if request.args.get('contenttype'):
                        response.content_type = request.args['contenttype']
                    return response
                return jsonify(result)
            else:
                return "Not Found", 404
        if request.method == 'DELETE':
            return jsonify(api.delete(path, user))
        if request.method == 'PUT':
            return jsonify(api.put(path, request.json, user)), 201
    except HTTPError as he:
        return jsonify({"error": he.reason}), he.getcode()


@api_bp.route("/schema", methods=['GET'])
@login_required
def schema():
    api = current_app.config['api']

    identity = flask_login.current_user
    user = api.schema.load_user_by_id(identity.user_id)
    if not user:
        return jsonify({"error": "Your identity does not have access to this service"}), 403
    return jsonify(serialize_schema(api.schema, user.admin))


@api_bp.route("/schema/groups", methods=['GET'])
@login_required
def schema_groups():
    api = current_app.config['api']

    identity = flask_login.current_user
    user = api.schema.load_user_by_id(identity.user_id)
    if not user:
        return jsonify({"error": "Your identity does not have access to this service"}), 403
    return jsonify(list(api.schema.groups.keys()))


@api_bp.route("/schema/groups/<group_name>/users", methods=['POST'])
@login_required
def schema_usergroups(group_name):
    api = current_app.config['api']

    identity = flask_login.current_user
    user = api.schema.load_user_by_id(identity.user_id)
    if not user:
        return jsonify({"error": "Your identity does not have access to this service"}), 403

    user_id = api.schema.decodeid(request.json['id'])
    api.schema.add_user_to_group(group_name, user_id)
    return jsonify({"success": 1})


@api_bp.route("/schema/groups/<group_name>/users/<user_id>", methods=['DELETE'])
@login_required
def schema_delete_user_from_group(group_name, user_id):
    api = current_app.config['api']

    identity = flask_login.current_user
    user = api.schema.load_user_by_id(identity.user_id)
    if not user:
        return jsonify({"error": "Your identity does not have access to this service"}), 403

    user_id = api.schema.decodeid(user_id)
    api.schema.remove_user_from_group(group_name, user_id)
    return jsonify({"success": 1})


@admin_bp.route("/schemas/<schema_id>")
@admin_required
def schema_editor(schema_id):
    return render_template('metaphor/schema_editor.html', schema_id=schema_id)


@admin_bp.route("/mutations/<mutation_id>")
@admin_required
def manage_mutation(mutation_id):
    return render_template('metaphor/manage_mutation.html', mutation_id=mutation_id)


@admin_bp.route("/")
@admin_required
def admin_index():
    return render_template('metaphor/admin.html')


@admin_bp.route("/api/schemas", methods=['GET', 'POST'])
@admin_required
def admin_api_schemas():
    api = current_app.config['api']
    factory = current_app.config['schema_factory']
    identity = flask_login.current_user
    user = api.schema.load_user_by_id(identity.user_id)
    if request.method == 'GET':
        schema_list = factory.list_schemas()
        return jsonify({
            "schemas": [serialize_schema(schema, user.admin) for schema in schema_list]
        })
    else:
        if request.json:
            if request.json.get('_from_id'):
                factory.copy_schema_from_id(request.json['_from_id'], request.json['name'])
                return jsonify({'ok': 1})
            else:
                factory.create_schema_from_import(request.json)
                return jsonify({'ok': 1})
        else:
            factory.create_new_schema()
            return jsonify({'ok': 1})


@admin_bp.route("/api/schemas/<schema_id>", methods=['GET', 'DELETE'])
@admin_required
def schema_editor_api(schema_id):
    api = current_app.config['api']
    factory = current_app.config['schema_factory']
    identity = flask_login.current_user
    user = api.schema.load_user_by_id(identity.user_id)
    if request.method == 'GET':
        schema = factory.load_schema(schema_id)
        return jsonify(serialize_schema(schema, user.admin))
    else:
        schema = load_checked_schema(schema_id)
        if factory.delete_schema(schema_id):
            return jsonify({'ok': 1})
        else:
            return jsonify({'error': 'Cannot delete current schema'}), 400


@admin_bp.route("/api/schemas/<schema_id>/specs", methods=['POST'])
@admin_required
def schema_editor_create_spec(schema_id):
    schema = load_checked_schema(schema_id)
    schema.create_spec(request.json['spec_name'])
    return jsonify({'success': 1})


@admin_bp.route("/api/schemas/<schema_id>/groups", methods=['POST'])
@admin_required
def schema_editor_create_group(schema_id):
    schema = load_checked_schema(schema_id)
    schema.create_group(request.json['group_name'])
    return jsonify({'success': 1})


@admin_bp.route("/api/schemas/<schema_id>/groups/<group_name>", methods=['DELETE'])
@admin_required
def schema_editor_delete_group(schema_id, group_name):
    schema = load_checked_schema(schema_id)
    schema.delete_group(group_name)
    return jsonify({'success': 1})


@admin_bp.route("/api/schemas/<schema_id>/groups/<group_name>/grants", methods=['POST'])
@admin_required
def schema_editor_create_grant(schema_id, group_name):
    schema = load_checked_schema(schema_id)
    schema.create_grant(group_name, request.json['grant_type'], request.json['url'])
    return jsonify({'success': 1})


@admin_bp.route("/api/schemas/<schema_id>/groups/<group_name>/grants/<grant_type>/<url>", methods=['DELETE'])
@admin_required
def schema_editor_delete_grant(schema_id, group_name, grant_type, url):
    schema = load_checked_schema(schema_id)
    schema.delete_grant(group_name, grant_type, url)
    return jsonify({'success': 1})



@admin_bp.route("/api/schemas/<schema_id>/calcs", methods=['POST'])
@admin_required
def schema_editor_calcs(schema_id):
    # resolve calc, return meta info for result (spec_name, is_collection, etc)
    factory = current_app.config['schema_factory']
    schema = factory.load_schema(schema_id)

    calc_str = request.json['calc_str']
    spec_name = request.json.get('spec_name', 'root')

    try:
        resolved_spec, is_collection = schema.resolve_calc_metadata(calc_str, spec_name)

        return jsonify({"meta": {
            "spec_name": resolved_spec.name,
            "is_collection": is_collection,
        }})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@admin_bp.route("/api/schemas/<schema_id>/specs/<spec_name>/fields", methods=['POST'])
@admin_required
def schema_editor_create_field(schema_id, spec_name):
    schema = load_checked_schema(schema_id)

    field_name = request.json['field_name']
    field_type = request.json['field_type']
    field_target = request.json['field_target']
    calc_str = request.json['calc_str']
    default = request.json.get('default')
    required = request.json.get('required') or False
    background = request.json.get('background') or False
    indexed = request.json.get('indexed') or False
    unique = request.json.get('unique') or False
    unique_global = request.json.get('unique_global') or False

    if spec_name != 'root' and spec_name not in schema.specs:
        return jsonify({"error": "Not Found"}), 404

    try:
        schema.create_field(
            spec_name,
            field_name,
            field_type,
            field_target,
            calc_str,
            default=default,
            required=required,
            background=background,
            indexed=indexed,
            unique=unique,
            unique_global=unique_global)
    except MalformedFieldException as me:
        return jsonify({"error": str(me)}), 400

    return jsonify({'success': 1})


@admin_bp.route("/api/schemas/<schema_id>/specs/<spec_name>", methods=['DELETE'])
@admin_required
def schema_editor_delete_spec(schema_id, spec_name):
    schema = load_checked_schema(schema_id)
    if schema.current:
        return jsonify({"error": "cannot alter current schema"}), 400

    if spec_name != 'root' and spec_name not in schema.specs:
        return jsonify({"error": "Not Found"}), 404
    try:
        schema.delete_spec(spec_name)
    except DependencyException as de:
        log.exception("DependencyException on DELETE for %s %s", schema_id, spec_name)
        return jsonify({"error": str(de)}), 400
    return jsonify({'success': 1})


@admin_bp.route("/api/schemas/<schema_id>/specs/<spec_name>/fields/<field_name>", methods=['DELETE', 'PATCH'])
@admin_required
def schema_editor_delete_field(schema_id, spec_name, field_name):
    schema = load_checked_schema(schema_id)
    if schema.current:
        return jsonify({"error": "cannot alter current schema"}), 400

    if request.method == 'DELETE':
        if spec_name != 'root' and spec_name not in schema.specs:
            return jsonify({"error": "Not Found"}), 404
        try:
            schema.delete_field(spec_name, field_name)
        except DependencyException as de:
            log.exception("DependencyException on DELETE for %s %s %s", schema_id, spec_name, field_name)
            return jsonify({"error": str(me)}), 400
    else:
        field_type = request.json['field_type']
        field_target = request.json['field_target']
        calc_str = request.json.get('calc_str')
        required = request.json.get('required') or False
        default = request.json.get('default')
        indexed = request.json.get('indexed') or False
        unique = request.json.get('unique') or False
        unique_global = request.json.get('unique_global') or False

        try:
            schema.update_field(spec_name, field_name, field_type, field_target, calc_str, default, required, indexed, unique, unique_global)
        except Exception as e:
            log.exception("Exception on PATCH for %s %s %s", schema_id, spec_name, field_name)
            return jsonify({"error": str(e)}), 400

    return jsonify({'success': 1})


@admin_bp.route("/api/mutations", methods=['GET', 'POST'])
@admin_required
def mutations():
    factory = current_app.config['schema_factory']

    if request.method == 'GET':
        return jsonify([serialize_mutation(m) for m in factory.list_ready_mutations()])

    elif request.method == 'POST':
        data = request.json

        from_schema = factory.load_schema(data['from_schema_id'])
        to_schema = factory.load_schema(data['to_schema_id'])
        if not from_schema.current:
            return jsonify({"error": "Target schema must be the current live schema"}), 403

        mutation = MutationFactory(from_schema, to_schema).create()

        factory.create_mutation(mutation)

        return jsonify(serialize_mutation(mutation))


@admin_bp.route("/api/mutations/<mutation_id>", methods=['GET', 'PATCH', 'DELETE'])
@admin_required
def single_mutation(mutation_id):
    factory = current_app.config['schema_factory']
    mutation = factory.load_mutation(mutation_id)

    if request.method == 'GET':
        return serialize_mutation(mutation)
    elif request.method == 'DELETE':
        factory.delete_mutation(mutation_id)
        return jsonify({"ok": 1})
    else:
        if request.json.get('promote') == True:
            log.info("Promoting mutation %s -> %s", mutation.from_schema.version, mutation.to_schema.version)
            mutation.mutate()
            return jsonify({"ok": 1})
        else:
            return jsonify({"error": "Unsupported option"}), 400


@admin_bp.route("/api/mutations/<mutation_id>", methods=['POST'])
@admin_required
def perform_mutation(mutation_id):
    factory = current_app.config['schema_factory']
    mutation = factory.load_mutation(mutation_id)
    mutation.mutate()

    return jsonify({'ok': 1})


@admin_bp.route("/api/mutations/<mutation_id>/steps", methods=['GET', 'POST'])
@admin_required
def mutation_steps(mutation_id):
    factory = current_app.config['schema_factory']
    mutation = factory.load_mutation(mutation_id)
    if request.method == 'GET':
        return jsonify(mutation.steps)
    else:
        data = request.json
        if data['action'] == "rename_spec":
            mutation.convert_delete_spec_to_rename(data['from_spec_name'], data['to_spec_name'])
            factory.save_mutation(mutation)
        elif data['action'] == "rename_field":
            mutation.convert_delete_field_to_rename(data['spec_name'], data['from_field_name'], data['to_field_name'])
            factory.save_mutation(mutation)
        elif data['action'] == "move":
            mutation.add_move_step(data['from_path'], data['to_path'])
            factory.save_mutation(mutation)
        else:
            return jsonify({"error": "Unknown step type"}), 400

        return jsonify(mutation.steps)

@admin_bp.route("/api/mutations/<mutation_id>/steps/<spec_name>", methods=['DELETE'])
@admin_required
def cancel_step(mutation_id, spec_name):
    factory = current_app.config['schema_factory']
    mutation = factory.load_mutation(mutation_id)
    mutation.cancel_rename_spec(spec_name)
    factory.save_mutation(mutation)
    return jsonify({'ok': 1})


@admin_bp.route("/api/mutations/<mutation_id>/steps/<spec_name>/<field_name>", methods=['DELETE'])
@admin_required
def cancel_field_step(mutation_id, spec_name, field_name):
    factory = current_app.config['schema_factory']
    mutation = factory.load_mutation(mutation_id)
    mutation.cancel_rename_field(spec_name, field_name)
    factory.save_mutation(mutation)
    return jsonify({'ok': 1})
