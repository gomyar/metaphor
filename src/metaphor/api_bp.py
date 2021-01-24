

from flask import Blueprint
from flask import render_template
from flask import current_app
from flask import request
from flask import jsonify

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


@search_bp.route("/<spec_name>", methods=['GET'])
def search(spec_name):
    query = request.args.get('query')
    api = current_app.config['api']
    return api.search_resource(spec_name, query)


@api_bp.route("/", methods=['GET'])
def api_root():
    api = current_app.config['api']
    if request.method == 'GET':
        root_data = dict((key, '/'+ key) for key in api.schema.root.fields.keys())
        return jsonify(root_data)


@api_bp.route("/<path:path>", methods=['GET', 'POST', 'DELETE', 'PUT', 'PATCH'])
def api(path):
    api = current_app.config['api']
    if request.method == 'POST':
        return jsonify(api.post(path, request.json))
    if request.method == 'PATCH':
        return jsonify(api.patch(path, request.json))
    if request.method == 'GET':
        return jsonify(api.get(path))
    if request.method == 'DELETE':
        return jsonify(api.delete(path))


@browser_bp.route("/", methods=['GET'])
def browser_root():
    api = current_app.config['api']
    resource = dict((key, '/' + key) for key in api.schema.root.fields.keys())
    spec = api.schema.root
    return render_template('metaphor/api_browser.html',
        path='/', resource=resource, spec=serialize_spec(spec), is_collection=False, can_post=False, is_linkcollection=False)


@browser_bp.route("/<path:path>", methods=['GET'])
def browser(path):
    api = current_app.config['api']
    resource = api.get(path)
    spec, is_collection, can_post, is_linkcollection = api.get_spec_for(path)
    return render_template('metaphor/api_browser.html',
        path=path, resource=resource, spec=serialize_spec(spec), is_collection=is_collection, can_post=can_post, is_linkcollection=is_linkcollection)



@admin_bp.route("/schema_editor")
def schema_editor():
    return render_template('metaphor/schema_editor.html')


@admin_bp.route("/schema_editor/api", methods=['GET'])
def schema_editor_api():
    admin_api = current_app.config['admin_api']
    return jsonify(admin_api.format_schema())


@admin_bp.route("/schema_editor/api/specs", methods=['POST'])
def schema_editor_create_spec():
    admin_api = current_app.config['admin_api']
    admin_api.create_spec(request.json['spec_name'])
    return jsonify({'success': 1})


@admin_bp.route("/schema_editor/api/specs/<spec_name>/fields", methods=['POST'])
def schema_editor_create_field(spec_name):
    admin_api = current_app.config['admin_api']

    field_name = request.json['field_name']
    field_type = request.json['field_type']
    field_target = request.json['field_target']
    calc_str = request.json['calc_str']

    admin_api.create_field(spec_name, field_name, field_type, field_target, calc_str)
    return jsonify({'success': 1})
