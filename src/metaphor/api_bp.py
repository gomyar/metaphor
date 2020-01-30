

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


@browser_bp.route("/", methods=['GET'])
def browser_root():
    def serialize_field(field):
        if field.field_type == 'calc':
            return {'type': 'calc', 'calc_str': field.calc_str}
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
    api = current_app.config['api']
    resource = dict((key, '/' + key) for key in api.schema.root.fields.keys())
    spec = api.schema.root
    return render_template('metaphor/api_browser.html',
        path='/', resource=resource, spec=serialize_spec(spec), is_collection=False)


@browser_bp.route("/<path:path>", methods=['GET'])
def browser(path):
    def serialize_field(field):
        if field.field_type == 'calc':
            return {'type': 'calc', 'calc_str': field.calc_str}
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
    api = current_app.config['api']
    resource = api.get(path)
    spec, is_collection = api.get_spec_for(path)
    return render_template('metaphor/api_browser.html',
        path=path, resource=resource, spec=serialize_spec(spec), is_collection=is_collection)



@admin_bp.route("/schema_editor")
def schema_editor():
    return render_template('metaphor/schema_editor.html')


@admin_bp.route("/schema_editor/api", methods=['GET'])
def schema_editor_api():
    schema = current_app.config['api'].schema
    schema_db = schema.db['metaphor_schema'].find_one()
    schema_json = {
        'version': 'tbd',
        'specs': schema_db['specs'] if schema_db else {},
        'root': schema_db['root'] if schema_db else {},
    }
    return jsonify(schema_json)


@admin_bp.route("/schema_editor/api/specs", methods=['POST'])
def schema_editor_create_spec():
    schema = current_app.config['api'].schema
    spec_name = request.json['spec_name']
    schema.db['metaphor_schema'].update(
        {'_id': schema._id},
        {"$set": {'specs.%s' % spec_name: {'fields': {}}}})
    schema.load_schema()
    return jsonify({'success': 1})


@admin_bp.route("/schema_editor/api/specs/<spec_name>/fields", methods=['POST'])
def schema_editor_create_field(spec_name):
    schema = current_app.config['api'].schema
    field_name = request.json['field_name']
    field_type = request.json['field_type']
    field_target = request.json['field_target']
    calc_str = request.json['calc_str']

    if field_type == 'calc':
        field_data = {'type': 'calc', 'calc_str': calc_str}
    elif field_type in ('int', 'str', 'float', 'bool'):
        field_data = {'type': field_type}
    else:
        field_data = {'type': field_type, 'target_spec_name': field_target}

    if spec_name == 'root':
        schema.db['metaphor_schema'].update(
            {'_id': schema._id},
            {"$set": {'root.%s' % (field_name,): field_data}})
    else:
        schema.db['metaphor_schema'].update(
            {'_id': schema._id},
            {"$set": {'specs.%s.fields.%s' % (spec_name, field_name): field_data}})
    schema.load_schema()
    return jsonify({'success': 1})
