

from flask import Blueprint
from flask import current_app
from flask import request
from flask import jsonify

from metaphor.schema_factory import SchemaFactory
from metaphor.resource import CollectionSpec


schema_bp = Blueprint('schema', __name__, template_folder='templates',
                      static_folder='static', url_prefix='/schema')


@schema_bp.route("/root", methods=['GET', 'POST'])
def roots_list():
    schema = current_app.config['schema']
    if request.method == 'GET':
        return jsonify([spec.serialize for spec in schema.root_spec.fields.values()])
    if request.method == 'POST':
        schema.add_root(request.json['name'], CollectionSpec(request.json['target']))
        SchemaFactory().save_schema(schema)
        return jsonify({})


@schema_bp.route("/specs/<spec_name>", methods=['GET', 'PATCH'])
def schema_update(spec_name):
    schema = current_app.config['schema']
    if request.method == 'GET':
        return schema.specs[name].serialize()
    elif request.method == 'PATCH':
        data = request.json
        for field_name, field_data in data.items():
            SchemaFactory().add_field_to_spec(schema, spec_name, field_name, field_data)
        SchemaFactory().save_schema(schema)
        return jsonify({})


@schema_bp.route("/specs", methods=['GET', 'POST'])
def schema_list():
    schema = current_app.config['schema']
    if request.method == 'POST':
        data = request.json
        SchemaFactory().add_resource_to_schema(schema, data['name'], data.get('fields', {}))
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

        return jsonify({'info': info})

@schema_bp.route("/", methods=['GET'])
def root_get():
    schema = current_app.config['schema']
    if request.method == 'GET':
        return jsonify([spec.serialize() for spec in schema.specs.values()])
