
from flask import Blueprint
from flask import current_app
from flask import request
from flask import jsonify
from flask import render_template

from metaphor.resource import CollectionSpec, ResourceLinkSpec


api_bp = Blueprint('api', __name__, template_folder='templates',
                   static_folder='static', url_prefix='/api')


@api_bp.route("/")
def api_root():
    return jsonify(current_app.config['api'].get('/', dict(request.args.items())))


@api_bp.route("/<path:path>", methods=['GET', 'POST', 'DELETE', 'PUT', 'PATCH'])
def api_call(path):
    api = current_app.config['api']
    if request.method == 'POST':
        return jsonify({'id': str(api.post(path, request.json))})
    elif request.method == 'DELETE':
        return jsonify({'id': str(api.unlink(path))})
    elif request.method == 'PUT':
        return jsonify({'id': str(api.put(path, request.json))})
    elif request.method == 'PATCH':
        return jsonify({'id': str(api.patch(path, request.json))})
    else:
        resource_data = api.get(path, dict(request.args.items()))
        return jsonify(resource_data)
