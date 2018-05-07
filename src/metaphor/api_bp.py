
from flask import Blueprint
from flask import current_app
from flask import request
from flask import jsonify
from flask import render_template

from metaphor.resource import CollectionSpec, ResourceLinkSpec


api_bp = Blueprint('api', __name__, template_folder='templates',
                   static_folder='static', url_prefix='/api')


def request_wants_html():
    best = request.accept_mimetypes \
        .best_match(['text/html', 'application/json'])
    return best == 'text/html' and \
        request.accept_mimetypes[best] > \
        request.accept_mimetypes['application/json']


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
        resource = api.build_resource(path)
        resource_data = api.get(path, dict(request.args.items()))
        if request_wants_html():
            return render_template('metaphor/api.html', resource=resource_data, spec=resource.spec.serialize())
        else:
            return jsonify(resource_data)
