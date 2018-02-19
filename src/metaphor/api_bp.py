
from flask import Blueprint
from flask import current_app
from flask import request
from flask import jsonify


api_bp = Blueprint('api', __name__, template_folder='templates',
                   static_folder='static', url_prefix='/api')


@api_bp.route("/")
def api_root():
    return jsonify(current_app.config['api'].get('/', dict(request.args.items())))


@api_bp.route("/<path:path>", methods=['GET', 'POST', 'DELETE', 'PATCH'])
def api_call(path):
    if request.method == 'POST':
        return jsonify({'id': str(current_app.config['api'].post(path, request.json))})
    elif request.method == 'DELETE':
        return jsonify({'id': str(current_app.config['api'].unlink(path))})
    elif request.method == 'PATCH':
        return jsonify({'id': str(current_app.config['api'].patch(path, request.json))})
    else:
        resource = current_app.config['api'].get(path, dict(request.args.items()))
        return jsonify(resource)
