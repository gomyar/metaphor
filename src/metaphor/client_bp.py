
from flask import Blueprint
from flask import render_template
from flask import current_app
from flask import request
from flask import jsonify
from flask_login import login_required

import flask_login

from .api_bp import serialize_schema


client_bp = Blueprint('client', __name__, template_folder='templates',
                      static_folder='static', url_prefix='/client')


@client_bp.route("/schema", methods=['GET'])
@login_required
def schema():
    api = current_app.config['api']
    spec = api.schema.root
    return jsonify(serialize_schema(api.schema))


@client_bp.route("/", methods=['GET'])
@login_required
def client_root():
    return render_template('metaphor/api_client.html')

@client_bp.route("/<path:path>", methods=['GET'])
@login_required
def client_path(path):
    return render_template('metaphor/api_client.html')
