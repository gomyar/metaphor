
import os
import logging
log = logging.getLogger('metaphor')

from flask import Blueprint
from flask import current_app
from flask import request
from flask import jsonify
from flask import render_template

from metaphor.schema_factory import SchemaFactory
from metaphor.resource import CollectionSpec


admin_bp = Blueprint(
    'admin',
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), 'templates'),
    static_folder=os.path.join(os.path.dirname(__file__), 'static/metaphor'),
    static_url_path='/static/metaphor', url_prefix='/admin')


@admin_bp.route("/schema", methods=['GET'])
def admin_schema():
    return render_template('metaphor/schema.html')


@admin_bp.route("/api/<path:path>", methods=['GET'])
def admin_api(path):
    return render_template('metaphor/api.html', resource_path=path)


@admin_bp.route("/api", methods=['GET'])
def admin_api_root():
    return render_template('metaphor/api.html', resource_path='')


@admin_bp.route("/", methods=['GET'])
def admin_root():
    return render_template('metaphor/admin.html')
