"""
    download.swagger
    ~~~~~~~~~~~~~~~~

    Module for swagger blueprint used by Fairdata Download Service.
"""
from flask import Blueprint, current_app, jsonify

from flask_swagger import swagger
from flask_swagger_ui import get_swaggerui_blueprint

download_api_swagger = Blueprint('download-swagger', __name__)
download_api_swagger_ui = get_swaggerui_blueprint(
    '/api/docs',
    '/swagger.json',
    config={'app_name': 'Fairdata Download Service'})

@download_api_swagger.route('/swagger.json')
def get_swagger():
    swag = swagger(current_app)
    swag['info']['version'] = '1.0.0'
    swag['info']['title'] = 'Fairdata Download API'
    return jsonify(swag)
