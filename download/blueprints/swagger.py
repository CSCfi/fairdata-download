"""
    download.swagger
    ~~~~~~~~~~~~~~~~

    Module for swagger blueprint used by Fairdata Download Service.
"""
from flask import Blueprint, current_app, jsonify

from flask_swagger import swagger
from flask_swagger_ui import get_swaggerui_blueprint

swagger_description = """
<h1>Overview</h1>
<div>
<p>
This page describes the behavior of the Fairdata download service, which is used
for downloading datasets, either in whole or in part, which have their data stored
in the <a href="https://ida.fairdata.fi/">Fairdata IDA service</a>.
</p>
<p>
The Fairdata download service provides a REST API for downloading previously generated
dataset packages or individual dataset files, requesting download package generation,
querying the status of available or pending dataset packages, and authorizing download
of dataset packages or individual files.
</p>
<p>
<i>NOTE: Only the /download API endpoint is publically accessible. All other endpoints
are restricted to trusted internal services and the documentation provided herein is for
the development and testing of internal integrations.</i>
</p>  
</div>
"""

download_api_swagger = Blueprint('download-swagger', __name__)
download_api_swagger_ui = get_swaggerui_blueprint(
    '',
    '/swagger.json',
    config={'app_name': 'Fairdata Download Service'})

@download_api_swagger.route('/swagger.json')
def get_swagger():
    swag = swagger(current_app)
    swag['info']['version'] = ''
    swag['info']['title'] = 'Fairdata Download Service API'
    swag['info']['description'] = swagger_description
    swag['info']['contact'] = {
        'name': 'Fairdata',
        'url': 'https://www.fairdata.fi',
        'email': 'servicedesk@csc.fi'
    }
    return jsonify(swag)
