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
  Download Service is used for downloading dataset files hosted in
  <a href="https://ida.fairdata.fi/">Fairdata IDA</a>. Service is needed to
  conform with the service description of Fairdata Services which specifies
  that <i>CSC is responsible for providing services used to describe and transfer
  metadata of materials among services as well as to find and distribute
  material</i>
  [<a href="http://digitalpreservation.fi/files/sopimukset/Liite4-Fairdata-palvelukuvaus.pdf">1</a>]
  (Section 4). Scope of the Download Service is the distribution of material.
</div>
<h1>Architecture</h1>
  <div>
    Download Service consists of the following configuration items:
    <ul>
      <li>Download Server</li>
      <li>Download Generator</li>
      <li>IDA Storage Volume</li>
      <li>Download Cache Volume</li>
      <li>RabbitMQ Server</li>
      <li>Nginx Proxy Server</li>
      <li>Sqlite Database</li>
    </ul>
    Additionally the service integrates with
    <a href="https://metax.fairdata.fi/">Metax API</a>.
  </div>
  <div style="display: flex">
    <img
      src="/static/download-service-component-diagram.png"
      title="Download Service Component Diagram"
      alt="Download Service Component Diagram"
      height="600"
      style="margin: auto">
  </div>
<h1>Models</h1>
  <div style="display: flex">
    <img
      src="/static/download-service-er-diagram.png"
      title="Download Service Entity Relationship Graph"
      alt="Download Service Entity Relationship Graph"
      height="600"
      style="margin: auto">
  </div>
<h1>Swagger UI</h1>
<div>
  This Swagger UI can be used to interact with the application. Click an
  endpoint, next click '<b>Try it out</b>' and then '<b>Execute</b>'. Example
  requests should be correct depending on the dataset data available in the
  deployment environment's Metax API instance state. For more detail, consult
  the <b>Models</b> section below.
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
    swag['info']['version'] = '1.0.1'
    swag['info']['title'] = 'Fairdata Download API'
    swag['info']['description'] = swagger_description
    swag['info']['contact'] = {
        'name': 'Fairdata',
        'url': 'https://fairdata.fi',
        'email': 'servicedesk@csc.fi'
    }
    return jsonify(swag)
