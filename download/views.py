"""
    download.views
    ~~~~~~~~~~~~~~

    Module for views used by Fairdata Download Service.
"""
import os.path

from flask import Blueprint, current_app, jsonify, request, send_file

download_service = Blueprint('download', __name__)

@download_service.route('/request', methods=['GET'])
def get_request():
    """Internally available end point for file generation request data.

    Returns JSON encoded details regarding zero or more downloadable
    dataset package files, and/or partial subset package files, which exist
    in the cache or are in the process of being generated.
    """
    dataset = request.args.get('dataset', '')

    return jsonify(
        dataset=dataset,
        package="5d0395179e01d178338732a27741915.zip",
        created="2019-08-23T18:38:03Z",
        size=2397293872,
        checksum="sha256:e01d178338732a277419d0395179e01d178338732a277419159e01d178338732a2774191",
        partial=[
            {
                "scope": ["/Licence.txt"],
                "package": "5d0395179e01d178338732a27741915_01d47419.zip",
                "created": "2019-08-23T18:38:03Z",
                "size": 84892382,
                "checksum": "sha256:e01d178338732a277419d0395179e01d178338732a277419159e01d178338732a2774191"
            }
        ]
    )

@download_service.route('/request', methods=['POST'])
def post_request():
    """Internally available end point for initiating file generation.

    Creates a new file generation request if no such already exists.
    """
    request_data = request.get_json()
    return jsonify(
        dataset=request_data['dataset'],
        status="request created"
    )

@download_service.route('/authorize', methods=['POST'])
def authorize():
    """Internally available end point for authorizing requesting clients.

    Requests a time-limited single-use token for download of a specific
    dataset package or file.
    """
    request_data = request.get_json()

    return jsonify(
        authorized="2019-08-29T11:19:03Z",
        dataset=request_data['dataset'],
        package=request_data['package'],
        token="YjY1NDNhYzcxYTk1ZDI2ZTA3ZjA2YzU2"
    )

@download_service.route('/download', methods=['GET'])
def download():
    """Publically accessible with valid single-use token.

    Allows downloading files with valid tokens, generated with authorize
    requests. Trusted access without token to certain internal services (e.g.
    PAS).
    """
    dataset = request.args.get('dataset', '')
    package = request.args.get('package', '')
    token = request.args.get('token', '')

    filename = os.path.join(
        current_app.config['DOWNLOAD_CACHE_DIR'],
        'datasets',
        package)

    # TODO: replace send_file with streamed content:
    # https://flask.palletsprojects.com/en/1.1.x/patterns/streaming/
    return send_file(filename, as_attachment=True)
