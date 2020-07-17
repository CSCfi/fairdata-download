"""
    download.views
    ~~~~~~~~~~~~~~

    Module for views used by Fairdata Download Service.
"""
import os.path

from flask import Blueprint, abort, current_app, jsonify, request, send_file
from pika.spec import BasicProperties

from .db import get_db
from .mq import get_mq
from .utils import format_datetime

download_service = Blueprint('download', __name__)

@download_service.route('/request', methods=['GET'])
def get_request():
    """Internally available end point for file generation request data.

    Returns JSON encoded details regarding zero or more downloadable
    dataset package files, and/or partial subset package files, which exist
    in the cache or are in the process of being generated.
    """
    dataset = request.args.get('dataset', '')

    request_row = get_db().cursor().execute(
        'SELECT status, initiated FROM request WHERE dataset_id = ?',
        (dataset,)
    ).fetchone()

    if request_row is None:
        abort(404)

    response = {}
    response['dataset'] = dataset
    response['status'] = request_row['status']
    response['initiated'] = format_datetime(request_row['initiated'])

    if request_row['status'] == 'generating':
        package_row = get_db().cursor().execute(
            'SELECT '
            '  filename, '
            '  generation_started '
            'FROM package '
            'WHERE dataset_id = ?',
            (dataset,)
        ).fetchone()

        response['generation_started'] = format_datetime(package_row['generation_started'])
        response['package'] = package_row['filename']
    elif request_row['status'] == 'available':
        package_row = get_db().cursor().execute(
            'SELECT '
            '  filename, '
            '  size_bytes, '
            '  checksum, '
            '  generation_completed, '
            '  generation_started '
            'FROM package '
            'WHERE dataset_id = ?',
            (dataset,)
        ).fetchone()

        response['generation_started'] = format_datetime(package_row['generation_started'])
        response['package'] = package_row['filename']
        response['size_bytes'] = package_row['size_bytes']
        response['generation_completed'] = format_datetime(package_row['generation_completed'])
        response['checksum'] = package_row['checksum']

    return jsonify(response)

@download_service.route('/request', methods=['POST'])
def post_request():
    """Internally available end point for initiating file generation.

    Creates a new file generation request if no such already exists.
    """
    request_data = request.get_json()
    dataset = request_data['dataset']

    db_conn = get_db()
    db_cursor = db_conn.cursor()

    created = False

    request_row = db_cursor.execute(
        'SELECT status, initiated FROM request WHERE dataset_id = ?',
        (dataset,)
    ).fetchone()

    if request_row is None:
        db_cursor.execute(
            'INSERT INTO request (dataset_id) VALUES (?)', (dataset,)
        )
        db_conn.commit()

        request_row = db_cursor.execute(
            'SELECT status, initiated FROM request WHERE dataset_id = ?',
            (dataset,)
        ).fetchone()

        get_mq().channel().basic_publish(
            exchange='requests',
            routing_key='requests',
            body=dataset,
            properties=BasicProperties(delivery_mode=2))

        created = True
        current_app.logger.info(
            "Created a new file generation request for dataset '%s'" % dataset)
    else:
        current_app.logger.info(
            "Found request with status '%s' for dataset '%s'" %
            (request_row['status'], dataset))

    return jsonify(
        created=created,
        dataset=dataset,
        initiated=format_datetime(request_row['initiated']),
        status=request_row['status']
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
        dataset + '.zip')

    # TODO: replace send_file with streamed content:
    # https://flask.palletsprojects.com/en/1.1.x/patterns/streaming/
    return send_file(filename, as_attachment=True)

@download_service.errorhandler(404)
def resource_not_found(error):
    """Error handler for HTTP 404."""
    return jsonify(
        error="Requested resource was not found in the server"), 404
