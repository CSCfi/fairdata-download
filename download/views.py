"""
    download.views
    ~~~~~~~~~~~~~~

    Module for views used by Fairdata Download Service.
"""
from datetime import datetime, timedelta
import os.path

from flask import Blueprint, abort, current_app, jsonify, request, send_file
from jwt import decode, encode, ExpiredSignatureError
from jwt.exceptions import DecodeError
from requests.exceptions import ConnectionError

from .db import get_db
from .metax import get_dataset
from .utils import convert_utc_timestamp, format_datetime

download_service = Blueprint('download', __name__)

@download_service.route('/request', methods=['GET'])
def get_request():
    """Internally available end point for file generation request data.

    Returns JSON encoded details regarding zero or more downloadable
    dataset package files, and/or partial subset package files, which exist
    in the cache or are in the process of being generated.
    """
    dataset = request.args.get('dataset', '')

    # Check dataset metadata in Metax API
    try:
        metax_response = get_dataset(dataset)
    except ConnectionError:
        abort(500)

    if metax_response.status_code != 200:
        current_app.logger.error(
            "Received unexpected status code '%s' from Metax API"
            % metax_response.status_code)
        abort(500)

    dataset_modified = datetime.fromisoformat(metax_response.json()['date_modified'])

    db_conn = get_db()
    db_cursor = db_conn.cursor()

    task_row = db_cursor.execute(
        'SELECT '
        '  p.initiated as initiated, '
        '  p.filename as filename, '
        '  p.size_bytes as size_bytes, '
        '  p.checksum as checksum, '
        '  t.status as status, '
        '  t.date_done as date_done '
        'FROM package p '
        'LEFT JOIN generate_task t '
        'ON p.task_id = t.task_id '
        'WHERE p.dataset_id = ? '
        'AND p.initiated > ?',
        (dataset, dataset_modified)
    ).fetchone()

    if task_row is None:
        abort(404)

    response = {}
    response['dataset'] = dataset
    response['status'] = task_row['status'] or 'PENDING'
    response['initiated'] = format_datetime(task_row['initiated'])

    if response['status'] == 'STARTED':
        response['package'] = task_row['filename']
    elif response['status'] == 'SUCCESS':
        response['package'] = task_row['filename']
        response['size_bytes'] = task_row['size_bytes']
        response['checksum'] = task_row['checksum']
        response['generated'] = format_datetime(task_row['date_done'])

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

    task_row = db_cursor.execute(
        'SELECT p.initiated as initiated, t.status as status '
        'FROM package p '
        'LEFT JOIN generate_task t '
        'ON p.task_id = t.task_id '
        'WHERE p.dataset_id = ?',
        (dataset,)
    ).fetchone()

    if task_row is None:
        from .celery import generate_task
        task = generate_task.delay(dataset)
        db_cursor.execute(
            'INSERT INTO package (dataset_id, task_id) VALUES (?, ?)',
            (dataset, task.id))
        db_conn.commit()
        current_app.logger.info(
            "Created a new file generation task with id '%s' for dataset '%s'"
            % (task.id, dataset))

        package_row = db_cursor.execute(
            'SELECT initiated '
            'FROM package '
            'WHERE dataset_id = ?',
            (dataset,)
        ).fetchone()

        created = True
        task_status = 'PENDING'
        initiated = format_datetime(package_row['initiated'])
    else:
        task_status = task_row['status'] or 'PENDING'
        initiated = format_datetime(task_row['initiated'])

        current_app.logger.info(
            "Found request with status '%s' for dataset '%s'" %
            (task_status, dataset))

    return jsonify(
        created=created,
        dataset=dataset,
        initiated=initiated,
        status=task_status
    )

@download_service.route('/authorize', methods=['POST'])
def authorize():
    """Internally available end point for authorizing requesting clients.

    Requests a time-limited single-use token for download of a specific
    dataset package or file.
    """
    request_data = request.get_json()

    dataset = request_data['dataset']
    package = request_data['package']

    # Check package database record
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    package_row = db_cursor.execute(
        'SELECT '
        '  initiated '
        'FROM package p '
        'WHERE p.dataset_id = ?',
        (dataset,)
    ).fetchone()

    if package_row is None:
        abort(404)

    initiated = convert_utc_timestamp(package_row['initiated'])

    # Check dataset metadata in Metax API
    try:
        metax_response = get_dataset(dataset)
    except ConnectionError:
        abort(500)

    if metax_response.status_code != 200:
        current_app.logger.error(
            "Received unexpected status code '%s' from Metax API"
            % metax_response.status_code)
        abort(500)

    metax_modified = datetime.fromisoformat(metax_response.json()['date_modified'])

    if initiated < metax_modified:
        current_app.logger.error(
            "Dataset %s has been modified since generation task was initialized"
            % dataset)
        abort(409)

    # Create JWT
    jwt_payload = {
        'exp': datetime.utcnow()
               + timedelta(minutes=current_app.config['JWT_TTL']),
        'dataset': dataset,
        'package': package
    }

    jwt_token = encode(
        jwt_payload,
        current_app.config['JWT_SECRET'],
        algorithm=current_app.config['JWT_ALGORITHM'])

    return jsonify(
        token=jwt_token.decode()
    )

@download_service.route('/download', methods=['GET'])
def download():
    """Publically accessible with valid single-use token.

    Allows downloading files with valid tokens, generated with authorize
    requests. Trusted access without token to certain internal services (e.g.
    PAS).
    """
    # Read auth token from request parameters
    auth_token = request.args.get('token', None)

    if auth_token is None:
        # Parse authorization header
        auth_header = request.headers.get('Authorization')
        if auth_header is None:
            abort(401)

        [auth_method, auth_token] = auth_header.split(' ')
        if auth_method != 'Bearer':
            current_app.logger.info(
                "Received invalid autorization method '%s'" % auth_method)
            abort(401)

    # Check is offered token has been used
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    download_row = db_cursor.execute(
        'SELECT * FROM download WHERE token = ?',
        (auth_token,)
    ).fetchone()

    if download_row is not None:
        current_app.logger.info('Received download request with used token.')
        abort(401)

    # Decode token
    try:
        jwt_payload = decode(
            auth_token,
            current_app.config['JWT_SECRET'],
            algorithms=[current_app.config['JWT_ALGORITHM']])
    except ExpiredSignatureError:
        current_app.logger.info('Received download request with expired token.')
        abort(401)
    except DecodeError:
        current_app.logger.info('Unable to decode offered token.')
        abort(401)

    dataset = jwt_payload['dataset']
    package = jwt_payload['package']

    # Check if package can be found in database
    package_row = db_cursor.execute(
        'SELECT initiated FROM package WHERE filename = ?',
        (package,)
    ).fetchone()

    if package_row is None:
        current_app.logger.error("Could not find database record for package "
                                 "'%s' with valid download token" % package)
        abort(404)

    initiated = convert_utc_timestamp(package_row['initiated'])

    # Check dataset metadata in Metax API
    try:
        metax_response = get_dataset(dataset)
    except ConnectionError:
        abort(500)

    if metax_response.status_code != 200:
        current_app.logger.error(
            "Received unexpected status code '%s' from Metax API"
            % metax_response.status_code)
        abort(500)

    metax_modified = datetime.fromisoformat(metax_response.json()['date_modified'])

    if initiated < metax_modified:
        current_app.logger.error(
            "Dataset %s has been modified since generation task was initialized"
            % dataset)
        abort(409)

    # Set download record to database
    db_cursor.execute(
        'INSERT INTO download (token, filename) VALUES (?, ?)', (auth_token, package)
    )
    db_conn.commit()

    filename = os.path.join(
        current_app.config['DOWNLOAD_CACHE_DIR'],
        'datasets',
        package)

    # TODO: replace send_file with streamed content:
    # https://flask.palletsprojects.com/en/1.1.x/patterns/streaming/
    return send_file(filename, as_attachment=True)

@download_service.errorhandler(401)
def unauthorized(error):
    """Error handler for HTTP 401."""
    return jsonify(
        error="Unauthorized to access the resource on the server"), 401

@download_service.errorhandler(404)
def resource_not_found(error):
    """Error handler for HTTP 404."""
    return jsonify(
        error="Requested resource was not found in the server"), 404

@download_service.errorhandler(409)
def conflict(error):
    """Error handler for HTTP 409."""
    return jsonify(
        error="Request conflicts with server data"), 409

@download_service.errorhandler(500)
def internal_server_error(error):
    """Error handler for HTTP 500."""
    return jsonify(
        error="Internal server error"), 500
