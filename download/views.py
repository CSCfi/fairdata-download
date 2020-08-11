"""
    download.views
    ~~~~~~~~~~~~~~

    Module for views used by Fairdata Download Service.
"""
from datetime import datetime, timedelta
from os import path

from flask import Blueprint, abort, current_app, jsonify, request, send_file
from jwt import decode, encode, ExpiredSignatureError
from jwt.exceptions import DecodeError
from requests.exceptions import ConnectionError

from .db import get_download_record, get_task_for_package, get_task_rows, \
                create_download_record, create_task_rows, get_package_row, \
                get_generate_scope_filepaths
from .metax import get_dataset_modified_from_metax, \
                   get_matching_dataset_files_from_metax, \
                   UnexpectedStatusCode, NoMatchingFilesFound
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
        dataset_modified = get_dataset_modified_from_metax(dataset)
    except ConnectionError:
        abort(500)
    except UnexpectedStatusCode:
        abort(500)

    # Check task rows from database
    task_rows = get_task_rows(dataset, dataset_modified)

    if len(task_rows) == 0:
        abort(404)

    # Formulate response
    response = {}
    response['dataset'] = dataset

    for task_row in task_rows:
        if not task_row['is_partial']:
            response['status'] = task_row['status']
            response['initiated'] = format_datetime(task_row['initiated'])

            if task_row['status'] == 'SUCCESS':
                package_row = get_package_row(task_row['task_id'])

                response['generated'] = format_datetime(task_row['date_done'])
                response['package'] = package_row['filename']
                response['size'] = package_row['size_bytes']
                response['checksum'] = package_row['checksum']
        else:
            if 'partial' not in response.keys():
                response['partial'] = []

            partial_task = {
                'scope': list(get_generate_scope_filepaths(task_row['task_id'])),
                'status': task_row['status'],
                'initiated': format_datetime(task_row['initiated'])
            }

            if task_row['status'] == 'SUCCESS':
                package_row = get_package_row(task_row['task_id'])

                partial_task['generated'] = format_datetime(
                    task_row['date_done'])
                partial_task['package'] = package_row['filename']
                partial_task['size'] = package_row['size_bytes']
                partial_task['checksum'] = package_row['checksum']

            response['partial'].append(partial_task)

    return jsonify(response)

@download_service.route('/request', methods=['POST'])
def post_request():
    """Internally available end point for initiating file generation.

    Creates a new file generation request if no such already exists.
    """
    request_data = request.get_json()
    dataset = request_data['dataset']
    scope = request_data.get('scope', [])

    # Check dataset metadata in Metax API
    try:
        dataset_modified = get_dataset_modified_from_metax(dataset)
    except ConnectionError:
        abort(500)
    except UnexpectedStatusCode:
        abort(500)

    try:
        generate_scope, project_identifier, is_partial = get_matching_dataset_files_from_metax(dataset, scope)
    except ConnectionError:
        abort(500)
    except UnexpectedStatusCode:
        abort(500)
    except NoMatchingFilesFound:
        abort(409)

    # Check existing tasks in database
    created = False

    task_rows = get_task_rows(dataset, dataset_modified)

    task_row = None
    for row in task_rows:
        if get_generate_scope_filepaths(row['task_id']) == generate_scope:
            task_row = row
            break

    # Create new task if no such already exists
    if not task_row:
        from .celery import generate_task
        task = generate_task.delay(
            dataset,
            project_identifier,
            list(generate_scope))

        task_row = create_task_rows(
            dataset, task.id, is_partial, generate_scope)

        created = True
    else:
        current_app.logger.info(
            "Found request with status '%s' for dataset '%s'" %
            (task_row['status'], dataset))

    # Formulate response
    response = {}
    response['dataset'] = dataset
    response['created'] = created

    if not is_partial:
        response['initiated'] = format_datetime(task_row['initiated'])
        response['status'] = task_row['status']

        if task_row['status'] == 'SUCCESS':
            package_row = get_package_row(task_row['task_id'])

            response['generated'] = format_datetime(task_row['date_done'])
            response['package'] = package_row['filename']
            response['size'] = package_row['size_bytes']
            response['checksum'] = package_row['checksum']
    else:
        partial_task = {
            'scope': list(generate_scope),
            'initiated': format_datetime(task_row['initiated']),
            'status': task_row['status'],
        }

        if task_row['status'] == 'SUCCESS':
            package_row = get_package_row(task_row['task_id'])

            partial_task['generated'] = format_datetime(task_row['date_done'])
            partial_task['package'] = package_row['filename']
            partial_task['size'] = package_row['size_bytes']
            partial_task['checksum'] = package_row['checksum']

        response['partial'] = [partial_task]

    return jsonify(response)

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
    task_row = get_task_for_package(dataset, package)

    if task_row is None:
        abort(404)

    initiated = convert_utc_timestamp(task_row['initiated'])

    # Check dataset metadata in Metax API
    try:
        dataset_modified = get_dataset_modified_from_metax(dataset)
    except ConnectionError:
        abort(500)
    except UnexpectedStatusCode:
        abort(500)

    if initiated < dataset_modified:
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

    download_row = get_download_record(auth_token)

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
    task_row = get_task_for_package(dataset, package)

    if task_row is None:
        current_app.logger.error("Could not find database record for package "
                                 "'%s' with valid download token" % package)
        abort(404)

    initiated = convert_utc_timestamp(task_row['initiated'])

    # Check dataset metadata in Metax API
    try:
        dataset_modified = get_dataset_modified_from_metax(dataset)
    except ConnectionError:
        abort(500)
    except UnexpectedStatusCode:
        abort(500)

    if initiated < dataset_modified:
        current_app.logger.error(
            "Dataset %s has been modified since generation task was initialized"
            % dataset)
        abort(409)

    # Check download record in database
    create_download_record(auth_token, package)

    filename = path.join(
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
