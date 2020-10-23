"""
    download.views
    ~~~~~~~~~~~~~~

    Module for views used by Fairdata Download Service.
"""
from datetime import datetime, timedelta
from marshmallow import ValidationError
from os import path

from flask import Blueprint, Response, abort, current_app, jsonify, request, \
                  stream_with_context
from jwt import decode, encode, ExpiredSignatureError
from jwt.exceptions import DecodeError
from requests.exceptions import ConnectionError

from .cache import get_datasets_dir
from .db import get_download_record, get_request_scopes, \
                get_task_for_package, get_task_rows, create_download_record, \
                create_request_scope, create_task_rows, get_package_row, \
                get_generate_scope_filepaths, update_download_record
from .metax import get_dataset_modified_from_metax, \
                   get_matching_project_identifier_from_metax, \
                   get_matching_dataset_files_from_metax, \
                   DatasetNotFound, UnexpectedStatusCode, \
                   MissingFieldsInResponse, NoMatchingFilesFound
from .utils import convert_utc_timestamp, format_datetime
from .model.requests import AuthorizePostData, DownloadQuerySchema, \
                            RequestsPostData, RequestsQuerySchema

download_service = Blueprint('download', __name__)

@download_service.route('/requests', methods=['GET'])
def get_request():
    """
    Internally available end point for file generation request data.

    Returns JSON encoded details regarding zero or more downloadable
    dataset package files, and/or partial subset package files, which exist
    in the cache or are in the process of being generated.
    ---
    tags:
      - generate tasks
    produces:
      - application/json
    parameters:
      - name: dataset
        in: query
        description: ID of the dataset whose package generation requests are
                     returned
        schema:
          type: string
          example: "1"
        required: true
    responses:
      200:
        description: Information about the active package generation requests
      404:
        description: No active requests for given dataset were found
      500:
        description: Unable to connect to Metax API or an unexpected status
                     code received
    """
    try:
        query = RequestsQuerySchema().load(request.args)
    except ValidationError as err:
        abort(400, str(err.messages))

    dataset = query.get('dataset')
    # Check dataset metadata in Metax API
    try:
        dataset_modified = get_dataset_modified_from_metax(dataset)
    except DatasetNotFound as err:
        abort(404, err)
    except ConnectionError:
        abort(500)
    except MissingFieldsInResponse:
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

            if task_row['status'] == 'SUCCESS':
                package_row = get_package_row(task_row['task_id'])

            for request_scope in get_request_scopes(task_row['task_id']):
                partial_task = {
                    'scope': list(request_scope),
                    'status': task_row['status'],
                    'initiated': format_datetime(task_row['initiated'])
                }

                if task_row['status'] == 'SUCCESS':
                    partial_task['generated'] = format_datetime(
                        task_row['date_done'])
                    partial_task['package'] = package_row['filename']
                    partial_task['size'] = package_row['size_bytes']
                    partial_task['checksum'] = package_row['checksum']

                response['partial'].append(partial_task)

    return jsonify(response)

@download_service.route('/requests', methods=['POST'])
def post_request():
    """Internally available end point for initiating file generation.

    Creates a new file generation request if no such already exists.
    ---
    tags:
      - generate tasks
    consumes:
      - application/json
    produces:
      - application/json
    parameters:
      - name: body
        in: body
        description: ID of the dataset whose files are included in generated
                     package
        schema:
          type: object
          properties:
            dataset:
              type: string
              example: "1"
            scope:
              type: array
              example: ["/project_x_FROZEN/Experiment_X/file_name_1"]
        required: true
    responses:
      200:
        description: Information about the matching package generation request
      409:
        description: No files matching specified scope was found
      500:
        description: Unable to connect to Metax API or an unexpected status
                     code received
    """
    try:
        request_data = RequestsPostData().load(request.get_json())
    except ValidationError as err:
        abort(400, str(err.messages))

    dataset = request_data.get('dataset')
    request_scope = request_data.get('scope', [])

    # Check dataset metadata in Metax API
    try:
        dataset_modified = get_dataset_modified_from_metax(dataset)
    except DatasetNotFound as err:
        abort(404, err)
    except ConnectionError:
        abort(500)
    except MissingFieldsInResponse:
        abort(500)
    except UnexpectedStatusCode:
        abort(500)

    try:
        generate_scope, project_identifier, is_partial = get_matching_dataset_files_from_metax(dataset, request_scope)
    except ConnectionError:
        abort(500)
    except UnexpectedStatusCode:
        abort(500)
    except NoMatchingFilesFound as err:
        abort(409, err)

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

        if is_partial:
            create_request_scope(task.id, request_scope)

        created = True
    else:
        current_app.logger.info(
            "Found request with status '%s' for dataset '%s'" %
            (task_row['status'], dataset))

        if set(request_scope) not in get_request_scopes(task_row['task_id']):
            create_request_scope(task_row['task_id'], request_scope)

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
            'scope': request_scope,
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
    ---
    tags:
      - downloads
    consumes:
      - application/json
    produces:
      - application/json
    parameters:
      - name: body
        in: body
        description: ID of the dataset whose package generation requests are
                     returned
        schema:
          type: object
          properties:
            dataset:
              type: string
              example: "1"
              required: true
            package:
              type: string
              required: false
            file:
              type: string
              example: "/project_x_FROZEN/Experiment_X/file_name_1"
              required: false
        required: true
    responses:
      200:
        description: Token for downloading the requested file or package
      400:
        description: No dataset file or active generated package matching
                     request was found
      409:
        description: Dataset was modified since the requested package was
                     generated
      500:
        description: Unable to connect to Metax API or an unexpected status
                     code received
    """
    try:
        request_data = AuthorizePostData().load(request.get_json())
    except ValidationError as err:
        abort(400, str(err.messages))

    dataset = request_data.get('dataset')
    package = request_data.get('package')

    if package is None:
        filename = request_data.get('filename') or abort(400)
        project_identifier = get_matching_project_identifier_from_metax(
            dataset,
            filename)
        if project_identifier is None:
            abort(500, "No matching project was found for dataset '%s' and filename '%s'" % (dataset, filename))

        # Create JWT
        jwt_payload = {
            'exp': datetime.utcnow()
                   + timedelta(minutes=current_app.config['JWT_TTL']),
            'dataset': dataset,
            'file': filename,
            'project': project_identifier
        }
    else:
        # Check package database record
        task_row = get_task_for_package(dataset, package) or abort(404)

        initiated = convert_utc_timestamp(task_row['initiated'])

        # Check dataset metadata in Metax API
        try:
            dataset_modified = get_dataset_modified_from_metax(dataset)
        except DatasetNotFound as err:
            abort(404, err)
        except ConnectionError:
            abort(500)
        except MissingFieldsInResponse:
            abort(500)
        except UnexpectedStatusCode:
            abort(500)

        if initiated < dataset_modified:
            current_app.logger.error(
                "Dataset %s has been modified since generation task was "
                "initialized"
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
    ---
    tags:
      - downloads
    consumes:
      - application/json
    produces:
      - application/octet-stream
    parameters:
      - name: dataset
        in: query
        description: Dataset whose file or package is to be downloaded
        schema:
          type: string
          example: "1"
        required: true
      - name: package
        in: query
        description: Package to be downloaded
        schema:
          type: string
      - name: file
        in: query
        description: File to be downloaded
        schema:
          type: string
          example: "/project_x_FROZEN/Experiment_X/file_name_1"
      - name: token
        in: query
        description: Token used to authorize the download
        schema:
          type: string
    responses:
      200:
        description: File or package to be downloaded with the provided token
      401:
        description: Unauthorized to download file due to unacceptable token
      404:
        description: Could not find requested package from database
      409:
        description: Dataset was modified since the requested package was
                     generated
      500:
        description: Unable to connect to Metax API or an unexpected status
                     code received
    """
    try:
        request_data = DownloadQuerySchema().load(request.args)
    except ValidationError as err:
        abort(400, str(err.messages))

    # Read auth token from request parameters
    auth_token = request_data.get('token')

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
        current_app.logger.info("Received download request with expired "
                                "token.")
        abort(401)
    except DecodeError:
        current_app.logger.info('Unable to decode offered token.')
        abort(401)

    dataset = jwt_payload['dataset']
    package = jwt_payload.get('package')

    if package is None:
        filepath = jwt_payload.get('file')
        project_identifier = jwt_payload.get('project')

        filename = path.join(
            current_app.config['IDA_DATA_ROOT'],
            'PDO_%s' % project_identifier,
            'files',
            project_identifier,
            ) + filepath
    else:
        # Check if package can be found in database
        task_row = get_task_for_package(dataset, package)

        if task_row is None:
            current_app.logger.error("Could not find database record for "
                                     "package '%s' with valid download token"
                                     % package)
            abort(404)

        initiated = convert_utc_timestamp(task_row['initiated'])

        # Check dataset metadata in Metax API
        try:
            dataset_modified = get_dataset_modified_from_metax(dataset)
        except DatasetNotFound as err:
            abort(404, err)
        except ConnectionError:
            abort(500)
        except MissingFieldsInResponse:
            abort(500)
        except UnexpectedStatusCode:
            abort(500)

        if initiated < dataset_modified:
            current_app.logger.error("Dataset %s has been modified since "
                                     "generation task was initialized"
                                     % dataset)
            abort(409)

        filename = path.join(get_datasets_dir(), package)

    def stream_response():
      download_id = create_download_record(auth_token, package or filepath)
      try:
        with open(filename, "rb") as f:
          chunk = f.read(128)
          while chunk != b"":
            yield chunk
            chunk = f.read(128)
        update_download_record(download_id)
      except:
        update_download_record(download_id, False)
        current_app.logger.error("Failed to stream file '%s'" % filename)

    response_headers= {
        'Content-Type': 'application/octet-stream',
        'Content-Disposition': 'attachment; filename="%s"'
        % (package or filepath.split('/')[-1])
    }
    return Response(stream_with_context(stream_response()),
                    headers=response_headers)

@download_service.errorhandler(400)
def bad_request(error):
    """Error handler for HTTP 400."""
    current_app.logger.error(error)
    return jsonify(name=error.name, error=str(error.description)), 400

@download_service.errorhandler(401)
def unauthorized(error):
    """Error handler for HTTP 401."""
    current_app.logger.error(error)
    return jsonify(name=error.name, error=str(error.description)), 401

@download_service.errorhandler(404)
def resource_not_found(error):
    """Error handler for HTTP 404."""
    current_app.logger.error(error)
    return jsonify(name=error.name, error=str(error.description)), 404

@download_service.errorhandler(409)
def conflict(error):
    """Error handler for HTTP 409."""
    current_app.logger.error(error)
    return jsonify(name=error.name, error=str(error.description)), 409

@download_service.errorhandler(500)
def internal_server_error(error):
    """Error handler for HTTP 500."""
    current_app.logger.error(error)
    return jsonify(name=error.name, error=str(error.description)), 500
