"""
    download.views
    ~~~~~~~~~~~~~~

    Module for views used by Fairdata Download Service.
"""
import json
from datetime import datetime, timedelta
from marshmallow import ValidationError
from os import path
from flask import Blueprint, Response, abort, current_app, jsonify, request, stream_with_context
from jwt import decode, encode, ExpiredSignatureError
from jwt.exceptions import DecodeError
from requests.exceptions import ConnectionError
from ..services import task_service
from ..services.cache import  housekeep_cache, get_datasets_dir, get_mock_notifications_dir
from ..services.db import get_download_record_by_token, get_request_scopes, \
                          get_task_for_package, get_task_id_for_package, get_task_rows, \
                          create_download_record, create_request_scope, \
                          create_subscription_row, create_task_rows, get_package_row, \
                          get_generate_scope_filepaths, finalize_download_record, extract_event
from ..services import metax
from ..services.metax import get_dataset_modified_from_metax, \
                             get_matching_project_identifier_from_metax, \
                             get_matching_dataset_files_from_metax, \
                             DatasetNotFound, UnexpectedStatusCode, \
                             MissingFieldsInResponse, NoMatchingFilesFound
from ..utils import convert_utc_timestamp, format_datetime, ida_service_is_offline, authenticate_trusted_service
from ..model.requests import AuthorizePostData, DownloadQuerySchema, \
                             RequestsPostData, RequestsQuerySchema, SubscribePostData, \
                             MockNotifyPostData
from ..events import construct_event_title

import requests
import urllib.parse
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


download_api = Blueprint('download-api', __name__)


def publish_event(event):
    """
    Publish the specified event to metrics.fairdata.fi
    """
    current_app.logger.debug("Publishing event: %s" % json.dumps(event))

    try:

        timestamp = event["started"]
        title = construct_event_title(event)

        current_app.logger.info("Publishing event: %s: %s" % (timestamp, title))

        try:
            api = current_app.config["FDWE_API"]
            token = current_app.config["FDWE_TOKEN"]
            environment = current_app.config.get("ENVIRONMENT", "DEV")
            url = "%s/report?token=%s&environment=%s&service=DOWNLOAD&scope=%s&timestamp=%s" % (
                api, token, environment, urllib.parse.quote(title), urllib.parse.quote(timestamp)
            )
            response = requests.post(url, verify=False)
            if response.status_code != 200:
                raise Exception(response.text)
        except BaseException as error:
            current_app.logger.error("Failed to publish event: %s" % str(error))

    except BaseException as error:
        current_app.logger.error("Malformed event: %s: %s" % (str(error), json.dumps(event)))


@download_api.route('/requests', methods=['GET'])
def get_request():
    """Internally available end point for file generation request data.

    Returns JSON encoded details regarding zero or more downloadable dataset package files, and/or partial subset package files, which exist in the cache or are in the process of being generated.
    ---
    tags:
      - Internal
    definitions:
      - schema:
          id: Dataset ID
          summary: Generation Task
          description: ID of the dataset for which the file or package is to be downloaded
          properties:
            dataset:
              description: ID of the dataset
              type: string
              example: "63da6f69-f9ea-4bb7-be5d-51fe9dae3440"
      - schema:
          id: Generation Task
          description: Task to generate a package to be downloaded
          properties:
            initiated:
              description: Timestamp of when the task was created
              type: datetime
              example: "2021-02-25T08:00:04+00:00"
            generated:
              description: Timestamp of when the package was generated
              type: datetime
              example: "2021-02-25T08:08:04+00:00"
            status:
              description: Status of the task
              type: string
              example: "SUCCESS"
              enum:
                - PENDING
                - STARTED
                - SUCCESS
                - FAILED
      - schema:
          id: Generation Task Scope
          description: Task to generate a package to be downloaded
          properties:
            scope:
              type: array
              items:
                type: string
                example: "/testdata/Experiment_1/baseline"
      - schema:
          id: Package
          description: Generated package available to be downloaded
          properties:
            package:
              description: File name of the generated package available for download
              type: string
              example: "63da6f69-f9ea-4bb7-be5d-51fe9dae3440_resdyamg.zip"
            checksum:
              description: SHA256 checksum of the package file
              type: string
              example: "sha256:8739c76e681f900923b900c9df0ef75cf421d39cabb54650c4b9ad19b6a76d85"
            size:
              description: Size of the generated package in bytes
              type: number
              example: "1526474"
      - schema:
          id: Partial Generation Tasks
          description: Generated package available to be downloaded
          properties:
            partial:
              type: array
              items:
                allOf:
                - $ref: "#/definitions/Generation Task"
                - $ref: "#/definitions/Generation Task Scope"
                - $ref: "#/definitions/Package"
    produces:
      - application/json
    parameters:
      - name: dataset
        in: query
        description: ID of the dataset for which the package generation requests are to be returned
        type: string
        example: "63da6f69-f9ea-4bb7-be5d-51fe9dae3440"
        required: true
    responses:
      200:
        description: Information about the active package generation requests
        schema:
          allOf:
            - $ref: "#/definitions/Dataset ID"
            - $ref: "#/definitions/Generation Task"
            - $ref: "#/definitions/Package"
            - $ref: "#/definitions/Partial Generation Tasks"
      401:
        description: Unauthorized request was received
      404:
        description: No active requests for given dataset were found
      500:
        description: Unable to connect to Metax API or an unexpected status code received
    """

    current_app.logger.debug("GET /requests: %s" % json.dumps(request.args))

    # Authenticate the trusted service making the request
    try:
        authenticate_trusted_service(current_app, request)
    except PermissionError as err:
        abort(401, str(err))

    # Validate request
    try:
        query = RequestsQuerySchema().load(request.args)
    except ValidationError as err:
        abort(400, str(err.messages))

    dataset = query.get('dataset')

    current_app.logger.debug("GET /requests: dataset = %s" % str(dataset))

    # Check active package generation tasks
    try:
        task_rows = task_service.get_active_tasks(dataset)
    except DatasetNotFound as err:
        abort(404, err)
    except ConnectionError:
        abort(500)
    except MissingFieldsInResponse:
        abort(500)
    except UnexpectedStatusCode:
        abort(500)
    except task_service.NoActiveTasksFound as err:
        abort(404, err)

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


@download_api.route('/requests', methods=['POST'])
def post_request():
    """Internally available end point for initiating dataset package file generation.

    Creates a new dataset package file generation request if no such request already exists.
    ---
    tags:
      - Internal
    definitions:
      - schema:
          id: Generation Task Created
          description: Whether or not a package generation task was created
          properties:
            created:
              type: string
              example: "True"
    consumes:
      - application/json
    produces:
      - application/json
    parameters:
      - name: dataset
        in: body
        description: ID of the dataset for which the package generation request is to be initiated
        type: string
        example: "63da6f69-f9ea-4bb7-be5d-51fe9dae3440"
        required: true
      - name: scope
        in: body
        description: List of one or more relative pathnames defining the scope of a partial dataset package
        type: string
        example: [ "/testdata/Experiment_1/baseline" ]
        required: false
    responses:
      200:
        description: Information about the initiated package generation request
        schema:
          allOf:
            - $ref: "#/definitions/Dataset ID"
            - $ref: "#/definitions/Generation Task Created"
            - $ref: "#/definitions/Partial Generation Tasks"
      401:
        description: Unauthorized request was received
      409:
        description: No pathname in the dataset matching a specified scope was found
      500:
        description: Unable to connect to Metax API or an unexpected status code received
    """

    current_app.logger.debug("POST /requests: %s" % json.dumps(request.get_json()))

    # Authenticate the trusted service making the request
    try:
        authenticate_trusted_service(current_app, request)
    except PermissionError as err:
        abort(401, str(err))

    # Validate request
    try:
        request_data = RequestsPostData().load(request.get_json())
    except ValidationError as err:
        abort(400, str(err.messages))

    dataset = request_data.get('dataset')
    request_scope = request_data.get('scope', [])

    current_app.logger.debug("POST /requests: dataset = %s" % str(dataset))
    current_app.logger.debug("POST /requests: scope = %s" % json.dumps(request_scope))

    # Check dataset metadata in Metax API
    try:
        task_row, project_identifier, is_partial, generate_scope = task_service.get_active_task(dataset, request_scope)
    except DatasetNotFound as err:
        abort(404, err)
    except ConnectionError:
        abort(500)
    except MissingFieldsInResponse:
        abort(500)
    except UnexpectedStatusCode:
        abort(500)
    except NoMatchingFilesFound as err:
        abort(409, err)

    created = False

    # Create new task if no such already exists
    if not task_row:
        from ..tasks import generate_task
        housekeep_cache()
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
    if current_app.config["ALWAYS_RUN_HOUSEKEEPING_IN_REQUEST_ENDPOINT"]:
        housekeep_cache()
    return jsonify(response)


@download_api.route('/subscribe', methods=['POST'])
def post_subscribe():
    """Internally available end point for subscribing to a package generation task.

    Creates a new file subscription record for an ongoing package generation task.
    ---
    tags:
      - Internal
    definitions:
      - schema:
          id: Subscription Data
          description: Base64 encoded data to be returned in the body of the notification sent to the specified notification URL
          properties:
            subscriptionData:
              type: string
              example: "637nNUwp+oiRkQgNfPit"
      - schema:
          id: Notify URL
          description: URL where the package generation notification will be posted
          properties:
            notifyURL:
              type: string
              example: "https://example.com/notify"
    consumes:
      - application/json
    produces:
      - application/json
    parameters:
      - name: notifyURL
        in: body
        description: URL where the package generation notification will be posted
        type: string
        example: "https://example.com/notify"
        required: true
      - name: subscriptionData
        in: body
        description: Base64 encoded data to be returned in the body of the notification sent to the specified notification URL
        type: string
        example: "637nNUwp+oiRkQgNfPit"
        required: true
    responses:
      201:
        description: Information about the subscription
        schema:
          allOf:
            - $ref: "#/definitions/Dataset ID"
            - $ref: "#/definitions/Generation Task Scope"
            - $ref: "#/definitions/Subscription Data"
            - $ref: "#/definitions/Notify URL"
      400:
        description: Invalid request was received
      401:
        description: Unauthorized request was received
      409:
        description: No matching ongoing package generation task was found
      500:
        description: Unable to connect to Metax API or an unexpected status code received
    """

    current_app.logger.debug("POST /subscribe: %s" % json.dumps(request.get_json()))

    # Authenticate the trusted service making the request
    try:
        authenticate_trusted_service(current_app, request)
    except PermissionError as err:
        abort(401, str(err))

    # Validate request
    try:
        request_data = SubscribePostData().load(request.get_json())
    except ValidationError as err:
        abort(400, str(err.messages))

    dataset = request_data.get('dataset')
    request_scope = request_data.get('scope', [])
    subscription_data = request_data.get('subscription_data', '')
    notify_url = request_data.get('notify_url')

    # Get corresponding package generation task
    try:
        task_row, project_identifier, is_partial, generate_scope = task_service.get_active_task(dataset, request_scope)
    except DatasetNotFound as err:
        abort(404, err)
    except ConnectionError:
        abort(500)
    except MissingFieldsInResponse:
        abort(500)
    except UnexpectedStatusCode:
        abort(500)
    except NoMatchingFilesFound as err:
        abort(409, err)

    if not task_row:
        abort(404, 'No matching package generation tasks were found.')
    elif task_row['status'] not in ['PENDING', 'STARTED', 'RETRY']:
        abort(409, "Status of the matching active package generation task is '%s'." % task_row['status'])

    create_subscription_row(task_row['task_id'], notify_url, subscription_data)

    return jsonify({
        'dataset': dataset,
        'scope': request_scope,
        'subscriptionData': subscription_data,
        'notifyURL': notify_url
    }), 201


@download_api.route('/mock_notify', methods=['POST'])
def post_mock_notify():
    """Internally available end point for mock reception of subscription notification once dataset package has been successfully generated. Used by automated testing.

    Creates a temporary file located in the download service cache with a filename corresponding to the base64 encoded subscription data.
    ---
    tags:
      - Testing
    definitions:
      - schema:
          id: Mock Subscription Notification
          description: Base64 encoded data to be returned in the body of the notification sent to the specified notification URL
          properties:
            subscriptionData:
              type: string
              example: "637nNUwp+oiRkQgNfPit"
    consumes:
      - application/json
    produces:
      - application/json
    parameters:
      - name: subscriptionData
        in: body
        description: Base64 encoded data to be returned in the body of the notification sent to the specified notification URL
        type: string
        example: "637nNUwp+oiRkQgNfPit"
        required: true
    responses:
      200:
        description: Confirmation of receipt of notification
        schema:
          allOf:
            - $ref: "#/definitions/Mock Subscription Notification"
      400:
        description: Invalid request was received
      500:
        description: Unable to connect to Metax API or an unexpected status code received
    """

    current_app.logger.debug("POST /mock_notify: %s" % json.dumps(request.get_json()))

    if current_app.config.get('ENVIRONMENT') not in [ 'DEV', 'TEST' ]:
        abort(401, "The /mock_notify endpoint is only available in a development or test environment")

    # Validate request
    try:
        request_data = MockNotifyPostData().load(request.get_json())
    except ValidationError as err:
        abort(400, str(err.messages))

    subscription_data = request_data.get('subscription_data')

    if not subscription_data:
        return jsonify({
            'error': 'subscriptionData not defined',
        }), 400

    # Write subscription data temp file
    mock_notification_file = "%s/%s" % (get_mock_notifications_dir(), subscription_data)
    with open(mock_notification_file, "w") as notification_file:
        notification_file.write("%s\n" % subscription_data)
        notification_file.close()

    return jsonify({
        'subscriptionData': subscription_data,
    }), 200


@download_api.route('/authorize', methods=['POST'])
def authorize():
    """Internally available end point for authorizing requesting clients.

    Requests a time-limited single-use token for download of a specific dataset package or file.
    ---
    tags:
      - Internal
    definitions:
      - schema:
          id: Authorize Response
          description: Response including authorization token for downloading a dataset file or package
          properties:
            token:
              type: string
              description: JWT encoded authorization token
              example: "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE2MTQzMjY5ODAsImRhdGFzZXQiOiIxIiwiZmlsZSI6Ii9wcm9qZWN0X3hfRlJPWkVOL0V4cGVyaW1lbnRfWC9maWxlX25hbWVfMSIsInByb2plY3QiOiJwcm9qZWN0X3gifQ.niz50oRduP6pRtjrpLUJzIF48cr0-15IbxsFXjg7emE"
    consumes:
      - application/json
    produces:
      - application/json
    parameters:
      - name: dataset
        in: body
        description: ID of the dataset
        type: string
        example: "63da6f69-f9ea-4bb7-be5d-51fe9dae3440"
        required: true
      - name: package
        in: body
        description: Name of the dataset package file to be downloaded (either a package or filename must be specified)
        type: string
        example: "63da6f69-f9ea-4bb7-be5d-51fe9dae3440_resdyamg.zip"
        required: false
      - name: file
        in: body
        description: Name of the individual file to be downloaded (either a package or filename must be specified)
        type: string
        example: "/testdata/Experiment_1/test_03.dat"
        required: false
    responses:
      200:
        description: Token for downloading the requested file or package
        schema:
          allOf:
            - $ref: "#/definitions/Authorize Response"
      401:
        description: Unauthorized request was received
      404:
        description: No dataset file or active generated package matching request was found
      409:
        description: Dataset was modified since the requested package was generated
      500:
        description: Unable to connect to Metax API or an unexpected status code received
    """

    current_app.logger.debug("POST /authorize: %s" % json.dumps(request.get_json()))

    # Authenticate the trusted service making the request
    try:
        authenticate_trusted_service(current_app, request)
    except PermissionError as err:
        abort(401, str(err))

    # Validate request
    try:
        request_data = AuthorizePostData().load(request.get_json())
    except ValidationError as err:
        abort(400, str(err.messages))

    dataset = request_data.get('dataset')
    package = request_data.get('package')

    if package is None:
        filename = request_data.get('filename') 
        if filename is None:
            abort(400, 'Missing file parameter')
        try:
            project_identifier = get_matching_project_identifier_from_metax(dataset, filename)
        except NoMatchingFilesFound as err:
            abort(404, err)

        # TODO: Add missing tests whether the file belongs to the specified dataset or the dataset has been
        #       modified (invalidated) since the authorization. Currently possible to authorize files
        #       which exist for the project but are not part of the dataset or when dataset is deprecated.
        #       C.f. https://jira.eduuni.fi/browse/CSCFAIRDATA-289

        # Verify current_app.config
        current_app.logger.debug(current_app.config)

        # Create JWT
        jwt_payload = {
            'exp': datetime.utcnow() + timedelta(minutes=current_app.config['JWT_TTL']),
            'dataset': dataset,
            'file': filename,
            'project': project_identifier
        }
    else:
        try:
            task_service.check_if_package_can_be_downloaded(dataset, package)
        except DatasetNotFound as err:
            abort(404, err)
        except ConnectionError:
            abort(500)
        except MissingFieldsInResponse:
            abort(500)
        except UnexpectedStatusCode:
            abort(500)
        except task_service.NoDatabaseRecordForPackageFound as err:
            abort(404, err)
        except task_service.PackageOutdated as err:
            abort(409, err)

        # Verify current_app.config
        current_app.logger.debug(current_app.config)

        # Create JWT
        jwt_payload = {
            'exp': datetime.utcnow() + timedelta(minutes=current_app.config['JWT_TTL']),
            'dataset': dataset,
            'package': package
        }

        # Because when the package cache is cleaned, we can lose package records, and the package records
        # are the only explicit means to link the download records with generation requests and thereby 
        # determine whether the package is complete or partial, and if partial what the scope was,  we will
        # include the package generation task id in the authorization token for the package download, so that
        # it persists as part of any subsequent download record in the stored token, and can be later used to
        # determine if the package was complete or partial, and for partial packages to obtain the scope of the
        # partial package for inclusion in the download metrics, even if the package record is subsequently lost
        # due to cleaning the package cache.

        jwt_payload['generated_by'] = get_task_id_for_package(package)

    jwt_token = encode(jwt_payload, current_app.config['JWT_SECRET'], algorithm=current_app.config['JWT_ALGORITHM'])

    return jsonify(token=jwt_token.decode())


@download_api.route('/download', methods=['GET'])
def download():
    """Publically accessible endpoint for file or package download with a valid single-use token.

    Allows downloading of an individual dataset file or dataset package with a valid authorization token, from which the package or filename to be downloaded will be obtained.
    ---
    tags:
      - Public
    consumes:
      - application/json
    produces:
      - application/octet-stream
    parameters:
      - name: token
        in: query
        description: Single-use JWT encoded token used to authorize the download of a specific file or package. The token contents will specify the project, dataset, and package or file to be downloaded.
        type: string
        example: "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE2MTQzMjY5ODAsImRhdGFzZXQiOiIxIiwiZmlsZSI6Ii9wcm9qZWN0X3hfRlJPWkVOL0V4cGVyaW1lbnRfWC9maWxlX25hbWVfMSIsInByb2plY3QiOiJwcm9qZWN0X3gifQ.niz50oRduP6pRtjrpLUJzIF48cr0-15IbxsFXjg7emE"
        required: true
    responses:
      200:
        description: File or package to be downloaded
      401:
        description: Unauthorized to download file due to unacceptable token
      404:
        description: Could not find the requested file or package
      409:
        description: Dataset was modified since the requested package was generated and the package is deemed invalid
      500:
        description: Unable to connect to Metax API or an unexpected status code received
    """

    current_app.logger.debug("GET /download: %s" % json.dumps(request.args))

    event = {}

    try:
        request_data = DownloadQuerySchema().load(request.args)
    except ValidationError as err:
        abort(400, str(err.messages))

    # Read auth token from request parameters
    auth_token = request_data.get('token')

    if current_app.config['ALWAYS_RUN_HOUSEKEEPING_IN_DOWNLOAD_ENDPOINT'] is True:
        housekeep_cache()

    download_row = get_download_record_by_token(auth_token)

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
        current_app.logger.info("Received download request with expired token.")
        abort(401)
    except DecodeError:
        current_app.logger.info('Unable to decode offered token.')
        abort(401)

    dataset = jwt_payload['dataset']
    package = jwt_payload.get('package')

    event["dataset"] = dataset

    if package is None:
        # If the IDA service is offline, report the download service unavailable (for file downloads)
        if ida_service_is_offline(current_app):
            abort(503, 'The IDA service is offline. Individual file download is currently unavailable.')

        filepath = jwt_payload.get('file')
        project_identifier = jwt_payload.get('project')

        event["type"] = "FILE"
        event["file"] = filepath

        filename = path.join(
            current_app.config['IDA_DATA_ROOT'],
            'PSO_%s' % project_identifier,
            'files',
            project_identifier,
            ) + filepath

        # TODO: Add missing tests whether the file belongs to the specified dataset or the dataset has been
        #       modified (invalidated) since the authorization. Currently possible to download guessed files
        #       which exist for the project but are not part of the dataset or when dataset is deprecated.
        #       C.f. https://jira.eduuni.fi/browse/CSCFAIRDATA-289

    else:
        try:
            task_service.check_if_package_can_be_downloaded(dataset, package)
        except DatasetNotFound as err:
            abort(404, err)
        except ConnectionError:
            abort(500)
        except MissingFieldsInResponse:
            abort(500)
        except UnexpectedStatusCode:
            abort(500)
        except task_service.NoDatabaseRecordForPackageFound as err:
            abort(404, err)
        except task_service.PackageOutdated as err:
            abort(409, err)

        filename = path.join(get_datasets_dir(), package)

    def stream_response():
      download_id = create_download_record(auth_token, package or filepath)
      try:
        with open(filename, "rb") as f:
          chunk = f.read(1024)
          while chunk != b"":
            yield chunk
            chunk = f.read(1024)
        finalize_download_record(download_id)
        publish_event(extract_event(download_id))
      except:
        current_app.logger.error("Failed to stream file '%s'" % filename)
        finalize_download_record(download_id, False)
        publish_event(extract_event(download_id))

    response_headers= {
        'Content-Type': 'application/octet-stream',
        'Content-Disposition': 'attachment; filename="%s"'
        % (package or filepath.split('/')[-1])
    }
    return Response(stream_with_context(stream_response()), headers=response_headers)


@download_api.errorhandler(400)
def bad_request(error):
    """Error handler for HTTP 400."""
    current_app.logger.error(error)
    return jsonify(name=error.name, error=str(error.description)), 400


@download_api.errorhandler(401)
def unauthorized(error):
    """Error handler for HTTP 401."""
    current_app.logger.error(error)
    return jsonify(name=error.name, error=str(error.description)), 401


@download_api.errorhandler(404)
def resource_not_found(error):
    """Error handler for HTTP 404."""
    current_app.logger.error(error)
    return jsonify(name=error.name, error=str(error.description)), 404


@download_api.errorhandler(409)
def conflict(error):
    """Error handler for HTTP 409."""
    current_app.logger.error(error)
    return jsonify(name=error.name, error=str(error.description)), 409


@download_api.errorhandler(500)
def internal_server_error(error):
    """Error handler for HTTP 500."""
    current_app.logger.error(error)
    return jsonify(name=error.name, error=str(error.description)), 500


@download_api.errorhandler(503)
def service_not_available(error):
    """Error handler for HTTP 503."""
    current_app.logger.error(error)
    return jsonify(name=error.name, error=str(error.description)), 503
