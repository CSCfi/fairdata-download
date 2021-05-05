"""
    download.views
    ~~~~~~~~~~~~~~

    Module for views used by Fairdata Download Service.
"""
from datetime import datetime, timedelta
from os import path

from flask import (
    Blueprint,
    Response,
    abort,
    current_app,
    jsonify,
    request,
    stream_with_context,
)
from jwt import DecodeError, ExpiredSignatureError, decode, encode
from marshmallow import ValidationError

# from jwt.exceptions import DecodeError
from requests.exceptions import ConnectionError

from ..model.requests import (
    AuthorizePostData,
    DownloadQuerySchema,
    RequestsPostData,
    RequestsQuerySchema,
    SubscribePostData,
)
from ..services import task_service
from ..services.cache import get_datasets_dir
from ..services.db import (
    create_download_record,
    create_request_scope,
    create_subscription_row,
    create_task_rows,
    get_download_record,
    get_package_row,
    get_request_scopes,
    update_download_record,
)
from ..services.metax import (
    DatasetNotFound,
    MissingFieldsInResponse,
    NoMatchingFilesFound,
    UnexpectedStatusCode,
    get_matching_project_identifier_from_metax,
)
from ..utils import format_datetime

download_api = Blueprint("download-api", __name__)


@download_api.route("/requests", methods=["GET"])
def get_request():
    """
    Internally available end point for file generation request data.

    Returns JSON encoded details regarding zero or more downloadable
    dataset package files, and/or partial subset package files, which exist
    in the cache or are in the process of being generated.
    ---
    tags:
      - Package Generation
    definitions:
      - schema:
          id: Dataset ID
          summary: Generation Task
          description: ID of the dataset whose file or package is to be downloaded
          properties:
            dataset:
              type: string
              description: ID of the dataset
              example: "1"
      - schema:
          id: Generation Task
          description: Task to generate a package to be downloaded
          properties:
            initiated:
              type: datetime
              description: Timestamp of when the task was created
              example: "2021-02-25T08:00:04+00:00"
            generated:
              type: datetime
              description: Timestamp of when the package was generated
              example: "2021-02-25T08:08:04+00:00"
            status:
              type: string
              description: Status of the task
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
                example: "/project_x_FROZEN/Experiment_X/file_name_1"
      - schema:
          id: Package
          description: Generated package available to be downloaded
          properties:
            package:
              type: string
              description: Filename of the package
              example: "1_s8jhbj0j.zip"
            checksum:
              type: string
              description: SHA256 checksum of the package file
              example: "sha256:8739c76e681f900923b900c9df0ef75cf421d39cabb54650c4b9ad19b6a76d85"
            size:
              type: number
              description: Size of the generated package in bytes
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
        description: ID of the dataset whose package generation requests are
                     returned
        schema:
          type: string
          example: "1"
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
      404:
        description: No active requests for given dataset were found
      500:
        description: Unable to connect to Metax API or an unexpected status
                     code received
    """
    # Validate request
    try:
        query = RequestsQuerySchema().load(request.args)
    except ValidationError as err:
        abort(400, str(err.messages))

    dataset = query.get("dataset")

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
    response["dataset"] = dataset

    for task_row in task_rows:
        if not task_row["is_partial"]:
            response["status"] = task_row["status"]
            response["initiated"] = format_datetime(task_row["initiated"])

            if task_row["status"] == "SUCCESS":
                package_row = get_package_row(task_row["task_id"])

                response["generated"] = format_datetime(task_row["date_done"])
                response["package"] = package_row["filename"]
                response["size"] = package_row["size_bytes"]
                response["checksum"] = package_row["checksum"]
        else:
            if "partial" not in response.keys():
                response["partial"] = []

            if task_row["status"] == "SUCCESS":
                package_row = get_package_row(task_row["task_id"])

            for request_scope in get_request_scopes(task_row["task_id"]):
                partial_task = {
                    "scope": list(request_scope),
                    "status": task_row["status"],
                    "initiated": format_datetime(task_row["initiated"]),
                }

                if task_row["status"] == "SUCCESS":
                    partial_task["generated"] = format_datetime(task_row["date_done"])
                    partial_task["package"] = package_row["filename"]
                    partial_task["size"] = package_row["size_bytes"]
                    partial_task["checksum"] = package_row["checksum"]

                response["partial"].append(partial_task)

    return jsonify(response)


@download_api.route("/requests", methods=["POST"])
def post_request():
    """Internally available end point for initiating file generation.

    Creates a new file generation request if no such already exists.
    ---
    tags:
      - Package Generation
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
      - name: body
        in: body
        description: ID of the dataset whose package generation requests are
                     returned
        schema:
          allOf:
            - $ref: "#/definitions/Dataset ID"
            - $ref: "#/definitions/Generation Task Scope"
        required: true
    responses:
      200:
        description: Information about the matching package generation request
        schema:
          allOf:
            - $ref: "#/definitions/Dataset ID"
            - $ref: "#/definitions/Generation Task Created"
            - $ref: "#/definitions/Partial Generation Tasks"
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

    dataset = request_data.get("dataset")
    request_scope = request_data.get("scope", [])

    # Check dataset metadata in Metax API
    try:
        (
            task_row,
            project_identifier,
            is_partial,
            generate_scope,
        ) = task_service.get_active_task(dataset, request_scope)
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

        task = generate_task.delay(dataset, project_identifier, list(generate_scope))

        task_row = create_task_rows(dataset, task.id, is_partial, generate_scope)

        if is_partial:
            create_request_scope(task.id, request_scope)

        created = True
    else:
        current_app.logger.info(
            "Found request with status '%s' for dataset '%s'"
            % (task_row["status"], dataset)
        )

        if set(request_scope) not in get_request_scopes(task_row["task_id"]):
            create_request_scope(task_row["task_id"], request_scope)

    # Formulate response
    response = {}
    response["dataset"] = dataset
    response["created"] = created

    if not is_partial:
        response["initiated"] = format_datetime(task_row["initiated"])
        response["status"] = task_row["status"]

        if task_row["status"] == "SUCCESS":
            package_row = get_package_row(task_row["task_id"])

            response["generated"] = format_datetime(task_row["date_done"])
            response["package"] = package_row["filename"]
            response["size"] = package_row["size_bytes"]
            response["checksum"] = package_row["checksum"]
    else:
        partial_task = {
            "scope": request_scope,
            "initiated": format_datetime(task_row["initiated"]),
            "status": task_row["status"],
        }

        if task_row["status"] == "SUCCESS":
            package_row = get_package_row(task_row["task_id"])

            partial_task["generated"] = format_datetime(task_row["date_done"])
            partial_task["package"] = package_row["filename"]
            partial_task["size"] = package_row["size_bytes"]
            partial_task["checksum"] = package_row["checksum"]

        response["partial"] = [partial_task]

    return jsonify(response)


@download_api.route("/subscribe", methods=["POST"])
def post_subscribe():
    """Internally available end point for subscribing to a package generation task.

    Creates a new file subscription record for an ongoing package generation task.
    ---
    tags:
      - Package Generation
    definitions:
      - schema:
          id: Subscription Data
          description: Optional base64 encoded data field to be echoed back once the notification is sent
          properties:
            subscriptionData:
              type: string
              example: "637nNUwp+oiRkQgNfPit"
      - schema:
          id: Notify URL
          description: URL where a package generation notification will be posted.
          properties:
            notifyURL:
              type: string
              example: "https://example.com/notify"
    consumes:
      - application/json
    produces:
      - application/json
    parameters:
      - name: body
        in: body
        description: Information about the task to be subscribed to and some subscription data
        schema:
          allOf:
            - $ref: "#/definitions/Dataset ID"
            - $ref: "#/definitions/Generation Task Scope"
            - $ref: "#/definitions/Subscription Data"
            - $ref: "#/definitions/Notify URL"
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
      409:
        description: No matching ongoing package generation task was found
      500:
        description: Unable to connect to Metax API or an unexpected status
                     code received
    """
    # Validate request
    try:
        request_data = SubscribePostData().load(request.get_json())
    except ValidationError as err:
        abort(400, str(err.messages))

    dataset = request_data.get("dataset")
    request_scope = request_data.get("scope", [])
    subscription_data = request_data.get("subscription_data", "")
    notify_url = request_data.get("notify_url")

    # Get corresponding package generation task
    try:
        (
            task_row,
            project_identifier,
            is_partial,
            generate_scope,
        ) = task_service.get_active_task(dataset, request_scope)
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
        abort(404, "No matching package generation tasks were found.")
    elif task_row["status"] not in ["PENDING", "STARTED"]:
        abort(
            409,
            "Status of the matching active package generation task is '%s'."
            % task_row["status"],
        )

    create_subscription_row(task_row["task_id"], notify_url, subscription_data)

    return (
        jsonify(
            {
                "dataset": dataset,
                "scope": request_scope,
                "subscriptionData": subscription_data,
                "notifyURL": notify_url,
            }
        ),
        201,
    )


@download_api.route("/authorize", methods=["POST"])
def authorize():
    """Internally available end point for authorizing requesting clients.

    Requests a time-limited single-use token for download of a specific
    dataset package or file.
    ---
    tags:
      - File or Package Download
    definitions:
      - schema:
          id: Package Authorize Request
          description: Request autorization token for downloading a generated package
          properties:
            dataset:
              type: string
              description: ID of the dataset
              example: "1"
            package:
              type: string
              description: File name of the generated package available for download
      - schema:
          id: File Authorize Request
          description: Request autorization token for downloading a dataset file
          properties:
            dataset:
              type: string
              description: ID of the dataset
              example: "1"
            file:
              type: string
              example: "/project_x_FROZEN/Experiment_X/file_name_1"
              description: Dataset file paths as described in Metax
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
      - name: body
        in: body
        description: ID of the dataset whose package generation requests are
                     returned
        schema:
          $ref: "#/definitions/File Authorize Request"
        required: true
    responses:
      200:
        description: Token for downloading the requested file or package
        schema:
          $ref: "#/definitions/Authorize Response"
      404:
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

    dataset = request_data.get("dataset")
    package = request_data.get("package")

    if package is None:
        filename = request_data.get('filename') or abort(400)
        try:
            project_identifier = get_matching_project_identifier_from_metax(
                dataset,
                filename)
        except NoMatchingFilesFound as err:
            abort(404, err)

        # Create JWT
        jwt_payload = {
            "exp": datetime.utcnow() + timedelta(minutes=current_app.config["JWT_TTL"]),
            "dataset": dataset,
            "file": filename,
            "project": project_identifier,
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

        # Create JWT
        jwt_payload = {
            "exp": datetime.utcnow() + timedelta(minutes=current_app.config["JWT_TTL"]),
            "dataset": dataset,
            "package": package,
        }

    jwt_token = encode(
        jwt_payload,
        current_app.config["JWT_SECRET"],
        algorithm=current_app.config["JWT_ALGORITHM"],
    )

    return jsonify(token=jwt_token.decode())


@download_api.route("/download", methods=["GET"])
def download():
    """Publically accessible with valid single-use token.

    Allows downloading files with valid tokens, generated with authorize
    requests. Trusted access without token to certain internal services (e.g.
    PAS).
    ---
    tags:
      - File or Package Download
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
    auth_token = request_data.get("token")

    if auth_token is None:
        # Parse authorization header
        auth_header = request.headers.get("Authorization")
        if auth_header is None:
            abort(401)

        [auth_method, auth_token] = auth_header.split(" ")
        if auth_method != "Bearer":
            current_app.logger.info(
                "Received invalid autorization method '%s'" % auth_method
            )
            abort(401)

    download_row = get_download_record(auth_token)

    if download_row is not None:
        current_app.logger.info("Received download request with used token.")
        abort(401)

    # Decode token
    try:
        jwt_payload = decode(
            auth_token,
            current_app.config["JWT_SECRET"],
            algorithms=[current_app.config["JWT_ALGORITHM"]],
        )
    except ExpiredSignatureError:
        current_app.logger.info("Received download request with expired " "token.")
        abort(401)
    except DecodeError:
        current_app.logger.info("Unable to decode offered token.")
        abort(401)

    dataset = jwt_payload["dataset"]
    package = jwt_payload.get("package")

    if package is None:
        filepath = jwt_payload.get("file")
        project_identifier = jwt_payload.get("project")

        filename = (
            path.join(
                current_app.config["IDA_DATA_ROOT"],
                "PSO_%s" % project_identifier,
                "files",
                project_identifier,
            )
            + filepath
        )
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
            update_download_record(download_id)
        except:
            update_download_record(download_id, False)
            current_app.logger.error("Failed to stream file '%s'" % filename)

    response_headers = {
        "Content-Type": "application/octet-stream",
        "Content-Disposition": 'attachment; filename="%s"'
        % (package or filepath.split("/")[-1]),
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
