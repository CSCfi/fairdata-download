"""
download.db
~~~~~~~~~~~

Database module for Fairdata Download Service.
"""
from os import path
import sqlite3
import json
import click
import pendulum
from flask import current_app, g
from flask.cli import AppGroup
from jwt import decode
from ..dto import Package
from ..utils import normalize_timestamp


def get_db():
    """
    Returns database connection from global scope, or connects to database if no conection is already established.
    """
    init_schema = False

    if 'db' not in g:
        if not path.isfile(current_app.config['DATABASE_FILE']):
            init_schema = True

        g.db = sqlite3.connect(
            current_app.config['DATABASE_FILE'],
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        g.db.row_factory = sqlite3.Row

        current_app.logger.debug(
            'Connected to database on %s' %
            (current_app.config['DATABASE_FILE'], ))

    if init_schema:
        init_db()

    return g.db


def close_db(e=None):
    """
    Removes database connection from global scope and disconnects from database.
    """
    db_conn = g.pop('db', None)

    if db_conn is not None:
        db_conn.close()

        current_app.logger.debug(
            'Disconnected from database on %s' %
            (current_app.config['DATABASE_FILE'], ))


def init_db():
    """
    Initializes database by creating tables that don't exist.
    """
    db_conn = get_db()

    with current_app.open_resource('sql/create_tables.sql') as migration_file:
        db_conn.executescript(migration_file.read().decode('utf8'))

    current_app.logger.debug(
        'Initialized database on %s' %
        (current_app.config['DATABASE_FILE'], ))


def get_download_record_by_token(token):
    """
    Returns a row from download table for a given authentication token.

    :param token: JWT encoded authentication token for which download row is fetched
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    return db_cursor.execute(
        'SELECT * FROM download WHERE token = ?',
        (token,)
    ).fetchone()


def get_download_record_by_id(download_id):
    """
    Returns a row from download table for a given record id

    :param download_id: integer id of the download record
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    return db_cursor.execute(
        'SELECT * FROM download WHERE id = ?',
        (download_id,)
    ).fetchone()


def get_request_scopes(task_id):
    """
    Returns a list of sets of scopes that have been requested and are fulfilled by specified partial file generation task.

    :param task_id: ID of the partial file generation task
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    generate_requests = db_cursor.execute(
        'SELECT id FROM generate_request WHERE task_id = ?',
        (task_id,)
    ).fetchall()

    request_scopes = []
    for generate_request in generate_requests:
        request_scope = db_cursor.execute(
            'SELECT prefix FROM generate_request_scope WHERE request_id = ?',
            (generate_request['id'],)
        ).fetchall()

        request_scopes.append(
            set(map(lambda scope_row: scope_row['prefix'], request_scope)))

    return request_scopes


def get_task_rows(dataset_id, initiated_after=''):
    """
    Returns a rows from file_generate table for a dataset.

    :param dataset_id: ID of dataset for which task rows are fetched
    :param initiated_after: timestamp after which fetched tasks may have been initialized
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    return db_cursor.execute(
        'SELECT initiated, date_done, task_id, status, is_partial '
        'FROM generate_task t '
        'LEFT JOIN package p '
        'ON t.task_id = p.generated_by '
        'WHERE t.dataset_id = ? '
        'AND t.initiated > ? '
        'AND ((t.status is "SUCCESS" and p.filename is not null) '
        '  OR (t.status is not "SUCCESS" and t.status is not "FAILURE")) ',
        (dataset_id, initiated_after)
    ).fetchall()


def create_subscription_row(task_id, notify_url, subscription_data):
    """
    Creates a new subscription for the specified package generation task
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    db_cursor.execute(
        'INSERT INTO subscription (task_id, notify_url, subscription_data) VALUES (?, ?, ?)',
        (task_id, notify_url, subscription_data)
    )

    db_conn.commit()

    current_app.logger.info(
        "Created a new subscription for package generation task '%s'"
        % (task_id))


def get_subscription_rows(task_id):
    """
    Fetch subscription rows for the specified package generation task
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    return db_cursor.execute(
        'SELECT notify_url, subscription_data FROM subscription WHERE task_id = ?',
        (task_id,)
    ).fetchall()


def delete_subscription_rows(task_id):
    """
    Delete subscription rows for the specified package generation task
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    db_cursor.execute('DELETE FROM subscription WHERE task_id = ?', (task_id,))

    db_conn.commit()

    current_app.logger.info(
        "Deleted subscription rows for package generation task '%s'"
        % (task_id))


def create_download_record(token, filename):
    """
    Creates a new download record for a given package with specified authentication token.

    :param token: JWT encoded authentication token used for authorizing file download
    :param filename: Filename of the downloaded package
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    db_cursor.execute(
        'INSERT INTO download (token, filename) VALUES (?, ?)',
        (token, filename)
    )

    db_conn.commit()

    current_app.logger.info(
        "Created a new download record for package '%s' with token '%s'"
        % (filename, token))

    return db_cursor.lastrowid


def finalize_download_record(download_id, successful=True):
    """
    Finalize (update) download record after the stream has ended, either
    successfully or with failure.

    :param download_id: ID of the download record in the database
    :param successful: Whether or not the download ended succesfully
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    status = 'SUCCESSFUL' if successful else 'FAILED'

    db_cursor.execute('UPDATE download SET status = ?, finished = DATETIME() WHERE id = ?', (status, download_id))

    db_conn.commit()

    current_app.logger.debug("Set status of download '%s' to '%s'" % (download_id, status))


def create_request_scope(task_id, request_scope):
    """
    Creates database rows for a file generation request that is fulfilled by given task.

    :param task_id: ID of the partial file generation task fulfilling specified scope
    :param request_scope: List of files or directories included in the scope as requested by client
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    db_cursor.execute(
        'INSERT INTO generate_request (task_id) VALUES (?)', (task_id,))

    request_id = db_cursor.lastrowid

    for prefix in request_scope:
        db_cursor.execute(
            'INSERT INTO generate_request_scope (request_id, prefix) VALUES (?, ?)',
            (request_id, prefix))

    db_conn.commit()

    current_app.logger.info(
        "Created a new file generation request with task_id '%s' and scope "
        "'%s'"
        % (task_id, request_scope))


def create_task_rows(dataset_id, task_id, is_partial, generate_scope):
    """
    Creates all the appropriate rows to generate_task and generate_scope tables for a given file generation task.

    :param dataset_id: ID of the dataset that the files belong to
    :param task_id: ID of the generation task
    :param is_partial: Boolean value specifying whether the package is partial ie. does not include all of the files in the dataset
    :param generate_scope: List of all the filepaths to be included in the generated package
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    db_cursor.execute(
        "INSERT INTO generate_task (dataset_id, task_id, status, is_partial) "
        "VALUES (?, ?, 'PENDING', ?)",
        (dataset_id, task_id, is_partial))

    for filepath in generate_scope:
        db_cursor.execute(
            "INSERT INTO generate_scope (task_id, filepath)"
            "VALUES (?, ?)",
            (task_id, filepath))

    db_conn.commit()

    current_app.logger.info(
        "Created a new file generation task with id '%s' and scope '%s' "
        "for dataset '%s'"
        % (task_id, generate_scope, dataset_id))

    return db_cursor.execute(
        'SELECT initiated, task_id, status, date_done '
        'FROM generate_task '
        'WHERE task_id = ?',
        (task_id,)
    ).fetchone()


def exists_in_database(filename):
    """
    Returns true if a package with a given filename can be found in the database.

    :param filename: Name of the package file
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    return db_cursor.execute(
        'SELECT * FROM package WHERE filename = ?',
        (filename,)
    ).fetchone() is not None


def get_cache_stats():
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    return db_cursor.execute(
        "SELECT count(*) as packages, "
        "       sum(size_bytes) as usage_bytes, "
        "       max(size_bytes) as largest_package_size, "
        "       min(size_bytes) as smallest_package_size "
        "FROM package"
    ).fetchone()


def get_active_packages():
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    q = db_cursor.execute(
        "SELECT p.filename as filename, "
        "       p.size_bytes as size_bytes, "
        "       t.date_done as generated_at, "
        "       max(d.finished) as last_downloaded, "
        "       count(d.finished) as no_downloads "
        "FROM package p "
        "JOIN generate_task t ON p.generated_by = t.task_id "
        "LEFT JOIN download d ON p.filename = d.filename AND d.status = 'SUCCESSFUL' "
        "GROUP BY p.filename"
    ).fetchall()

    packages = []
    for row in q:
        filename = str(row["filename"])
        size_bytes = int(row["size_bytes"])
        generated_at = row["generated_at"]
        last_downloaded = row["last_downloaded"]
        no_downloads = int(row["no_downloads"])
        datetimes = {"generated_at": generated_at, "last_downloaded": last_downloaded}
        converted = {}
        for k, v in datetimes.items():
            if isinstance(v, str):
                if "." in v:
                    a = v.split(".")
                    v = a[0]
                converted[k] = pendulum.from_format(v, "YYYY-MM-DD HH:mm:ss")
            elif isinstance(v, int):
                converted[k] = pendulum.from_timestamp(v / 1e3)  # sqlite microsecond timestamp
        packages.append(Package(filename, size_bytes, no_downloads, **converted))
    return packages


def delete_package_rows(filenames):
    """
    Delete package rows for packages with the specified file names
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    db_cursor.execute('DELETE FROM package WHERE filename IN (?)', (', '.join(filenames),))

    db_conn.commit()

    current_app.logger.info(
        "Deleted %s package rows"
        % (len(filenames)))


def get_package(task_id):
    """
    Returns package record for a given task.

    :param task_id: ID of the task that initiated the package generation
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    return db_cursor.execute('SELECT * FROM package WHERE generated_by = ?', (task_id,)).fetchone()


def get_generate_scope_filepaths(task_id):
    """
    Returns list of filepaths included in specified task scope.

    :param task_id: ID of the task whose scope is to be fetched
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    scope_rows = db_cursor.execute('SELECT filepath FROM generate_scope WHERE task_id = ?', (task_id,)).fetchall()

    return set(map(lambda scope_row: scope_row['filepath'], scope_rows))


def get_task_id_for_package(package):
    """
    Returns id of the file generation task for a package.

    :param package: Filename of the package whose task id is fetched
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    task_id = None

    row = db_cursor.execute('SELECT generated_by FROM package WHERE filename = ?', (package,)).fetchone()

    if row and len(row) > 0:
        task_id = row[0]

    return task_id


def get_task(package, task_id = None):
    """
    Retrieves the task record associated with the specified task_id, if provided, else with the specified package filename, if possible
    """
    db = get_db()
    db_cursor = db.cursor()

    if not task_id:
        task_id = get_task_id_for_package(package)

    if not task_id:
        task_id = ''

    return db_cursor.execute('SELECT * FROM generate_task WHERE task_id = ?', (task_id,)).fetchone()


def get_dataset_id_for_package(package):
    """
    Returns dataset id of the file generation task for a package.

    :param package: Filename of the package whose dataset id is fetched
    """
    task = get_task(package)
    if task:
        return task['dataset_id']
    return None


def update_package_generation_timestamps(package, timestamp):
    """
    Updates the package task record initiated and generated timestamps; used by automated testing.

    :param package: Filename of the package whose task id is fetched
    :param timestamp: Timestamp string to be recorded, matching the format "YYYY-MM-DD hh:mm:ss"
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    task_id = get_task_id_for_package(package)

    db_cursor.execute('UPDATE generate_task SET initiated = ?, date_done = ? WHERE task_id = ?', (timestamp, timestamp, task_id))

    db_conn.commit()


def update_package_file_size(package, size_bytes):
    """
    Updates the package record file size; used by automated testing.

    :param package: Filename of the package whose task id is fetched
    :param size_bytes: Integer size in bytes to be set
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    db_cursor.execute('UPDATE package SET size_bytes = ? WHERE filename = ?', (size_bytes, package))

    db_conn.commit()


def get_scope(task_id):
    """
    Determines and returns scope for the specified partial package generation task_id
    """
    db = get_db()
    db_cursor = db.cursor()

    generate_request = db_cursor.execute('SELECT id FROM generate_request WHERE task_id = ? ORDER BY id DESC LIMIT 1', (task_id,)).fetchone()

    scope = []

    if generate_request:
        rows = db_cursor.execute('SELECT prefix FROM generate_request_scope WHERE request_id = ?', (generate_request['id'],)).fetchall()
        for row in rows:
            scope.append(str(row["prefix"]))

    if len(scope) > 0:
        return scope

    return None


def extract_event(download_id):
    current_app.logger.debug("Extracting event for download id %s" % download_id)
    download = get_download_record_by_id(download_id)
    event = {}
    token = decode(download["token"], current_app.config["JWT_SECRET"], algorithms=[current_app.config["JWT_ALGORITHM"]])
    dataset = token["dataset"]
    event["dataset"] = dataset
    file = token.get("file")
    if file:
        event["type"] = "FILE"
        event["file"] = file
    else:
        package = token["package"]
        task = get_task(package, token.get("generated_by"))
        if task:
            if task["is_partial"] == 0:
                event["type"] = "COMPLETE"
            else:
                scope = get_scope(task["task_id"])
                if scope:
                    event["type"] = "PARTIAL"
                    event["scope"] = scope
        if not event.get("type"):
            event["type"] = "PACKAGE"
            event["package"] = package
    if download["status"] == "SUCCESSFUL":
        event["status"] = "SUCCESS"
    else:
        event["status"] = "FAILURE"
    event["started"] = normalize_timestamp(download["started"])
    event["finished"] = normalize_timestamp(download["finished"])
    current_app.logger.debug("Extracted event for download id %s: %s" % (download_id, json.dumps(event)))
    return event


db_cli = AppGroup('db', help='Run operations against database')


@db_cli.command('init')
def init_db_command():
    """Ensure all of the required database tables exist."""
    init_db()
    click.echo('Initialized the database.')


def init_app(app):
    """Hooks database extension to given Flask application.

    :param app: Flask application to hook the module into
    """
    app.teardown_appcontext(close_db)
    app.cli.add_command(db_cli)
