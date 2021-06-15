"""
    download.db
    ~~~~~~~~~~~

    Database module for Fairdata Download Service.
"""
import sqlite3
from os import path

import click
from flask import current_app, g
from flask.cli import AppGroup


def get_db():
    """Returns database connection from global scope, or connects to database
    if no conection is already established.

    """
    init_schema = False

    if "db" not in g:
        if not path.isfile(current_app.config["DATABASE_FILE"]):
            init_schema = True

        g.db = sqlite3.connect(
            current_app.config["DATABASE_FILE"], detect_types=sqlite3.PARSE_DECLTYPES
        )
        g.db.row_factory = sqlite3.Row

        current_app.logger.debug(
            "Connected to database on %s" % (current_app.config["DATABASE_FILE"],)
        )

    if init_schema:
        init_db()

    return g.db


def close_db(e=None):
    """Removes database connection from global scope and disconnects from
    database.

    """
    db_conn = g.pop("db", None)

    if db_conn is not None:
        db_conn.close()

        current_app.logger.debug(
            "Disconnected from database on %s" % (current_app.config["DATABASE_FILE"],)
        )


def init_db():
    """Initializes database by creating tables that don't exist."""
    db_conn = get_db()

    with current_app.open_resource("sql/create_tables.sql") as migration_file:
        db_conn.executescript(migration_file.read().decode("utf8"))

    current_app.logger.debug(
        "Initialized database on %s" % (current_app.config["DATABASE_FILE"],)
    )


def get_download_record(token):
    """Returns a row from download table for a given authentication token.

    :param token: JWT encoded authentication token for which download row is
                  fetched
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    return db_cursor.execute(
        "SELECT * FROM download WHERE token = ?", (token,)
    ).fetchone()


def get_request_scopes(task_id):
    """Returns a list of sets of scopes that have been requested and are
    fulfilled by specified partial file generation task.

    :param task_id: ID of the partial file generation task
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    generate_requests = db_cursor.execute(
        "SELECT id FROM generate_request WHERE task_id = ?", (task_id,)
    ).fetchall()

    request_scopes = []
    for generate_request in generate_requests:
        request_scope = db_cursor.execute(
            "SELECT prefix FROM generate_request_scope WHERE request_id = ?",
            (generate_request["id"],),
        ).fetchall()

        request_scopes.append(
            set(map(lambda scope_row: scope_row["prefix"], request_scope))
        )

    return request_scopes


def get_task_rows(dataset_id, initiated_after=""):
    """Returns a rows from file_generate table for a dataset.

    :param dataset_id: ID of dataset for which task rows are fetched
    :param initiated_after: timestamp after which fetched tasks may have been
                            initialized
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    return db_cursor.execute(
        "SELECT initiated, date_done, task_id, status, is_partial "
        "FROM generate_task t "
        "LEFT JOIN package p "
        "ON t.dataset_id = ? "
        "AND t.initiated > ? "
        "AND t.task_id = p.generated_by "
        'WHERE (t.status is "SUCCESS" and p.filename is not null) '
        'OR (t.status is not "SUCCESS" and t.status is not "FAILURE") ',
        (dataset_id, initiated_after),
    ).fetchall()


def create_subscription_row(task_id, notify_url, subscription_data):
    """Creates a new subscription for the specified package generation task"""
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    db_cursor.execute(
        "INSERT INTO subscription (task_id, notify_url, subscription_data) VALUES (?, ?, ?)",
        (task_id, notify_url, subscription_data),
    )

    db_conn.commit()

    current_app.logger.info(
        "Created a new subscription for package generation task '%s'" % (task_id)
    )


def get_subscription_rows(task_id):
    """Fetch subscription rows for the specified package generation task"""
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    return db_cursor.execute(
        "SELECT notify_url, subscription_data FROM subscription WHERE task_id = ?",
        (task_id,),
    ).fetchall()


def delete_subscription_rows(task_id):
    """Delete subscription rows for the specified package generation task"""
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    db_cursor.execute("DELETE FROM subscription WHERE task_id = ?", (task_id,))

    db_conn.commit()

    current_app.logger.info(
        "Deleted subscription rows for package generation task '%s'" % (task_id)
    )


def create_download_record(token, filename):
    """Creates a new download record for a given package with specified
    authentication token.

    :param token: JWT encoded authentication token used for authorizing file
                  download
    :param filename: Filename of the downloaded package
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    db_cursor.execute(
        "INSERT INTO download (token, filename) VALUES (?, ?)", (token, filename)
    )

    db_conn.commit()

    current_app.logger.info(
        "Created a new download record for package '%s' with token '%s'"
        % (filename, token)
    )

    return db_cursor.lastrowid


def update_download_record(download_id, successful=True):
    """Update download record after the stream has ended.

    :param download_id: ID of the download record in the database
    :param successful: Whether or not the download ended succesfully
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    status = "SUCCESSFUL" if successful else "FAILED"

    db_cursor.execute(
        "UPDATE download SET status = ?, finished = DATETIME() WHERE id = ?",
        (status, download_id),
    )

    db_conn.commit()

    current_app.logger.debug(
        "Set status of download '%s' to '%s'" % (download_id, status)
    )


def create_request_scope(task_id, request_scope):
    """Creates database rows for a file generation request that is fulfilled by
    given task.

    :param task_id: ID of the partial file generation task fulfilling specified
                    scope
    :param request_scope: List of files or directories included in the scope as
                          requested by client
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    db_cursor.execute("INSERT INTO generate_request (task_id) VALUES (?)", (task_id,))

    request_id = db_cursor.lastrowid

    for prefix in request_scope:
        db_cursor.execute(
            "INSERT INTO generate_request_scope (request_id, prefix) VALUES (?, ?)",
            (request_id, prefix),
        )

    db_conn.commit()

    current_app.logger.info(
        "Created a new file generation request with task_id '%s' and scope "
        "'%s'" % (task_id, request_scope)
    )


def create_task_rows(dataset_id, task_id, is_partial, generate_scope):
    """Creates all the appropriate rows to generate_task and generate_scope
    tables for a given file generation task.

    :param dataset_id: ID of the dataset that the files belong to
    :param task_id: ID of the generation task
    :param is_partial: Boolean value specifying whether the package is partial
                       ie. does not include all of the files in the dataset
    :param generate_scope: List of all the filepaths to be included in the
                           generated package
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    db_cursor.execute(
        "INSERT INTO generate_task (dataset_id, task_id, status, is_partial) "
        "VALUES (?, ?, 'PENDING', ?)",
        (dataset_id, task_id, is_partial),
    )

    for filepath in generate_scope:
        db_cursor.execute(
            "INSERT INTO generate_scope (task_id, filepath)" "VALUES (?, ?)",
            (task_id, filepath),
        )

    db_conn.commit()

    current_app.logger.info(
        "Created a new file generation task with id '%s' and scope '%s' "
        "for dataset '%s'" % (task_id, generate_scope, dataset_id)
    )

    return db_cursor.execute(
        "SELECT initiated, task_id, status, date_done "
        "FROM generate_task "
        "WHERE task_id = ?",
        (task_id,),
    ).fetchone()


def exists_in_database(filename):
    """Returns true if a package with a given filename can be found in the
       database.

    :param filename: Name of the package file
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    return (
        db_cursor.execute(
            "SELECT * FROM package WHERE filename = ?", (filename,)
        ).fetchone()
        is not None
    )


def get_package_row(generated_by):
    """Returns row from package table for a given task.

    :param generated_by: ID of the task that initiated the package generation
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    return db_cursor.execute(
        "SELECT filename, size_bytes, checksum "
        "FROM package "
        "WHERE generated_by = ?",
        (generated_by,),
    ).fetchone()


def get_generate_scope_filepaths(task_id):
    """Returns list of filepaths included in specified task scope.

    :param task_id: ID of the task whose scope is to be fetched
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    scope_rows = db_cursor.execute(
        "SELECT filepath " "FROM generate_scope " "WHERE task_id = ?", (task_id,)
    ).fetchall()

    return set(map(lambda scope_row: scope_row["filepath"], scope_rows))


def get_task_for_package(dataset_id, package):
    """Returns initiated timestamp of a file generation task for a package.

    :param dataset_id: ID of the dataset for which the package is generated
    :param package: Filename of the package whose task initiation is fetched
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    return db_cursor.execute(
        "SELECT "
        "  t.initiated "
        "FROM generate_task t "
        "JOIN package p "
        "ON t.dataset_id = ? "
        "AND p.filename = ? "
        "AND t.task_id = p.generated_by ",
        (dataset_id, package),
    ).fetchone()


db_cli = AppGroup("db", help="Run operations against database.")


@db_cli.command("init")
def init_db_command():
    """Ensure all of the required database tables exist."""
    init_db()
    click.echo("Initialized the database.")


def init_app(app):
    """Hooks database extension to given Flask application.

    :param app: Flask application to hook the module into
    """
    app.teardown_appcontext(close_db)
    app.cli.add_command(db_cli)
