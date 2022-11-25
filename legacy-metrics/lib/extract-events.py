import os
import sqlite3
import json
import sys
from dateutil.parser import parse
from jwt import decode


db = None;


def get_db():
    """
    Returns database connection from global scope, or connects to database if no conection is already established.
    """

    global db

    if not db:

        if not "DATABASE_SNAPSHOT_FILE" in os.environ:
            print("Error: 'DATABASE_SNAPSHOT_FILE' is not defined in environment", file=sys.stderr)
            os.abort()
    
        database_file = os.environ.get("DATABASE_SNAPSHOT_FILE")
    
        if not os.path.isfile(database_file):
            print("Error: Could not find database file '%s'" % database_file, file=sys.stderr)
            os.abort()
    
        print("Opening database file: %s" % database_file, file=sys.stderr)

        # open db in read-only mode
        db = sqlite3.connect("file:%s?mode=ro" % database_file, detect_types=sqlite3.PARSE_DECLTYPES, uri=True)
    
        db.row_factory = sqlite3.Row

    return db


def close_db(e=None):
    """
    Removes database connection from global scope and disconnects from database.
    """

    global db

    if db is not None:
        db.close()


def get_download_records(limit = None):
    """
    Returns rows from download table, up to the optionally specified limit, 

    :param token: JWT encoded authentication token for which download row is fetched
    """
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    query = "SELECT * FROM download ORDER BY started DESC"

    if limit:
        query = "%s LIMIT %s" % (query, str(limit))

    return db_cursor.execute(query).fetchall()


def normalize_timestamp(timestamp):
    """
    Returns the input timestamp string as a normalized ISO UTC timestamp YYYY-MM-DDThh:mm:ssZ
    """
    try:
        return parse(timestamp).strftime("%Y-%m-%dT%H:%M:%SZ")
    except TypeError:
        return None


def decode_token(token):
    """
    Decodes the specified token and returns the decoded token object
    """
    return decode(token, os.environ.get("JWT_SECRET"), algorithms=["HS256"], verify=False)


def get_task(dataset, package, task_id):
    """
    Retrieves the task record associated with the specified task_id, if provided, else with the
    specified package filename, if the package record exists, else the task_id of a single existing
    task for the dataset, if only one exists, else returns None
    """
    db = get_db()
    db_cursor = db.cursor()

    # If not provided, attempt to retrieve task id from package record
    if not task_id:
        row = db_cursor.execute('SELECT generated_by FROM package WHERE filename = ?', (package,)).fetchone()
        if row and len(row) > 0:
            task_id = row[0]

    if task_id:
        return db_cursor.execute('SELECT * FROM generate_task WHERE task_id = ?', (task_id,)).fetchone()

    # Attempt to retrieve task if single existing task record for the specified dataset
    rows = db_cursor.execute('SELECT * FROM generate_task WHERE dataset_id = ? LIMIT 2', (dataset,)).fetchall()
    if len(rows) == 1:
        return rows[0]

    return None


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


#--------------------------------------------------------------------------------

download_records = get_download_records()

events = []

for record in download_records:
    event = {}
    token = decode_token(record["token"])
    dataset = token["dataset"]
    event["dataset"] = dataset
    file = token.get("file")
    if file:
        event["type"] = "FILE"
        event["file"] = file
    else:
        package = token["package"]
        task = get_task(dataset, package, token.get("generated_by"))
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
    if record["status"] == "SUCCESSFUL":
        event["status"] = "SUCCESS"
    else:
        event["status"] = "FAILURE"
    event["started"] = normalize_timestamp(record["started"])
    event["finished"] = normalize_timestamp(record["finished"])
    events.append(event)

print(json.dumps(events))

close_db()
