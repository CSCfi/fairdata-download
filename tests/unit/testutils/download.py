import json
import os
from download.services import db


def create_dataset(flask_app, descriptor):
    dataset = inject_database_records(flask_app, descriptor)
    # inject_ida_file(flask_app, descriptor)
    inject_cache_file(flask_app, dataset)
    return dataset


def get_test_data(table_name, descriptor):
    test_data_file = "tests/unit/test_data/download_db/%s/%s.json" % (table_name, descriptor)
    if os.path.isfile(test_data_file):
        with open(test_data_file, "r") as test_data:
            return json.loads(test_data.read())
    return None


def inject_database_records(flask_app, descriptor):
    dataset = {}
    dataset['csc_project'] = '2009999'
    dataset['project_identifier'] = '2009999'
    with flask_app.app_context():
        db_conn = db.get_db()
        db_cursor = db_conn.cursor()

        # Inject generate_task database row
        generate_task = get_test_data('generate_task', descriptor)
        if generate_task:
            db_cursor.execute(
                "INSERT INTO generate_task ( "
                "  id, "
                "  task_id, "
                "  dataset_id, "
                "  is_partial, "
                "  status, "
                "  initiated, "
                "  date_done, "
                "  result, "
                "  traceback, "
                "  retries"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (
                    generate_task['id'],
                    generate_task['task_id'],
                    generate_task['dataset_id'],
                    generate_task['is_partial'],
                    generate_task['status'],
                    generate_task['initiated'],
                    generate_task['date_done'],
                    generate_task['result'],
                    generate_task['traceback'],
                    generate_task['retries']
                )
            )
            dataset['dataset_id'] = generate_task['dataset_id']

        # Inject generate_scope database rows
        generate_scopes = get_test_data('generate_scope', descriptor)
        if generate_scopes:
            for generate_scope in generate_scopes:
                db_cursor.execute(
                    "INSERT INTO generate_scope (id, task_id, filepath) "
                    "VALUES (?, ?, ?)", (
                        generate_scope['id'],
                        generate_scope['task_id'],
                        generate_scope['filepath']
                    )
                )
            dataset['files'] = list(map(
                lambda s: s['filepath'],
                generate_scopes
            ))

        # Inject generate_request database rows
        generate_request = get_test_data('generate_request', descriptor)
        if generate_request:
            db_cursor.execute(
                "INSERT INTO generate_request (id, task_id) "
                "VALUES (?, ?)", (
                    generate_request['id'],
                    generate_request['task_id']
                )
            )

        # Inject generate_request_scopes database rows
        generate_request_scopes = get_test_data('generate_request_scope', descriptor)
        if generate_request_scopes:
            for generate_request_scope in generate_request_scopes:
                db_cursor.execute(
                    "INSERT INTO generate_request_scope ( "
                    "  id, "
                    "  request_id, "
                    "  prefix "
                    ") VALUES (?, ?, ?)", (
                        generate_request_scope['id'],
                        generate_request_scope['request_id'],
                        generate_request_scope['prefix']
                    )
                )

        # Inject generate_request_scopes database rows
        package = get_test_data('package', descriptor)
        if package:
            db_cursor.execute(
                "INSERT INTO package ( "
                "  id, "
                "  filename, "
                "  size_bytes, "
                "  checksum, "
                "  generated_by "
                ") VALUES (?, ?, ?, ?, ?)", (
                    package['id'],
                    package['filename'],
                    package['size_bytes'],
                    package['checksum'],
                    package['generated_by']
                )
            )
            dataset['package'] = package['filename']

        db_conn.commit()

    return dataset


def inject_ida_file(flask_app, dataset):
    test_dir = os.path.join(flask_app.config['IDA_DATA_ROOT'], dataset['dataset'])
    if not os.path.exists(test_dir):
        os.makedirs(test_dir)
    for filepath in dataset['files']:
      test_file = test_dir + filepath
      with open(test_file, 'w+') as f:
          f.write('test')


def inject_cache_file(flask_app, dataset):
    if 'package' in dataset.keys():
        test_package = os.path.join(
            flask_app.config['DOWNLOAD_CACHE_DIR'], 'datasets', dataset['package'])
        with open(test_package, 'w+') as testfile:
            testfile.write('testcontent')
