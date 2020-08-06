from datetime import datetime, timedelta
from json import loads
import os
import shutil
import sqlite3
import tempfile

import pytest

from jwt import decode, encode, ExpiredSignatureError
from jwt.exceptions import DecodeError
from pika.spec import BasicProperties

from download import create_flask_app
from download.db import init_db, close_db, get_db
from download.mq import init_mq, close_mq, get_mq

@pytest.fixture
def ida_dir():
    ida_dir = tempfile.mkdtemp()

    yield ida_dir

    shutil.rmtree(ida_dir)

@pytest.fixture
def cache_dir():
    cache_dir = tempfile.mkdtemp()

    os.makedirs(os.path.join(cache_dir, 'datasets'))

    yield cache_dir

    shutil.rmtree(cache_dir)

@pytest.fixture
def db():
    db_fd, db = tempfile.mkstemp()

    yield db

    os.close(db_fd)
    os.unlink(db)

@pytest.fixture
def flask_app(cache_dir, ida_dir, db):
    flask_app = create_flask_app()

    flask_app.config['TESTING'] = True
    flask_app.config['DOWNLOAD_CACHE_DIR'] = cache_dir
    flask_app.config['DATABASE_FILE'] = db
    flask_app.config['IDA_DATA_ROOT'] = ida_dir

    with flask_app.app_context():
        init_db()

    return flask_app

@pytest.fixture
def client(flask_app):
    return flask_app.test_client()

@pytest.fixture
def runner(flask_app):
    return flask_app.test_cli_runner()

@pytest.fixture
def recorder():
    class Recorder(object):
        called = False

    return Recorder()

@pytest.fixture
def celery_task():
    class CeleryTask(object):
        id = 1
    return CeleryTask()

@pytest.fixture
def metax_response():
    class MetaxResponse(object):
        status_code = 200
        text = '{ "date_modified": "2020-07-04T18:06:24+03:00" }'

        def json(self):
            return loads(self.text)

    return MetaxResponse()

@pytest.fixture(autouse=True)
def mock_requests_get(monkeypatch):
    def skip_requesting_test(url):
        pytest.skip("Test tried to request resource over network. This is not "
                    "acceptable and the requesting function should be mocked "
                    "instead.")

    monkeypatch.setattr('requests.get', skip_requesting_test)

@pytest.fixture
def mock_init_db(monkeypatch, recorder):
    def fake_init_db():
        recorder.called = True

    monkeypatch.setattr('download.db.init_db', fake_init_db)

@pytest.fixture
def mock_init_mq(monkeypatch, recorder):
    def fake_init_mq():
        recorder.called = True

    monkeypatch.setattr('download.mq.init_mq', fake_init_mq)

@pytest.fixture
def mock_celery(monkeypatch, recorder, celery_task):
    def mock_generate_task(dataset):
        recorder.called = True
        return celery_task

    monkeypatch.setattr(
        'download.celery.generate_task.delay', mock_generate_task)

@pytest.fixture
def mock_metax(monkeypatch, recorder, metax_response):
    def mock_get_metax(resource):
        recorder.called = True
        return metax_response

    monkeypatch.setattr('download.metax.get_metax', mock_get_metax)

@pytest.fixture
def not_found_dataset():
    TEST_NOT_FOUND_DATASET = 'test_dataset_01'

    return {'pid': TEST_NOT_FOUND_DATASET}

@pytest.fixture
def pending_dataset(flask_app):
    TEST_PENDING_DATASET = 'test_dataset_02'

    with flask_app.app_context():
        # Add database record
        db_conn = get_db()
        db_cursor = db_conn.cursor()

        # Packages
        db_cursor.execute(
            "INSERT INTO package (dataset_id, task_id, initiated) "
            "VALUES (?, 'task1', '2020-07-20 14:05:21')",
            (TEST_PENDING_DATASET,)
        )
        db_conn.commit()

    # Add ida file
    test_dir = os.path.join(flask_app.config['IDA_DATA_ROOT'], TEST_PENDING_DATASET)
    os.makedirs(test_dir)
    test_file = os.path.join(test_dir, 'test.txt')
    with open(test_file, 'w+') as f:
        f.write('test')

    return {'pid': TEST_PENDING_DATASET}

@pytest.fixture
def started_dataset(flask_app):
    TEST_STARTED_DATASET = 'test_dataset_03'
    TEST_STARTED_PACKAGE = TEST_STARTED_DATASET + '.zip'
    TEST_STARTED_TASK = 'test_task_03'

    with flask_app.app_context():
        # Add database record
        db_conn = get_db()
        db_cursor = db_conn.cursor()

        db_cursor.execute(
            "INSERT INTO package (dataset_id, task_id, filename, initiated) "
            "VALUES (?, ?, ?, '2020-07-20 14:05:21')",
            (TEST_STARTED_DATASET, TEST_STARTED_TASK, TEST_STARTED_PACKAGE)
        )
        db_cursor.execute(
            "INSERT INTO generate_task (task_id, status) "
            "VALUES (?, 'STARTED')",
            (TEST_STARTED_TASK,)
        )

        db_conn.commit()

    # Add ida file
    test_dir = os.path.join(flask_app.config['IDA_DATA_ROOT'], TEST_STARTED_DATASET)
    os.makedirs(test_dir)
    test_file = os.path.join(test_dir, 'test.txt')
    with open(test_file, 'w+') as f:
        f.write('test')

    return {'pid': TEST_STARTED_DATASET}

@pytest.fixture
def available_dataset(flask_app):
    TEST_AVAILABLE_DATASET = 'test_dataset_04'
    TEST_AVAILABLE_PACKAGE = TEST_AVAILABLE_DATASET + '.zip'
    TEST_AVAILABLE_TASK = 'test_task_04'

    with flask_app.app_context():
        # Add database record
        db_conn = get_db()
        db_cursor = db_conn.cursor()

        db_cursor.execute(
            "INSERT INTO package (dataset_id, task_id, filename, initiated, size_bytes, checksum) "
            "VALUES (?, ?, ?, '2020-07-20 14:05:21', '19070072', 'sha256:98fcf5d6c57d0484bcb50f9c99f4870f5a45c70b62ce6e6edbbcabffa479094e')",
            (TEST_AVAILABLE_DATASET, TEST_AVAILABLE_TASK, TEST_AVAILABLE_PACKAGE)
        )
        db_cursor.execute(
            "INSERT INTO generate_task (task_id, status, date_done) "
            "VALUES (?, 'SUCCESS', '2020-08-07 08:44:31.186078')",
            (TEST_AVAILABLE_TASK,)
        )
        db_conn.commit()

    # Add ida file
    test_dir = os.path.join(flask_app.config['IDA_DATA_ROOT'], TEST_AVAILABLE_DATASET)
    os.makedirs(test_dir)
    test_file = os.path.join(test_dir, 'test.txt')
    with open(test_file, 'w+') as f:
        f.write('test')

    # Add cache file
    test_package = os.path.join(
        flask_app.config['DOWNLOAD_CACHE_DIR'], 'datasets', TEST_AVAILABLE_PACKAGE)
    with open(test_package, 'w+') as testfile:
        testfile.write('testcontent')

    return {'pid': TEST_AVAILABLE_DATASET, 'package': TEST_AVAILABLE_PACKAGE}

@pytest.fixture
def valid_auth_token(flask_app, available_dataset):
    expired = datetime.utcnow() + timedelta(hours=flask_app.config['JWT_TTL'])
    jwt_payload = {
        'exp': expired,
        'dataset': available_dataset['pid'],
        'package': available_dataset['package']
    }

    jwt_token = encode(
        jwt_payload,
        flask_app.config['JWT_SECRET'],
        algorithm=flask_app.config['JWT_ALGORITHM'])

    return jwt_token.decode()

@pytest.fixture
def expired_auth_token(flask_app, available_dataset):
    expired = datetime.utcnow() - timedelta(hours=1)
    jwt_payload = {
        'exp': expired,
        'dataset': available_dataset['pid'],
        'package': available_dataset['package']
    }

    jwt_token = encode(
        jwt_payload,
        flask_app.config['JWT_SECRET'],
        algorithm=flask_app.config['JWT_ALGORITHM'])

    return jwt_token.decode()

@pytest.fixture
def not_found_dataset_auth_token(flask_app, not_found_dataset):
    expired = datetime.utcnow() + timedelta(hours=flask_app.config['JWT_TTL'])
    jwt_payload = {
        'exp': expired,
        'dataset': not_found_dataset['pid'],
        'package': not_found_dataset['pid']
    }

    jwt_token = encode(
        jwt_payload,
        flask_app.config['JWT_SECRET'],
        algorithm=flask_app.config['JWT_ALGORITHM'])

    return jwt_token.decode()

@pytest.fixture
def invalid_auth_token(flask_app):
    return 'invalid'
