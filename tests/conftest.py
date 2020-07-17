import os
import shutil
import sqlite3
import tempfile

import pytest

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

        db_cursor.execute(
            "INSERT INTO request (dataset_id, status, initiated) "
            "VALUES (?, 'pending', '2020-07-20 13:03:21')",
            (TEST_PENDING_DATASET,)
        )
        db_conn.commit()

        # Add queue message
        channel = get_mq().channel()

        channel.basic_publish(exchange='requests',
            routing_key='requests',
            body=TEST_PENDING_DATASET,
            properties=BasicProperties(delivery_mode = 2))

    # Add ida file
    test_dir = os.path.join(flask_app.config['IDA_DATA_ROOT'], TEST_PENDING_DATASET)
    os.makedirs(test_dir)
    test_file = os.path.join(test_dir, 'test.txt')
    with open(test_file, 'w+') as f:
        f.write('test')

    return {'pid': TEST_PENDING_DATASET}

@pytest.fixture
def generating_dataset(flask_app):
    TEST_GENERATING_DATASET = 'test_dataset_03'
    TEST_GENERATING_PACKAGE = TEST_GENERATING_DATASET + '.zip'

    with flask_app.app_context():
        # Add database record
        db_conn = get_db()
        db_cursor = db_conn.cursor()

        db_cursor.execute(
            "INSERT INTO request (dataset_id, status, initiated) "
            "VALUES (?, 'generating', '2020-07-20 14:03:21')",
            (TEST_GENERATING_DATASET,)
        )
        db_cursor.execute(
            "INSERT INTO package (dataset_id, filename, generation_started) "
            "VALUES (?, ?, '2020-07-20 14:05:21')",
            (TEST_GENERATING_DATASET, TEST_GENERATING_PACKAGE)
        )
        db_conn.commit()

        # Add queue message
        channel = get_mq().channel()

        channel.basic_publish(exchange='requests',
            routing_key='requests',
            body=TEST_GENERATING_DATASET,
            properties=BasicProperties(delivery_mode = 2))

    # Add ida file
    test_dir = os.path.join(flask_app.config['IDA_DATA_ROOT'], TEST_GENERATING_DATASET)
    os.makedirs(test_dir)
    test_file = os.path.join(test_dir, 'test.txt')
    with open(test_file, 'w+') as f:
        f.write('test')

    return {'pid': TEST_GENERATING_DATASET}

@pytest.fixture
def available_dataset(flask_app):
    TEST_AVAILABLE_DATASET = 'test_dataset_04'
    TEST_AVAILABLE_PACKAGE = TEST_AVAILABLE_DATASET + '.zip'

    with flask_app.app_context():
        # Add database record
        db_conn = get_db()
        db_cursor = db_conn.cursor()

        db_cursor.execute(
            "INSERT INTO request (dataset_id, status, initiated) "
            "VALUES (?, 'available', '2020-07-20 15:03:21')",
            (TEST_AVAILABLE_DATASET,)
        )
        db_cursor.execute(
            "INSERT INTO package (dataset_id, filename, generation_started, generation_completed, size_bytes, checksum) "
            "VALUES (?, ?, '2020-07-20 14:05:21', '2020-07-20 14:15:21', '19070072', 'sha256:98fcf5d6c57d0484bcb50f9c99f4870f5a45c70b62ce6e6edbbcabffa479094e')",
            (TEST_AVAILABLE_DATASET, TEST_AVAILABLE_PACKAGE)
        )
        db_conn.commit()

        # Add queue message
        channel = get_mq().channel()

        channel.basic_publish(exchange='requests',
            routing_key='requests',
            body=TEST_AVAILABLE_DATASET,
            properties=BasicProperties(delivery_mode = 2))

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
