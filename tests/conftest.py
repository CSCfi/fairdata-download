import os
import shutil
import tempfile

import pytest

from download import create_flask_app
from download.db import init_db

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
def flask_app(cache_dir, db):
    flask_app = create_flask_app()

    flask_app.config['TESTING'] = True
    flask_app.config['DOWNLOAD_CACHE_DIR'] = cache_dir
    flask_app.config['DATABASE_FILE'] = db

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
def not_found_dataset():
    TEST_NOT_FOUND_DATASET = 'test_dataset_01'

    return {'pid': TEST_NOT_FOUND_DATASET}

@pytest.fixture
def available_dataset(flask_app):
    TEST_AVAILABLE_DATASET = 'test_dataset_04'
    TEST_AVAILABLE_PACKAGE = TEST_AVAILABLE_DATASET + '.zip'

    # Add cache file
    test_package = os.path.join(
        flask_app.config['DOWNLOAD_CACHE_DIR'], 'datasets', TEST_AVAILABLE_PACKAGE)
    with open(test_package, 'w+') as testfile:
        testfile.write('testcontent')

    return {'pid': TEST_AVAILABLE_DATASET, 'package': TEST_AVAILABLE_PACKAGE}
