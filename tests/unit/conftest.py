import json
import os
import shutil
import sqlite3
import tempfile

import pytest

from pika.spec import BasicProperties
from requests.exceptions import ConnectionError

from download import create_flask_app
from download.services import mq
from download.services.db import init_db, close_db, get_db

from testutils.download import create_dataset
from testutils.misc import CeleryTask, Recorder
from testutils.metax import MetaxDatasetResponse, MetaxDatasetFilesResponse

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
def recorder():
    return Recorder()

@pytest.fixture
def celery_task():
    return CeleryTask()

@pytest.fixture(autouse=True)
def mock_requests_get(monkeypatch):
    def skip_requesting_test(url):
        pytest.skip("Test tried to request resource over network. This is not "
                    "acceptable and the requesting function should be mocked "
                    "instead.")

    monkeypatch.setattr('requests.get', skip_requesting_test)

@pytest.fixture
def mock_celery(monkeypatch, recorder, celery_task):
    def mock_generate_task(dataset, project_identifier, scope):
        recorder.called = True
        return celery_task

    monkeypatch.setattr(
        'download.celery.generate_task.delay', mock_generate_task)

@pytest.fixture
def metax_dataset_available(monkeypatch):
    def metax_get_available(url, auth={}):
        return MetaxDatasetResponse("no-tasks", 200)
    monkeypatch.setattr('requests.get', metax_get_available)

@pytest.fixture
def metax_dataset_not_found(monkeypatch):
    def metax_get_not_found(url, auth={}):
        return MetaxDatasetResponse("not-found", 404)
    monkeypatch.setattr('requests.get', metax_get_not_found)

@pytest.fixture
def metax_cannot_connect(monkeypatch):
    def metax_get_cannot_connect(url, auth={}):
        raise ConnectionError
    monkeypatch.setattr('requests.get', metax_get_cannot_connect)

@pytest.fixture
def metax_missing_fields(monkeypatch):
    def metax_get_metax_missing_fields(url, auth={}):
        return MetaxDatasetResponse("missing-fields", 200)
    monkeypatch.setattr('requests.get', metax_get_metax_missing_fields)

@pytest.fixture
def metax_unexpected_status_code(monkeypatch):
    def metax_get_unexpected_status_code(url, auth={}):
        return MetaxDatasetResponse("missing-fields", 521)
    monkeypatch.setattr('requests.get', metax_get_unexpected_status_code)

@pytest.fixture
def metax_dataset_files_available(monkeypatch):
    def metax_get_files_available(url, auth={}):
        return MetaxDatasetFilesResponse("no-tasks", 200)
    monkeypatch.setattr('requests.get', metax_get_files_available)

@pytest.fixture
def mock_metax(monkeypatch, recorder):
    def mock_get_metax(url, auth={}):
        recorder.called = True
        if url.endswith('files'):
            return MetaxDatasetFilesResponse("no-tasks", 200)
        else:
            return MetaxDatasetResponse("no-tasks", 200)

    monkeypatch.setattr('requests.get', mock_get_metax)

@pytest.fixture
def mock_metax_modified(monkeypatch, recorder):
    def mock_get_metax(url, auth={}):
        recorder.called = True
        if url.endswith('files'):
            return MetaxDatasetFilesResponse("success-modified", 200)
        else:
            return MetaxDatasetResponse("success-modified", 200)

    monkeypatch.setattr('requests.get', mock_get_metax)

@pytest.fixture
def not_found_task(flask_app):
    return {
        "dataset_id": "1",
        "project_identifier": "2009999",
        "files": ["/test1/file1.txt"]
    }

@pytest.fixture
def pending_task(flask_app):
    return create_dataset(flask_app, "pending")

@pytest.fixture
def pending_partial_task(flask_app):
    return create_dataset(flask_app, "pending-partial")

@pytest.fixture
def started_task(flask_app):
    return create_dataset(flask_app, "started")

@pytest.fixture
def success_task(flask_app):
    return create_dataset(flask_app, "success")

@pytest.fixture
def success_no_package_task(flask_app):
    return create_dataset(flask_app, "success-no-package")

@pytest.fixture
def success_partial_task(flask_app):
    return create_dataset(flask_app, "success-partial")

@pytest.fixture
def success_partial_2_task(flask_app):
    return create_dataset(flask_app, "success-partial-2")
