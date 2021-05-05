import json
import pytest

from datetime import datetime, timedelta
from jwt import decode, encode, ExpiredSignatureError, DecodeError
# from jwt.exceptions import DecodeError
from requests.exceptions import ConnectionError

from download.services.metax import UnexpectedStatusCode

@pytest.fixture
def client(flask_app):
    return flask_app.test_client()

@pytest.fixture
def get_matching_dataset_files_connection_error(monkeypatch):
    def _get_matching_dataset_files_connection_error(dataset_id, scope):
        raise ConnectionError
    monkeypatch.setattr('download.services.metax.get_matching_dataset_files_from_metax', _get_matching_dataset_files_connection_error)

@pytest.fixture
def get_matching_dataset_files_unexpected_status_code(monkeypatch):
    def _get_matching_dataset_files_unexpected_status_code(dataset_id, scope):
        raise UnexpectedStatusCode
    monkeypatch.setattr('download.services.metax.get_matching_dataset_files_from_metax', _get_matching_dataset_files_unexpected_status_code)

@pytest.fixture
def mock_get_mq(monkeypatch, recorder):
    def fake_get_mq():
        recorder.called = True

    monkeypatch.setattr('download.services.mq.get_mq', fake_get_mq)

@pytest.fixture
def mock_celery_inspect(monkeypatch, recorder):
    def mock_ping(self):
        with open("tests/unit/test_data/celery-inspect-ping.json", "r") as test_data:
            return json.loads(test_data.read())

    monkeypatch.setattr('celery.app.control.Inspect.ping', mock_ping)

@pytest.fixture
def valid_auth_token(client, flask_app, success_task):
    expired = datetime.utcnow() + timedelta(hours=flask_app.config['JWT_TTL'])
    jwt_payload = {
        'exp': expired,
        'dataset': success_task['dataset_id'],
        'package': success_task['package']
    }

    jwt_token = encode(
        jwt_payload,
        flask_app.config['JWT_SECRET'],
        algorithm=flask_app.config['JWT_ALGORITHM'])

    return jwt_token.decode()

@pytest.fixture
def valid_auth_token_for_file(client, flask_app, not_found_task):
    expired = datetime.utcnow() + timedelta(hours=flask_app.config['JWT_TTL'])
    jwt_payload = {
        'exp': expired,
        'dataset': not_found_task['dataset_id'],
        'file': '/test1/file1.txt',
        'project': '2009999',
    }

    jwt_token = encode(
        jwt_payload,
        flask_app.config['JWT_SECRET'],
        algorithm=flask_app.config['JWT_ALGORITHM'])

    return jwt_token.decode()

@pytest.fixture
def expired_auth_token(client, flask_app, success_task):
    expired = datetime.utcnow() - timedelta(hours=1)
    jwt_payload = {
        'exp': expired,
        'dataset': success_task['dataset_id'],
        'package': success_task['package']
    }

    jwt_token = encode(
        jwt_payload,
        flask_app.config['JWT_SECRET'],
        algorithm=flask_app.config['JWT_ALGORITHM'])

    return jwt_token.decode()

@pytest.fixture
def not_found_task_auth_token(client, flask_app, not_found_task):
    expired = datetime.utcnow() + timedelta(hours=flask_app.config['JWT_TTL'])
    jwt_payload = {
        'exp': expired,
        'dataset': not_found_task['dataset_id'],
        'package': 'test.zip'
    }

    jwt_token = encode(
        jwt_payload,
        flask_app.config['JWT_SECRET'],
        algorithm=flask_app.config['JWT_ALGORITHM'])

    return jwt_token.decode()
