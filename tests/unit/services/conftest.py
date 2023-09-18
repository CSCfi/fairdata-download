import os
import time
import pytest

os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture
def runner(flask_app):
    return flask_app.test_cli_runner()


@pytest.fixture
def mock_init_db(monkeypatch, recorder):
    def fake_init_db():
        recorder.called = True

    monkeypatch.setattr('download.services.db.init_db', fake_init_db)


@pytest.fixture
def mock_init_mq(monkeypatch, recorder):
    def fake_init_mq():
        recorder.called = True

    monkeypatch.setattr('download.services.mq.init_mq', fake_init_mq)
