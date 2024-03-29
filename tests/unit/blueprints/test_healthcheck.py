import os
import time
import pytest
import requests

os.environ["TZ"] = "UTC"
time.tzset()


@pytest.mark.usefixtures("mock_celery_inspect", "mock_get_mq")
class TestGetHealthCheck:
    endpoint = '/health/'

    def test_not_found(self, client):
        response = client.get(self.endpoint)
        assert response.status_code == 200
