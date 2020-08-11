import pytest
import requests

@pytest.mark.usefixtures("mock_metax")
class TestGetRequest:
    endpoint = '/request'

    def test_not_found(self, client, not_found_dataset):
        query_string = {
            'dataset': not_found_dataset['pid']
        }
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 404

    def test_pending(self, client, pending_dataset):
        query_string = {
            'dataset': pending_dataset['pid']
        }
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 200

    def test_started(self, client, started_dataset):
        query_string = {
            'dataset': started_dataset['pid']
        }
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 200

    def test_available(self, client, available_dataset):
        query_string = {
            'dataset': available_dataset['pid']
        }
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 200

@pytest.mark.usefixtures("mock_celery", "mock_metax")
class TestPostRequest:
    endpoint = '/request'

    def test_not_found(self, client, recorder, not_found_dataset):
        json = {
            'dataset': not_found_dataset['pid']
        }
        response = client.post(self.endpoint, json=json)
        json_response = response.get_json()

        assert response.status_code == 200
        assert json_response['created'] is True
        assert recorder.called is True

    def test_pending(self, client, recorder, pending_dataset):
        json = {
            'dataset': pending_dataset['pid']
        }
        response = client.post(self.endpoint, json=json)
        json_response = response.get_json()

        assert response.status_code == 200
        assert json_response['created'] is False
        assert recorder.called

@pytest.mark.usefixtures("mock_metax")
class TestPostAuthorize:
    endpoint = '/authorize'

    def test_available(self, client, recorder, available_dataset):
        json = {
            'dataset': available_dataset['pid'],
            'package': available_dataset['package']
        }
        response = client.post(self.endpoint, json=json)
        assert recorder.called
        assert response.status_code == 200

@pytest.mark.usefixtures("mock_metax")
class TestGetDownload:
    endpoint = '/download'

    def test_valid_available(self, client, recorder, available_dataset, valid_auth_token):
        client.environ_base['HTTP_AUTHORIZATION'] = 'Bearer ' + valid_auth_token
        query_string = {
            'dataset': available_dataset['pid']
        }
        response = client.get(self.endpoint, query_string=query_string)
        assert recorder.called
        assert response.status_code == 200

        # Verify token is single-use
        response = client.get(self.endpoint)
        assert response.status_code == 401

    def test_download_unauthorized(self, client):
        response = client.get(self.endpoint)
        assert response.status_code == 401

    def test_download_invalid_auth_type(self, client, valid_auth_token):
        client.environ_base['HTTP_AUTHORIZATION'] = 'Basic ' + 'user:pass'
        response = client.get(self.endpoint)
        assert response.status_code == 401

    def test_download_empty_token(self, client, valid_auth_token):
        client.environ_base['HTTP_AUTHORIZATION'] = 'Basic '
        response = client.get(self.endpoint)
        assert response.status_code == 401

    def test_download_expired_token(self, client, expired_auth_token):
        client.environ_base['HTTP_AUTHORIZATION'] = 'Bearer ' + expired_auth_token
        response = client.get(self.endpoint)
        assert response.status_code == 401

    def test_download_non_found_package(self, client, not_found_dataset_auth_token):
        client.environ_base['HTTP_AUTHORIZATION'] = 'Bearer ' + not_found_dataset_auth_token
        response = client.get(self.endpoint)
        assert response.status_code == 404

    def test_download_invalid_token(self, client, invalid_auth_token):
        client.environ_base['HTTP_AUTHORIZATION'] = 'Bearer ' + invalid_auth_token
        response = client.get(self.endpoint)
        assert response.status_code == 401
