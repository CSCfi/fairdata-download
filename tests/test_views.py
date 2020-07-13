
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

    def test_available(self, client, available_dataset):
        query_string = {
            'dataset': available_dataset['pid']
        }
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 200

class TestPostRequest:
    endpoint = '/request'

    def test_not_found(self, client, not_found_dataset):
        json = {
            'dataset': not_found_dataset['pid']
        }
        response = client.post(self.endpoint, json=json)
        json_response = response.get_json()

        assert response.status_code == 200
        assert json_response['created'] is True

    def test_pending(self, client, pending_dataset):
        json = {
            'dataset': pending_dataset['pid']
        }
        response = client.post(self.endpoint, json=json)
        json_response = response.get_json()

        assert response.status_code == 200
        assert json_response['created'] is False

class TestPostAuthorize:
    endpoint = '/authorize'

    def test_available(self, client, available_dataset):
        json = {
            'dataset': available_dataset['pid'],
            'package': available_dataset['package']
        }
        response = client.post(self.endpoint, json=json)
        assert response.status_code == 200

class TestGetDownload:
    endpoint = '/download'

    def test_available(self, client, available_dataset):
        query_string = {
            'package': available_dataset['package']
        }
        response = client.get(self.endpoint, query_string=query_string)
        assert response.status_code == 200
