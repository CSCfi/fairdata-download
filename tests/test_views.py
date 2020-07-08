
def test_get_request(client, available_dataset):
    query_string = {
        'dataset': available_dataset['pid']
    }
    response = client.get('/request', query_string=query_string)
    assert response.status_code == 200

def test_post_request(client, not_found_dataset):
    json = {
        'dataset': not_found_dataset['pid']
    }
    response = client.post('/request', json=json)
    assert response.status_code == 200

def test_authorize(client, available_dataset):
    json = {
        'dataset': available_dataset['pid'],
        'package': available_dataset['package']
    }
    response = client.post('/authorize', json=json)
    assert response.status_code == 200

def test_download(client, available_dataset):
    query_string = {
        'package': available_dataset['package']
    }
    response = client.get('/download', query_string=query_string)
    assert response.status_code == 200

