import json
import pytest
from urllib import request
from urllib.error import HTTPError

def test_get_requests(test_config):
    download_url = test_config['DOWNLOAD_URL']
    test_dataset_pid = test_config['TEST_DATASET_PID']

    req = '%s/requests?dataset=%s' % (download_url, test_dataset_pid)
    try:
        resp = request.urlopen(req)

        assert resp.status == 200
    except HTTPError as e:
      if e.code == 404:
          print("No active requests for dataset '%s' was found" % test_dataset_pid)
      else:
          pytest.fail()

def test_post_requests(test_config):
    download_url = test_config['DOWNLOAD_URL']
    test_dataset_pid = test_config['TEST_DATASET_PID']

    data = json.dumps({ 'dataset': test_dataset_pid }).encode()
    req = request.Request('%s/requests' % (download_url), data=data)
    req.add_header('Content-Type', 'application/json')
    try:
        resp = request.urlopen(req)
        resp_body = json.loads(resp.read())

        assert resp_body['dataset'] == test_dataset_pid
        assert resp_body['status'] in ['PENDING', 'SUCCESS']
    except HTTPError as e:
        print(e)
        pytest.fail()

def test_post_authorize(test_config):
    download_url = test_config['DOWNLOAD_URL']
    test_dataset_pid = test_config['TEST_DATASET_PID']
    test_dataset_file = test_config['TEST_DATASET_FILE']

    data = json.dumps({
      'dataset': test_dataset_pid,
      'file': test_dataset_file
      }).encode()
    req = request.Request('%s/authorize' % download_url, data=data)
    req.add_header('Content-Type', 'application/json')

    try:
        resp = request.urlopen(req)
        resp_body = json.loads(resp.read())

        auth_token = resp_body['token']
    except HTTPError as e:
        pytest.fail()

    req = '%s/download?token=%s&dataset=%s&file=%s' % (download_url, auth_token, test_dataset_pid, test_dataset_file)
    try:
        resp = request.urlopen(req)

        assert resp.status == 200
        assert resp.getheader('Content-Type') == 'application/octet-stream'
        assert 'attachment' in resp.getheader('Content-Disposition')
        assert 'filename' in resp.getheader('Content-Disposition')
    except HTTPError as e:
        pytest.fail()
