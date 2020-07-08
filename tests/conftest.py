import os
import shutil
import tempfile

import pytest

from download import create_flask_app

@pytest.fixture
def cache_dir():
    cache_dir = tempfile.mkdtemp()

    os.makedirs(os.path.join(cache_dir, 'datasets'))

    yield cache_dir

    shutil.rmtree(cache_dir)

@pytest.fixture
def flask_app(cache_dir):
    flask_app = create_flask_app()

    flask_app.config['TESTING'] = True
    flask_app.config['DOWNLOAD_CACHE_DIR'] = cache_dir

    return flask_app

@pytest.fixture
def client(flask_app):
    return flask_app.test_client()

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
