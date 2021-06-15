import configparser
import os

import pytest


@pytest.fixture
def test_config():
    test_conf = {
        "DOWNLOAD_URL": "http://0.0.0.0:5000",
        "TEST_DATASET_PID": "1",
        "TEST_DATASET_FILE": "/project_x_FROZEN/Experiment_X/file_name_1",
    }

    if "DOWNLOAD_SERVICE_TEST_DATA" in os.environ:
        config = configparser.ConfigParser()
        config.read(os.environ.get("DOWNLOAD_SERVICE_TEST_DATA"))
        if "testdata" in config:
            for param in test_conf.keys():
                if param in config["testdata"]:
                    test_conf[param] = config["testdata"][param]

    return test_conf
