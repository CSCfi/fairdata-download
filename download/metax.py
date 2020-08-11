"""
    download.metax
    ~~~~~~~~~~~~~~

    Metax API integration module for Fairdata Download Service.

    Currently API v1 is used.
"""
from datetime import datetime

from flask import current_app
from requests import get
from requests.exceptions import ConnectionError

from .utils import startswithpath

class UnexpectedStatusCode(Exception):
    pass

class NoMatchingFilesFound(Exception):
    pass

def get_metax(resource):
    """Retrieves resource from Metax API

    :param resource: resource to be requested from the API
    :raises ConnectionError: Application is unable to connect to Metax API
    """
    url = current_app.config['METAX_URL'] + 'rest/v1/' + resource
    try:
        current_app.logger.debug("Requesting Metax API '%s'" % url)
        return get(url)
    except ConnectionError:
        current_app.logger.error(
            "Unable to connect to Metax API on '%s'"
            % current_app.config['METAX_URL'])
        raise

def get_dataset(dataset):
    """"Requests dataset metadata from Metax API.

    :param dataset: ID of dataset which metadata is retrieved
    :raises ConnectionError: Application is unable to connect to Metax API
    """
    return get_metax('datasets/%s' % dataset)

def get_dataset_files(dataset):
    """"Requests dataset files metadata from Metax API.

    :param dataset: ID of dataset which files' metadata is retrieved
    :raises ConnectionError: Application is unable to connect to Metax API
    """
    return get_metax('datasets/%s/files' % dataset)

def get_dataset_modified_from_metax(dataset_id):
    try:
        metax_response = get_dataset(dataset_id)
    except ConnectionError:
        raise

    if metax_response.status_code != 200:
        current_app.logger.error(
            "Received unexpected status code '%s' from Metax API"
            % metax_response.status_code)
        raise UnexpectedStatusCode

    return datetime.fromisoformat(metax_response.json()['date_modified'])

def get_matching_dataset_files_from_metax(dataset_id, scope):
    try:
        metax_files_response = get_dataset_files(dataset_id)
    except ConnectionError:
        abort(500)

    if metax_files_response.status_code != 200:
        current_app.logger.error(
            "Received unexpected status code '%s' from Metax API"
            % metax_response.status_code)
        raise UnexpectedStatusCode

    dataset_files = set(map(lambda metax_file: metax_file['file_path'],
                            metax_files_response.json()))

    generate_scope = set(filter(
        lambda dataset_file: len(scope) == 0 or
        len(set(filter(
            lambda scopefile: startswithpath(scopefile, dataset_file),
            scope))) > 0,
        dataset_files))

    if len(generate_scope) == 0:
        current_app.logger.error("Could not find files matching request "
                                 "for dataset '%s' with scope '%s'"
                                 % (dataset_id, scope))
        raise NoMatchingFilesFound

    project_identifier = list(map(
        lambda metax_file: metax_file['project_identifier'],
        metax_files_response.json()))[0]

    is_partial = 0 if generate_scope == dataset_files else 1

    return generate_scope, project_identifier, is_partial
