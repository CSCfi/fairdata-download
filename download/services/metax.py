"""
    download.metax
    ~~~~~~~~~~~~~~

    Metax API integration module for Fairdata Download Service.

    Currently API v1 is used.
"""
import requests
from datetime import datetime

from flask import current_app
from requests.exceptions import ConnectionError

from ..utils import convert_timestamp_to_utc, startswithpath

class UnexpectedStatusCode(Exception):
    pass

class DatasetNotFound(Exception):
    def __init__(self, *args):
        if args[0]:
            self.dataset = args[0]

    def __str__(self):
        if self.dataset:
            return "Dataset '%s' was not found in Metax API" % self.dataset
        else:
            return "Dataset was not found in Metax API"

class MissingFieldsInResponse(Exception):
    def __init__(self, *args):
        if args[0]:
            self.fields = args[0]

    def __str__(self):
        if self.fieldname:
            return ("Missing fields '%s' in Metax API response"
                    % self.fields)
        else:
            return "Missing field in Metax API response"

class NoMatchingFilesFound(Exception):
    def __init__(self, *args):
        if args[0]:
            self.dataset = args[0]

    def __str__(self):
        if self.dataset:
            return ("No matching files for the dataset '%s' was found in "
                    "Metax API" % self.dataset)
        else:
            return "No matching files for the dataset was found in Metax API"

def get_metax(resource):
    """Retrieves resource from Metax API

    :param resource: resource to be requested from the API
    :raises ConnectionError: Application is unable to connect to Metax API
    """
    url = current_app.config['METAX_URL'] + 'rest/v1/' + resource
    try:
        current_app.logger.debug("Requesting Metax API '%s'" % url)
        return requests.get(
            url,
            auth=(current_app.config['METAX_USER'],
                  current_app.config['METAX_PASS']))
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
    try:
        metax_response = get_metax('datasets/%s' % dataset)

        if metax_response.status_code == 404:
            current_app.logger.error(
                "Dataset '%s' not found in Metax API" % dataset)
            raise DatasetNotFound(dataset)
        elif metax_response.status_code != 200:
            current_app.logger.error(
                "Received unexpected status code '%s' from Metax API"
                % metax_response.status_code)
            raise UnexpectedStatusCode

        return metax_response.json()
    except ConnectionError:
        raise

def get_dataset_files(dataset):
    """"Requests dataset files metadata from Metax API.

    :param dataset: ID of dataset which files' metadata is retrieved
    :raises ConnectionError: Application is unable to connect to Metax API
    """
    try:
        metax_response = get_metax('datasets/%s/files' % dataset)

        if metax_response.status_code != 200:
            current_app.logger.error(
                "Received unexpected status code '%s' from Metax API"
                % metax_response.status_code)
            raise UnexpectedStatusCode

        return metax_response.json()
    except ConnectionError:
        raise

def get_dataset_modified_from_metax(dataset_id):
    try:
        metax_response = get_dataset(dataset_id)
    except ConnectionError:
        raise
    except DatasetNotFound:
        raise
    except UnexpectedStatusCode:
        raise

    date_modified = metax_response.get('date_modified')

    if date_modified:
        return convert_timestamp_to_utc(date_modified)
    else:
        date_created = metax_response.get('date_created')
        if date_created:
            return convert_timestamp_to_utc(datetime.fromisoformat(date_created))
        else:
            current_app.logger.error(("'date_modified' field for dataset '%s' "
                                      "could not be found in Metax API response "
                                      % dataset_id))
            raise MissingFieldsInResponse(['date_modified', 'date_created'])


def get_matching_project_identifier_from_metax(dataset_id, filepath):
    try:
        metax_files_response = get_dataset_files(dataset_id)
    except ConnectionError:
        raise
    except UnexpectedStatusCode:
        raise

    matching_file = list(filter(
        lambda metax_file: metax_file['file_path'] == filepath,
        metax_files_response))

    return matching_file[-1].get('project_identifier') if len(matching_file) > 0 else None

def get_matching_dataset_files_from_metax(dataset_id, scope):
    try:
        metax_files_response = get_dataset_files(dataset_id)
    except ConnectionError:
        raise
    except UnexpectedStatusCode:
        raise

    dataset_files = set(map(lambda metax_file: metax_file['file_path'],
                            metax_files_response))

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
        raise NoMatchingFilesFound(dataset_id)

    project_identifier = list(map(
        lambda metax_file: metax_file['project_identifier'],
        metax_files_response))[0]

    is_partial = 0 if generate_scope == dataset_files else 1

    return generate_scope, project_identifier, is_partial
