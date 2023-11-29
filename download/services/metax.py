"""
    download.metax
    ~~~~~~~~~~~~~~

    Metax API integration module for Fairdata Download Service.

    Supports both version 1 and 3 of the Metax API, determined by configuration.
"""
import requests
import json
from flask import current_app
from requests.exceptions import ConnectionError
from ..utils import normalize_timestamp, startswithpath


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

    metax_version = int(current_app.config.get('METAX_VERSION', 1))

    current_app.logger.debug("Talking to Metax API version %d" % metax_version)

    metax_url = current_app.config['METAX_URL'].rstrip('/')
    resource = resource.lstrip('/')

    if metax_version >= 3 or '/rest/v1' in metax_url:
        url = "%s/%s" % (metax_url, resource)
    else:
        url = "%s/rest/v1/%s" % (metax_url, resource)

    try:
        current_app.logger.debug("Requesting Metax API '%s'" % url)
        if metax_version >= 3:
            headers = { "Authorization": "Token %s" % current_app.config['METAX_PASS'] }
            return requests.get(url, headers=headers)
        else:
            auth = (current_app.config['METAX_USER'], current_app.config['METAX_PASS'])
            return requests.get(url, auth=auth)
    except ConnectionError:
        current_app.logger.error("Unable to connect to Metax API on '%s'" % url)
        raise


def get_dataset(dataset):
    """"Requests dataset metadata from Metax API.

    :param dataset: ID of dataset which metadata is retrieved
    :raises ConnectionError: Application is unable to connect to Metax API
    """
    try:
        current_app.logger.debug("Retrieving details for dataset %s" % dataset)
        metax_response = get_metax('datasets/%s' % dataset)

        if metax_response.status_code == 404:
            current_app.logger.error(
                "Dataset '%s' not found in Metax API" % dataset)
            raise DatasetNotFound(dataset)
        elif metax_response.status_code != 200:
            current_app.logger.error("Received unexpected status code '%s' from Metax API" % metax_response.status_code)
            raise UnexpectedStatusCode

        current_app.logger.debug("Successfully retrieved details for dataset %s" % dataset)
        return metax_response.json()
    except ConnectionError:
        raise


def get_dataset_files(dataset):
    """"Requests dataset files metadata from Metax API.

    :param dataset: ID of dataset which files' metadata is retrieved
    :raises ConnectionError: Application is unable to connect to Metax API
    """
    try:
        current_app.logger.debug("Retrieving files for dataset %s" % dataset)

        if current_app.config.get('METAX_VERSION', 1) >= 3:
            metax_response = get_metax('datasets/%s/files?pagination=false' % dataset)
        else:
            metax_response = get_metax('datasets/%s/files' % dataset)

        if metax_response.status_code != 200:
            current_app.logger.error(
                "Received unexpected status code '%s' from Metax API"
                % metax_response.status_code)
            raise UnexpectedStatusCode

        current_app.logger.debug("Successfully retrieved files for dataset %s" % dataset)

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

    metax_version = current_app.config.get('METAX_VERSION', 1)

    if metax_version >= 3:
        modified = metax_response.get('modified')
    else:
        modified = metax_response.get('date_modified')

    if modified:
        return normalize_timestamp(modified)
    else:
        if metax_version >= 3:
            created = metax_response.get('created')
        else:
            created = metax_response.get('date_created')
        if created:
            return normalize_timestamp(created)
        else:
            current_app.logger.error(("modified timestamp for dataset '%s' could not be found in Metax API response " % dataset_id))
            if metax_version >= 3:
                raise MissingFieldsInResponse(['modified', 'created'])
            else:
                raise MissingFieldsInResponse(['date_modified', 'date_created'])


def get_matching_project_identifier_from_metax(dataset_id, filepath):
    """
    This function serves two purposes. It both ensures that the specified file belongs
    to the specified dataset (raising NoMatchingFilesFound if not) and returns the
    project to which the file belongs.
    """
    try:
        metax_files_response = get_dataset_files(dataset_id)
    except ConnectionError:
        raise
    except UnexpectedStatusCode:
        raise

    if current_app.config.get('METAX_VERSION', 1) >= 3:
        matching_files = list(filter(lambda metax_file: metax_file['pathname'] == filepath, metax_files_response))
    else:
        matching_files = list(filter(lambda metax_file: metax_file['file_path'] == filepath, metax_files_response))

    if len(matching_files) == 0:
        raise NoMatchingFilesFound(dataset_id)

    if current_app.config.get('METAX_VERSION', 1) >= 3:
        return matching_files[-1].get('csc_project')
    else:
        return matching_files[-1].get('project_identifier')


def get_matching_dataset_files_from_metax(dataset_id, scope):
    try:
        metax_files_response = get_dataset_files(dataset_id)
    except ConnectionError:
        raise
    except UnexpectedStatusCode:
        raise

    if current_app.config.get('METAX_VERSION', 1) >= 3:
        dataset_files = set(map(lambda metax_file: metax_file['pathname'], metax_files_response))
    else:
        dataset_files = set(map(lambda metax_file: metax_file['file_path'], metax_files_response))

    generate_scope = set(filter(
        lambda dataset_file: len(scope) == 0 or
        len(set(filter(lambda scopefile: startswithpath(scopefile, dataset_file), scope))) > 0, dataset_files))

    if len(generate_scope) == 0:
        current_app.logger.error("Could not find files matching request "
                                 "for dataset '%s' with scope '%s'"
                                 % (dataset_id, scope))
        raise NoMatchingFilesFound(dataset_id)

    if current_app.config.get('METAX_VERSION', 1) >= 3:
        project_identifier = list(map(lambda metax_file: metax_file['csc_project'], metax_files_response))[0]
    else:
        project_identifier = list(map(lambda metax_file: metax_file['project_identifier'], metax_files_response))[0]

    is_partial = 0 if generate_scope == dataset_files else 1

    return generate_scope, project_identifier, is_partial
