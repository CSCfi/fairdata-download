"""
    download.service.task_service
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Package generation task service.
"""
import os
import time
from requests.exceptions import ConnectionError
from .. import utils
from . import db, metax, cache
from .metax import DatasetNotFound, MissingFieldsInResponse, NoMatchingFilesFound, UnexpectedStatusCode

os.environ["TZ"] = "UTC"
time.tzset()


class NoActiveTasksFound(Exception):

    def __init__(self, *args):
        if args[0]:
            self.dataset = args[0]

    def __str__(self):
        return ("No active file generation tasks were found for dataset "
                "'%s'" % self.dataset)


class NoDatabaseRecordForPackageFound(Exception):

    def __init__(self, *args):
        if args[0]:
            self.package = args[0]

    def __str__(self):
        return ("Could not find database record for package '%s' with valid "
                "download token" % self.package)


class PackageOutdated(Exception):

    def __init__(self, *args):
        if args[0]:
            self.dataset = args[0]
            self.package = args[1]

    def __str__(self):
        return ("Dataset %s has been modified since generation task for "
                "package %s was initialized" % (self.dataset, self.package))


def get_active_tasks(dataset_id):
    """Get all of the available package generation tasks for a dataset.

    :param dataset_id: ID of the dataset
    :raises ConnectionError: Application is unable to connect to Metax API
    :raises MissingFieldsInResponse: Some required fields were not found in Metax API
                                     response
    :raises UnexpectedStatusCode: Unexpected status code was received from Metax API
    :raises NoMatchingFilesFound: No dataset files matching the request scope were found
                                  in Metax API
    :raises NoActiveTasksFound: No available package generation tasks were found for the
                                given dataset.
    """
    try:
        dataset_modified = metax.get_dataset_modified_from_metax(dataset_id)
    except DatasetNotFound as err:
        raise
    except ConnectionError:
        raise
    except MissingFieldsInResponse:
        raise
    except UnexpectedStatusCode:
        raise

    task_rows = db.get_task_rows(dataset_id, dataset_modified)

    if len(task_rows) == 0:
        raise NoActiveTasksFound(dataset_id)
    else:
        return task_rows


def get_active_task(dataset_id, request_scope=[]):
    """Get package generation task for specified dataset matching given request scope.

    :param dataset_id: ID of the dataset
    :param request_scope: Scope of the package as specified in the API request
    :raises ConnectionError: Application is unable to connect to Metax API
    :raises MissingFieldsInResponse: Some required fields were not found in Metax API
                                     response
    :raises UnexpectedStatusCode: Unexpected status code was received from Metax API
    :raises NoMatchingFilesFound: No dataset files matching the request scope were found
                                  in Metax API
    """
    try:
        dataset_modified = metax.get_dataset_modified_from_metax(dataset_id)
    except DatasetNotFound as err:
        raise
    except ConnectionError:
        raise
    except MissingFieldsInResponse:
        raise
    except UnexpectedStatusCode:
        raise

    try:
        generate_scope, project_identifier, is_partial = metax.get_matching_dataset_files_from_metax(dataset_id, request_scope)
    except ConnectionError:
        raise
    except UnexpectedStatusCode:
        raise
    except NoMatchingFilesFound as err:
        raise

    # Check existing tasks in database
    task_rows = db.get_task_rows(dataset_id, dataset_modified)

    for row in task_rows:
        if db.get_generate_scope_filepaths(row['task_id']) == generate_scope:
            return row, project_identifier, is_partial, generate_scope

    return None, project_identifier, is_partial, generate_scope


def check_if_package_can_be_downloaded(dataset_id, package):
    """Get package generation task for specified dataset matching given request scope.

    If any cache issues are identified, such as ghost files not known to database or
    outdated packages older than the dataset modification timestamp, the cache will be
    cleaned accordingly; thus no separate cron jobs are needed to keep the cache valid
    and clean, as cleanup will occur as package authorization and/or download occurs
    (note: pruning of cache as the cache volume limit is reached is handled automatically
    prior to each package generation).

    :param dataset_id: ID of the dataset
    :param request_scope: Scope of the package as specified in the API request
    :raises ConnectionError: Application is unable to connect to Metax API
    :raises MissingFieldsInResponse: Some required fields were not found in Metax API
                                     response
    :raises UnexpectedStatusCode: Unexpected status code was received from Metax API
    :raises NoMatchingFilesFound: No dataset files matching the request scope were found
                                  in Metax API
    :raises PackageOutdatad: Dataset has been modified later than the package was generated
    """

    # Check if package generation task can be found in database

    task_row = db.get_task(package)

    if task_row is None:
        # A package file on disk is not known to the database, so raise an exception
        raise NoDatabaseRecordForPackageFound(package)

    initiated = utils.normalize_timestamp(task_row['initiated'])

    # Check dataset metadata in Metax API

    dataset_modified = metax.get_dataset_modified_from_metax(dataset_id)

    if initiated < dataset_modified:
        # The package is older than the last modified timestamp of the dataset, so raise an exception
        raise PackageOutdated(dataset_id, package)

    return True
