"""
    download.metax
    ~~~~~~~~~~~~~~

    Metax API integration module for Fairdata Download Service.

    Currently API v1 is used.
"""
from flask import current_app
from requests import get
from requests.exceptions import ConnectionError

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
