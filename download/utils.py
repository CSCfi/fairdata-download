"""
    download.utils
    ~~~~~~~~~~~~~~~~~~

    Utility module for Fairdata Download Service.
"""
import os
import time
import logging
import dateutil.parser
from datetime import datetime
from gunicorn import glogging    
from celery import Celery
from celery.signals import after_setup_logger

os.environ["TZ"] = "UTC"
time.tzset()

LOG_ENTRY_FORMAT = '%(asctime)s (%(process)d) %(levelname)s %(message)s'
TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


@after_setup_logger.connect
def normalize_celery_logging(logger, *args, **kwargs):
    for handler in logger.handlers:
        handler.setFormatter(logging.Formatter(LOG_ENTRY_FORMAT, TIMESTAMP_FORMAT))


def normalize_logging(app = None):
    os.environ["TZ"] = "UTC"
    time.tzset()
    loggers = [logging.getLogger()]  # get the root logger
    loggers = loggers + [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    loggers = loggers + glogging.loggers()
    if app:
        loggers.append(app.logger)
    for logger in loggers:
        for handler in logger.handlers:
            handler.setFormatter(logging.Formatter(LOG_ENTRY_FORMAT, TIMESTAMP_FORMAT))


def normalize_timestamp(timestamp):
    """
    Returns the input timestamp as a normalized ISO 8601 UTC timestamp string YYYY-MM-DDThh:mm:ssZ
    """

    # Sniff the input timestamp value and convert to a UTC datetime instance as needed
    if isinstance(timestamp, str):
        timestamp = datetime.utcfromtimestamp(dateutil.parser.parse(timestamp).timestamp())
    elif isinstance(timestamp, float) or isinstance(timestamp, int):
        timestamp = datetime.utcfromtimestamp(timestamp)
    elif not isinstance(timestamp, datetime):
        raise Exception("Invalid timestamp value")

    # Return the normalized ISO UTC timestamp string
    return timestamp.strftime(TIMESTAMP_FORMAT)


def startswithpath(prefix, filepath, sep='/'):
    """Checks whether files in a filepath string match to files in a given path
    prefix.

    :param prefix: String of a subpath to match
    :param filepath: String of a filepath to match against given prefix
    :param sep: Path separator string
    """
    prefixpath = prefix.split(sep)
    path = filepath.split(sep)
    for i in range(len(prefixpath)):
        if prefixpath[i] != path[i]:
            return False
    return True


def ida_service_is_offline(current_app):
    """If the IDA service is offline, determined by the presence of the OFFLINE sentinel
    file, log a warning and return True, else return False. Only log on the first detection,
    not on subsequent iterations, until such time as the sentinel file no longer exists.
    """
    sentinel_file="%s/control/OFFLINE" % current_app.config['IDA_DATA_ROOT']
    current_app.logger.debug("IDA sentinel file pathname: %s" % sentinel_file)
    if os.path.exists(sentinel_file):
        current_app.logger.debug("The IDA service is offline")
        if not current_app.config.get('IDA_OFFLINE_REPORTED', False):
            current_app.logger.warning("The IDA service is offline. Package generation and file download is paused.")
            current_app.config['IDA_OFFLINE_REPORTED'] = True
        return True
    else:
        current_app.logger.debug("The IDA service is online")
        if current_app.config.get('IDA_OFFLINE_REPORTED', False):
            current_app.logger.warning("The IDA service is again online. Package generation and file download is resumed.")
            current_app.config['IDA_OFFLINE_REPORTED'] = False
        return False


def authenticate_trusted_service(current_app, request):
    token = current_app.config.get("TRUSTED_SERVICE_TOKEN")
    auth_header = request.headers.get('Authorization')
    if token is None:
        msg = "Missing trusted service token configuration"
        current_app.logger.error(msg)
        raise Exception(msg)
    if auth_header is None:
        msg = "Missing authorization header"
        current_app.logger.warning(msg)
        raise PermissionError(msg)
    try:
        [auth_method, auth_token] = auth_header.split(' ')
    except:
        msg = "Malformed authorization header"
        current_app.logger.warning(msg)
        raise PermissionError(msg)
    if auth_method != 'Bearer':
        msg = "Invalid authorization method"
        current_app.logger.warning(msg)
        raise PermissionError(msg)
    if auth_token is None:
        msg = "Missing authorization token"
        current_app.logger.warning(msg)
        raise PermissionError(msg)
    if auth_token != token:
        msg = "Invalid service authorization token"
        current_app.logger.warning(msg)
        raise PermissionError(msg)
