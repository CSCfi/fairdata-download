"""
    download.utils
    ~~~~~~~~~~~~~~~~~~

    Utility module for Fairdata Download Service.
"""
import os
import pytz
import dateutil.parser
from datetime import datetime


def normalize_timestamp(timestamp):
    """
    Returns the input Posix or ISO timestamp string as a normalized ISO UTC timestamp YYYY-MM-DDThh:mm:ssZ
    """
    try:
        return datetime.utcfromtimestamp(dateutil.parser.parse(timestamp).timestamp()).strftime("%Y-%m-%dT%H:%M:%SZ")
    except TypeError:
        return None


def convert_utc_timestamp(utc_timestamp):
    """Converts a string from UTC naive form to UTC localtime.

    :param utc_timestamp: UTC naive timestamp string to be formatted in local
                          timezone
    """
    return datetime.fromisoformat(utc_timestamp + '+00:00').astimezone()


def convert_timestamp_to_utc(timestamp):
    """Converts a string from UTC naive form to UTC localtime.

    :param timestamp: Timestamp in an arbitrary timezone
    """
    return datetime.fromisoformat(timestamp).astimezone(pytz.utc)


def format_datetime(utc_timestamp):
    """Formats given timestamp to the form returned by the Download Service.

    :param iso_datetime: UTC naive timestamp string to be formatted in local
                         timezone
    """
    return convert_utc_timestamp(utc_timestamp).isoformat(timespec='seconds')


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
