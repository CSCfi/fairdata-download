"""
    download.utils
    ~~~~~~~~~~~~~~~~~~

    Utility module for Fairdata Download Service.
"""
from datetime import datetime
import pytz

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

def select_packages_to_be_removed(clear_size, active_packages):
    """Selects packages that will be pruned from the cache volume.

    :param clear_size: Amount of storage in bytes that needs be cleared
    :param active_packages: List of data about the packages in the cache. Includes filename, size in bytes, timestamps
                            when the package was generated and last downloaded, and the overall number of downloads.
    """
    # TODO: implement management policy
    return active_packages
