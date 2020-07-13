"""
    download.utils
    ~~~~~~~~~~~~~~~~~~

    Utility module for Fairdata Download Service.
"""
from datetime import datetime

def convert_utc_timestamp(utc_timestamp):
    """Converts a string from UTC naive form to UTC localtime.

    :param utc_timestamp: UTC naive timestamp string to be formatted in local
                          timezone
    """
    return datetime.fromisoformat(utc_timestamp + '+00:00').astimezone()

def format_datetime(utc_timestamp):
    """Formats given timestamp to the form returned by the Download Service.

    :param iso_datetime: UTC naive timestamp string to be formatted in local
                         timezone
    """
    return convert_utc_timestamp(utc_timestamp).isoformat(timespec='seconds')
