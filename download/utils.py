"""
    download.utils
    ~~~~~~~~~~~~~~~~~~

    Utility module for Fairdata Download Service.
"""
from dataclasses import asdict
from datetime import datetime
from typing import List

import pendulum
import pytz

from .config import GB
from .dto import Package


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


def select_packages_to_be_removed(clear_size: int, active_packages: List[Package]):
    """Selects packages that will be pruned from the cache volume.

    :param clear_size: Amount of storage in bytes that needs be cleared
    :param active_packages: List of data about the packages in the cache. Includes filename, size in bytes, timestamps
                            when the package was generated and last downloaded, and the overall number of downloads.
    """

    ranked_packages = []
    expired_packages = []
    removable = []

    # constants
    now = pendulum.now()
    expired_bytes = 0

    for package in active_packages:
        # Select packages that have been expired = no downloads and more than a week since generation
        if package.generated_at.diff(now).in_days() > 7 and package.no_downloads == 0:
            package.expired = True
        elif package.last_downloaded:
            # Packages older than 30 days are consider expired
            if package.last_downloaded.diff(now).in_days() > 30:
                package.expired = True
        if package.expired:
            expired_packages.append(package)
            expired_bytes += package.size_bytes

    # Do expired packages fill the clear quota? If so make removable same as expired
    if expired_bytes >= clear_size:
        removable = expired_packages

    # If expired packages did not fill clear size quota or there was no expired packages
    if len(removable) != len(expired_packages) or len(expired_packages) == 0:
        for package in active_packages:
            # If package has downloads and is not expired, it should have a rank
            if (
                not package.expired
                and package.no_downloads != 0
                and package.last_downloaded
            ):
                rank = package.no_downloads * 10
                rank += 30 - package.last_downloaded.diff(now).in_days()
                if package.size_bytes <= GB:
                    rank += 50

                # simplified version of package.size_bytes > GB and package.size_bytes < GB * 10
                elif GB < package.size_bytes < GB * 10:
                    rank += 20
                package.rank = rank
                ranked_packages.append(package)
    ranked_packages.sort()

    ranked_bytes = 0
    if expired_bytes < clear_size:
        removable += expired_packages
    if len(ranked_packages) != 0 and expired_bytes < clear_size:
        for package in ranked_packages:
            removable.append(package)
            ranked_bytes += package.size_bytes
            if ranked_bytes + expired_bytes > clear_size:
                break

    return removable, expired_packages, ranked_packages
