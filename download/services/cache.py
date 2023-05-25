"""
    download.cache
    ~~~~~~~~~~~~~~

    Cache management module for Fairdata Download Service.

    Each time a listing of available packages for a dataset requested, the service retrieves the
    modification timestamp of the dataset and excludes any packages which were generated earlier
    than when the dataset was last modified.
    
    Each time a download token is requested for a package, or when the package is requested for
    download, the service will check if the dataset modification timestamp is later than the package
    generation timestamp, and if so, the request will be refused with a 409 response

    Each time before a new package file is generated, as part of the background process, the service
    will perform its housekeeping operations, ensuring that:

    - no ghost files exist on disk which are not known to the database
    - no invalid / outdated packages exist which are older than the modification timestamp of their dataset
    - if the cache volume exceeds the specified volume limit, it will remove packages to cleanup space accordingly

    In this way, the service will never report, authorize for download, nor return an invalid package
    that is older than the dataset.

    Furthermore, no cron jobs have to be run to validate packages in the cache or check cache volume usage,
    rather the cache is regularly managed as part of normal operations as new packages are generated.
    Later, if we so choose, we could decouple the housekeeping from the package generation and run it
    explicitly via a cron job, if that is later decided to be more optimal.
"""
import os
import pendulum

from dataclasses import asdict
from typing import List
from flask import current_app
from flask.cli import AppGroup
from tabulate import tabulate

from . import db
from . import metax
from ..dto import Package
from ..utils import normalize_timestamp


GB = 1073741824


def perform_housekeeping():
    message = "Performing package cache housekeeping"
    current_app.logger.info(message)
    status = message
    message = purge_ghost_files()
    status = status + "\n\n" + message
    message = validate_package_cache()
    status = status + "\n\n" + message
    message = cleanup_package_cache()
    status = status + "\n\n" + message
    return status


def purge_ghost_files():
    """Purge files from cache that cannot be found in the database."""
    message = "Purging ghost files from cache that cannot be found in the database"
    current_app.logger.info(message)
    status = message
    source_root = os.path.join(current_app.config['DOWNLOAD_CACHE_DIR'], 'datasets')
    removed = []
    for root, dirs, files in os.walk(source_root):
        for name in files:
            if not db.exists_in_database(name):
                os.remove(os.path.join(root, name))
                removed.append(name)
    if len(removed) > 0:
        message = f"Removed {len(removed)} ghost files"
        current_app.logger.info(message)
        status = status + "\n" + message
        message = f"Removed ghost file names: {removed}"
        current_app.logger.info(message)
        status = status + "\n" + message
    else:
        message = "No ghost files found"
        current_app.logger.info(message)
        status = status + "\n" + message
    return status


def validate_package_cache():
    message = "Performing package cache validation against dataset modification timestamps"
    current_app.logger.info(message)
    status = message
    active_packages = db.get_active_packages()
    message = "Active packages retrieved from database:\n" + tabulate([asdict(i) for i in active_packages], headers="keys")
    current_app.logger.debug(message)
    remove = identify_invalid_packages(active_packages)
    if len(remove) > 0:
        message = "Invalid packages to be removed from cache:\n" + tabulate([asdict(i) for i in remove], headers="keys")
        status = status + "\n" + message
        current_app.logger.info(message)
        remove_cache_files(remove)
    else:
        message = "No invalid packages found"
        status = status + "\n" + message
        current_app.logger.info(message)
    return status


def cleanup_package_cache():
    message = "Performing package cache cleanup to increase available cache storage space"
    current_app.logger.info(message)
    status = message
    cache_stats = db.get_cache_stats()
    cache_usage = cache_stats["usage_bytes"]
    cache_usage_int = int(cache_usage or 0) # Convert to int in order to prevent NoneType errors
    cache_purge_threshold = int(current_app.config["CACHE_PURGE_THRESHOLD"])
    cache_purge_target = int(current_app.config["CACHE_PURGE_TARGET"])
    if cache_usage_int > 0 and cache_usage_int > cache_purge_threshold:
        clear_size = cache_usage_int - cache_purge_target
        active_packages = db.get_active_packages()
        message = "Active packages retrieved from database:\n" + tabulate([asdict(i) for i in active_packages], headers="keys")
        current_app.logger.debug(message)
        remove, expired, ranked = select_packages_to_be_removed(clear_size, active_packages)
        if len(remove) > 0:
            message = "Packages to be removed from cache:\n" + tabulate([asdict(i) for i in remove], headers="keys")
            status = status + "\n" + message
            current_app.logger.info(message)
            remove_cache_files(remove)
        else:
            message = "No packages needed to be removed from the cache"
            status = status + "\n" + message
            current_app.logger.info(message)
    else:
        message = "Cache storage consumption is acceptable"
        status = status + "\n" + message
        current_app.logger.debug(message)
    return status


def identify_invalid_packages(active_packages: List[Package]):
    """Selects packages that are older than the last modification timestamp of their dataset

    :param active_packages: List of data about the packages in the cache. Includes filename, size in bytes, timestamps
                            when the package was generated and last downloaded, and the overall number of downloads.
    """

    if current_app:
        current_app.logger.debug("identify_invalid_packages")

    invalid_packages = []

    for package in active_packages:
        if current_app:
            current_app.logger.debug("check dataset modification timestamp")
        try:
            package_generated = package.generated_at
            dataset_id = db.get_dataset_id_for_package(package.filename)
            dataset_modified = metax.get_dataset_modified_from_metax(dataset_id)
            package_generated_ts = normalize_timestamp(package_generated.strftime("%Y-%m-%dT%H:%M:%S%z"))
            dataset_modified_ts = normalize_timestamp(dataset_modified.strftime("%Y-%m-%dT%H:%M:%S%z"))
            if current_app:
                current_app.logger.debug("Package generated: %s Dataset modified: %s" % (package_generated_ts, dataset_modified_ts))
            if package_generated < dataset_modified:
                if current_app:
                    current_app.logger.info("Package %s invalid as it was generated earlier (%s) than the dataset %s was last modified (%s)" % (
                        package.filename,
                        package_generated_ts,
                        dataset_id,
                        dataset_modified_ts
                    ))
                invalid_packages.append(package)
        except Exception as e:
            if current_app:
                current_app.logger.debug("Error checking validity of package %s: %s" % (package.filename, str(e)))

    return invalid_packages


def select_packages_to_be_removed(clear_size: int, active_packages: List[Package]):
    """Selects packages that will be pruned from the cache volume due to cache volume limits.

    :param clear_size: Amount of storage in bytes that needs be cleared
    :param active_packages: List of data about the packages in the cache. Includes filename, size in bytes, timestamps
                            when the package was generated and last downloaded, and the overall number of downloads.
    """

    if current_app:
        current_app.logger.debug("select_packages_to_be_removed")

    ranked_packages = []
    expired_packages = []
    removable_packages = []

    # constants
    now = pendulum.now()
    expired_bytes = 0

    for package in active_packages:
        if current_app:
            current_app.logger.debug("check download activity and age")
        # If the package is not already expired, and has had no downloads and is older than 7 days, or is older than 30 days, mark as expired
        if not package.expired:
            if package.generated_at.diff(now).in_days() > 7 and package.no_downloads == 0:
                if current_app:
                    current_app.logger.info("Package %s expired as it is older than a week with no downloads" % package.filename)
                package.expired = True
            elif package.last_downloaded and package.last_downloaded.diff(now).in_days() > 30:
                if current_app:
                    current_app.logger.info("Package %s expired as it is older than a month" % package.filename)
                package.expired = True
        if package.expired:
            expired_packages.append(package)
            expired_bytes += package.size_bytes

    # Do expired packages fill the clear quota? If so make removable same as expired
    if expired_bytes >= clear_size:
        removable_packages = expired_packages

    # If expired packages did not fill clear size quota or there was no expired packages
    if len(removable_packages) != len(expired_packages) or len(expired_packages) == 0:
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
        removable_packages += expired_packages
    if len(ranked_packages) != 0 and expired_bytes < clear_size:
        for package in ranked_packages:
            removable_packages.append(package)
            ranked_bytes += package.size_bytes
            if ranked_bytes + expired_bytes > clear_size:
                break

    return removable_packages, expired_packages, ranked_packages


def print_statistics():
    cache_stats = db.get_cache_stats()
    table_headers = [
        "no packages",
        "overall bytes",
        "largest package",
        "smallest package",
    ]
    stats = "Cache usage statistics:\n" + tabulate([cache_stats], headers=table_headers)
    current_app.logger.info(stats)
    return stats


def remove_cache_files(files_list: List[Package] = None):
    if not files_list:
        current_app.logger.warning(f"Empty file list to be removed from cache, aborting")
        return
    file_names = None
    if files_list:
        file_names = [i.filename for i in files_list]
    removed = []
    source_root = os.path.join(current_app.config["DOWNLOAD_CACHE_DIR"], "datasets")
    for root, dirs, files in os.walk(source_root):
        for name in files:
            if file_names:
                if name in file_names:
                    os.remove(os.path.join(root, name))
                    removed.append(name)
    db.delete_package_rows(removed)
    current_app.logger.info(f"Removed {len(removed)} files")
    current_app.logger.info(f"Removed file names: {removed}")


def get_datasets_dir():
    cache_dir = os.path.join(current_app.config['DOWNLOAD_CACHE_DIR'], 'datasets')

    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    return cache_dir


def get_mock_notifications_dir():
    mock_notifications_dir = os.path.join(current_app.config['DOWNLOAD_CACHE_DIR'], 'mock_notifications')

    if not os.path.exists(mock_notifications_dir):
        os.makedirs(mock_notifications_dir)

    return mock_notifications_dir


cache_cli = AppGroup(
    "cache", help="Run maintentance operations against " "download cache."
)


@cache_cli.command("housekeep")
def housekeep_command():
    """Execute cache housekeeping operation."""
    print(perform_housekeeping())


@cache_cli.command("validate")
def housekeep_command():
    """Execute cache validation operation."""
    print(validate_package_cache())


@cache_cli.command("cleanup")
def housekeep_command():
    """Execute cache housekeeping operation."""
    print(cleanup_package_cache())


@cache_cli.command("purge")
def purge_command():
    """Execute cache purge operation."""
    print(purge_ghost_files())


@cache_cli.command("stats")
def stats_command():
    """Print general cache volume usage statistics."""
    print(print_statistics())


def init_app(app):
    """Hooks cache module to given Flask application.

    :param app: Flask application to hook module into.
    """
    app.cli.add_command(cache_cli)
