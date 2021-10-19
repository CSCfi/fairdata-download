"""
    download.cache
    ~~~~~~~~~~~~~~

    Cache management module for Fairdata Download Service.
"""
import os
from dataclasses import asdict
from typing import List

from click import option
from flask import current_app
from flask.cli import AppGroup
from tabulate import tabulate

from .. import utils
from ..dto import Package
from . import db


def housekeep_cache():
    cache_stats = db.get_cache_stats()
    cache_usage = cache_stats["usage_bytes"]
    cache_purge_threshold = int(current_app.config["CACHE_PURGE_THRESHOLD"])
    cache_purge_target = int(current_app.config["CACHE_PURGE_TARGET"])

    if cache_usage != None and cache_usage > cache_purge_threshold:
        clear_size = cache_usage - cache_purge_target
        active_packages = db.get_active_packages()
        current_app.logger.info("active packages retrieved from database:\n"
                                + tabulate([asdict(i) for i in active_packages], headers="keys"))

        remove, expired, ranked = utils.select_packages_to_be_removed(
            clear_size, active_packages
        )

        current_app.logger.info(
            "Packages to be removed from cache:\n"
            + tabulate([asdict(i) for i in remove], headers="keys")
        )
        enable_file_deletion = current_app.config["ENABLE_CACHE_FILE_DELETION"]
        if enable_file_deletion:
            remove_cache_files(remove)
        else:
            current_app.logger.warning("cache file deletion is turned off, no files were deleted")

            current_app.logger.info("file ranking results from cache management:\n"
                                    + tabulate([asdict(i) for i in ranked], headers="keys"))
            current_app.logger.info("expired files according to cache management:\n"
                                    + tabulate([asdict(i) for i in expired], headers="keys"))
    else:
        current_app.logger.debug("Cache usage level is safe")


def print_statistics():
    cache_stats = db.get_cache_stats()
    table_headers = [
        "no packages",
        "overall bytes",
        "largest package",
        "smallest package",
    ]
    current_app.logger.info(
        "Cache usage statistics:\n" + tabulate([cache_stats], headers=table_headers)
    )


def remove_cache_files(files_list: List[Package] = None):
    if not files_list:
        current_app.logger.error(f"Empty file list to be removed from cache, aborting")
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


def purge():
    """Purge files from cache that cannot be found in the database."""
    remove_cache_files()


def get_datasets_dir():
    cache_dir = os.path.join(current_app.config['DOWNLOAD_CACHE_DIR'],
                           'datasets')

    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    return cache_dir


cache_cli = AppGroup(
    "cache", help="Run maintentance operations against " "download cache."
)


@cache_cli.command("housekeep")
def housekeep_command():
    """Execute cache housekeeping operation."""
    housekeep_cache()


@cache_cli.command("stats")
def stats_command():
    """Print general cache volume usage statistics."""
    print_statistics()

@cache_cli.command("purge")
def purge_command():
    """Execute cache purge operation."""
    purge()

def init_app(app):
    """Hooks cache module to given Flask application.

    :param app: Flask application to hook module into.
    """
    app.cli.add_command(cache_cli)
