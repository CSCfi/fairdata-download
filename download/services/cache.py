"""
    download.cache
    ~~~~~~~~~~~~~~

    Cache management module for Fairdata Download Service.
"""
import os

from flask import current_app
from flask.cli import AppGroup
from tabulate import tabulate

from . import db
from .. import utils

def housekeep_cache():
    cache_stats = db.get_cache_stats()
    cache_usage = cache_stats['usage_bytes']
    cache_purge_threshold = current_app.config['CACHE_PURGE_THRESHOLD']
    cache_purge_target = current_app.config['CACHE_PURGE_TARGET']

    if cache_usage != None and cache_usage > cache_purge_threshold:
        clear_size = cache_usage - cache_purge_target
        active_packages = db.get_active_packages()

        packages_to_be_removed = utils.select_packages_to_be_removed(clear_size, active_packages)

        table_headers = ['file name', 'size bytes', 'generated at', 'last downloaded', 'number of downloads']
        current_app.logger.debug("Packages to be removed from cache:\n" + tabulate(packages_to_be_removed, headers=table_headers))
    else:
        current_app.logger.debug('Cache usage level is safe')

def print_statistics():
    cache_stats = db.get_cache_stats()
    table_headers = ['no packages', 'overall bytes', 'largest package', 'smallest package']
    current_app.logger.info("Cache usage statistics:\n" + tabulate([cache_stats], headers=table_headers))


def purge():
    """Purge files from cache that cannot be found in the database."""
    source_root = os.path.join(current_app.config["DOWNLOAD_CACHE_DIR"], "datasets")

    removed = 0
    for root, dirs, files in os.walk(source_root):
        for name in files:
            if not db.exists_in_database(name):
                os.remove(os.path.join(root, name))
                removed += 1

    current_app.logger.debug("Removed %s files" % removed)


def get_datasets_dir():
    cache_dir = os.path.join(current_app.config["DOWNLOAD_CACHE_DIR"], "datasets")

    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    return cache_dir

cache_cli = AppGroup('cache', help='Run maintentance operations against '
                                   'download cache.')

@cache_cli.command('housekeep')
def housekeep_command():
    """Execute cache housekeeping operation."""
    housekeep_cache()

@cache_cli.command('stats')
def stats_command():
    """Print general cache volume usage statistics."""
    print_statistics()

@cache_cli.command('purge')

cache_cli = AppGroup(
    "cache", help="Run maintentance operations against " "download cache."
)


@cache_cli.command("purge")
def purge_command():
    """Execute cache purge operation."""
    purge()


def init_app(app):
    """Hooks cache module to given Flask application.

    :param app: Flask application to hook module into.
    """
    app.cli.add_command(cache_cli)
