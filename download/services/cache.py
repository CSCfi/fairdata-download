"""
    download.cache
    ~~~~~~~~~~~~~~

    Cache management module for Fairdata Download Service.
"""
import os

from flask import current_app
from flask.cli import AppGroup

from .db import exists_in_database


def purge():
    """Purge files from cache that cannot be found in the database."""
    source_root = os.path.join(current_app.config["DOWNLOAD_CACHE_DIR"], "datasets")

    removed = 0
    for root, dirs, files in os.walk(source_root):
        for name in files:
            if not exists_in_database(name):
                os.remove(os.path.join(root, name))
                removed += 1

    current_app.logger.debug("Removed %s files" % removed)


def get_datasets_dir():
    cache_dir = os.path.join(current_app.config["DOWNLOAD_CACHE_DIR"], "datasets")

    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    return cache_dir


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
