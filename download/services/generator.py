"""
    download.generator
    ~~~~~~~~~~~~~~~~~~

    Package generator module for Fairdata Download Service.
"""
import hashlib
import os
import sys
import requests
import tempfile
from zipfile import ZipFile, ZIP_DEFLATED

from click import option
from flask import current_app
from flask.cli import AppGroup

from .cache import get_datasets_dir, perform_housekeeping
from .db import get_db, get_subscription_rows, delete_subscription_rows
from ..utils import ida_service_is_offline


def generate(dataset, project_identifier, scope, requestor_id):
    """Generates downloadable compressed file next dataset in request queue.

    :param dataset: ID of dataset for which generated package files belong to
    :param project_identifier: Identifier of project that the dataset belongs
                               to.
    :param scope: Iteratable object containing files to be included in package.
    :param requestor_id: ID of task requesting file generation.
    """

    output_filehandle, output_filename = tempfile.mkstemp(
        suffix='.zip',
        prefix=dataset + '_',
        dir=get_datasets_dir())

    # Before generating new package file, perform housekeeping on package cache
    try:
        perform_housekeeping()
    except Exception as err:
        current_app.logger.error("Error encountered while performing package cache housekeeping: %s" % str(err))

    # Generate file
    current_app.logger.info("Generating download file for dataset '%s' with %s scoped files" % (dataset, len(scope)))

    source_root = os.path.join(
        current_app.config['IDA_DATA_ROOT'],
        'PSO_%s' % project_identifier,
        'files',
        project_identifier)

    with ZipFile(output_filename, 'w', ZIP_DEFLATED) as myzip:
        for root, dirs, files in os.walk(source_root):
            for name in files:
                absolute_filename = os.path.join(root, name)
                filename = absolute_filename.replace(source_root, '')
                if filename not in scope:
                    continue

                current_app.logger.debug(
                    "Adding '%s' to zip archive." % (filename,))
                myzip.write(absolute_filename, arcname='.'+filename)

    # Calculate file metadata
    output_filesize = os.path.getsize(output_filename)

    sha256_hash = hashlib.sha256()
    with open(output_filename, "rb") as output_file:
        current_app.logger.debug("Calculating checksum.")
        for byte_block in iter(lambda: output_file.read(4096), b""):
            sha256_hash.update(byte_block)

    output_checksum = 'sha256:' + sha256_hash.hexdigest()

    # If the IDA service is offline (having gone offline since generation of the package began), discard
    # the generated package file (assume potentially corrupted) and otherwise do nothing...
    if ida_service_is_offline(current_app):
        current_app.logger.warn("Discarding download file '%s' of size %s bytes." % (os.path.basename(output_filename), output_filesize))
        os.remove(output_filename)
        return

    current_app.logger.info("Generated download file '%s' of size %s bytes." % (os.path.basename(output_filename), output_filesize))

    # Insert package metadata to database
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    db_cursor.execute(
        "INSERT INTO package (filename, checksum, size_bytes, generated_by) "
        "VALUES (?, ?, ?, ?) ",
        (os.path.basename(output_filename), output_checksum, output_filesize, requestor_id)
    )
    db_conn.commit()

    # Send subscription notifications and delete subscription rows
    try:
        for subscription_row in get_subscription_rows(requestor_id):
            current_app.logger.debug("Posting subscription notification to '%s'" % subscription_row['notify_url'])
            requests.post(
                subscription_row['notify_url'],
                json={ 'subscriptionData': subscription_row['subscription_data'] }
            )
    
        delete_subscription_rows(requestor_id)
    except requests.exceptions.RequestException as e:
        current_app.logger.error("Error posting subscription notification: %s" % str(e))


generator_cli = AppGroup('generator', help='Run download file generator operations.')


@generator_cli.command('generate')
@option('--dataset', help='Dataset for which the package is generated')
@option('--project_identifier', help='Project identifier matching dataset')
@option('--scope', multiple=True, help='Scope for partial package generation')
def generate_command(dataset, project_identifier, scope):
    """Poll request from message queue and generate download file for requested
    dataset.

    :param dataset: ID of dataset for which generated package files belong to
    """
    generate(dataset, project_identifier, scope, 'click')


def init_app(app):
    """Hooks generator module to given Flask application.

    :param app: Flask application to hook module into.
    """
    app.cli.add_command(generator_cli)
