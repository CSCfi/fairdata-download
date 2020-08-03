"""
    download.generator
    ~~~~~~~~~~~~~~~~~~

    Package generator module for Fairdata Download Service.
"""
import hashlib
import os
import tempfile
from zipfile import ZipFile, ZIP_DEFLATED

from click import option
from flask import current_app
from flask.cli import AppGroup

from .db import get_db, close_db

def generate(dataset):
    """Generates downloadable compressed file next dataset in request queue.

    :param dataset: ID of dataset for which generated package files belong to
    """
    output_filehandle, output_filename = tempfile.mkstemp(
        suffix='.zip',
        prefix=dataset + '_',
        dir=os.path.join(current_app.config['DOWNLOAD_CACHE_DIR'], 'datasets'))

    # Update package filename in database
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    db_cursor.execute(
        "UPDATE package set filename = ? where dataset_id = ?",
        (os.path.basename(output_filename), dataset)
    )
    db_conn.commit()
    close_db()

    # Generate file
    current_app.logger.info(
        "Generating download file for dataset '%s'..." %
        (dataset))

    source_root = os.path.join(current_app.config['IDA_DATA_ROOT'], dataset)

    with ZipFile(output_filename, 'w', ZIP_DEFLATED) as myzip:
        for root, dirs, files in os.walk(source_root):
            for name in files:
                filename = os.path.join(root, name)
                arcname = filename.replace(source_root, '.')
                current_app.logger.debug(
                    "Adding '%s' to zip archive." %
                    (arcname,))
                myzip.write(filename, arcname=arcname)

    # Calculate file metadata
    output_filesize = os.path.getsize(output_filename)

    sha256_hash = hashlib.sha256()
    with open(output_filename, "rb") as output_file:
        current_app.logger.debug("Calculating checksum.")
        for byte_block in iter(lambda: output_file.read(4096), b""):
            sha256_hash.update(byte_block)

    output_checksum = 'sha256:' + sha256_hash.hexdigest()

    current_app.logger.info(
        "Generated download file '%s' of size %s bytes." %
        (os.path.basename(output_filename), output_filesize))

    # Update database after successful file generation
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    db_cursor.execute(
        "UPDATE package "
        "SET "
        "  size_bytes = ?, "
        "  checksum = ? "
        "WHERE filename is ?",
        (output_filesize, output_checksum, os.path.basename(output_filename))
    )
    db_conn.commit()

generator_cli = AppGroup('generator', help='Run download file generator operations.')

@generator_cli.command('generate')
@option('--dataset', help='Dataset for which the package is generated')
def generate_command(dataset):
    """Poll request from message queue and generate download file for requested
    dataset.

    :param dataset: ID of dataset for which generated package files belong to
    """
    generate(dataset)

def init_app(app):
    """Hooks generator module to given Flask application.

    :param app: Flask application to hook module into.
    """
    app.cli.add_command(generator_cli)
