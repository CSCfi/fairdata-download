"""
    download.generator
    ~~~~~~~~~~~~~~~~~~

    Package generator module for Fairdata Download Service.
"""
import hashlib
import os
import tempfile
from zipfile import ZipFile, ZIP_DEFLATED

from flask import current_app
from flask.cli import AppGroup

from .db import get_db, close_db
from .mq import get_mq, close_mq

def generate():
    """Generates downloadable compressed file next dataset in request queue."""
    # Get next request in queue
    channel = get_mq().channel()
    method_frame, header_frame, body = channel.basic_get('requests')
    if not method_frame:
        current_app.logger.info("No message was found in queue")
        return

    dataset = body.decode()
    current_app.logger.debug(
        "Found request for dataset '%s' in message queue." % (dataset, ))

    # Check request in database
    db_conn = get_db()
    db_cursor = db_conn.cursor()

    request_row = db_cursor.execute(
        'SELECT status FROM request WHERE dataset_id = ?',
        (dataset,)
    ).fetchone()

    if request_row is None:
        current_app.logger.error(
            "Found message in queue for dataset '%s' that does not have a request in database" %
            (dataset,))
        channel.basic_ack(method_frame.delivery_tag)
        return
    if request_row['status'] != 'pending':
        current_app.logger.error(
            "Found message in queue for dataset '%s' with request status '%s'" %
            (dataset, request_row['status']))
        channel.basic_ack(method_frame.delivery_tag)
        return

    # Create package record in database
    output_filehandle, output_filename = tempfile.mkstemp(
        suffix='.zip',
        prefix=dataset + '_',
        dir=os.path.join(current_app.config['DOWNLOAD_CACHE_DIR'], 'datasets'))

    db_cursor.execute(
        "INSERT INTO package (dataset_id, filename) VALUES (?, ?)",
        (dataset, os.path.basename(output_filename))
    )
    db_cursor.execute(
        "UPDATE request SET status = 'generating' WHERE dataset_id is ?",
        (dataset,)
    )
    db_conn.commit()
    close_db()

    current_app.logger.debug(
        "Set request status for dataset '%s' to 'generating'." % (dataset, ))

    # Ack request from queue
    channel.basic_ack(method_frame.delivery_tag)
    close_mq()

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
        "  checksum = ?, "
        "  generation_completed = DATETIME('now', 'localtime') "
        "WHERE filename is ?",
        (output_filesize, output_checksum, os.path.basename(output_filename))
    )
    db_cursor.execute(
        "UPDATE request SET status = 'available' WHERE dataset_id is ?",
        (dataset,)
    )
    db_conn.commit()

    current_app.logger.debug(
        "Set request status for dataset '%s' to 'available'." % (dataset, ))

generator_cli = AppGroup('generator', help='Run download file generator operations.')

@generator_cli.command('generate')
def generate_command():
    """Poll request from message queue and generate download file for requested
    dataset.

    """
    generate()

def init_app(app):
    """Hooks generator module to given Flask application.

    :param app: Flask application to hook module into.
    """
    app.cli.add_command(generator_cli)
