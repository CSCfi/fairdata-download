"""
    download.db
    ~~~~~~~~~~~

    Database module for Fairdata Download Service.
"""
import sqlite3

import click
from flask import current_app, g
from flask.cli import AppGroup

def get_db():
    """Returns database connection from global scope, or connects to database
    if no conection is already established.

    """
    if 'db' not in g:
        g.db = sqlite3.connect(
            current_app.config['DATABASE_FILE'],
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        g.db.row_factory = sqlite3.Row

        current_app.logger.debug(
            'Connected to database on %s' %
            (current_app.config['DATABASE_FILE'], ))

    return g.db

def close_db(e=None):
    """Removes database connection from global scope and disconnects from
    database.

    """
    db_conn = g.pop('db', None)

    if db_conn is not None:
        db_conn.close()

        current_app.logger.debug(
            'Disconnected from database on %s' %
            (current_app.config['DATABASE_FILE'], ))

def init_db():
    """Initializes database by (re-)creating tables.

    """
    db_conn = get_db()

    with current_app.open_resource('create_tables.sql') as migration_file:
        db_conn.executescript(migration_file.read().decode('utf8'))

    current_app.logger.debug(
        'Initialized new database on %s' %
        (current_app.config['DATABASE_FILE'], ))

db_cli = AppGroup('db', help='Run operations against database.')

@db_cli.command('init')
def init_db_command():
    """Drop any existing tables and create new ones."""
    if (click.confirm('All of the existing records will be deleted. Do you '
                      'want to continue?')):
        init_db()
        click.echo('Initialized the database.')

def init_app(app):
    """Hooks database extension to given Flask application.

    :param app: Flask application to hook the module into
    """
    app.teardown_appcontext(close_db)
    app.cli.add_command(db_cli)
