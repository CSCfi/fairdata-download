"""
    download.mq
    ~~~~~~~~~~~

    Message queue module for Fairdata Download Service.
"""
import click
from flask import current_app, g
from flask.cli import AppGroup
from pika import BlockingConnection, ConnectionParameters, PlainCredentials

def get_mq():
    """Returns message queue connection from current context or creates new
    connection if one is not available.

    """
    if 'mq' not in g:
        credentials = PlainCredentials(
            username=current_app.config['MQ_USER'],
            password=current_app.config['MQ_PASS'])
        connection_params = ConnectionParameters(
            host=current_app.config['MQ_HOST'],
            virtual_host=current_app.config['MQ_VHOST'],
            credentials=credentials)
        g.mq = BlockingConnection(connection_params)

        current_app.logger.debug(
            'Connected to message queue on %s' %
            (current_app.config['MQ_HOST'], ))

    return g.mq


def close_mq(e=None):
    """Removes and closes message queue connection from current context if one
    is available.

    """
    mq_conn = g.pop('mq', None)

    if mq_conn is not None:
        mq_conn.close()

        current_app.logger.debug(
            'Disconnected from message queue on %s' %
            (current_app.config['MQ_HOST'], ))

def init_mq():
    """Initializes message queue by (re-)creating used exchanges and queues.

    """
    channel = get_mq().channel()

    channel.queue_delete('celery')
    channel.exchange_delete('celery')

    channel.exchange_declare(
        exchange='celery',
        exchange_type='direct',
        durable=True)
    request_queue = channel.queue_declare(
        queue='celery',
        durable=True)
    channel.queue_bind(exchange='celery', queue=request_queue.method.queue)

    current_app.logger.debug(
        'Initialized new message queue on %s' %
        (current_app.config['MQ_HOST'], ))

mq_cli = AppGroup('mq', help='Run operations against message queue.')

@mq_cli.command('init')
def init_mq_command():
    """Initialize message exchange and queue."""
    if (click.confirm('All of the existing messages will be deleted. Do you '
                      'want to continue?')):
        init_mq()
        click.echo('Initialized the message queue.')

def init_app(app):
    """Hooks message queue extension to given Flask application.

    :param app: Flask application to hook module into
    """
    app.teardown_appcontext(close_mq)
    app.cli.add_command(mq_cli)
