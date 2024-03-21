"""
    download.mq
    ~~~~~~~~~~~

    Message queue module for Fairdata Download Service.
"""
import json
import click
from flask import current_app, g
from flask.cli import AppGroup
from pika import BlockingConnection, ConnectionParameters, PlainCredentials
from pika.exceptions import AMQPConnectionError
from socket import gaierror
from .db import get_task_rows_for_status, get_generate_scope_filepaths, update_task_id, update_task_status, get_request_scopes
from ..utils import normalize_timestamp
import threading


queue_lock = threading.Lock()


class UnableToConnectToMQ(Exception):
    def __init__(self, *args):
        if args[0]:
            self.host = args[0]

    def __str__(self):
        if self.host:
            return "Unable to connect to message queue on %s" % self.host
        else:
            return "Unable to connect to message queue"


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
        try:
            g.mq = BlockingConnection(connection_params)

            current_app.logger.debug(
                'Connected to message queue on %s' %
                (current_app.config['MQ_HOST'], ))
        except (AMQPConnectionError, OSError, gaierror):
            current_app.logger.error(
                'Unable to connect to message queue on %s' %
                (current_app.config['MQ_HOST'], ))
            raise UnableToConnectToMQ(current_app.config['MQ_HOST'])

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


def reload_queue():
    from ..tasks import generate_task
    global queue_lock
    with queue_lock:
        task_rows = get_task_rows_for_status('PENDING')
        if len(task_rows) > 0:
            return
        task_rows = get_task_rows_for_status('NEW')
        if len(task_rows) == 0:
            return
        datasets = []
        added_tasks = []
        for task_row in task_rows:
            dataset_id = task_row['dataset_id']
            if dataset_id not in datasets:
                datasets.append(dataset_id)
                task_id = task_row['task_id']
                project_identifier = task_id.split()[0]
                generate_scope = get_generate_scope_filepaths(task_id)
                task = generate_task.delay(dataset_id, project_identifier, list(generate_scope))
                update_task_id(task_id, task.id)
                update_task_status(task.id, 'PENDING')
                added_tasks.append({
                    "id": task_row['id'],
                    "task_id": task.id,
                    "dataset_id": task_row['dataset_id'],
                    "is_partial": task_row['is_partial'],
                    "status": "PENDING",
                    "initiated": task_row['initiated'],
                    "date_done": task_row['date_done'],
                    "retries": task_row['retries']
                })
        return(added_tasks)


def task_rows_to_json(task_rows):
    tasks = []
    for task_row in task_rows:
        task_scope = set()
        for scope in get_request_scopes(task_row['task_id']):
            for path in scope:
                task_scope.add(path)
        task_scope = sorted(list(task_scope))
        tasks.append({
            "id": task_row['id'],
            "task_id": task_row['task_id'],
            "dataset": task_row['dataset_id'],
            "scope": task_scope,
            "is_partial": (task_row['is_partial'] == 1),
            "status": task_row['status'],
            "initiated": normalize_timestamp(task_row['initiated']) if task_row['initiated'] is not None else None,
            "completed": normalize_timestamp(task_row['date_done']) if task_row['date_done'] is not None else None,
            "retries": task_row['retries']
        })
    return json.dumps(tasks, indent=4)


mq_cli = AppGroup('mq', help='Run operations against message queue')


@mq_cli.command('init')
def init_mq_command():
    """Initialize message exchange and queue."""
    if (click.confirm('All of the existing messages will be deleted. Do you '
                      'want to continue?')):
        init_mq()
        click.echo('Initialized the message queue.')


@mq_cli.command('new')
def status_new_mq_command():
    print(task_rows_to_json(get_task_rows_for_status('NEW')))


@mq_cli.command('pending')
def status_pending_mq_command():
    print(task_rows_to_json(get_task_rows_for_status('PENDING')))


@mq_cli.command('success')
def status_success_mq_command():
    print(task_rows_to_json(get_task_rows_for_status('SUCCESS')))


@mq_cli.command('retry')
def status_retry_mq_command():
    print(task_rows_to_json(get_task_rows_for_status('RETRY')))


@mq_cli.command('failed')
def status_failed_mq_command():
    print(task_rows_to_json(get_task_rows_for_status('FAILED')))


@mq_cli.command('reload')
def reload_mq_command():
    task_rows = get_task_rows_for_status('PENDING')
    if len(task_rows) > 0:
        print("Pending tasks exist. No tasks added to queue.")
    else:
        task_rows = get_task_rows_for_status('NEW')
        if len(task_rows) == 0:
            print("No new tasks exist. No tasks added to queue.")
        else:
            print(task_rows_to_json(reload_queue()))


def init_app(app):
    """Hooks message queue extension to given Flask application.

    :param app: Flask application to hook module into
    """
    app.teardown_appcontext(close_mq)
    app.cli.add_command(mq_cli)
