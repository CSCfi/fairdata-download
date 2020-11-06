"""
    download.healthcheck
    ~~~~~~~~~~~~~~~~~~~~

    Module for health monitoring end points used by Fairdata Download Service.
"""
from datetime import datetime, timedelta

from flask import Blueprint, abort, current_app, jsonify
from celery.app.control import Inspect

from ..services import mq
from ..services.mq import UnableToConnectToMQ, get_mq

healthcheck = Blueprint('healthcheck', __name__)

@healthcheck.route('/', methods=['GET'])
def get_health():
    """
    Internally available end point for health monitoring.
    ---
    tags:
      - health monitoring
    produces:
      - application/json
    responses:
      200:
        description: Health status of the service
        schema:
          type: object
          properties:
            generator_status:
              type: string
              example: "UNKNOWN"
            server_status:
              type: string
              example: "OK"
            server_time:
              type: string
              example: "2020-10-30T11:14:20+00:00"
    """
    from ..celery import celery_app

    errors = []
    try:
        mq.get_mq()
    except UnableToConnectToMQ as err:
        errors.append(str(err))

    try:
        generator_status = Inspect(app=celery_app).ping()
        if generator_status == None:
            errors.append('Could not ping the generator')
    except ConnectionResetError:
        errors.append('Connection to generator was reset')

    if len(errors) > 0:
        abort(500, errors)

    return jsonify({
      'server_time': datetime.utcnow().astimezone().isoformat(timespec='seconds'),
      'server_status': 'OK',
      'message_queue_status': 'OK',
      'generator_status': generator_status
    })

@healthcheck.errorhandler(500)
def internal_server_error(error):
    """Error handler for HTTP 500."""
    return jsonify(name=error.name, error=error.description), 500
