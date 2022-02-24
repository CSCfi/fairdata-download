"""
    download
    ~~~~~~~~

    Python package for Fairdata Download Service application.
"""
import logging
import os

from flask import Flask

from .services import cache, db, generator, mq
from .blueprints.download_api import download_api
from .blueprints.healthcheck import healthcheck

logger = logging.getLogger(__name__)

def create_flask_app():
    """"Application Factory for Download Service Flask application"""
    app = Flask(__name__, instance_relative_config=True)

    app.config.from_object('download.config')
    if 'DOWNLOAD_SERVICE_SETTINGS' in os.environ:
        app.config.from_envvar('DOWNLOAD_SERVICE_SETTINGS')

    # Logging
    try:
        gunicorn_error_logger = logging.getLogger('gunicorn.error')
        app.logger.handlers.extend(gunicorn_error_logger.handlers)

    except Exception as e:
        logger.error(e)

    cache.init_app(app)
    db.init_app(app)
    mq.init_app(app)
    generator.init_app(app)

    app.register_blueprint(download_api)
    app.register_blueprint(healthcheck, url_prefix='/health')

    if os.environ.get('FLASK_ENV') != 'production':
        from .blueprints.swagger import download_api_swagger, \
                                        download_api_swagger_ui
        app.register_blueprint(download_api_swagger)
        app.register_blueprint(download_api_swagger_ui)
        app.logger.setLevel(logging.DEBUG)
    elif os.environ.get('FLASK_ENV') == 'production':
        app.logger.setLevel(logging.ERROR)

    return app

flask_app = create_flask_app()
