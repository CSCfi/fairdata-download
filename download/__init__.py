"""
    download
    ~~~~~~~~

    Python package for Fairdata Download Service application.
"""
import os
import time
import datetime
import logging
from flask import Flask
from .services import cache, db, generator, mq
from .blueprints.download_api import download_api
from .blueprints.healthcheck import healthcheck
from .utils import normalize_logging

os.umask(0o007)
os.environ["TZ"] = "UTC"
time.tzset()

app_initialized = False


def create_flask_app():
    """"Application Factory for Download Service Flask application"""

    global app_initialized
    global flask_app
    
    # Check if the app has already been initialized
    if app_initialized:
        flask_app.logger.info("Flask app is already initialized. Avoiding re-initialization.")
        return flask_app

    app = Flask(__name__, instance_relative_config=True)
    
    app_initialized = True

    app.logger.setLevel(logging.INFO)

    # We only want to use the gunicorn logging handlers, not the default flask logging handlers
    try:
        app.logger.handlers = logging.getLogger('gunicorn.error').handlers
    except Exception as e:
        app.logger.error(e)

    normalize_logging(app)

    app.logger.info("TZ=%s" % str(time.tzname))
    app.logger.info('INITIALIZING APP CONFIGURATION')

    app.config.from_object('download.config')

    if 'DOWNLOAD_SETTINGS' in os.environ:
        app.logger.info("Loading DOWNLOAD_SETTINGS: %s" % os.environ['DOWNLOAD_SETTINGS'])
        app.config.from_envvar('DOWNLOAD_SETTINGS')
    else:
        pathname = "%s/config/settings.cfg" % os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if os.path.exists(pathname):
            app.logger.info("Loading deployed download service settings: %s" % pathname)
            app.config.from_pyfile(pathname)
 
    cache.init_app(app)
    db.init_app(app)
    mq.init_app(app)
    generator.init_app(app)

    app.register_blueprint(download_api)
    app.register_blueprint(healthcheck, url_prefix='/health')

    if os.environ.get('FLASK_DEBUG'):
        from .blueprints.swagger import download_api_swagger, download_api_swagger_ui
        app.register_blueprint(download_api_swagger)
        app.register_blueprint(download_api_swagger_ui)
        app.logger.setLevel(logging.DEBUG)
    else:
        app.logger.setLevel(logging.ERROR)

    if app.config["METAX_URL"].endswith('/v3/'):
        app.config['METAX_VERSION'] = 3
    else:
        app.config['METAX_VERSION'] = 1
    app.logger.info("METAX VERSION: %s" % app.config['METAX_VERSION'])

    return app

if not app_initialized:
    flask_app = create_flask_app()
