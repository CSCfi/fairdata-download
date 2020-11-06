"""
    download
    ~~~~~~~~

    Python package for Fairdata Download Service application.
"""
import os

from flask import Flask

from . import cache, db, generator, mq
from .views import download_service

def create_flask_app():
    """"Application Factory for Download Service Flask application"""
    app = Flask(__name__, instance_relative_config=True)

    app.config.from_object('download.config')
    if 'DOWNLOAD_SERVICE_SETTINGS' in os.environ:
        app.config.from_envvar('DOWNLOAD_SERVICE_SETTINGS')

    cache.init_app(app)
    db.init_app(app)
    mq.init_app(app)
    generator.init_app(app)

    app.register_blueprint(download_service)

    if os.environ.get('FLASK_ENV') != 'production':
        from .swagger import download_service_swagger, \
                             download_service_swagger_ui
        app.register_blueprint(download_service_swagger)
        app.register_blueprint(download_service_swagger_ui,
                               url_prefix='/api/docs')

    return app

flask_app = create_flask_app()
