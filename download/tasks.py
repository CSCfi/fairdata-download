"""
    download.celery
    ~~~~~~~~~~~~~~~

    Celery application module for Fairdata Download Service.
"""
from celery import Celery, Task

from . import create_flask_app
from .services.generator import generate


def create_celery_app(app=None):
    """Celery application factory. Hooks task execution into Flask application
    context.

    At the current moment Celery has reportedly a bug where timezone setting is
    ignored and UTC is used (see https://github.com/celery/celery/issues/4842).
    As a workaround UTC timezone will be used in database, and timestamps are
    transformed to configured timezone on application level.

    :param app: Flask application to hook celery application into
    """
    app = app or create_flask_app()

    class ContextTask(Task):
        """Superclass for binding Celery class task execution to Flask
        application context.

        """

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery_broker_url = "pyamqp://%s:%s@%s/%s" % (
        app.config["MQ_USER"],
        app.config["MQ_PASS"],
        app.config["MQ_HOST"],
        app.config["MQ_VHOST"],
    )
    celery_broker_backend = "db+sqlite:///%s" % app.config["DATABASE_FILE"]

    celery = Celery(
        "generator", broker=celery_broker_url, backend=celery_broker_backend
    )

    celery.conf.database_table_names = {
        "task": "generate_task",
        "group": "generate_taskgroup",
    }
    celery.Task = ContextTask

    return celery


celery_app = create_celery_app()


@celery_app.task(name="generate-task", track_started=True, bind=True)
def generate_task(self, dataset, project_identifier, scope):
    """Celery task for generating download packages in background."""
    return generate(dataset, project_identifier, scope, self.request.id)
