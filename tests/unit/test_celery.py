import os
import time
from download.tasks import generate_task

os.environ["TZ"] = "UTC"
time.tzset()


def test_generate_task(mock_celery, pending_task):
    task = generate_task.delay(pending_task['dataset_id'], '2009999', pending_task['files'])
    assert task
