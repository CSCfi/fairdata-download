from download.tasks import generate_task


def test_generate_task(mock_celery, pending_task):
    task = generate_task.delay(pending_task['dataset_id'],
                               pending_task['project_identifier'],
                               pending_task['files'])

    assert task
