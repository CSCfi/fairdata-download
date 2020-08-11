from download.celery import generate_task

def test_generate_task(mock_celery, pending_dataset):
    task = generate_task.delay(pending_dataset['pid'], pending_dataset['project_identifier'], pending_dataset['files'])

    assert task
