def test_generate_not_found_task(runner, not_found_task):
    result = runner.invoke(args=[
        'generator',
        'generate',
        '--dataset', not_found_task['dataset_id'],
        '--project_identifier', not_found_task['project_identifier'],
        '--scope', not_found_task['files']
        ])

    assert not result.exception

def test_generate_pending_task(runner, pending_task):
    result = runner.invoke(args=[
        'generator',
        'generate',
        '--dataset', pending_task['dataset_id'],
        '--project_identifier', pending_task['project_identifier'],
        '--scope', pending_task['files']
        ])

    assert not result.exception
