def test_generate_not_found_dataset(runner, not_found_dataset):
    result = runner.invoke(args=[
        'generator',
        'generate',
        '--dataset', not_found_dataset['pid'],
        '--project_identifier', not_found_dataset['project_identifier'],
        '--scope', not_found_dataset['files']
        ])

    assert not result.exception

def test_generate_pending_dataset(runner, pending_dataset):
    result = runner.invoke(args=[
        'generator',
        'generate',
        '--dataset', pending_dataset['pid'],
        '--project_identifier', pending_dataset['project_identifier'],
        '--scope', pending_dataset['files']
        ])

    assert not result.exception
