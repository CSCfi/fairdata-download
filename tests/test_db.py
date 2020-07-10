import pytest
import sqlite3

from download.db import get_db

def test_close(flask_app):
    with flask_app.app_context():
        db_conn = get_db()
        assert db_conn is get_db()

    with pytest.raises(sqlite3.ProgrammingError) as programming_error:
        db_conn.execute('SELECT 1')

    assert 'closed' in str(programming_error.value)

def test_init_command_confirm(runner, mock_init_db, recorder):
    result = runner.invoke(args=['db', 'init'], input='y\n')

    assert not result.exception
    assert 'Do you want to continue?' in result.output
    assert 'Initialized' in result.output
    assert recorder.called

def test_init_command_unconfirm(runner, mock_init_db, recorder):
    result = runner.invoke(args=['db', 'init'], input='N\n')

    assert not result.exception
    assert 'Do you want to continue?' in result.output
    assert 'Initialized' not in result.output
    assert not recorder.called
