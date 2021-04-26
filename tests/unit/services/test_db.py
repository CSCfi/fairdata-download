import pytest
import sqlite3

from download.services.db import get_db

def test_close(flask_app):
    with flask_app.app_context():
        db_conn = get_db()
        assert db_conn is get_db()

    with pytest.raises(sqlite3.ProgrammingError) as programming_error:
        db_conn.execute('SELECT 1')

    assert 'closed' in str(programming_error.value)

def test_init_command(runner, mock_init_db, recorder):
    result = runner.invoke(args=['db', 'init'])

    assert not result.exception
    assert 'Initialized' in result.output
    assert recorder.called
